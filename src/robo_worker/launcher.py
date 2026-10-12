"""Run one job in a k8s Job pod and read the outcome back off the state claim."""

import base64
import copy
import json
import logging
import os
import subprocess
from dataclasses import asdict, fields
from pathlib import Path
from typing import Any, get_args

import anyio

from robo_worker.config import Config
from robo_worker.proc_output import stderr_tail
from robo_worker.runner import OutcomeKind, RunOutcome, RunSettings, RunSpec
from robo_worker.secret_files import write_private
from robo_worker.store import JobStore

_log = logging.getLogger(__name__)

# The control plane writes the run spec, and the job pod writes the outcome. Both sit in the
# job directory on the shared state claim.
RUN_SPEC_NAME = "run_spec.json"
OUTCOME_NAME = "outcome.json"
# How often the launcher asks the API server about a job pod. A task runs for minutes.
POD_POLL_SECONDS = 5.0
# A finished Job object stays this long, so a caller can still read its pod logs.
JOB_TTL_SECONDS = 3600
# Image pull and pod start count toward activeDeadlineSeconds. The runner's own clock must
# stop claude first, so the pod records a "timeout" outcome before k8s kills it.
POD_DEADLINE_MARGIN_SECONDS = 600
# A short API server outage must not fail a job whose pod still runs: about one minute.
MAX_POLL_ERRORS = 12
JOB_ID_LABEL = "robo-worker/job-id"
ATTEMPT_LABEL = "robo-worker/attempt"
_KUBECTL_TIMEOUT = "--request-timeout=30s"
_OUTCOME_KINDS = frozenset(get_args(OutcomeKind.__value__))


class LauncherError(RuntimeError):
    """A cluster call failed, or the job template or the control-plane pod is not usable."""


def job_name(job_id: str, attempt: int) -> str:
    return f"robo-worker-{job_id}-{attempt}"


def env_secret_name(job_id: str, attempt: int) -> str:
    return f"robo-worker-env-{job_id}-{attempt}"


def outcome_to_json(outcome: RunOutcome) -> str:
    return json.dumps(asdict(outcome))


def outcome_from_json(text: str) -> RunOutcome:
    """Parse an outcome file. Raise ValueError when it is not a valid outcome."""
    data = json.loads(text)
    if not isinstance(data, dict) or set(data) != {f.name for f in fields(RunOutcome)}:
        raise ValueError(f"an outcome needs exactly the keys of RunOutcome, got {data!r:.200}")
    if data["kind"] not in _OUTCOME_KINDS or not isinstance(data["reason"], str):
        raise ValueError(f"unknown outcome kind {data['kind']!r:.50} or bad reason")
    for key in ("result", "usage"):
        if data[key] is not None and not isinstance(data[key], dict):
            raise ValueError(f"outcome {key} must be an object or null")
    resets_at = data["resets_at"]
    # bool is a subclass of int, so `true` must not pass as a time.
    number = isinstance(resets_at, int | float) and not isinstance(resets_at, bool)
    if resets_at is not None and not number:
        raise ValueError("outcome resets_at must be a number or null")
    return RunOutcome(**data)


def _condition(job: dict[str, Any], kind: str) -> dict[str, Any] | None:
    for condition in job.get("status", {}).get("conditions") or []:
        if condition.get("type") == kind and condition.get("status") == "True":
            return condition
    return None


class K8sLauncher:
    """The production LaunchFn: one `claude -p` task, one k8s Job, one pod."""

    def __init__(
        self, cfg: Config, template: Path, *, poll_seconds: float = POD_POLL_SECONDS
    ) -> None:
        self._cfg = cfg
        self._template = template
        self._poll = poll_seconds
        self._store = JobStore(cfg.state_root)

    async def __call__(
        self, spec: RunSpec, settings: RunSettings, *, cancel: anyio.Event
    ) -> RunOutcome:
        template = self._load_template()
        own_pod = await self._own_pod()
        name = job_name(spec.job_id, spec.attempt)
        # An old attempt's outcome must never answer for this one.
        (self._store.job_dir(spec.job_id) / OUTCOME_NAME).unlink(missing_ok=True)
        self._write_spec_file(spec)
        job = await self._create(self._job_object(template, own_pod, spec))
        uid = job.get("metadata", {}).get("uid")
        try:
            if not isinstance(uid, str) or not uid:
                raise LauncherError(f"kubectl create returned Job {name} without a uid")
            await self._create(self._env_secret(spec, uid))
        except LauncherError:
            # Without its Secret the pod waits until the deadline, so remove the Job now.
            await self._delete_job(name)
            raise
        return await self._wait(name, spec.job_id, cancel)

    async def attach(self, job_id: str, attempt: int, *, cancel: anyio.Event) -> RunOutcome | None:
        """Wait for an attempt that an earlier control plane started. None: no such Job exists."""
        name = job_name(job_id, attempt)
        if await self._get_job(name) is None:
            return None
        return await self._wait(name, job_id, cancel)

    # The job object.

    def _load_template(self) -> dict[str, Any]:
        try:
            template = json.loads(self._template.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise LauncherError(f"cannot read the job template {self._template}: {exc}") from exc
        if not isinstance(template, dict):
            raise LauncherError(f"the job template {self._template} must be a JSON object")
        return template

    async def _own_pod(self) -> tuple[str, dict[str, Any]]:
        """Return the image and the config volume of this control-plane pod."""
        pod_name = os.environ.get("POD_NAME")
        if not pod_name:
            raise LauncherError(
                "POD_NAME is not set. Set it from fieldRef metadata.name in the env of "
                "deploy/k8s/deployment.yaml: the launcher copies the image and the config "
                "volume of its own pod into each job pod."
            )
        pod = json.loads(await self._kubectl("get", "pod", pod_name, "-o", "json"))
        try:
            image = pod["spec"]["containers"][0]["image"]
            config = next(v for v in pod["spec"]["volumes"] if v["name"] == "config")
        except (KeyError, IndexError, TypeError, StopIteration) as exc:
            raise LauncherError(
                f"pod {pod_name} has no image or no volume named config: {exc!r}"
            ) from exc
        return image, config

    def _job_object(
        self, template: dict[str, Any], own_pod: tuple[str, dict[str, Any]], spec: RunSpec
    ) -> dict[str, Any]:
        image, config_volume = own_pod
        job = copy.deepcopy(template)
        labels = {JOB_ID_LABEL: spec.job_id, ATTEMPT_LABEL: str(spec.attempt)}
        try:
            job.setdefault("metadata", {})["name"] = job_name(spec.job_id, spec.attempt)
            job["metadata"].setdefault("labels", {}).update(labels)
            job_spec = job["spec"]
            pod = job_spec["template"]
            pod.setdefault("metadata", {}).setdefault("labels", {}).update(labels)
            container = pod["spec"]["containers"][0]
            container["image"] = image
            container["args"] = [*container["args"], "--job-id", spec.job_id]
            volumes = pod["spec"]["volumes"]
            env_volume = next(v for v in volumes if v["name"] == "env")
            env_volume["secret"]["secretName"] = env_secret_name(spec.job_id, spec.attempt)
            pod["spec"]["volumes"] = [v for v in volumes if v["name"] != "config"]
            pod["spec"]["volumes"].append(config_volume)
        except (KeyError, IndexError, TypeError, StopIteration) as exc:
            raise LauncherError(
                f"the job template {self._template} lacks a part the launcher sets: {exc!r}. "
                "It needs spec.template.spec.containers[0].args and an env secret volume."
            ) from exc
        job_spec["activeDeadlineSeconds"] = (
            self._cfg.max_job_seconds + self._cfg.kill_grace_seconds + POD_DEADLINE_MARGIN_SECONDS
        )
        job_spec["ttlSecondsAfterFinished"] = JOB_TTL_SECONDS
        return job

    def _env_secret(self, spec: RunSpec, job_uid: str) -> dict[str, Any]:
        name = job_name(spec.job_id, spec.attempt)
        owner = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "name": name,
            "uid": job_uid,
            "controller": False,
            # True would need update access to jobs/finalizers in the Role.
            "blockOwnerDeletion": False,
        }
        data = {k: base64.b64encode(v.encode()).decode() for k, v in spec.env.items()}
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {
                "name": env_secret_name(spec.job_id, spec.attempt),
                "labels": {JOB_ID_LABEL: spec.job_id, ATTEMPT_LABEL: str(spec.attempt)},
                "ownerReferences": [owner],
            },
            "data": data,
        }

    def _write_spec_file(self, spec: RunSpec) -> None:
        # The env holds the plan token and the claim is NFS, so the env goes in a Secret only.
        data = {k: v for k, v in asdict(spec).items() if k != "env"}
        path = self._store.job_dir(spec.job_id) / RUN_SPEC_NAME
        write_private(path, json.dumps(data, default=str))

    # The wait.

    async def _wait(self, name: str, job_id: str, cancel: anyio.Event) -> RunOutcome:
        errors = 0
        while True:
            if cancel.is_set():
                await self._delete_job(name)
                return RunOutcome("cancelled", "cancelled by a caller")
            try:
                job = await self._get_job(name)
                errors = 0
            except LauncherError as exc:
                errors += 1
                if errors > MAX_POLL_ERRORS:
                    raise
                _log.warning("job %s: cannot read Job %s, will retry: %s", job_id, name, exc)
            else:
                outcome = self._outcome_of(job, job_id)
                if outcome is not None:
                    return outcome
            with anyio.move_on_after(self._poll):
                await cancel.wait()

    def _outcome_of(self, job: dict[str, Any] | None, job_id: str) -> RunOutcome | None:
        """The outcome of a finished Job, or None while it runs."""
        if job is None:
            return self._read_outcome(job_id) or RunOutcome(
                "interrupted", "pod_failed: the Job is gone and the pod recorded no outcome"
            )
        if _condition(job, "Complete") is not None:
            return self._read_outcome(job_id) or RunOutcome(
                "interrupted", "pod_failed: the pod exited 0 but recorded no outcome"
            )
        failed = _condition(job, "Failed")
        if failed is None:
            return None
        recorded = self._read_outcome(job_id)
        if recorded is not None:
            return recorded
        reason = f"{failed.get('reason', 'unknown')}: {failed.get('message', '')}".strip()
        if failed.get("reason") == "DeadlineExceeded":
            return RunOutcome("timeout", f"the job pod ran past activeDeadlineSeconds ({reason})")
        return RunOutcome("interrupted", f"pod_failed: {reason}")

    def _read_outcome(self, job_id: str) -> RunOutcome | None:
        path = self._store.job_dir(job_id) / OUTCOME_NAME
        try:
            return outcome_from_json(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError, TypeError) as exc:
            _log.error("job %s: the outcome file %s is not valid: %s", job_id, path, exc)
            return None

    # kubectl.

    async def _create(self, obj: dict[str, Any]) -> dict[str, Any]:
        # create, never apply: an existing name is a bug in the attempt count, not an update.
        out = await self._kubectl("create", "-f", "-", "-o", "json", stdin=json.dumps(obj))
        return json.loads(out)

    async def _get_job(self, name: str) -> dict[str, Any] | None:
        result = await self._run("get", "job", name, "-o", "json")
        if result.returncode != 0:
            if b"NotFound" in result.stderr:
                return None
            raise self._error(("get", "job", name), result.returncode, result.stderr)
        return json.loads(result.stdout)

    async def _delete_job(self, name: str) -> None:
        # Background propagation deletes the pod and the owned env Secret with the Job.
        await self._kubectl(
            "delete", "job", name, "--cascade=background", "--ignore-not-found", "--wait=false"
        )

    async def _kubectl(self, *args: str, stdin: str = "") -> bytes:
        result = await self._run(*args, stdin=stdin)
        if result.returncode != 0:
            raise self._error(args, result.returncode, result.stderr)
        return result.stdout

    async def _run(self, *args: str, stdin: str = "") -> subprocess.CompletedProcess[bytes]:
        argv = [self._cfg.kubectl_bin, "-n", self._cfg.k8s_namespace, _KUBECTL_TIMEOUT, *args]
        try:
            # input is always given, so kubectl never reads the service's own stdin.
            return await anyio.run_process(argv, input=stdin.encode(), check=False)
        except OSError as exc:
            raise LauncherError(
                f"cannot run {self._cfg.kubectl_bin}: {exc}. Check the bins.kubectl config key."
            ) from exc

    @staticmethod
    def _error(args: tuple[str, ...], code: int, stderr: bytes) -> LauncherError:
        return LauncherError(f"kubectl {' '.join(args)} exited {code}: {stderr_tail(stderr)}")
