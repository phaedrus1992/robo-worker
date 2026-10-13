"""Run each attempt of a job in its own k8s Job pod, and read the outcome back."""

import base64
import copy
import json
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio

from robo_worker.attempt import (
    AttemptSpec,
    PodFileError,
    PodPaths,
    outcome_from_json,
    read_pod_text,
    spec_to_json,
)
from robo_worker.checkout import checkout_path
from robo_worker.config import Config
from robo_worker.proc_output import stderr_tail
from robo_worker.repos import parse_repo
from robo_worker.runner import RunOutcome
from robo_worker.secret_files import write_private

_log = logging.getLogger(__name__)

# How often the launcher asks the API server about a job pod. A task runs for minutes.
POD_POLL_SECONDS = 5.0
# The longest wait between polls while the API server does not answer.
MAX_POLL_BACKOFF_SECONDS = 60.0
# A finished Job object stays this long, so a caller can still read its pod logs.
JOB_TTL_SECONDS = 3600
# Image pull and pod start count toward activeDeadlineSeconds. The runner's own clock must
# stop claude first, so the pod records a "timeout" outcome before k8s kills it.
POD_DEADLINE_MARGIN_SECONDS = 600
# The NFS client of the control plane can cache "no such file" for up to 60 seconds, so a
# Job that completed gets this long for its outcome file to show up.
OUTCOME_GRACE_SECONDS = 60.0
JOB_ID_LABEL = "robo-worker/job-id"
ATTEMPT_LABEL = "robo-worker/attempt"
TOKEN_KEY = "claude-oauth-token"
_KUBECTL_TIMEOUT = "--request-timeout=30s"
_IMAGE_ID_PREFIXES = ("docker-pullable://", "docker://")


class LauncherError(RuntimeError):
    """A cluster call failed, or the job template or the control-plane pod is not usable."""


def job_name(job_id: str, attempt: int) -> str:
    return f"robo-worker-{job_id}-{attempt}"


def token_secret_name(job_id: str, attempt: int) -> str:
    return f"robo-worker-token-{job_id}-{attempt}"


def _condition(job: dict[str, Any], kind: str) -> dict[str, Any] | None:
    status = job.get("status")
    conditions = status.get("conditions") if isinstance(status, dict) else None
    for condition in conditions or []:
        if (
            isinstance(condition, dict)
            and condition.get("type") == kind
            and condition.get("status") == "True"
        ):
            return condition
    return None


@dataclass(frozen=True)
class OwnPod:
    """What a job pod copies from the control-plane pod."""

    image: str
    config_volume: dict[str, Any]


class K8sLauncher:
    """The production LaunchFn: one attempt, one k8s Job, one pod."""

    def __init__(
        self, cfg: Config, template: Path, *, poll_seconds: float = POD_POLL_SECONDS
    ) -> None:
        self._cfg = cfg
        self._template_path = template
        self._poll = poll_seconds
        self._own: OwnPod | None = None
        self._template: dict[str, Any] | None = None

    async def check(self) -> None:
        """Read the job template and this pod once. Raise LauncherError naming the fix."""
        self._template = self._load_template()
        self._own = await self._own_pod()

    async def __call__(self, spec: AttemptSpec, token: str, *, cancel: anyio.Event) -> RunOutcome:
        if self._own is None or self._template is None:
            await self.check()
        own, template = self._own, self._template
        if own is None or template is None:
            raise LauncherError("the launcher check did not set the pod and the template")
        paths = PodPaths.for_job(self._cfg.state_root, spec.job_id)
        write_private(paths.spec, spec_to_json(spec))
        name = job_name(spec.job_id, spec.attempt)
        job = await self._create(self._job_object(template, own, spec))
        try:
            uid = job.get("metadata", {}).get("uid")
            if not isinstance(uid, str) or not uid:
                raise LauncherError(f"kubectl create returned Job {name} without a uid")
            await self._create(self._token_secret(spec, token, uid))
        except BaseException:
            # Without its Secret the pod waits until the deadline, so remove the Job now.
            with anyio.CancelScope(shield=True):
                await self._delete_quietly(name)
            raise
        return await self._wait(name, spec.job_id, spec.attempt, cancel)

    async def attach(self, job_id: str, attempt: int, *, cancel: anyio.Event) -> RunOutcome | None:
        """Wait for an attempt that an earlier control plane started.

        None means no Job and no outcome exist for that attempt.
        """
        name = job_name(job_id, attempt)
        if await self._get_job(name) is None:
            # The Job can be gone after its TTL while its outcome is on the claim.
            return self._read_outcome(job_id, attempt)
        return await self._wait(name, job_id, attempt, cancel)

    # The job object.

    def _load_template(self) -> dict[str, Any]:
        path = self._template_path
        try:
            template = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise LauncherError(f"cannot read the job template {path}: {exc}") from exc
        if not isinstance(template, dict):
            raise LauncherError(f"the job template {path} must be a JSON object")
        return template

    async def _own_pod(self) -> OwnPod:
        pod_name = os.environ.get("POD_NAME")
        if not pod_name:
            raise LauncherError(
                "POD_NAME is not set. Set it from fieldRef metadata.name in the env of "
                "deploy/k8s/deployment.yaml: the launcher copies the image and the config "
                "volume of its own pod into each job pod."
            )
        pod = await self._kubectl_json("get", "pod", pod_name, "-o", "json")
        try:
            image_id = pod["status"]["containerStatuses"][0]["imageID"]
            config = next(v for v in pod["spec"]["volumes"] if v["name"] == "config")
        except (KeyError, IndexError, TypeError, StopIteration) as exc:
            raise LauncherError(
                f"pod {pod_name} has no image digest or no volume named config: {exc!r}"
            ) from exc
        for prefix in _IMAGE_ID_PREFIXES:
            image_id = image_id.removeprefix(prefix)
        if "@sha256:" not in image_id:
            raise LauncherError(f"pod {pod_name} reports image {image_id!r}, not a digest")
        # The digest, not the tag: a job pod runs the exact build of this control plane.
        return OwnPod(image=image_id, config_volume=config)

    def _job_object(
        self, template: dict[str, Any], own: OwnPod, spec: AttemptSpec
    ) -> dict[str, Any]:
        job = copy.deepcopy(template)
        labels = {JOB_ID_LABEL: spec.job_id, ATTEMPT_LABEL: str(spec.attempt)}
        try:
            job.setdefault("metadata", {})["name"] = job_name(spec.job_id, spec.attempt)
            job["metadata"].setdefault("labels", {}).update(labels)
            pod = job["spec"]["template"]
            pod.setdefault("metadata", {}).setdefault("labels", {}).update(labels)
            pod_spec = pod["spec"]
            container = pod_spec["containers"][0]
            container["image"] = own.image
            args = ["--job-id", spec.job_id, "--attempt", str(spec.attempt)]
            container["args"] = [*container["args"], *args]
            self._set_mounts(pod_spec, container, spec)
        except (KeyError, IndexError, TypeError, StopIteration) as exc:
            raise LauncherError(
                f"the job template {self._template_path} lacks a part the launcher sets: "
                f"{exc!r}. It needs containers[0].args and the volumes state, work, and token."
            ) from exc
        pod_spec["volumes"] = [v for v in pod_spec["volumes"] if v["name"] != "config"]
        pod_spec["volumes"].append(own.config_volume)
        # A retry repeats side effects (a push, a PR, a comment), whatever the template says.
        job["spec"]["backoffLimit"] = 0
        pod_spec["restartPolicy"] = "Never"
        # The runner may need two kill grace periods to stop claude, and then writes the outcome.
        pod_spec["terminationGracePeriodSeconds"] = 2 * self._cfg.kill_grace_seconds + 15
        job["spec"]["activeDeadlineSeconds"] = (
            self._cfg.max_job_seconds + self._cfg.kill_grace_seconds + POD_DEADLINE_MARGIN_SECONDS
        )
        job["spec"]["ttlSecondsAfterFinished"] = JOB_TTL_SECONDS
        return job

    def _set_mounts(
        self, pod_spec: dict[str, Any], container: dict[str, Any], spec: AttemptSpec
    ) -> None:
        """Mount only this job's pod directory and this repo's checkout."""
        paths = PodPaths.for_job(self._cfg.state_root, spec.job_id)
        repo = parse_repo(spec.repo)
        checkout = checkout_path(self._cfg.workspace_root, repo)
        mounts = {m["name"]: m for m in container["volumeMounts"]}
        mounts["state"]["mountPath"] = str(paths.root)
        mounts["state"]["subPath"] = str(paths.root.relative_to(self._cfg.state_root))
        mounts["work"]["mountPath"] = str(checkout)
        mounts["work"]["subPath"] = str(checkout.relative_to(self._cfg.workspace_root))
        token = next(v for v in pod_spec["volumes"] if v["name"] == "token")
        token["secret"]["secretName"] = token_secret_name(spec.job_id, spec.attempt)
        env = [e for e in container.get("env", []) if e.get("name") != "HOME"]
        container["env"] = [*env, {"name": "HOME", "value": str(paths.home)}]

    def _token_secret(self, spec: AttemptSpec, token: str, job_uid: str) -> dict[str, Any]:
        owner = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "name": job_name(spec.job_id, spec.attempt),
            "uid": job_uid,
            "controller": False,
            # True would need update access to jobs/finalizers in the Role.
            "blockOwnerDeletion": False,
        }
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {
                "name": token_secret_name(spec.job_id, spec.attempt),
                "labels": {JOB_ID_LABEL: spec.job_id, ATTEMPT_LABEL: str(spec.attempt)},
                "ownerReferences": [owner],
            },
            "data": {TOKEN_KEY: base64.b64encode(token.encode("utf-8")).decode("ascii")},
        }

    # The wait.

    async def _wait(self, name: str, job_id: str, attempt: int, cancel: anyio.Event) -> RunOutcome:
        errors = 0
        missing_outcome_since: float | None = None
        while True:
            if cancel.is_set():
                return await self._cancel(name)
            try:
                job = await self._get_job(name)
            except LauncherError as exc:
                # Never end the job while its pod may still run: a second pod would start in
                # the same checkout. Keep asking, with a longer wait each time.
                errors += 1
                _log.warning("job %s: cannot read Job %s (try %d): %s", job_id, name, errors, exc)
                await self._sleep(min(self._poll * 2**errors, MAX_POLL_BACKOFF_SECONDS), cancel)
                continue
            errors = 0
            outcome, missing_outcome_since = self._outcome_of(
                job, job_id, attempt, missing_outcome_since
            )
            if outcome is not None:
                return outcome
            await self._sleep(self._poll, cancel)

    @staticmethod
    async def _sleep(seconds: float, cancel: anyio.Event) -> None:
        with anyio.move_on_after(seconds):
            await cancel.wait()

    async def _cancel(self, name: str) -> RunOutcome:
        """Delete the Job, then wait until it is gone, so the repo is free only after the pod."""
        deadline = anyio.current_time() + 2 * self._cfg.kill_grace_seconds + 60
        while anyio.current_time() < deadline:
            try:
                await self._delete_job(name)
                if await self._get_job(name) is None:
                    break
            except LauncherError as exc:
                _log.warning("cancel of Job %s: %s", name, exc)
            await anyio.sleep(max(self._poll, 0.01))
        else:
            _log.error("Job %s is still there after the cancel wait", name)
        return RunOutcome("cancelled", "cancelled by a caller")

    def _outcome_of(
        self,
        job: dict[str, Any] | None,
        job_id: str,
        attempt: int,
        missing_since: float | None,
    ) -> tuple[RunOutcome | None, float | None]:
        """The outcome of a finished Job, or (None, ...) while it runs or its file is late."""
        failed = _condition(job, "Failed") if job is not None else None
        complete = job is None or _condition(job, "Complete") is not None
        if not complete and failed is None:
            return None, None
        recorded = self._read_outcome_or_error(job_id, attempt)
        if isinstance(recorded, RunOutcome):
            return recorded, None
        if failed is not None:
            reason = f"{failed.get('reason', 'unknown')}: {failed.get('message', '')}".strip()
            if failed.get("reason") == "DeadlineExceeded":
                why = f"the job pod ran past activeDeadlineSeconds ({reason})"
                return RunOutcome("timeout", why), None
            return RunOutcome("interrupted", f"pod_failed: {reason}"), None
        now = anyio.current_time()
        missing_since = now if missing_since is None else missing_since
        if recorded is None and now - missing_since < OUTCOME_GRACE_SECONDS:
            return None, missing_since
        why = recorded or "the pod finished but recorded no outcome"
        return RunOutcome("interrupted", f"pod_failed: {why}"), None

    def _read_outcome_or_error(self, job_id: str, attempt: int) -> RunOutcome | str | None:
        """The recorded outcome, the reason it is not valid, or None when there is no file."""
        path = PodPaths.for_job(self._cfg.state_root, job_id).outcome(attempt)
        try:
            return outcome_from_json(read_pod_text(path))
        except FileNotFoundError:
            return None
        except PodFileError as exc:
            _log.error("job %s: %s", job_id, exc)
            return f"the outcome file is not valid: {exc}"

    def _read_outcome(self, job_id: str, attempt: int) -> RunOutcome | None:
        recorded = self._read_outcome_or_error(job_id, attempt)
        return recorded if isinstance(recorded, RunOutcome) else None

    # kubectl.

    async def _create(self, obj: dict[str, Any]) -> dict[str, Any]:
        # create, never apply: an existing name is a bug in the attempt count, not an update.
        return await self._kubectl_json("create", "-f", "-", "-o", "json", stdin=json.dumps(obj))

    async def _get_job(self, name: str) -> dict[str, Any] | None:
        result = await self._run("get", "job", name, "-o", "json")
        if result.returncode != 0:
            if b"NotFound" in result.stderr:
                return None
            raise self._error(("get", "job", name), result.returncode, result.stderr)
        return self._parse(("get", "job", name), result.stdout)

    async def _delete_job(self, name: str) -> None:
        # Background propagation deletes the pod and the owned token Secret with the Job.
        await self._kubectl(
            "delete", "job", name, "--cascade=background", "--ignore-not-found", "--wait=false"
        )

    async def _delete_quietly(self, name: str) -> None:
        try:
            await self._delete_job(name)
        except LauncherError as exc:
            _log.error("cannot delete Job %s after a failed launch: %s", name, exc)

    async def _kubectl_json(self, *args: str, stdin: str = "") -> dict[str, Any]:
        return self._parse(args, await self._kubectl(*args, stdin=stdin))

    @staticmethod
    def _parse(args: tuple[str, ...], out: bytes) -> dict[str, Any]:
        try:
            data = json.loads(out)
        except ValueError as exc:
            raise LauncherError(
                f"kubectl {' '.join(args)} returned output that is not JSON"
            ) from exc
        if not isinstance(data, dict):
            raise LauncherError(f"kubectl {' '.join(args)} returned JSON that is not an object")
        return data

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
