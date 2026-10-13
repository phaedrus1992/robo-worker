import base64
import json
from dataclasses import replace
from pathlib import Path

import anyio
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from robo_worker import launcher as launcher_module
from robo_worker.attempt import AttemptSpec, PodPaths, spec_from_json
from robo_worker.config import Config
from robo_worker.launcher import (
    JOB_TTL_SECONDS,
    POD_DEADLINE_MARGIN_SECONDS,
    K8sLauncher,
    LauncherError,
    OwnPod,
    job_name,
)
from tests.conftest import FakeKubectl

pytestmark = pytest.mark.anyio

JOB_ID = "a" * 32
NAME = f"robo-worker-{JOB_ID}-2"
TOKEN = "sk-ant-oat01-secret"
DONE = {
    "kind": "done",
    "reason": "ok",
    "result": {"status": "done"},
    "usage": None,
    "resets_at": None,
    "checkout_problem": None,
}
SPEC = AttemptSpec(
    job_id=JOB_ID,
    attempt=2,
    repo="o/r",
    tags=["lang-python"],
    prompt="do it",
    session_id="b" * 36,
    resume=True,
)


@pytest.fixture(autouse=True)
def pod_dir(config: Config) -> Path:
    # The scheduler stages the pod directory before any launch.
    paths = PodPaths.for_job(config.state_root, JOB_ID)
    paths.root.mkdir(parents=True)
    return paths.root


def _launcher(config: Config, template: Path | None = None) -> K8sLauncher:
    return K8sLauncher(config, template or config.job_template, poll_seconds=0)


async def _launch(config: Config, cancel: anyio.Event | None = None) -> launcher_module.RunOutcome:
    return await _launcher(config)(SPEC, TOKEN, cancel=cancel or anyio.Event())


async def test_launcher_creates_the_job_then_an_owned_token_secret(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.complete_job(outcome=DONE)

    outcome = await _launch(config)

    assert outcome.kind == "done"
    assert all(argv[:1] == ["create"] for argv in fake_kubectl.writes())  # never apply
    job, secret = fake_kubectl.created()
    assert job["metadata"]["name"] == NAME
    assert job["metadata"]["labels"]["robo-worker/job-id"] == JOB_ID
    assert job["metadata"]["labels"]["robo-worker/attempt"] == "2"
    pod = job["spec"]["template"]
    assert pod["metadata"]["labels"]["robo-worker/attempt"] == "2"
    container = pod["spec"]["containers"][0]
    # The digest of the control plane's own image, not its moving tag.
    assert container["image"] == fake_kubectl.own_image
    assert container["args"][-4:] == ["--job-id", JOB_ID, "--attempt", "2"]
    volumes = {v["name"]: v for v in pod["spec"]["volumes"]}
    assert volumes["token"]["secret"]["secretName"] == f"robo-worker-token-{JOB_ID}-2"
    assert volumes["config"]["configMap"]["name"] == "robo-worker-config-h4sh"
    expected = config.max_job_seconds + config.kill_grace_seconds + POD_DEADLINE_MARGIN_SECONDS
    assert job["spec"]["activeDeadlineSeconds"] == expected
    assert job["spec"]["ttlSecondsAfterFinished"] == JOB_TTL_SECONDS
    assert pod["spec"]["terminationGracePeriodSeconds"] == 2 * config.kill_grace_seconds + 15

    assert secret["kind"] == "Secret"
    assert secret["metadata"]["name"] == f"robo-worker-token-{JOB_ID}-2"
    decoded = {k: base64.b64decode(v).decode() for k, v in secret["data"].items()}
    assert decoded == {"claude-oauth-token": TOKEN}
    owner = secret["metadata"]["ownerReferences"][0]
    assert (owner["kind"], owner["name"], owner["uid"]) == ("Job", NAME, fake_kubectl.uid)
    assert owner["blockOwnerDeletion"] is False

    paths = PodPaths.for_job(config.state_root, JOB_ID)
    assert spec_from_json(paths.spec.read_text()) == SPEC
    assert TOKEN not in paths.spec.read_text()


async def test_a_job_pod_mounts_only_its_own_pod_dir_and_checkout(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.complete_job(outcome=DONE)

    await _launch(config)

    job, _ = fake_kubectl.created()
    container = job["spec"]["template"]["spec"]["containers"][0]
    mounts = {m["name"]: m for m in container["volumeMounts"]}
    paths = PodPaths.for_job(config.state_root, JOB_ID)
    assert mounts["state"]["subPath"] == f"jobs/{JOB_ID}/pod"
    assert mounts["state"]["mountPath"] == str(paths.root)
    assert mounts["work"]["subPath"] == "o/r"
    assert mounts["work"]["mountPath"] == str(config.workspace_root / "o" / "r")
    assert {"name": "HOME", "value": str(paths.home)} in container["env"]


async def test_the_no_retry_fields_are_forced(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    template = json.loads(config.job_template.read_text())
    template["spec"]["backoffLimit"] = 3
    template["spec"]["template"]["spec"]["restartPolicy"] = "OnFailure"
    edited = tmp_path / "edited.json"
    edited.write_text(json.dumps(template))
    fake_kubectl.complete_job(outcome=DONE)

    await _launcher(config, edited)(SPEC, TOKEN, cancel=anyio.Event())

    job, _ = fake_kubectl.created()
    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["template"]["spec"]["restartPolicy"] == "Never"


async def test_an_old_attempts_outcome_never_answers_for_a_new_one(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    paths = PodPaths.for_job(config.state_root, JOB_ID)
    paths.outcome(1).write_text(json.dumps(DONE))
    fake_kubectl.fail_job("BackoffLimitExceeded")

    outcome = await _launch(config)

    assert outcome.kind == "interrupted"
    assert "BackoffLimitExceeded" in outcome.reason


async def test_attach_returns_none_when_no_job_and_no_outcome_exist(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.no_job()

    assert await _launcher(config).attach(JOB_ID, 2, cancel=anyio.Event()) is None
    assert fake_kubectl.writes() == []


async def test_attach_reads_the_outcome_of_a_job_gone_after_its_ttl(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    # The pod finished while the control plane was down, and the TTL removed the Job.
    PodPaths.for_job(config.state_root, JOB_ID).outcome(2).write_text(json.dumps(DONE))
    fake_kubectl.no_job()

    outcome = await _launcher(config).attach(JOB_ID, 2, cancel=anyio.Event())

    assert outcome is not None
    assert outcome.kind == "done"


async def test_attach_waits_for_a_running_job(config: Config, fake_kubectl: FakeKubectl) -> None:
    fake_kubectl.complete_job(outcome=DONE)

    outcome = await _launcher(config).attach(JOB_ID, 2, cancel=anyio.Event())

    assert outcome is not None
    assert outcome.kind == "done"
    assert fake_kubectl.writes() == []


async def test_a_pod_past_its_deadline_is_a_timeout(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    # A retry of a timed-out task times out again, so this is not an interruption.
    fake_kubectl.fail_job("DeadlineExceeded")

    assert (await _launch(config)).kind == "timeout"


async def test_a_completed_pod_without_an_outcome_is_interrupted_after_the_grace(
    config: Config, fake_kubectl: FakeKubectl, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launcher_module, "OUTCOME_GRACE_SECONDS", 0.2)
    fake_kubectl.complete_job(outcome=None)

    outcome = await _launch(config)

    assert outcome.kind == "interrupted"
    assert "recorded no outcome" in outcome.reason


async def test_a_corrupt_outcome_names_the_problem(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.complete_job(outcome='{"kind": "won"}')

    outcome = await _launch(config)

    assert outcome.kind == "interrupted"
    assert "not valid" in outcome.reason


async def test_poll_errors_do_not_end_a_job_that_may_still_run(
    config: Config, fake_kubectl: FakeKubectl, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_KUBECTL_GET_JOB_FAILS", "20")
    fake_kubectl.complete_job(outcome=DONE)

    assert (await _launch(config)).kind == "done"


async def test_cancel_deletes_the_job_and_waits_until_it_is_gone(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.hang_job()
    cancel = anyio.Event()
    cancel.set()

    outcome = await _launch(config, cancel)

    assert outcome.kind == "cancelled"
    assert fake_kubectl.deleted() == [NAME]
    verbs = fake_kubectl.verbs()
    last_delete = max(i for i, a in enumerate(verbs) if a[:2] == ["delete", "job"])
    assert ["get", "job", NAME, "-o", "json"] in verbs[last_delete + 1 :]


async def test_a_failed_secret_create_deletes_the_job_and_keeps_the_error(
    config: Config, fake_kubectl: FakeKubectl, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_KUBECTL_FAIL_SECRET", "1")

    with pytest.raises(LauncherError, match="Forbidden"):
        await _launch(config)

    assert fake_kubectl.deleted() == [NAME]


async def test_check_names_the_downward_api_entry(
    config: Config, fake_kubectl: FakeKubectl, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("POD_NAME")

    with pytest.raises(LauncherError, match="POD_NAME"):
        await _launcher(config).check()


async def test_check_refuses_a_bad_template(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    bad = tmp_path / "bad-template.json"
    bad.write_text("{not json")

    with pytest.raises(LauncherError, match="job template"):
        await _launcher(config, bad).check()


async def test_check_refuses_an_image_without_a_digest(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.set_image("ghcr.io/phaedrus1992/robo-worker:main")

    with pytest.raises(LauncherError, match="not a digest"):
        await _launcher(config).check()


async def test_kubectl_output_that_is_not_json_is_a_launcher_error(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.set_image("garbage")

    with pytest.raises(LauncherError, match="not JSON"):
        await _launcher(config).check()


async def test_a_kubectl_failure_names_the_command(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    with pytest.raises(LauncherError, match=r"kubectl .*get pod"):
        await _launcher(replace(config, kubectl_bin="false")).check()


@settings(max_examples=60, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    job_id=st.from_regex(r"[0-9a-f]{32}", fullmatch=True),
    attempt=st.integers(min_value=1, max_value=10_000),
    resume=st.booleans(),
)
def test_job_object_invariants(config: Config, job_id: str, attempt: int, resume: bool) -> None:
    launcher = _launcher(config)
    template = json.loads(config.job_template.read_text())
    before = json.dumps(template, sort_keys=True)
    own = OwnPod(image="img@sha256:" + "d" * 64, config_volume={"name": "config", "configMap": {}})
    spec = replace(SPEC, job_id=job_id, attempt=attempt, resume=resume)

    job = launcher._job_object(template, own, spec)

    assert json.dumps(template, sort_keys=True) == before  # the template is not changed
    assert job["metadata"]["name"] == job_name(job_id, attempt)
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["args"][-4:] == ["--job-id", job_id, "--attempt", str(attempt)]
    assert job["spec"]["backoffLimit"] == 0
    assert pod["restartPolicy"] == "Never"
    assert [v for v in pod["volumes"] if v["name"] == "config"] == [own.config_volume]
    mounts = {m["name"]: m for m in pod["containers"][0]["volumeMounts"]}
    assert mounts["state"]["subPath"] == f"jobs/{job_id}/pod"
