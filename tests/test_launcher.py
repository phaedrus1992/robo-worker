import base64
import json
from dataclasses import replace
from pathlib import Path
from typing import get_args

import anyio
import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.config import Config
from robo_worker.launcher import (
    JOB_TTL_SECONDS,
    POD_DEADLINE_MARGIN_SECONDS,
    K8sLauncher,
    LauncherError,
    outcome_from_json,
    outcome_to_json,
)
from robo_worker.runner import OutcomeKind, RunOutcome, RunSettings, RunSpec
from robo_worker.store import JobStore
from tests.conftest import FakeKubectl

pytestmark = pytest.mark.anyio

JOB_ID = "a" * 32
NAME = f"robo-worker-{JOB_ID}-2"
DONE = {
    "kind": "done",
    "reason": "ok",
    "result": {"status": "done"},
    "usage": None,
    "resets_at": None,
}


@pytest.fixture(autouse=True)
def job_dir(config: Config) -> Path:
    # The scheduler creates the job, and so its directory, before any launch.
    path = JobStore(config.state_root).job_dir(JOB_ID)
    path.mkdir(parents=True)
    return path


def _spec(cfg: Config, tmp_path: Path) -> RunSpec:
    return RunSpec(
        job_id=JOB_ID,
        cwd=cfg.workspace_root / "o" / "r",
        env={"HOME": "/state/home", "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat01-secret"},
        prompt="do it",
        session_id="b" * 36,
        resume=False,
        mcp_config_path=tmp_path / "mcp.json",
        stream_path=tmp_path / "stream.jsonl",
        stderr_path=tmp_path / "stderr.log",
        attempt=2,
    )


def _launcher(config: Config) -> K8sLauncher:
    return K8sLauncher(config, config.job_template, poll_seconds=0)


async def test_launcher_creates_the_job_then_an_owned_env_secret(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    store = JobStore(config.state_root)
    fake_kubectl.complete_job(outcome=DONE)

    outcome = await _launcher(config)(
        _spec(config, tmp_path), RunSettings.from_config(config), cancel=anyio.Event()
    )

    assert outcome.kind == "done"
    assert outcome.result == {"status": "done"}
    assert all(argv[:1] == ["create"] for argv in fake_kubectl.writes())  # never apply
    job, secret = fake_kubectl.created()
    assert job["kind"] == "Job"
    assert job["metadata"]["name"] == NAME
    assert job["metadata"]["labels"]["robo-worker/job-id"] == JOB_ID
    assert job["metadata"]["labels"]["robo-worker/attempt"] == "2"
    pod = job["spec"]["template"]
    assert pod["metadata"]["labels"]["robo-worker/job-id"] == JOB_ID
    container = pod["spec"]["containers"][0]
    assert container["image"] == fake_kubectl.own_image
    assert container["args"][-2:] == ["--job-id", JOB_ID]
    assert "envFrom" not in container
    volumes = {v["name"]: v for v in pod["spec"]["volumes"]}
    assert volumes["env"]["secret"]["secretName"] == f"robo-worker-env-{JOB_ID}-2"
    # The ConfigMap name carries the kustomize hash, so it comes from the launcher's own pod.
    assert volumes["config"]["configMap"]["name"] == "robo-worker-config-h4sh"
    expected = config.max_job_seconds + config.kill_grace_seconds + POD_DEADLINE_MARGIN_SECONDS
    assert job["spec"]["activeDeadlineSeconds"] == expected
    assert job["spec"]["ttlSecondsAfterFinished"] == JOB_TTL_SECONDS
    assert job["spec"]["backoffLimit"] == 0

    assert secret["kind"] == "Secret"
    assert secret["metadata"]["name"] == f"robo-worker-env-{JOB_ID}-2"
    decoded = {k: base64.b64decode(v).decode() for k, v in secret["data"].items()}
    assert decoded == _spec(config, tmp_path).env
    owner = secret["metadata"]["ownerReferences"][0]
    assert (owner["kind"], owner["name"], owner["uid"]) == ("Job", NAME, fake_kubectl.uid)
    assert owner["blockOwnerDeletion"] is False

    written = json.loads((store.job_dir(JOB_ID) / "run_spec.json").read_text())
    assert written["prompt"] == "do it"
    assert written["attempt"] == 2
    assert written["cwd"] == str(config.workspace_root / "o" / "r")
    assert "env" not in written  # the env holds the plan token, and the claim is NFS


async def test_an_old_outcome_never_answers_for_a_new_attempt(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    store = JobStore(config.state_root)
    (store.job_dir(JOB_ID) / "outcome.json").write_text(json.dumps(DONE))
    fake_kubectl.fail_job("BackoffLimitExceeded")

    outcome = await _launcher(config)(
        _spec(config, tmp_path), RunSettings.from_config(config), cancel=anyio.Event()
    )

    assert outcome.kind == "interrupted"


async def test_attach_returns_none_when_the_job_is_gone(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.no_job()

    outcome = await _launcher(config).attach(JOB_ID, 2, cancel=anyio.Event())

    assert outcome is None
    assert fake_kubectl.writes() == []


async def test_attach_reads_the_outcome_of_a_running_job(
    config: Config, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.complete_job(outcome=DONE)

    outcome = await _launcher(config).attach(JOB_ID, 2, cancel=anyio.Event())

    assert outcome is not None
    assert outcome.kind == "done"
    assert fake_kubectl.writes() == []


async def test_a_pod_that_dies_without_an_outcome_is_interrupted(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.fail_job("BackoffLimitExceeded")

    outcome = await _launcher(config)(
        _spec(config, tmp_path), RunSettings.from_config(config), cancel=anyio.Event()
    )

    assert outcome.kind == "interrupted"
    assert "pod_failed" in outcome.reason
    assert "BackoffLimitExceeded" in outcome.reason


async def test_a_pod_past_its_deadline_is_a_timeout(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    # A retry of a timed-out task times out again, so this is not an interruption.
    fake_kubectl.fail_job("DeadlineExceeded")

    outcome = await _launcher(config)(
        _spec(config, tmp_path), RunSettings.from_config(config), cancel=anyio.Event()
    )

    assert outcome.kind == "timeout"


async def test_cancel_deletes_the_job(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    fake_kubectl.hang_job()
    cancel = anyio.Event()
    cancel.set()

    outcome = await _launcher(config)(
        _spec(config, tmp_path), RunSettings.from_config(config), cancel=cancel
    )

    assert outcome.kind == "cancelled"
    assert fake_kubectl.deleted() == [NAME]


async def test_a_missing_pod_name_names_the_downward_api_entry(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("POD_NAME")

    with pytest.raises(LauncherError, match="POD_NAME"):
        await _launcher(config)(
            _spec(config, tmp_path), RunSettings.from_config(config), cancel=anyio.Event()
        )
    assert fake_kubectl.writes() == []


async def test_a_bad_template_is_a_launcher_error(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    bad = tmp_path / "bad-template.json"
    bad.write_text("{not json")
    launcher = K8sLauncher(config, bad, poll_seconds=0)

    with pytest.raises(LauncherError, match="job template"):
        await launcher(
            _spec(config, tmp_path), RunSettings.from_config(config), cancel=anyio.Event()
        )
    assert fake_kubectl.writes() == []


async def test_a_kubectl_failure_names_the_command(
    config: Config, tmp_path: Path, fake_kubectl: FakeKubectl
) -> None:
    launcher = K8sLauncher(replace(config, kubectl_bin="false"), config.job_template)

    with pytest.raises(LauncherError, match="kubectl get pod"):
        await launcher(
            _spec(config, tmp_path), RunSettings.from_config(config), cancel=anyio.Event()
        )


_FLAT = st.dictionaries(st.text(max_size=5), st.integers() | st.text(max_size=5), max_size=3)
_OUTCOMES = st.builds(
    RunOutcome,
    kind=st.sampled_from(get_args(OutcomeKind.__value__)),
    reason=st.text(),
    result=st.none() | _FLAT,
    usage=st.none() | _FLAT,
    resets_at=st.none() | st.floats(allow_nan=False, allow_infinity=False),
)


@given(_OUTCOMES)
def test_outcome_round_trips_through_json(outcome: RunOutcome) -> None:
    assert outcome_from_json(outcome_to_json(outcome)) == outcome


@pytest.mark.parametrize(
    "text",
    [
        "[]",
        json.dumps({**DONE, "extra": 1}),
        json.dumps({**DONE, "kind": "won"}),
        json.dumps({**DONE, "reason": 3}),
        json.dumps({**DONE, "result": [1]}),
        json.dumps({**DONE, "resets_at": True}),
        json.dumps({**DONE, "resets_at": "soon"}),
    ],
)
def test_outcome_from_json_refuses_a_bad_file(text: str) -> None:
    with pytest.raises(ValueError):
        outcome_from_json(text)
