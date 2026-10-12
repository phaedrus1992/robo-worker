import json
import os
import subprocess
import sys
from collections.abc import Mapping
from datetime import date
from functools import partial
from pathlib import Path
from typing import Any

import anyio
import pytest

from robo_worker.config import Config
from robo_worker.runner import RunOutcome, RunSettings, RunSpec, run_claude
from robo_worker.scheduler import Scheduler
from robo_worker.store import JobStore

FIXTURES = Path(__file__).parent / "fixtures"
FAKES = Path(__file__).parent / "fakes"
AGENT_TOKEN = "a" * 40
MAC_TOKEN = "m" * 40
FAKE_ENV_NAMES = (
    "FAKE_CLAUDE_FIXTURES",
    "FAKE_CLAUDE_EXIT",
    "FAKE_CLAUDE_SLEEP",
    "FAKE_CLAUDE_RECORD",
    "FAKE_CLAUDE_IGNORE_TERM",
    "FAKE_CLAUDE_TOUCH",
    "FAKE_CLAUDE_STDERR",
    "FAKE_CLAUDE_PIDFILE",
    "FAKE_CLAUDE_SKIP_STDIN",
    "FAKE_LLMENV_OUTPUT",
    "FAKE_LLMENV_EXIT",
    "FAKE_LLMENV_SLEEP",
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def isolated_home(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Keep the developer's ~/.claude settings out of every test."""
    home = tmp_path_factory.mktemp("home")
    monkeypatch.setenv("HOME", str(home))
    return home


def write_secret(path: Path, text: str) -> Path:
    path.write_text(text)
    path.chmod(0o600)
    return path


def _wrapper(path: Path, script: Path) -> Path:
    path.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
    path.chmod(0o755)
    return path


@pytest.fixture
def fake_claude(tmp_path: Path) -> Path:
    return _wrapper(tmp_path / "bin-claude", FAKES / "fake_claude.py")


@pytest.fixture
def fake_llmenv(tmp_path: Path) -> Path:
    return _wrapper(tmp_path / "bin-llmenv", FAKES / "fake_llmenv.py")


class FakeKubectl:
    """Drives tests/fakes/fake_kubectl.py: the job state it reports, and the calls it saw."""

    uid = "0f0e0d0c-job-uid"
    own_image = "ghcr.io/phaedrus1992/robo-worker@sha256:" + "c" * 64

    def __init__(self, state_dir: Path) -> None:
        self._dir = state_dir
        (state_dir / "uid").write_text(self.uid)
        (state_dir / "image").write_text(self.own_image)
        self.hang_job()

    def _set(self, **job: object) -> None:
        (self._dir / "job.json").write_text(json.dumps(job))

    def complete_job(self, *, outcome: Mapping[str, object]) -> None:
        self._set(state="complete", outcome=outcome)

    def fail_job(self, reason: str) -> None:
        self._set(state="failed", reason=reason)

    def hang_job(self) -> None:
        self._set(state="active")

    def no_job(self) -> None:
        self._set(state="none")

    def calls(self) -> list[dict[str, Any]]:
        path = self._dir / "calls.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def _verbs(self) -> list[list[str]]:
        verbs = []
        for call in self.calls():
            argv = call["argv"]
            while argv and argv[0].startswith("-"):
                argv = argv[2:] if argv[0] == "-n" else argv[1:]
            verbs.append(argv)
        return verbs

    def writes(self) -> list[list[str]]:
        """The argv of each call that changes the cluster."""
        return [a for a in self._verbs() if a[:1] in (["create"], ["apply"], ["delete"], ["patch"])]

    def created(self) -> list[dict[str, Any]]:
        return [json.loads(c["stdin"]) for c in self.calls() if "create" in c["argv"]]

    def deleted(self) -> list[str]:
        return [a[2] for a in self._verbs() if a[:2] == ["delete", "job"]]


@pytest.fixture
def fake_kubectl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: Config) -> FakeKubectl:
    state_dir = tmp_path / "kubectl-state"
    state_dir.mkdir()
    monkeypatch.setenv("FAKE_KUBECTL_DIR", str(state_dir))
    monkeypatch.setenv("FAKE_KUBECTL_JOBS_DIR", str(config.state_root / "jobs"))
    monkeypatch.setenv("POD_NAME", "robo-worker-7d9f-abcde")
    return FakeKubectl(state_dir)


@pytest.fixture
def isolated_git(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the developer's git config (signing, hooks) out of test repos."""
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for role in ("AUTHOR", "COMMITTER"):
        monkeypatch.setenv(f"GIT_{role}_NAME", "test")
        monkeypatch.setenv(f"GIT_{role}_EMAIL", "test@example.invalid")


def make_origin(tmp_path: Path, owner: str = "o", name: str = "r") -> Path:
    """Create a local repo with one commit, used as the clone source."""
    origin = tmp_path / "origin" / owner / name
    origin.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", "-b", "main", str(origin)], check=True)
    (origin / "README.md").write_text("hi\n")
    subprocess.run(["git", "-C", str(origin), "add", "README.md"], check=True)
    subprocess.run(["git", "-C", str(origin), "commit", "-q", "-m", "init"], check=True)
    return origin


@pytest.fixture
def config(tmp_path: Path, fake_claude: Path, fake_llmenv: Path) -> Config:
    kubectl = _wrapper(tmp_path / "bin-kubectl", FAKES / "fake_kubectl.py")
    secrets_dir = tmp_path / "secrets"
    secrets_dir.mkdir()
    callers = f'[callers]\nagent = "{AGENT_TOKEN}"\nmac = "{MAC_TOKEN}"\n'
    return Config(
        listen_host="127.0.0.1",
        listen_port=8765,
        public_url="http://127.0.0.1:8765",
        allowed_hosts=("127.0.0.1:*",),
        workspace_root=tmp_path / "work",
        state_root=tmp_path / "state",
        job_template=Path(__file__).parent.parent / "deploy" / "k8s" / "job-pod.json",
        callers_file=write_secret(secrets_dir / "callers.toml", callers),
        oauth_token_file=write_secret(secrets_dir / "oauth", "sk-ant-oat01-test\n"),
        oauth_token_created=date.today(),
        token_warn_days=30,
        allowed_key_sources=("none",),
        max_parallel_jobs=2,
        max_parallel_asks=2,
        max_job_seconds=20,
        max_ask_seconds=20,
        max_prepare_seconds=20,
        kill_grace_seconds=1,
        max_interruptions=2,
        rate_limit_retry_seconds=60,
        default_tags=(),
        deny_tags=("blocked-tag",),
        deny_bundles=("ben-style",),
        deny_output_styles=("ben",),
        extra_env=FAKE_ENV_NAMES,
        claude_bin=str(fake_claude),
        llmenv_bin=str(fake_llmenv),
        git_bin="git",
        kubectl_bin=str(kubectl),
        k8s_namespace="phaedrus",
        clone_url_template=f"file://{tmp_path}/origin/{{owner}}/{{name}}",
    )


class LocalLauncher:
    """The LaunchFn the tests use: run the task in this process, through the fake claude."""

    def __init__(self, store: JobStore) -> None:
        self._store = store

    async def __call__(
        self, spec: RunSpec, settings: RunSettings, *, cancel: anyio.Event
    ) -> RunOutcome:
        on_event = partial(self._store.append_event, spec.job_id)
        return await run_claude(spec, settings, cancel=cancel, on_event=on_event)

    async def attach(self, job_id: str, attempt: int, *, cancel: anyio.Event) -> RunOutcome | None:
        # An in-process run stops with the process, so nothing survives a restart.
        return None


def make_scheduler(config: Config, store: JobStore, **kwargs: Any) -> Scheduler:
    """A Scheduler wired to the in-process launcher, for tests."""
    kwargs.setdefault("launch", LocalLauncher(store))
    return Scheduler(config, store, **kwargs)
