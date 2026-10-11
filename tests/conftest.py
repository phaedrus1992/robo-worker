import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from robo_worker.config import Config

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
    "FAKE_LLMENV_OUTPUT",
    "FAKE_LLMENV_EXIT",
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


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
        callers_file=write_secret(secrets_dir / "callers.toml", callers),
        oauth_token_file=write_secret(secrets_dir / "oauth", "sk-ant-oat01-test\n"),
        oauth_token_created=date.today(),
        token_warn_days=30,
        allowed_key_sources=("none",),
        max_parallel_jobs=2,
        max_parallel_asks=2,
        max_job_seconds=20,
        max_ask_seconds=20,
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
        clone_url_template=f"file://{tmp_path}/origin/{{owner}}/{{name}}",
    )
