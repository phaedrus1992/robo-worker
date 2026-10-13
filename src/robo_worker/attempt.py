"""One attempt of a job: the files the control plane and the job pod share, and the pod's work.

The job pod does every step that touches repo data: the checkout, `llmenv export`, the
worker environment, claude, and the dirty check after the run. The control plane never runs
a program on data that a job pod wrote. It only parses the pod's files, with the limits in
`read_pod_text`. Design: docs/deploy.md, "What a job pod can reach".
"""

import json
import os
import shutil
import stat
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from typing import Any, get_args

import anyio

from robo_worker.checkout import (
    CheckoutError,
    DirtyCheckout,
    checkout_path,
    is_dirty,
    prepare_checkout,
    sync_checkout,
)
from robo_worker.config import Config
from robo_worker.env import (
    EnvError,
    build_worker_env,
    check_bundles,
    check_claude_settings,
    llmenv_export,
    validate_tags,
)
from robo_worker.repos import parse_repo
from robo_worker.runner import EventSink, OutcomeKind, RunOutcome, RunSettings, RunSpec, run_claude

# The pod's files are under <state_root>/jobs/<id>/pod. The job pod mounts this directory only.
POD_DIR_NAME = "pod"
# The pod's own events. The control plane copies them into events.jsonl after the run, so
# each file on the NFS claim has one writer.
POD_EVENT_KINDS = frozenset({"stream_error", "rate_limit", "stdin_error"})
# A pod file larger than this is not read: the pod controls its size.
MAX_POD_FILE_BYTES = 1_000_000
_OUTCOME_KINDS = frozenset(get_args(OutcomeKind.__value__))
# These outcomes end the job, so a dirty checkout after them pauses the repo.
ENDING_KINDS = frozenset({"done", "failed", "timeout", "auth", "cancelled"})


class PodFileError(ValueError):
    """A pod file is missing, not a regular file, too large, or not valid."""


@dataclass(frozen=True)
class AttemptSpec:
    """What the control plane tells the job pod. It holds no secret."""

    job_id: str
    attempt: int
    repo: str
    tags: list[str]
    prompt: str
    session_id: str
    # True when claude ran for this job before, so this attempt uses --resume.
    resume: bool


@dataclass(frozen=True)
class PodPaths:
    root: Path

    @classmethod
    def for_job(cls, state_root: Path, job_id: str) -> PodPaths:
        return cls(state_root / "jobs" / job_id / POD_DIR_NAME)

    @property
    def home(self) -> Path:
        return self.root / "home"

    @property
    def llmenv_config(self) -> Path:
        return self.home / ".config" / "llmenv"

    @property
    def spec(self) -> Path:
        return self.root / "attempt.json"

    @property
    def mcp_config(self) -> Path:
        return self.root / "mcp.json"

    @property
    def stream(self) -> Path:
        return self.root / "stream.jsonl"

    @property
    def stderr(self) -> Path:
        return self.root / "stderr.log"

    @property
    def events(self) -> Path:
        return self.root / "pod-events.jsonl"

    def outcome(self, attempt: int) -> Path:
        # One file per attempt, so an old pod that writes late never answers for a new one.
        return self.root / f"outcome-{attempt}.json"


class StagingError(RuntimeError):
    """The control plane cannot prepare the pod directory for an attempt."""


def _real_dir(path: Path) -> None:
    """Make ``path`` a real directory, and remove a link or a file that a pod left there."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        path.mkdir()
        return
    if stat.S_ISDIR(info.st_mode):
        return
    path.unlink()
    path.mkdir()


def stage_pod_dir(paths: PodPaths, llmenv_master: Path) -> None:
    """Prepare the pod directory, with a fresh copy of the llmenv config in the pod's HOME.

    A pod can change its own directory, so each attempt gets a new copy, and no step follows
    a link that a pod made. llmenv runs `git pull` on its config, so the copy is writable.
    """
    if not (llmenv_master / "config.yaml").is_file():
        raise StagingError(
            f"no llmenv config at {llmenv_master}. Clone the llmenv config repo there "
            "(docs/deploy.md, one-time setup)."
        )
    paths.root.parent.mkdir(parents=True, exist_ok=True)
    for path in (paths.root, paths.home, paths.home / ".config"):
        _real_dir(path)
    target = paths.llmenv_config
    if target.is_symlink() or target.is_file():
        target.unlink()
    elif target.exists():
        shutil.rmtree(target)
    shutil.copytree(llmenv_master, target, symlinks=True)


def read_pod_text(path: Path) -> str:
    """Read a file that a job pod wrote, without following a link the pod planted."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise PodFileError(f"cannot open pod file {path}: {exc}") from exc
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise PodFileError(f"pod file {path} is not a regular file")
        if info.st_size > MAX_POD_FILE_BYTES:
            raise PodFileError(f"pod file {path} has {info.st_size} bytes, more than the limit")
        return handle.read(MAX_POD_FILE_BYTES).decode("utf-8", errors="replace")


def _parse_object(text: str, what: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
    except ValueError as exc:
        raise PodFileError(f"{what} is not JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise PodFileError(f"{what} must be a JSON object")
    return data


def spec_to_json(spec: AttemptSpec) -> str:
    return json.dumps(asdict(spec))


def spec_from_json(text: str) -> AttemptSpec:
    """Parse attempt.json. Raise PodFileError when it is not a valid AttemptSpec."""
    data = _parse_object(text, "attempt.json")
    if set(data) != {f.name for f in fields(AttemptSpec)}:
        raise PodFileError(f"attempt.json needs exactly the AttemptSpec keys, got {sorted(data)}")
    checks = {
        "job_id": str,
        "attempt": int,
        "repo": str,
        "prompt": str,
        "session_id": str,
        "resume": bool,
    }
    for key, kind in checks.items():
        # bool is a subclass of int, so `true` must not pass as an attempt number.
        if not isinstance(data[key], kind) or (kind is int and isinstance(data[key], bool)):
            raise PodFileError(f"attempt.json {key} must be {kind.__name__}")
    try:
        data["tags"] = validate_tags(data["tags"])
    except ValueError as exc:
        raise PodFileError(f"attempt.json tags: {exc}") from exc
    return AttemptSpec(**data)


def outcome_to_json(outcome: RunOutcome) -> str:
    return json.dumps(asdict(outcome))


def _optional(data: dict[str, Any], key: str, kind: type | tuple[type, ...]) -> None:
    value = data[key]
    if value is not None and (isinstance(value, bool) or not isinstance(value, kind)):
        raise PodFileError(f"outcome {key} must be {kind} or null")


def outcome_from_json(text: str) -> RunOutcome:
    """Parse an outcome file. Raise PodFileError when it is not a valid outcome."""
    data = _parse_object(text, "the outcome")
    if set(data) != {f.name for f in fields(RunOutcome)}:
        raise PodFileError(f"the outcome needs exactly the RunOutcome keys, got {sorted(data)}")
    if not isinstance(data["kind"], str) or data["kind"] not in _OUTCOME_KINDS:
        raise PodFileError(f"unknown outcome kind {data['kind']!r:.50}")
    if not isinstance(data["reason"], str):
        raise PodFileError("outcome reason must be a string")
    _optional(data, "result", dict)
    _optional(data, "usage", dict)
    _optional(data, "resets_at", (int, float))
    _optional(data, "checkout_problem", str)
    return RunOutcome(**data)


def read_pod_events(path: Path) -> list[tuple[str, dict[str, Any]]]:
    """Return the valid events of pod-events.jsonl. Lines that are not valid are skipped."""
    try:
        text = read_pod_text(path)
    except FileNotFoundError:
        return []
    events = []
    for line in text.splitlines():
        try:
            data = json.loads(line)
        except ValueError:
            continue
        if (
            isinstance(data, dict)
            and data.get("kind") in POD_EVENT_KINDS
            and isinstance(data.get("data"), dict)
        ):
            events.append((data["kind"], data["data"]))
    return events


def pod_event_sink(path: Path) -> EventSink:
    def append(kind: str, data: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as sink:
            sink.write(json.dumps({"kind": kind, "data": data}) + "\n")

    return append


async def _checkout(cfg: Config, spec: AttemptSpec) -> Path | RunOutcome:
    repo = parse_repo(spec.repo)
    path = checkout_path(cfg.workspace_root, repo)
    args = (cfg.git_bin, cfg.workspace_root, repo, cfg.clone_url_template, cfg.max_prepare_seconds)
    if not spec.resume:
        try:
            return await prepare_checkout(*args)
        except DirtyCheckout as exc:
            return RunOutcome("checkout_paused", f"checkout_dirty: {exc}")
        except CheckoutError as exc:
            return RunOutcome("prepare_failed", f"checkout failed: {exc}")
    if not (path / ".git").exists():
        # A fresh clone holds none of the session's work, so a resume there is not a resume.
        return RunOutcome(
            "prepare_failed",
            f"the checkout {path} of this started job is gone, so its session cannot resume",
        )
    try:
        # A resume runs on the tree its own session left, dirty or not.
        return await sync_checkout(*args)
    except CheckoutError as exc:
        return RunOutcome("checkout_paused", f"checkout_unavailable: {exc}")


async def _worker_env(
    cfg: Config, spec: AttemptSpec, cwd: Path, parent: Mapping[str, str], token: str
) -> dict[str, str]:
    llmenv_vars = await llmenv_export(
        cfg.llmenv_bin, cwd, spec.tags, parent, cfg.max_prepare_seconds
    )
    check_bundles(llmenv_vars, cfg.deny_bundles)
    check_claude_settings(llmenv_vars, parent.get("HOME"))
    return build_worker_env(
        parent=parent, llmenv_vars=llmenv_vars, oauth_token=token, extra_env=cfg.extra_env
    )


async def _checkout_problem(cfg: Config, cwd: Path) -> str | None:
    try:
        dirty = await is_dirty(cfg.git_bin, cwd, cfg.max_prepare_seconds)
    except CheckoutError as exc:
        return f"checkout_unknown: {exc}"
    if dirty:
        return f"checkout_dirty: {cwd} has uncommitted changes"
    return None


async def run_attempt(
    cfg: Config,
    spec: AttemptSpec,
    token: str,
    *,
    parent: Mapping[str, str],
    cancel: anyio.Event,
) -> RunOutcome:
    """Prepare the checkout and the environment, run claude, and check the tree after it."""
    paths = PodPaths.for_job(cfg.state_root, spec.job_id)
    paths.home.mkdir(parents=True, exist_ok=True)
    cwd = await _checkout(cfg, spec)
    if isinstance(cwd, RunOutcome):
        return cwd
    pod_parent = {**parent, "HOME": str(paths.home)}
    try:
        env = await _worker_env(cfg, spec, cwd, pod_parent, token)
    except EnvError as exc:
        return RunOutcome("prepare_failed", str(exc))
    run_spec = RunSpec(
        job_id=spec.job_id,
        cwd=cwd,
        env=env,
        prompt=spec.prompt,
        session_id=spec.session_id,
        resume=spec.resume,
        mcp_config_path=paths.mcp_config,
        stream_path=paths.stream,
        stderr_path=paths.stderr,
        attempt=spec.attempt,
    )
    settings = RunSettings.from_config(cfg)
    on_event = pod_event_sink(paths.events)
    outcome = await run_claude(run_spec, settings, cancel=cancel, on_event=on_event)
    if outcome.kind not in ENDING_KINDS:
        return outcome
    return replace(outcome, checkout_problem=await _checkout_problem(cfg, cwd))
