"""Fake kubectl binary for tests.

It records each call in FAKE_KUBECTL_DIR/calls.jsonl and answers from FAKE_KUBECTL_DIR/job.json,
which the FakeKubectl fixture writes. A job in the "complete" state writes its outcome into the
job directory under FAKE_KUBECTL_JOBS_DIR, as the job pod would.
"""

import json
import os
import sys
from pathlib import Path

_NAME_PREFIX = "robo-worker-"


def _record(state_dir: Path, argv: list[str], stdin: str) -> None:
    with (state_dir / "calls.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"argv": argv, "stdin": stdin}) + "\n")


def _strip_globals(argv: list[str]) -> list[str]:
    """Drop the leading -n <namespace> and --flag options, so argv starts at the verb."""
    while argv and argv[0].startswith("-"):
        argv = argv[2:] if argv[0] == "-n" else argv[1:]
    return argv


def _create(state_dir: Path, stdin: str) -> int:
    obj = json.loads(stdin)
    obj.setdefault("metadata", {})["uid"] = (state_dir / "uid").read_text()
    print(json.dumps(obj))
    return 0


def _job_status(state_dir: Path, name: str) -> int:
    job = json.loads((state_dir / "job.json").read_text())
    if job["state"] == "none":
        print(f'Error from server (NotFound): jobs.batch "{name}" not found', file=sys.stderr)
        return 1
    status: dict[str, object] = {"active": 1}
    if job["state"] == "complete":
        job_id = name.removeprefix(_NAME_PREFIX).rsplit("-", 1)[0]
        job_dir = Path(os.environ["FAKE_KUBECTL_JOBS_DIR"]) / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        (job_dir / "outcome.json").write_text(json.dumps(job["outcome"]))
        status = {"conditions": [{"type": "Complete", "status": "True"}]}
    elif job["state"] == "failed":
        condition = {"type": "Failed", "status": "True", "reason": job["reason"], "message": "x"}
        status = {"conditions": [condition]}
    print(json.dumps({"kind": "Job", "metadata": {"name": name}, "status": status}))
    return 0


def _get(state_dir: Path, argv: list[str]) -> int:
    kind, name = argv[1], argv[2]
    if kind == "pod":
        image = (state_dir / "image").read_text()
        config = {"name": "config", "configMap": {"name": "robo-worker-config-h4sh"}}
        spec = {"containers": [{"image": image}], "volumes": [config]}
        print(json.dumps({"kind": "Pod", "metadata": {"name": name}, "spec": spec}))
        return 0
    if kind == "job":
        return _job_status(state_dir, name)
    print(f"fake kubectl: unexpected get {argv}", file=sys.stderr)
    return 64


def main() -> int:
    state_dir = Path(os.environ["FAKE_KUBECTL_DIR"])
    stdin = "" if sys.stdin.isatty() else sys.stdin.read()
    _record(state_dir, sys.argv[1:], stdin)
    argv = _strip_globals(sys.argv[1:])
    if argv[:1] == ["create"]:
        return _create(state_dir, stdin)
    if argv[:1] == ["get"]:
        return _get(state_dir, argv)
    if argv[:1] == ["delete"]:
        return 0
    print(f"fake kubectl: unexpected arguments {sys.argv[1:]}", file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main())
