"""Fake kubectl binary for tests.

It records each call in FAKE_KUBECTL_DIR/calls.jsonl and answers from FAKE_KUBECTL_DIR/job.json,
which the FakeKubectl fixture writes. A job in the "complete" state writes its outcome into the
pod directory under FAKE_KUBECTL_JOBS_DIR, as the job pod would. FAKE_KUBECTL_FAIL_SECRET makes
a Secret create fail. FAKE_KUBECTL_GET_JOB_FAILS makes that many `get job` calls fail first.
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


def _set_job(state_dir: Path, **job: object) -> None:
    (state_dir / "job.json").write_text(json.dumps(job))


def _create(state_dir: Path, stdin: str) -> int:
    obj = json.loads(stdin)
    if obj.get("kind") == "Secret" and os.environ.get("FAKE_KUBECTL_FAIL_SECRET"):
        print("Error from server (Forbidden): secrets is forbidden", file=sys.stderr)
        return 1
    obj.setdefault("metadata", {})["uid"] = (state_dir / "uid").read_text()
    print(json.dumps(obj))
    return 0


def _write_outcome(name: str, outcome: object) -> None:
    job_id, attempt = name.removeprefix(_NAME_PREFIX).rsplit("-", 1)
    pod_dir = Path(os.environ["FAKE_KUBECTL_JOBS_DIR"]) / job_id / "pod"
    pod_dir.mkdir(parents=True, exist_ok=True)
    text = outcome if isinstance(outcome, str) else json.dumps(outcome)
    (pod_dir / f"outcome-{attempt}.json").write_text(text)


def _fail_get_job(state_dir: Path) -> bool:
    left = int(os.environ.get("FAKE_KUBECTL_GET_JOB_FAILS", "0"))
    counter = state_dir / "get-job-fails"
    done = int(counter.read_text()) if counter.exists() else 0
    if done >= left:
        return False
    counter.write_text(str(done + 1))
    return True


def _job_status(state_dir: Path, name: str) -> int:
    if _fail_get_job(state_dir):
        print("Unable to connect to the server: dial tcp: i/o timeout", file=sys.stderr)
        return 1
    job = json.loads((state_dir / "job.json").read_text())
    if job["state"] == "none":
        print(f'Error from server (NotFound): jobs.batch "{name}" not found', file=sys.stderr)
        return 1
    status: dict[str, object] = {"active": 1}
    if job["state"] == "complete":
        if job["outcome"] is not None:
            _write_outcome(name, job["outcome"])
        status = {"conditions": [{"type": "Complete", "status": "True"}]}
    elif job["state"] == "failed":
        condition = {"type": "Failed", "status": "True", "reason": job["reason"], "message": "x"}
        status = {"conditions": [condition]}
    print(json.dumps({"kind": "Job", "metadata": {"name": name}, "status": status}))
    return 0


def _pod(state_dir: Path, name: str) -> int:
    image = (state_dir / "image").read_text()
    if image == "garbage":
        print("not json at all")
        return 0
    config = {"name": "config", "configMap": {"name": "robo-worker-config-h4sh"}}
    container = {"image": "ghcr.io/phaedrus1992/robo-worker:main"}
    spec = {"containers": [container], "volumes": [config]}
    status = {"containerStatuses": [{"imageID": image}]}
    print(json.dumps({"kind": "Pod", "metadata": {"name": name}, "spec": spec, "status": status}))
    return 0


def main() -> int:
    state_dir = Path(os.environ["FAKE_KUBECTL_DIR"])
    stdin = "" if sys.stdin.isatty() else sys.stdin.read()
    _record(state_dir, sys.argv[1:], stdin)
    argv = _strip_globals(sys.argv[1:])
    if argv[:1] == ["create"]:
        return _create(state_dir, stdin)
    if argv[:2] == ["get", "pod"]:
        return _pod(state_dir, argv[2])
    if argv[:2] == ["get", "job"]:
        return _job_status(state_dir, argv[2])
    if argv[:2] == ["delete", "job"]:
        _set_job(state_dir, state="none")
        return 0
    print(f"fake kubectl: unexpected arguments {sys.argv[1:]}", file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main())
