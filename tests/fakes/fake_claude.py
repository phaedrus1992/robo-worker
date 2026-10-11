"""Fake claude binary for tests. It replays stream-json fixtures.

FAKE_CLAUDE_FIXTURES holds fixture paths split by os.pathsep. Run N (counted from the lines
in FAKE_CLAUDE_RECORD) replays fixture N, or the last fixture when N is past the end.
FAKE_CLAUDE_SKIP_STDIN makes the fake sleep without reading its prompt.
"""

import json
import os
import signal
import sys
import time
from pathlib import Path


def _run_index(record: str | None) -> int:
    if not record or not os.path.exists(record):
        return 0
    with open(record, encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def main() -> int:
    if os.environ.get("FAKE_CLAUDE_IGNORE_TERM"):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if os.environ.get("FAKE_CLAUDE_SKIP_STDIN"):
        time.sleep(float(os.environ.get("FAKE_CLAUDE_SLEEP", "0")))
        return 0
    prompt = sys.stdin.read()
    record = os.environ.get("FAKE_CLAUDE_RECORD")
    index = _run_index(record)
    if record:
        entry = {
            "argv": sys.argv[1:],
            "stdin": prompt,
            "env": sorted(os.environ),
            "cwd": os.getcwd(),
        }
        with open(record, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")
    fixtures = os.environ["FAKE_CLAUDE_FIXTURES"].split(os.pathsep)
    sys.stdout.write(Path(fixtures[min(index, len(fixtures) - 1)]).read_text(encoding="utf-8"))
    sys.stdout.flush()
    touch = os.environ.get("FAKE_CLAUDE_TOUCH")
    if touch:
        Path(touch).write_text("dirty\n", encoding="utf-8")
    time.sleep(float(os.environ.get("FAKE_CLAUDE_SLEEP", "0")))
    return int(os.environ.get("FAKE_CLAUDE_EXIT", "0"))


if __name__ == "__main__":
    sys.exit(main())
