"""Fake llmenv binary for tests. It prints a recorded export, or fails."""

import os
import sys
import time
from pathlib import Path


def main() -> int:
    if sys.argv[1:] != ["export"]:
        print(f"fake llmenv: unexpected arguments {sys.argv[1:]}", file=sys.stderr)
        return 64
    code = int(os.environ.get("FAKE_LLMENV_EXIT", "0"))
    if code:
        print("fake llmenv: export failed on purpose", file=sys.stderr)
        return code
    time.sleep(float(os.environ.get("FAKE_LLMENV_SLEEP", "0")))
    output = os.environ.get("FAKE_LLMENV_OUTPUT")
    if output:
        sys.stdout.write(Path(output).read_text(encoding="utf-8"))
    tags = os.environ.get("LLMENV_EXTRA_TAGS", "")
    sys.stdout.write(f"export LLMENV_ACTIVE_TAGS='{tags}'\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
