import subprocess
from pathlib import Path

import pytest

from robo_worker.checkout import (
    CheckoutError,
    DirtyCheckout,
    checkout_path,
    is_dirty,
    prepare_checkout,
)
from robo_worker.repos import RepoName
from tests.conftest import make_origin

pytestmark = [pytest.mark.anyio, pytest.mark.usefixtures("isolated_git")]
REPO = RepoName("o", "r")


def _template(tmp_path: Path) -> str:
    return f"file://{tmp_path}/origin/{{owner}}/{{name}}"


async def test_clones_a_missing_checkout(tmp_path: Path) -> None:
    make_origin(tmp_path)
    path = await prepare_checkout("git", tmp_path / "work", REPO, _template(tmp_path))
    assert path == checkout_path(tmp_path / "work", REPO) == tmp_path / "work" / "o" / "r"
    assert (path / "README.md").read_text() == "hi\n"
    assert not await is_dirty("git", path)


async def test_fetches_an_existing_checkout(tmp_path: Path) -> None:
    origin = make_origin(tmp_path)
    path = await prepare_checkout("git", tmp_path / "work", REPO, _template(tmp_path))
    (origin / "NEW.md").write_text("new\n")
    subprocess.run(["git", "-C", str(origin), "add", "NEW.md"], check=True)
    subprocess.run(["git", "-C", str(origin), "commit", "-q", "-m", "new"], check=True)
    await prepare_checkout("git", tmp_path / "work", REPO, _template(tmp_path))
    log = subprocess.run(
        ["git", "-C", str(path), "log", "--oneline", "origin/main"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert len(log.stdout.splitlines()) == 2


async def test_refuses_a_dirty_checkout(tmp_path: Path) -> None:
    make_origin(tmp_path)
    path = await prepare_checkout("git", tmp_path / "work", REPO, _template(tmp_path))
    (path / "scratch.txt").write_text("wip\n")
    with pytest.raises(DirtyCheckout, match=r"resume_queue\('o/r'\)"):
        await prepare_checkout("git", tmp_path / "work", REPO, _template(tmp_path))


async def test_clone_failure_names_the_command(tmp_path: Path) -> None:
    with pytest.raises(CheckoutError, match="clone"):
        await prepare_checkout("git", tmp_path / "work", REPO, _template(tmp_path))
