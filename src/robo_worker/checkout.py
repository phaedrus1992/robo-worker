"""Keep one persistent checkout per repo under the workspace root."""

from pathlib import Path

import anyio

from robo_worker.repos import RepoName

_STDERR_TAIL = 500


class CheckoutError(RuntimeError):
    """A git command for the checkout failed."""


class DirtyCheckout(CheckoutError):
    """The checkout has uncommitted changes, so a job must not start on it."""


async def _git(git_bin: str, args: list[str], cwd: Path | None = None) -> str:
    result = await anyio.run_process([git_bin, *args], cwd=cwd, check=False)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")[-_STDERR_TAIL:].strip()
        raise CheckoutError(f"git {' '.join(args)} failed (cwd {cwd}): {stderr}")
    return result.stdout.decode("utf-8", errors="replace")


def checkout_path(workspace_root: Path, repo: RepoName) -> Path:
    return workspace_root / repo.owner / repo.name


async def is_dirty(git_bin: str, path: Path) -> bool:
    return bool((await _git(git_bin, ["status", "--porcelain"], cwd=path)).strip())


async def prepare_checkout(
    git_bin: str, workspace_root: Path, repo: RepoName, clone_url_template: str
) -> Path:
    """Clone or fetch the checkout for ``repo`` and return its path, refusing a dirty tree."""
    path = checkout_path(workspace_root, repo)
    if (path / ".git").exists():
        await _git(git_bin, ["fetch", "--prune", "origin"], cwd=path)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        url = clone_url_template.format(owner=repo.owner, name=repo.name)
        await _git(git_bin, ["clone", "--quiet", "--", url, str(path)])
    if await is_dirty(git_bin, path):
        raise DirtyCheckout(
            f"{path} has uncommitted changes. Clean it, then call resume_queue('{repo.slug}')."
        )
    return path
