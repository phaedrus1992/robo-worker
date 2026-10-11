"""Keep one persistent checkout per repo under the workspace root."""

from pathlib import Path

import anyio

from robo_worker.proc_output import stderr_tail
from robo_worker.repos import RepoName


class CheckoutError(RuntimeError):
    """A git command for the checkout failed."""


class DirtyCheckout(CheckoutError):
    """The checkout has uncommitted changes, so a job must not start on it."""


async def _git(git_bin: str, args: list[str], timeout: float, cwd: Path | None = None) -> str:
    command = f"git {' '.join(args)}"
    try:
        with anyio.fail_after(timeout):
            result = await anyio.run_process([git_bin, *args], cwd=cwd, check=False)
    except TimeoutError:
        raise CheckoutError(
            f"{command} ran longer than max_prepare_seconds ({timeout}s) (cwd {cwd})"
        ) from None
    except OSError as exc:
        raise CheckoutError(f"cannot run {git_bin}: {exc}. Check the bins.git config key.") from exc
    if result.returncode != 0:
        raise CheckoutError(f"{command} failed (cwd {cwd}): {stderr_tail(result.stderr)}")
    return result.stdout.decode("utf-8", errors="replace")


def checkout_path(workspace_root: Path, repo: RepoName) -> Path:
    return workspace_root / repo.owner / repo.name


async def is_dirty(git_bin: str, path: Path, timeout: float) -> bool:
    return bool((await _git(git_bin, ["status", "--porcelain"], timeout, cwd=path)).strip())


async def prepare_checkout(
    git_bin: str, workspace_root: Path, repo: RepoName, clone_url_template: str, timeout: float
) -> Path:
    """Clone or fetch the checkout for ``repo`` and return its path, refusing a dirty tree."""
    path = checkout_path(workspace_root, repo)
    if (path / ".git").exists():
        await _git(git_bin, ["fetch", "--prune", "origin"], timeout, cwd=path)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        url = clone_url_template.format(owner=repo.owner, name=repo.name)
        await _git(git_bin, ["clone", "--quiet", "--", url, str(path)], timeout)
    if await is_dirty(git_bin, path, timeout):
        raise DirtyCheckout(
            f"{path} has uncommitted changes. Clean it, then call resume_queue('{repo.slug}')."
        )
    return path
