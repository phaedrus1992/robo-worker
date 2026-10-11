"""Parse and check GitHub repo names of the form owner/name."""

import re
from dataclasses import dataclass

# Shape only: GitHub owner and repo character sets and lengths. Meaning checks follow in code.
_SHAPE = re.compile(r"([A-Za-z0-9-]{1,39})/([A-Za-z0-9._-]{1,100})")


class RepoNameError(ValueError):
    """The text is not a usable GitHub owner/name."""


@dataclass(frozen=True)
class RepoName:
    owner: str
    name: str

    @property
    def slug(self) -> str:
        return f"{self.owner}/{self.name}"


def parse_repo(raw: object) -> RepoName:
    """Return the owner and name in ``raw``, or raise RepoNameError with the reason."""
    if not isinstance(raw, str):
        raise RepoNameError(f"repo must be a string like 'owner/name', got {type(raw).__name__}")
    match = _SHAPE.fullmatch(raw)
    if match is None:
        raise RepoNameError(f"repo {raw!r} is not 'owner/name'. Give the GitHub owner and name.")
    owner, name = match.groups()
    if owner.startswith("-") or owner.endswith("-"):
        raise RepoNameError(f"repo owner {owner!r} cannot start or end with '-'")
    if name in {".", ".."}:
        raise RepoNameError(f"repo name {name!r} is not a repo")
    if name.endswith(".git"):
        raise RepoNameError(f"repo name {name!r} ends with '.git'. Give the name without it.")
    return RepoName(owner, name)
