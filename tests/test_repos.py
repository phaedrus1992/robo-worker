import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.repos import RepoName, RepoNameError, parse_repo


def test_parses_owner_and_name() -> None:
    repo = parse_repo("phaedrus1992/open-strix")
    assert repo == RepoName("phaedrus1992", "open-strix")
    assert repo.slug == "phaedrus1992/open-strix"


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "noslash",
        "a/b/c",
        "../x",
        "o/..",
        "o/.",
        "-o/r",
        "o-/r",
        "o/r.git",
        "o/r\n",
        "o /r",
        "o/r;rm",
        "a" * 40 + "/r",
        "o/" + "r" * 101,
    ],
)
def test_refuses_bad_names(raw: str) -> None:
    with pytest.raises(RepoNameError):
        parse_repo(raw)


def test_refuses_non_string() -> None:
    with pytest.raises(RepoNameError, match="must be a string"):
        parse_repo(42)


@given(st.text(max_size=80))
def test_any_text_parses_or_raises_repo_name_error(raw: str) -> None:
    try:
        repo = parse_repo(raw)
    except RepoNameError:
        return
    assert "/" not in repo.owner
    assert "/" not in repo.name
    assert repo.name not in {".", ".."}
