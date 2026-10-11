import json
import os
import shlex
from pathlib import Path

import anyio
import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.env import (
    EnvError,
    TagError,
    build_worker_env,
    check_bundles,
    check_claude_settings,
    llmenv_export,
    parse_export,
    validate_tags,
)

# Same shape as real `llmenv export` output: single quotes, a multi-line value, and the
# '\'' escape for a quote inside a value.
EXPORT = """export CLAUDE_CONFIG_DIR='/home/w/.cache/llmenv/claude-code/3/abc'
export LLMENV_ACTIVE_BUNDLES='base,python-dev'
export LLMENV_ICM_CONTEXT='## llmenv context
Bundles: `base`

the tags'\\'' keywords'
export ANTHROPIC_BASE_URL='https://openrouter.ai/api'
"""


def test_parse_export_handles_quotes_and_newlines() -> None:
    env = parse_export(EXPORT)
    assert env["CLAUDE_CONFIG_DIR"] == "/home/w/.cache/llmenv/claude-code/3/abc"
    assert env["LLMENV_ICM_CONTEXT"].endswith("the tags' keywords")
    assert "\n" in env["LLMENV_ICM_CONTEXT"]


@pytest.mark.parametrize("text", ["export 'unterminated", "export 1BAD=x", "export NOEQUALS"])
def test_parse_export_refuses_bad_output(text: str) -> None:
    with pytest.raises(EnvError):
        parse_export(text)


def test_worker_env_is_an_allowlist() -> None:
    parent = {
        "HOME": "/home/w",
        "PATH": "/usr/bin",
        "LANG": "C.UTF-8",
        "GH_CONFIG_DIR": "/home/w/.config/gh",
        "SECRET_THING": "x",
        "ANTHROPIC_API_KEY": "sk-or-v1-leak",
        "CLAUDE_CODE_OAUTH_TOKEN": "inherited",
    }
    env = build_worker_env(
        parent=parent,
        llmenv_vars=parse_export(EXPORT),
        oauth_token="sk-ant-oat01-real",
        extra_env=["GH_CONFIG_DIR", "ANTHROPIC_AUTH_TOKEN"],
    )
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == "sk-ant-oat01-real"
    assert env["GH_CONFIG_DIR"] == "/home/w/.config/gh"
    assert "SECRET_THING" not in env
    assert not [name for name in env if name.startswith("ANTHROPIC_")]


@given(
    st.dictionaries(st.text(min_size=1, max_size=20), st.text(max_size=5)),
    st.dictionaries(st.text(min_size=1, max_size=20), st.text(max_size=5)),
    st.lists(st.text(min_size=1, max_size=20), max_size=5),
)
def test_no_blocked_name_ever_reaches_the_worker(
    parent: dict[str, str], llmenv_vars: dict[str, str], extra: list[str]
) -> None:
    env = build_worker_env(
        parent=parent, llmenv_vars=llmenv_vars, oauth_token="tok", extra_env=extra
    )
    assert not [n for n in env if n.startswith(("ANTHROPIC_", "CLAUDE_CODE_USE_"))]
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == "tok"


def test_check_bundles_refuses_denied() -> None:
    with pytest.raises(EnvError, match="ben-style"):
        check_bundles({"LLMENV_ACTIVE_BUNDLES": "base, ben-style"}, ["ben-style"])
    check_bundles({"LLMENV_ACTIVE_BUNDLES": "base"}, ["ben-style"])
    check_bundles({}, ["ben-style"])


def test_api_key_helper_is_refused(tmp_path: Path) -> None:
    (tmp_path / "settings.json").write_text(json.dumps({"apiKeyHelper": "/bin/key"}))
    with pytest.raises(EnvError, match="apiKeyHelper"):
        check_claude_settings({"CLAUDE_CONFIG_DIR": str(tmp_path)}, None)


def test_local_settings_are_checked_too(tmp_path: Path) -> None:
    (tmp_path / "settings.local.json").write_text(json.dumps({"apiKeyHelper": "/bin/key"}))
    with pytest.raises(EnvError, match=r"settings\.local\.json"):
        check_claude_settings({"CLAUDE_CONFIG_DIR": str(tmp_path)}, None)


def test_settings_env_map_with_a_blocked_name_is_refused(tmp_path: Path) -> None:
    env = {"ANTHROPIC_BASE_URL": "https://example.invalid", "FOO": "1"}
    (tmp_path / "settings.json").write_text(json.dumps({"env": env}))
    with pytest.raises(EnvError, match="ANTHROPIC_BASE_URL"):
        check_claude_settings({"CLAUDE_CONFIG_DIR": str(tmp_path)}, None)


def test_default_config_dir_is_home_dot_claude(tmp_path: Path) -> None:
    (tmp_path / ".claude").mkdir()
    (tmp_path / ".claude" / "settings.json").write_text(json.dumps({"apiKeyHelper": "x"}))
    with pytest.raises(EnvError, match="apiKeyHelper"):
        check_claude_settings({}, str(tmp_path))


def test_settings_without_helper_pass(tmp_path: Path) -> None:
    (tmp_path / "settings.json").write_text(json.dumps({"env": {"FOO": "1"}}))
    check_claude_settings({"CLAUDE_CONFIG_DIR": str(tmp_path)}, None)
    check_claude_settings({"CLAUDE_CONFIG_DIR": str(tmp_path / "missing")}, None)
    check_claude_settings({}, None)


def test_corrupt_settings_name_the_file(tmp_path: Path) -> None:
    (tmp_path / "settings.json").write_text("{bad")
    with pytest.raises(EnvError, match=r"settings\.json"):
        check_claude_settings({"CLAUDE_CONFIG_DIR": str(tmp_path)}, None)


def test_unreadable_settings_name_the_file(tmp_path: Path) -> None:
    (tmp_path / "settings.json").mkdir()
    with pytest.raises(EnvError, match=r"settings\.json"):
        check_claude_settings({"CLAUDE_CONFIG_DIR": str(tmp_path)}, None)


def test_validate_tags() -> None:
    assert validate_tags(["lang-python", "host:still", "a.b_c"]) == [
        "lang-python",
        "host:still",
        "a.b_c",
    ]


@pytest.mark.parametrize("tags", ["lang-python", [""], ["a,b"], ["a b"], ["x" * 65], [5]])
def test_validate_tags_refuses_bad_input(tags: object) -> None:
    with pytest.raises(TagError):
        validate_tags(tags)


@pytest.mark.anyio
async def test_llmenv_export_passes_tags(tmp_path: Path, fake_llmenv: Path) -> None:
    env = await llmenv_export(
        str(fake_llmenv), tmp_path, ["lang-python", "dev"], dict(os.environ), 20
    )
    assert env["LLMENV_ACTIVE_TAGS"] == "lang-python, dev"


@pytest.mark.anyio
async def test_llmenv_export_failure_names_the_tags(tmp_path: Path, fake_llmenv: Path) -> None:
    parent = {**os.environ, "FAKE_LLMENV_EXIT": "3"}
    with pytest.raises(EnvError, match=r"lang-python.*exit 3.*failed on purpose"):
        await llmenv_export(str(fake_llmenv), tmp_path, ["lang-python"], parent, 20)


@pytest.mark.anyio
async def test_slow_llmenv_export_times_out(tmp_path: Path, fake_llmenv: Path) -> None:
    parent = {**os.environ, "FAKE_LLMENV_SLEEP": "30"}
    with anyio.fail_after(10), pytest.raises(EnvError, match="max_prepare_seconds"):
        await llmenv_export(str(fake_llmenv), tmp_path, ["lang-python"], parent, 1)


@pytest.mark.anyio
async def test_missing_llmenv_names_the_config_key(tmp_path: Path) -> None:
    with pytest.raises(EnvError, match=r"bins\.llmenv"):
        await llmenv_export(str(tmp_path / "none"), tmp_path, [], dict(os.environ), 20)


_ENV_NAMES = st.from_regex(r"[A-Za-z_][A-Za-z0-9_]{0,20}", fullmatch=True)
_VALUES = st.text(alphabet=st.characters(exclude_categories=("Cs",), exclude_characters="\x00"))


@given(st.dictionaries(_ENV_NAMES, _VALUES, max_size=5))
def test_parse_export_round_trips_quoted_values(env: dict[str, str]) -> None:
    text = "".join(f"export {name}={shlex.quote(value)}\n" for name, value in env.items())
    assert parse_export(text) == env


@given(st.lists(st.text(max_size=10), max_size=5))
def test_accepted_tags_survive_the_extra_tags_join(tags: list[str]) -> None:
    try:
        accepted = validate_tags(tags)
    except TagError:
        return
    if accepted:
        assert ", ".join(accepted).split(", ") == accepted
