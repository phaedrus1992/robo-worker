import re
import tomllib
from datetime import date
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from robo_worker.config import ConfigError, check_clone_template, load_config

EXAMPLE = Path(__file__).parent.parent / "config.example.toml"
DEPLOYED = Path(__file__).parent.parent / "deploy" / "k8s" / "config.toml"


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(text)
    return path


def _example_with(tmp_path: Path, old: str, new: str) -> Path:
    text = EXAMPLE.read_text()
    assert old in text, old
    return _write(tmp_path, text.replace(old, new, 1))


def test_example_config_loads() -> None:
    cfg = load_config(EXAMPLE)
    assert cfg.listen_port == 8765
    assert cfg.public_url == "http://robo-worker.phaedrus.svc:8765"
    assert cfg.oauth_token_created == date(2026, 10, 12)
    assert cfg.deny_output_styles == ("ben",)
    assert cfg.workspace_root == Path("/work")
    assert cfg.job_template == Path("/etc/robo-worker/config/job-pod.json")
    assert cfg.kubectl_bin == "kubectl"
    assert cfg.k8s_namespace == "phaedrus"


def test_deployed_config_loads() -> None:
    # The deployed config and the example must keep the same keys.
    cfg = load_config(DEPLOYED)
    assert cfg.k8s_namespace == "phaedrus"
    assert cfg.workspace_root == Path("/work")


def test_public_url_trailing_slash_is_removed(tmp_path: Path) -> None:
    path = _example_with(tmp_path, ':8765"\n# Host', ':8765/"\n# Host')
    assert load_config(path).public_url == "http://robo-worker.phaedrus.svc:8765"


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("[limits]", "[limitz]", "missing [limits] table"),
        ("max_job_seconds = 14400\n", "", "missing limits.max_job_seconds"),
        ("max_prepare_seconds = 600", "max_prepare_seconds = 0", "greater than 0"),
        ("max_job_seconds = 14400", "max_job_seconds = 0", "greater than 0"),
        ("max_job_seconds = 14400", 'max_job_seconds = "1"', "must be int"),
        ("max_job_seconds = 14400", "max_job_seconds = true", "must be int"),
        ("listen_port = 8765", "listen_port = 70000", "between 1 and 65535"),
        ('"/work"', '"work"', "absolute path"),
        ('"/etc/robo-worker/config/job-pod.json"', '"job-pod.json"', "absolute path"),
        ('namespace = "phaedrus"', 'namespace = ""', "k8s.namespace must not be empty"),
        ("[k8s]", "[k8z]", "missing [k8s] table"),
        ('kubectl = "kubectl"\n', "", "missing bins.kubectl"),
        ("deny_tags = []", 'deny_tags = [""]', "non-empty strings"),
        ("{owner}/{name}.git", "{owner}.git", "must contain"),
        ("{owner}/{name}.git", "{owner}/{name}/{x}.git", "only {owner} and {name}"),
        (
            "oauth_token_created = 2026-10-12",
            "oauth_token_created = 2026-10-12T09:00:00",
            "a date without a time",
        ),
    ],
)
def test_bad_values_name_the_key(tmp_path: Path, old: str, new: str, message: str) -> None:
    with pytest.raises(ConfigError, match=re.escape(message)):
        load_config(_example_with(tmp_path, old, new))


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read config"):
        load_config(tmp_path / "nope.toml")


def test_invalid_toml(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not valid TOML"):
        load_config(_write(tmp_path, "[server"))


@pytest.mark.parametrize(
    "template",
    ["{owner.real}/{name}", "{owner[0]}/{name}", "{owner!r}/{name}", "{owner:>9}/{name}", "{"],
)
def test_clone_template_refuses_field_tricks(template: str) -> None:
    with pytest.raises(ConfigError, match="clone_url_template"):
        check_clone_template(template)


@given(st.text(max_size=40))
def test_clone_template_returns_or_raises_config_error(template: str) -> None:
    try:
        checked = check_clone_template(template)
    except ConfigError:
        return
    assert checked.format(owner="o", name="n")


def _keys(path: Path) -> set[str]:
    data = tomllib.loads(path.read_text())
    return {f"{section}.{key}" for section, table in data.items() for key in table}


def test_deployed_config_has_the_same_keys_as_the_example() -> None:
    # kustomize reads files only from deploy/k8s, so the deployed config is a copy.
    assert _keys(DEPLOYED) == _keys(EXAMPLE)
