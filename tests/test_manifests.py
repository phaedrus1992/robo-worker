"""The rules the manifests must keep. A plain text check, so the tests need no YAML parser."""

import json
from pathlib import Path

import pytest

K8S = Path(__file__).parent.parent / "deploy" / "k8s"
MANIFESTS = [*sorted(K8S.glob("*.yaml")), K8S / "job-pod.json"]
_RESTRICTED_POD = {
    "runAsNonRoot": True,
    "runAsUser": 10001,
    "runAsGroup": 10001,
    "fsGroup": 10001,
    "fsGroupChangePolicy": "OnRootMismatch",
    "seccompProfile": {"type": "RuntimeDefault"},
}
_RESTRICTED_CONTAINER = {
    "allowPrivilegeEscalation": False,
    "readOnlyRootFilesystem": True,
    "capabilities": {"drop": ["ALL"]},
}


def _job_pod() -> dict:
    return json.loads((K8S / "job-pod.json").read_text())


@pytest.mark.parametrize("path", MANIFESTS, ids=lambda p: p.name)
@pytest.mark.parametrize(
    "word", ["hostPath", "hostNetwork", "hostPID", "privileged", "docker.sock"]
)
def test_no_host_access(path: Path, word: str) -> None:
    assert word not in path.read_text()


def test_a_job_pod_is_never_retried() -> None:
    # A retry repeats side effects: a push, a PR, a comment.
    job = _job_pod()
    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["template"]["spec"]["restartPolicy"] == "Never"


def test_the_job_pod_is_restricted() -> None:
    pod = _job_pod()["spec"]["template"]["spec"]
    assert pod["securityContext"] == _RESTRICTED_POD
    assert pod["containers"][0]["securityContext"] == _RESTRICTED_CONTAINER
    # The job pod talks only to the control plane's Service, never to the API server.
    assert pod["automountServiceAccountToken"] is False


def test_the_job_pod_mounts_no_shared_state_by_itself() -> None:
    # The launcher sets a subPath per job. A template used as is must not mount whole claims.
    pod = _job_pod()["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert "envFrom" not in container
    assert "env" not in container
    mounts = {m["name"]: m for m in container["volumeMounts"]}
    assert mounts["state"]["subPath"] == "set-by-the-launcher"
    assert mounts["work"]["subPath"] == "set-by-the-launcher"
    assert mounts["token"]["mountPath"] == "/run/robo-worker/token"
    # The job pod never mounts the callers file or the token Secret of the control plane.
    assert "secrets" not in {v["name"] for v in pod["volumes"]}


def test_the_job_pod_carries_no_image() -> None:
    # The launcher copies the image of its own pod, so there is one tag to change.
    assert "image" not in _job_pod()["spec"]["template"]["spec"]["containers"][0]


def test_the_control_plane_is_restricted() -> None:
    text = (K8S / "deployment.yaml").read_text()
    for line in (
        "runAsNonRoot: true",
        "runAsUser: 10001",
        "fsGroup: 10001",
        "fsGroupChangePolicy: OnRootMismatch",
        "type: RuntimeDefault",
        "allowPrivilegeEscalation: false",
        "readOnlyRootFilesystem: true",
        'drop: ["ALL"]',
        "maxSurge: 0",
        "replicas: 1",
    ):
        assert line in text, line


def test_the_role_is_namespaced_and_create_only() -> None:
    text = (K8S / "role.yaml").read_text()
    assert "kind: Role\n" in text
    assert "ClusterRole" not in text
    verbs = " ".join(line for line in text.splitlines() if "verbs:" in line)
    assert "patch" not in verbs
    assert "update" not in verbs
    assert "*" not in verbs
    # The launcher never reads a Secret, so the Role cannot either.
    secrets_rule = text[text.index('resources: ["secrets"]') :].splitlines()[1]
    assert "get" not in secrets_rule


def test_the_deployed_config_names_the_mounted_template() -> None:
    text = (K8S / "config.toml").read_text()
    assert 'job_template = "/etc/robo-worker/config/job-pod.json"' in text
    args = _job_pod()["spec"]["template"]["spec"]["containers"][0]["args"]
    assert args == ["job", "--config", "/etc/robo-worker/config/config.toml"]
