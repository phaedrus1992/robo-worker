import robo_worker


def test_version_is_set() -> None:
    assert robo_worker.__version__ == "0.1.0"
