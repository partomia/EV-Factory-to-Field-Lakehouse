"""The short CAI runtime version is shared by every image in a release, including Scala."""
from ci.register_deploy import RUNTIME, choose_runtime

PINNED = RUNTIME
RELEASE = "2026.08.1-b5"


def test_short_version_does_not_select_the_last_image():
    candidates = [
        (RELEASE, PINNED),
        (RELEASE, "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-workbench-scala2.12-standard:" + RELEASE),
    ]
    assert choose_runtime(candidates, RELEASE, PINNED) == PINNED


def test_missing_pin_prefers_python311_standard():
    scala = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-workbench-scala2.12-standard:" + RELEASE
    cuda = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-cuda:" + RELEASE
    standard = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:" + RELEASE
    candidates = [(RELEASE, scala), (RELEASE, cuda), (RELEASE, standard)]
    assert choose_runtime(candidates, RELEASE, "not-listed") == standard


def test_no_short_version_keeps_the_pin():
    assert choose_runtime([], None, PINNED) == PINNED
