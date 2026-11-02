"""
CAI Application entry. Script = app/launch_app.py, Python 3.11 runtime, 2 vCPU / 8 GB.

Streamlit is a child process. Replacing the kernel with exec makes CAI treat the application
as stopped while the port is still down.
"""
import os
import subprocess
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        cwd = Path.cwd()
        return cwd if (cwd / "app" / "app.py").exists() else Path("/home/cdsw")


ROOT = _repo_root()
PORT = os.environ.get("CDSW_APP_PORT", "8090")
HOST = os.environ.get("EVX_APP_HOST", "127.0.0.1")
LOG = ROOT / "evx_app.log"


def _note(msg: str) -> None:
    print(msg, flush=True)
    try:
        with LOG.open("a") as fh:
            fh.write(msg + "\n")
    except OSError:
        pass


def _packages_ok() -> bool:
    """st.dataframe needs pyarrow; recent pyarrow releases require numpy 2.x, which this runtime lacks."""
    try:
        import impala.dbapi  # noqa: F401
        import pyarrow  # noqa: F401
        import streamlit  # noqa: F401
        return True
    except ImportError:
        return False


def _ensure_app_packages() -> None:
    """The runtime has no streamlit. A model build's cdsw-build.sh does not run for an application."""
    if _packages_ok():
        return
    _note("pip install streamlit, impyla and pyarrow<18 (matches runtime numpy 1.26)")
    code = subprocess.call(
        [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--only-binary=:all:",
         "--upgrade", "pyarrow>=14,<18", "pandas", "streamlit>=1.32", "impyla==0.21.0"],
        cwd=ROOT)
    _note(f"pip exit {code}")
    if code != 0 or not _packages_ok():
        raise SystemExit("pip install of the workbench packages failed")


_ensure_app_packages()

cmd = [sys.executable, "-m", "streamlit", "run", str(ROOT / "app" / "app.py"),
       "--server.port", PORT, "--server.address", HOST, "--server.headless", "true",
       "--server.enableCORS", "false", "--server.enableXsrfProtection", "false",
       "--browser.gatherUsageStats", "false", "--server.fileWatcherType", "none",
       "--server.runOnSave", "false"]
env = {**os.environ, "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS", "1"),
       "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS", "1"),
       "MKL_NUM_THREADS": os.environ.get("MKL_NUM_THREADS", "1")}
restarts = 0
while True:
    print(f"[launch_app] cwd={ROOT} restart={restarts}", flush=True)
    t0 = time.time()
    code = subprocess.call(cmd, cwd=ROOT, env=env)
    print(f"[launch_app] streamlit exited status={code} after {time.time() - t0:.0f}s", flush=True)
    restarts = 0 if time.time() - t0 > 300 else restarts + 1
    if restarts >= 5:
        raise SystemExit("streamlit keeps exiting at start")
    time.sleep(3)
