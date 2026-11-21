"""Fetch unstructured object bytes for the vision workbench. Structured paths come from Impala."""
from __future__ import annotations

import io
import logging
import os
import re
from pathlib import Path

S3A = re.compile(r"^s3a?://([^/]+)/(.+)$")
LOG = logging.getLogger("evx.media")


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path(os.getcwd())


def landing_path(image_ref: str, landing_uri: str) -> Path:
    """image_ref is qc_xray/2026-11-05/CELL.png under the landing prefix."""
    base = landing_uri.replace("s3a://", "").replace("s3://", "")
    if base.startswith("/"):
        base = base.lstrip("/")
    # local laptop: config overlay uses data/landing
    if not landing_uri.startswith("s3"):
        return Path(landing_uri) / image_ref
    return Path("/") / base / image_ref  # not used for local read


def read_local(root: Path, image_ref: str) -> bytes | None:
    path = root / "data" / "landing" / image_ref
    if path.is_file():
        return path.read_bytes()
    path = root / "data" / "generated" / image_ref
    return path.read_bytes() if path.is_file() else None


_LAST_FAILURE: str | None = None


def last_failure() -> str | None:
    """Stage, exception class and ClientError code of the last read_s3 failure, for the page; no message text."""
    return _LAST_FAILURE


def _log_failure(stage: str, exc: Exception) -> None:
    """Exception class and the first line of its message only: never the client config or the environment."""
    global _LAST_FAILURE
    resp = getattr(exc, "response", None)
    code = resp.get("Error", {}).get("Code") if isinstance(resp, dict) else None
    msg = (str(exc).splitlines() or [""])[0][:160]
    _LAST_FAILURE = f"{stage}: {type(exc).__name__}{f' [{code}]' if code else ''}"
    LOG.warning("read_s3 %s failed: %s%s: %s", stage, type(exc).__name__, f" [{code}]" if code else "", msg)


STATIC_KEY_PROVIDERS = ("env", "shared-credentials-file", "custom-process", "sso")


def credential_session():
    """botocore session without the static-key providers, so the workspace identity (web identity, container or
    instance role) is used instead of a stray key that S3 rejects."""
    import botocore.session

    core = botocore.session.get_session()
    resolver = core.get_component("credential_provider")
    for method in STATIC_KEY_PROVIDERS:
        resolver.remove(method)
    return core


def _s3_client(boto3):
    if not hasattr(boto3, "Session"):
        return boto3.client("s3")
    return boto3.Session(botocore_session=credential_session()).client("s3")


class IdBrokerHTTPError(Exception):
    def __init__(self, status: int, stage: str = "idbroker"):
        super().__init__(f"HTTP {status}")
        self.status, self.stage = status, stage


_VENDED: dict = {"creds": None, "until": 0.0}


def _idbroker_url(name: str = "idbroker_url") -> str:
    import json

    return json.loads((_repo_root() / "config" / "lakehouse.json").read_text())[name]


def _expiry(value) -> float:
    """Epoch seconds from the IDBroker Expiration (epoch ms, epoch s or ISO 8601); 10 minutes when absent."""
    import time
    from datetime import datetime

    if value in (None, ""):
        return time.time() + 600
    if isinstance(value, (int, float)) or str(value).isdigit():
        v = float(value)
        return (v / 1000 if v > 1e11 else v) - 60
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() - 60


def vended_credentials(user: str, password: str) -> dict:
    """Short-lived S3 credentials from the data lake IDBroker (RAZ-mapped role) for the workload user, cached in
    memory until shortly before they expire. aws-cab takes a Knox token, not basic auth: the token comes from the
    knoxtoken endpoint with the workload user's basic auth. Raises IdBrokerHTTPError on a non-200 answer."""
    import time

    import requests

    if _VENDED["creds"] and time.time() < _VENDED["until"]:
        return _VENDED["creds"]
    t = requests.get(_idbroker_url("idbroker_token_url"), auth=(user, password), timeout=20)
    token = t.json().get("access_token") if t.status_code == 200 else None
    if not token:
        raise IdBrokerHTTPError(t.status_code, "idbroker-token")
    r = requests.get(_idbroker_url(), headers={"Authorization": f"Bearer {token}"}, timeout=20)
    if r.status_code != 200:
        raise IdBrokerHTTPError(r.status_code)
    body = r.json()
    src = body.get("Credentials", body) if isinstance(body.get("Credentials"), dict) else body

    def pick(*names):
        return next((src[n] for n in names if src.get(n)), None)

    creds = {"aws_access_key_id": pick("accessKeyId", "AccessKeyId"),
             "aws_secret_access_key": pick("secretAccessKey", "SecretAccessKey"),
             "aws_session_token": pick("sessionToken", "SessionToken")}
    if not all(creds.values()):
        raise KeyError("credential field missing")
    _VENDED.update(creds=creds, until=_expiry(pick("expiration", "Expiration")))
    return creds


def _idbroker_failure(exc: Exception) -> None:
    """Status or exception class only: never the body, the user, the password or a credential field."""
    global _LAST_FAILURE
    _LAST_FAILURE = (f"{exc.stage}: HTTP {exc.status}" if isinstance(exc, IdBrokerHTTPError)
                     else f"idbroker: {type(exc).__name__}")
    LOG.warning("read_s3 %s", _LAST_FAILURE)


def read_s3(uri: str) -> bytes | None:
    global _LAST_FAILURE
    _LAST_FAILURE = None
    m = S3A.match(uri)
    if not m:
        return None
    try:
        import boto3
    except ImportError as e:
        _log_failure("import boto3", e)
        return None
    bucket, key = m.group(1), m.group(2)
    user, password = os.environ.get("EVX_WORKLOAD_USER"), os.environ.get("EVX_WORKLOAD_PASSWORD")
    if user and password:
        try:
            client = boto3.client("s3", **vended_credentials(user, password))
        except Exception as e:  # noqa: BLE001
            _idbroker_failure(e)
            return None
    else:
        client = _s3_client(boto3)
    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as e:  # noqa: BLE001
        _log_failure("get_object", e)
        return None


def load_object(image_ref: str, raw_path: str | None, landing_uri: str) -> bytes | None:
    """Prefer immutable raw_path from bronze; fall back to landing layout on disk."""
    root = _repo_root()
    if raw_path:
        if raw_path.startswith("s3"):
            data = read_s3(raw_path.replace("s3a://", "s3://"))
            if data:
                return data
        p = Path(raw_path)
        if p.is_file():
            return p.read_bytes()
    if image_ref:
        data = read_local(root, image_ref)
        if data:
            return data
        if landing_uri.startswith("s3"):
            data = read_s3(f"{landing_uri.replace('s3a://', 's3://')}/{image_ref}")
            if data:
                return data
    return None


def png_with_box(png_bytes: bytes, box: str | None):
    """Return a PIL Image with the extracted overhang rectangle drawn, or None if decode fails."""
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        return None
    img = Image.open(io.BytesIO(png_bytes)).convert("L")
    if box:
        parts = [int(float(x)) for x in box.split(",")]
        if len(parts) == 4:
            draw = ImageDraw.Draw(img)
            draw.rectangle(parts, outline=255, width=2)
    return img
