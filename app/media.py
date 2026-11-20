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
    try:
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
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
