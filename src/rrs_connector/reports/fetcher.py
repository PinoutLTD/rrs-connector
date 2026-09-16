"""Report archive download from IPFS gateways."""

import logging
import time
import urllib.parse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

import requests

LOGGER = logging.getLogger(__name__)

# Report archives are a few MB; anything far larger is not a HA report.
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
CHUNK_BYTES = 64 * 1024
FILE_MODE = 0o600


class ReportDownloadError(RuntimeError):
    """The archive could not be downloaded now; a later run may succeed."""


class ReportTooLargeError(ReportDownloadError):
    """The content itself is too large; retrying will not help."""


@dataclass(frozen=True)
class DownloadSettings:
    gateways: Sequence[str]
    timeout_seconds: int
    max_attempts: int
    backoff_seconds: int


def gateway_url(gateway: str, cid: str) -> str:
    return urllib.parse.urljoin(gateway.rstrip("/") + "/", "ipfs/" + cid)


def download_report(
    cid: str,
    destination: Path,
    settings: DownloadSettings,
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Download `cid` to `destination`, trying each gateway in order.

    Each gateway gets `max_attempts` tries with exponential backoff for
    network errors and retryable HTTP statuses. The file is written to a
    temporary name and moved into place only when complete. Returns the size.
    """

    if session is None:
        with requests.Session() as own_session:
            return download_report(cid, destination, settings, own_session, sleep)

    http = session
    errors: list[str] = []

    for gateway in settings.gateways:
        url = gateway_url(gateway, cid)
        for attempt in range(1, settings.max_attempts + 1):
            try:
                return _stream_to_file(url, destination, settings.timeout_seconds, http)
            except requests.RequestException as e:
                status = e.response.status_code if e.response is not None else None
                retryable = status is None or status in RETRYABLE_HTTP_STATUSES
                errors.append(f"{url} attempt {attempt}: {e}")
                if not retryable or attempt == settings.max_attempts:
                    LOGGER.warning("Gateway %s failed for %s: %s", gateway, cid, e)
                    break
                delay = settings.backoff_seconds * 2 ** (attempt - 1)
                LOGGER.info(
                    "Download attempt %d/%d of %s failed (%s), retrying in %ds",
                    attempt,
                    settings.max_attempts,
                    url,
                    e,
                    delay,
                )
                sleep(delay)

    raise ReportDownloadError(
        f"cannot download {cid} from any gateway: {errors[-1] if errors else 'none'}"
    )


def _stream_to_file(
    url: str, destination: Path, timeout_seconds: int, http: requests.Session
) -> int:
    started = time.monotonic()
    partial = destination.with_name(destination.name + ".partial")
    received = 0
    try:
        with http.get(url, stream=True, timeout=timeout_seconds) as response:
            response.raise_for_status()
            declared = int(response.headers.get("Content-Length") or 0)
            if declared > MAX_ARCHIVE_BYTES:
                raise ReportTooLargeError(
                    f"{url}: declared size {declared} exceeds {MAX_ARCHIVE_BYTES}"
                )
            with open(partial, "wb") as file:
                partial.chmod(FILE_MODE)
                for chunk in response.iter_content(chunk_size=CHUNK_BYTES):
                    received += len(chunk)
                    if received > MAX_ARCHIVE_BYTES:
                        raise ReportTooLargeError(
                            f"{url}: archive exceeds {MAX_ARCHIVE_BYTES} bytes"
                        )
                    file.write(chunk)
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)

    LOGGER.info(
        "Downloaded %s (%d bytes) in %.1fs", url, received, time.monotonic() - started
    )
    return received
