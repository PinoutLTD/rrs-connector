import stat

import pytest
import requests

from rrs_connector.reports import fetcher
from rrs_connector.reports.fetcher import (
    DownloadSettings,
    ReportDownloadError,
    ReportTooLargeError,
    download_report,
    gateway_url,
)

CID = "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"
PINATA = "https://gateway.pinata.cloud/"
IPFS_IO = "https://ipfs.io"


class FakeResponse:
    def __init__(self, status: int = 200, body: bytes = b"", headers=None) -> None:
        self.status_code = status
        self.body = body
        self.headers = headers or {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def iter_content(self, chunk_size: int):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start : start + chunk_size]


class FakeSession:
    def __init__(self, responses: dict[str, list]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, stream: bool, timeout: int):
        assert stream is True
        self.calls.append(url)
        outcome = self.responses[url].pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def settings(*gateways: str, attempts: int = 3) -> DownloadSettings:
    return DownloadSettings(
        gateways=list(gateways),
        timeout_seconds=10,
        max_attempts=attempts,
        backoff_seconds=2,
    )


def test_gateway_url_normalizes_trailing_slash() -> None:
    assert gateway_url(PINATA, CID) == f"https://gateway.pinata.cloud/ipfs/{CID}"
    assert gateway_url(IPFS_IO, CID) == f"https://ipfs.io/ipfs/{CID}"


def test_downloads_to_destination_with_private_mode(tmp_path) -> None:
    body = b"zip bytes" * 10_000
    session = FakeSession({gateway_url(PINATA, CID): [FakeResponse(body=body)]})
    destination = tmp_path / "archive.zip"

    size = download_report(
        CID, destination, settings(PINATA), session, sleep=lambda _: None
    )

    assert size == len(body)
    assert destination.read_bytes() == body
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert not (tmp_path / "archive.zip.partial").exists()


def test_retries_retryable_status_with_backoff(tmp_path) -> None:
    url = gateway_url(PINATA, CID)
    session = FakeSession(
        {
            url: [
                FakeResponse(503),
                requests.ConnectionError("reset"),
                FakeResponse(body=b"ok"),
            ]
        }
    )
    delays: list[float] = []

    download_report(
        CID, tmp_path / "a.zip", settings(PINATA), session, sleep=delays.append
    )

    assert delays == [2, 4]
    assert (tmp_path / "a.zip").read_bytes() == b"ok"


def test_non_retryable_status_moves_to_next_gateway(tmp_path) -> None:
    session = FakeSession(
        {
            gateway_url(PINATA, CID): [FakeResponse(404)],
            gateway_url(IPFS_IO, CID): [FakeResponse(body=b"ok")],
        }
    )
    delays: list[float] = []

    download_report(
        CID, tmp_path / "a.zip", settings(PINATA, IPFS_IO), session, sleep=delays.append
    )

    assert session.calls == [gateway_url(PINATA, CID), gateway_url(IPFS_IO, CID)]
    assert delays == []


def test_raises_when_every_gateway_fails(tmp_path) -> None:
    url = gateway_url(PINATA, CID)
    session = FakeSession({url: [FakeResponse(504), FakeResponse(504)]})
    destination = tmp_path / "a.zip"

    with pytest.raises(ReportDownloadError, match="cannot download"):
        download_report(
            CID,
            destination,
            settings(PINATA, attempts=2),
            session,
            sleep=lambda _: None,
        )

    assert not destination.exists()
    assert not (tmp_path / "a.zip.partial").exists()


def test_rejects_declared_oversize_archive(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(fetcher, "MAX_ARCHIVE_BYTES", 100)
    url = gateway_url(PINATA, CID)
    session = FakeSession(
        {url: [FakeResponse(body=b"x", headers={"Content-Length": "1000"})]}
    )

    with pytest.raises(ReportTooLargeError, match="declared size"):
        download_report(CID, tmp_path / "a.zip", settings(PINATA), session)


def test_rejects_streamed_oversize_archive_without_partial_file(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(fetcher, "MAX_ARCHIVE_BYTES", 100)
    monkeypatch.setattr(fetcher, "CHUNK_BYTES", 40)
    url = gateway_url(PINATA, CID)
    session = FakeSession({url: [FakeResponse(body=b"x" * 200, headers={})]})

    with pytest.raises(ReportTooLargeError, match="exceeds"):
        download_report(CID, tmp_path / "a.zip", settings(PINATA), session)

    assert list(tmp_path.iterdir()) == []
