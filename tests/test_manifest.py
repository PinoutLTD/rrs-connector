import json
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rrs_connector.reports.decryptor import DecryptedFile
from rrs_connector.reports.manifest import (
    CONTRACT_VERSION,
    MANIFEST_FILE_NAME,
    build_manifest,
    describe_files,
    write_manifest,
)

CLIENT_ID = "qube-block-a-301"
SENDER_ADDRESS = "4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN"
CID = "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"
DATALOG_TIMESTAMP = datetime(2026, 9, 14, 8, 12, 31, tzinfo=UTC)
PROCESSED_AT = datetime(2026, 9, 14, 9, 0, 0, tzinfo=UTC)


@pytest.fixture
def report(tmp_path: Path) -> Path:
    directory = tmp_path / "reports" / CLIENT_ID / "datalog_94_1789114351000"
    (directory / "decrypted").mkdir(parents=True)
    (directory / "archive.zip").write_bytes(b"encrypted archive")
    return directory


def decrypted(directory: Path, names: dict[str, bytes]) -> list[DecryptedFile]:
    files = []
    for name, data in names.items():
        path = directory / "decrypted" / name
        path.write_bytes(data)
        files.append(DecryptedFile(name, path, len(data)))
    return files


def manifest_for(directory: Path, files: list[DecryptedFile]) -> dict:
    return build_manifest(
        client_id=CLIENT_ID,
        sender_address=SENDER_ADDRESS,
        datalog_index=94,
        datalog_timestamp=DATALOG_TIMESTAMP,
        cid=CID,
        directory=directory,
        archive_path=directory / "archive.zip",
        decrypted_dir=directory / "decrypted",
        files=describe_files(files, directory),
        processed_at=PROCESSED_AT,
    )


def test_manifest_describes_the_report(report: Path) -> None:
    files = decrypted(
        report,
        {
            "home-assistant.log": b"log line\n",
            "issue_description.json": b'{"type": "entities_health_problems"}',
        },
    )

    manifest = manifest_for(report, files)

    assert manifest == {
        "contract_version": CONTRACT_VERSION,
        "report_id": f"{CLIENT_ID}/datalog_94_1789114351000",
        "client_id": CLIENT_ID,
        "sender_address": SENDER_ADDRESS,
        "datalog_index": 94,
        "datalog_timestamp": "2026-09-14T08:12:31+00:00",
        "cid": CID,
        "processed_at": "2026-09-14T09:00:00+00:00",
        "archive": {"path": "archive.zip", "size_bytes": 17},
        "decrypted_dir": "decrypted",
        "issue_file": "decrypted/issue_description.json",
        "files": [
            {
                "name": "home-assistant.log",
                "path": "decrypted/home-assistant.log",
                "size_bytes": 9,
            },
            {
                "name": "issue_description.json",
                "path": "decrypted/issue_description.json",
                "size_bytes": 36,
            },
        ],
    }


def test_report_without_an_issue_file_has_no_issue(report: Path) -> None:
    files = decrypted(report, {"home-assistant.log": b"log line\n"})

    manifest = manifest_for(report, files)

    assert manifest["issue_file"] is None
    assert [file["name"] for file in manifest["files"]] == ["home-assistant.log"]


def test_written_manifest_is_private_json_without_leftovers(report: Path) -> None:
    manifest = manifest_for(report, decrypted(report, {"trace.saved_traces": b"{}"}))

    path = write_manifest(report, manifest)

    assert path == report / MANIFEST_FILE_NAME
    assert json.loads(path.read_text("utf-8")) == manifest
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert list(report.glob("*.partial")) == []


def test_rewriting_a_manifest_replaces_it(report: Path) -> None:
    write_manifest(report, manifest_for(report, decrypted(report, {"a.log": b"one"})))

    manifest = manifest_for(report, decrypted(report, {"b.log": b"two"}))
    path = write_manifest(report, manifest)

    assert json.loads(path.read_text("utf-8"))["files"] == manifest["files"]
