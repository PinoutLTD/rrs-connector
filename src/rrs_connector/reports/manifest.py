"""The connector → admin layer contract.

Every processed report gets a `manifest.json` written atomically as the last
step, so its presence in a report directory means "this report is complete and
may be read". The admin layer (Odoo helpdesk) polls report directories, reads
manifests, and never touches the connector's SQLite state.
"""

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from rrs_connector.reports.permissions import PRIVATE

# Bumped only on incompatible changes; readers must reject unknown versions.
CONTRACT_VERSION = 1
MANIFEST_FILE_NAME = "manifest.json"
# The name rrs-ha-integration gives the issue that triggered the report.
ISSUE_FILE_NAME = "issue_description.json"


@dataclass(frozen=True)
class ManifestFile:
    """One decrypted file, described relative to the report directory."""

    name: str
    path: str
    size_bytes: int


def report_id(client_id: str, directory: Path) -> str:
    """Stable identifier of a report: its path under `reports/`."""

    return f"{client_id}/{directory.name}"


def build_manifest(
    *,
    client_id: str,
    sender_address: str,
    datalog_index: int,
    datalog_timestamp: datetime,
    cid: str,
    directory: Path,
    archive_path: Path,
    decrypted_dir: Path,
    files: list[ManifestFile],
    processed_at: datetime,
) -> dict:
    issue_file = next(
        (file.path for file in files if file.name == ISSUE_FILE_NAME), None
    )
    return {
        "contract_version": CONTRACT_VERSION,
        "report_id": report_id(client_id, directory),
        "client_id": client_id,
        "sender_address": sender_address,
        "datalog_index": datalog_index,
        "datalog_timestamp": datalog_timestamp.isoformat(),
        "cid": cid,
        "processed_at": processed_at.isoformat(),
        "archive": {
            "path": relative_path(archive_path, directory),
            "size_bytes": archive_path.stat().st_size,
        },
        "decrypted_dir": relative_path(decrypted_dir, directory),
        # None when the report carries logs only (a report sent without an
        # issue): the admin layer still has the logs, but no structured issue.
        "issue_file": issue_file,
        "files": [
            {"name": file.name, "path": file.path, "size_bytes": file.size_bytes}
            for file in files
        ],
    }


def relative_path(path: Path, directory: Path) -> str:
    return path.relative_to(directory).as_posix()


def describe_files(files, directory: Path) -> list[ManifestFile]:
    """Describe decrypted files relative to the report directory."""

    return sorted(
        (
            ManifestFile(file.name, relative_path(file.path, directory), file.size)
            for file in files
        ),
        key=lambda file: file.name,
    )


def write_manifest(
    directory: Path, manifest: dict, file_mode: int = PRIVATE.file_mode
) -> Path:
    """Write `manifest.json` atomically; a partial file is never visible."""

    path = directory / MANIFEST_FILE_NAME
    staging_path = path.with_name(path.name + ".partial")
    staging_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    staging_path.chmod(file_mode)
    os.replace(staging_path, path)
    return path
