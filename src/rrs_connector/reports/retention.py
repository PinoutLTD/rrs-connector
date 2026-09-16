"""Deleting what has already been handed over.

Reports are not kept forever: an active site sends several a day, and the
decrypted files are plaintext logs from a client's home. Two ages, counted
from the moment the report was processed:

- the decrypted files go first, as soon as the admin layer has had time to
  read them and attach them to a ticket;
- the archive goes later. It is encrypted and can be downloaded from IPFS
  again while the client's pin lives, so it is the cheaper copy to keep.

When both are gone the report directory, manifest included, is removed: an
empty directory would only advertise a report whose files no longer exist.
"""

import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rrs_connector.state.store import StateStore

LOGGER = logging.getLogger(__name__)


@dataclass
class RetentionResult:
    decrypted_removed: int = 0
    archives_removed: int = 0
    reports_removed: int = 0
    freed_bytes: int = 0


def directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def remove_directory(path: Path) -> int:
    freed = directory_size(path)
    shutil.rmtree(path)
    return freed


def older_than(processed_at: datetime | None, days: int, now: datetime) -> bool:
    if processed_at is None or days <= 0:
        return False
    # SQLite returns naive datetimes; everything is stored in UTC.
    if processed_at.tzinfo is None:
        processed_at = processed_at.replace(tzinfo=UTC)
    return processed_at <= now - timedelta(days=days)


def apply_retention(
    store: StateStore,
    keep_decrypted_days: int,
    keep_archive_days: int,
    now: datetime | None = None,
) -> RetentionResult:
    """Delete artifacts past their age; `0` days keeps that kind forever."""

    result = RetentionResult()
    if keep_decrypted_days <= 0 and keep_archive_days <= 0:
        return result

    now = now or datetime.now(UTC)

    for record in store.list_report_artifact_records():
        decrypted_gone = record.decrypted_dir is None
        archive_gone = record.archive_path is None

        if record.decrypted_dir and older_than(
            record.processed_at, keep_decrypted_days, now
        ):
            decrypted_dir = Path(record.decrypted_dir)
            if decrypted_dir.exists():
                result.freed_bytes += remove_directory(decrypted_dir)
            store.clear_report_artifact_paths(record.datalog_entry_id, decrypted=True)
            result.decrypted_removed += 1
            decrypted_gone = True

        if record.archive_path and older_than(
            record.processed_at, keep_archive_days, now
        ):
            archive_path = Path(record.archive_path)
            if archive_path.exists():
                result.freed_bytes += archive_path.stat().st_size
                archive_path.unlink()
            store.clear_report_artifact_paths(record.datalog_entry_id, archive=True)
            result.archives_removed += 1
            archive_gone = True

        if decrypted_gone and archive_gone and record.meta_path:
            report_dir = Path(record.meta_path).parent
            if report_dir.exists():
                result.freed_bytes += remove_directory(report_dir)
            store.clear_report_artifact_paths(record.datalog_entry_id, meta=True)
            result.reports_removed += 1

    if result.decrypted_removed or result.archives_removed or result.reports_removed:
        LOGGER.info(
            "Retention: removed decrypted files of %d report(s), %d archive(s), "
            "%d report directory(ies), freeing %.1f MiB",
            result.decrypted_removed,
            result.archives_removed,
            result.reports_removed,
            result.freed_bytes / (1024 * 1024),
        )
    return result
