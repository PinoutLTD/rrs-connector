from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rrs_connector.reports.retention import apply_retention
from rrs_connector.state.db import (
    create_db_engine,
    create_session_factory,
    initialize_database,
)
from rrs_connector.state.models import DatalogStatus
from rrs_connector.state.store import StateStore

ADDRESS = "4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN"
NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    engine = create_db_engine(tmp_path / "state.sqlite3")
    initialize_database(engine)
    store = StateStore(create_session_factory(engine))
    store.sync_senders(
        [
            type(
                "SenderConfig",
                (),
                {
                    "client_id": "qube-block-a-301",
                    "robonomics_address": ADDRESS,
                    "description": "",
                    "enabled": True,
                },
            )()
        ]
    )
    return store


def make_report(store: StateStore, data_dir: Path, index: int, age_days: float) -> Path:
    sender = store.get_sender_record_by_address(ADDRESS)
    timestamp = NOW - timedelta(days=age_days)
    store.add_datalog_entry(
        sender_id=sender.id,
        datalog_index=index,
        datalog_timestamp=timestamp,
        raw_payload="Qm" + "x" * 44,
        cid="Qm" + "x" * 44,
        status=DatalogStatus.PROCESSED,
    )
    entry = store.get_datalog_entry_record(sender.id, index, timestamp)

    report = data_dir / "reports" / "qube-block-a-301" / f"datalog_{index}_0"
    (report / "decrypted").mkdir(parents=True)
    (report / "archive.zip").write_bytes(b"archive")
    (report / "decrypted" / "home-assistant.log").write_bytes(b"log" * 100)
    (report / "manifest.json").write_text("{}")
    store.upsert_report_artifact(
        entry.id,
        archive_path=report / "archive.zip",
        decrypted_dir=report / "decrypted",
        meta_path=report / "manifest.json",
        processed_at=timestamp,
    )
    return report


def test_fresh_reports_are_left_alone(store, tmp_path: Path) -> None:
    report = make_report(store, tmp_path, 1, age_days=1)

    result = apply_retention(store, 7, 30, now=NOW)

    assert result.decrypted_removed == 0
    assert (report / "decrypted").exists()
    assert (report / "archive.zip").exists()


def test_decrypted_files_go_first(store, tmp_path: Path) -> None:
    report = make_report(store, tmp_path, 1, age_days=8)

    result = apply_retention(store, 7, 30, now=NOW)

    assert result.decrypted_removed == 1
    assert result.archives_removed == 0
    assert result.freed_bytes > 0
    # The encrypted archive and the manifest stay: the report is still known.
    assert not (report / "decrypted").exists()
    assert (report / "archive.zip").exists()
    assert (report / "manifest.json").exists()


def test_old_report_is_removed_whole(store, tmp_path: Path) -> None:
    report = make_report(store, tmp_path, 1, age_days=31)

    result = apply_retention(store, 7, 30, now=NOW)

    assert (result.decrypted_removed, result.archives_removed) == (1, 1)
    assert result.reports_removed == 1
    assert not report.exists()


def test_state_stops_pointing_at_deleted_files(store, tmp_path: Path) -> None:
    make_report(store, tmp_path, 1, age_days=31)

    apply_retention(store, 7, 30, now=NOW)

    (record,) = store.list_report_artifact_records()
    assert record.decrypted_dir is None
    assert record.archive_path is None
    assert record.meta_path is None


def test_zero_days_keeps_everything(store, tmp_path: Path) -> None:
    report = make_report(store, tmp_path, 1, age_days=400)

    result = apply_retention(store, 0, 0, now=NOW)

    assert result == type(result)()
    assert (report / "decrypted").exists()


def test_missing_files_do_not_stop_the_pass(store, tmp_path: Path) -> None:
    report = make_report(store, tmp_path, 1, age_days=31)
    # Someone removed the directory by hand between runs.
    import shutil

    shutil.rmtree(report)

    result = apply_retention(store, 7, 30, now=NOW)

    assert result.decrypted_removed == 1
    assert (record := store.list_report_artifact_records()[0]).archive_path is None
    assert record.decrypted_dir is None
