import hashlib
import logging
import shutil
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rrs_connector.config import EnvSettings, NetworkConfig, SenderRegistryConfig
from rrs_connector.pipeline import (
    NOT_A_REPORT_MESSAGE,
    datetime_to_ms,
    extract_report_cid,
    ms_to_datetime,
    report_dir,
    run_once,
)
from rrs_connector.proton_pass import SecretUnavailableError
from rrs_connector.reports.fetcher import ReportDownloadError, ReportTooLargeError
from rrs_connector.robonomics.datalog_reader import DatalogRecord, DatalogScan
from rrs_connector.state.db import create_db_engine, create_session_factory
from rrs_connector.state.models import DatalogStatus
from rrs_connector.state.store import StateStore

ADDRESS_1 = "4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN"
ADDRESS_2 = "4Ff5w7XuzrfnuMT25GYijtu3w2DoRbjNSBgym4TRgPSh279f"

CID_1 = "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"
CID_2 = "QmUqNnzdZnic61UYTuKT9EzBNzMW6jc5uHSFk4Xzd3iM93"
CID_3 = "QmZK64M7M31mkMsDd8yQa1dfX4a4KeDCyaUMsTuzsKq6LC"

TIMESTAMP_1 = 1780065423000
TIMESTAMP_2 = 1780072623000
TIMESTAMP_3 = 1780079823000


class FakeReader:
    """Serves in-memory records with the reader's cursor semantics."""

    def __init__(self) -> None:
        self.records: dict[str, list[DatalogRecord]] = {}
        self.failing_addresses: set[str] = set()
        self.gap_addresses: set[str] = set()

    def publish(
        self, address: str, index: int, timestamp_ms: int, payload: str
    ) -> None:
        self.records.setdefault(address, []).append(
            DatalogRecord(address, index, timestamp_ms, payload)
        )

    def list_new_records(
        self, sender_address: str, cursor_timestamp_ms: int | None
    ) -> DatalogScan:
        if sender_address in self.failing_addresses:
            raise ConnectionError("node is unavailable")
        records = self.records.get(sender_address, [])
        if cursor_timestamp_ms is None:
            return DatalogScan(records[-1:], reached_cursor=True)
        newer = [r for r in records if r.timestamp_ms >= cursor_timestamp_ms]
        return DatalogScan(
            newer, reached_cursor=sender_address not in self.gap_addresses
        )


class ArchiveDownloader:
    """Serves the golden HA archive, optionally failing the first calls."""

    def __init__(self, archive: Path, failures: int = 0, error=None) -> None:
        self.archive = archive
        self.failures = failures
        self.error = error or ReportDownloadError("gateway timeout")
        self.calls: list[str] = []

    def __call__(self, cid: str, destination: Path, settings) -> int:
        self.calls.append(cid)
        if self.failures:
            self.failures -= 1
            raise self.error
        shutil.copyfile(self.archive, destination)
        return destination.stat().st_size


def unavailable_download(cid: str, destination: Path, settings) -> int:
    raise ReportDownloadError("gateways are unreachable in collection tests")


def registry_for(*senders: tuple[str, str]) -> SenderRegistryConfig:
    return SenderRegistryConfig.model_validate(
        {
            "senders": [
                {
                    "client_id": client_id,
                    "robonomics_address": address,
                    "description": client_id,
                    "enabled": True,
                }
                for client_id, address in senders
            ]
        }
    )


@pytest.fixture
def env_settings(tmp_path: Path, ha_report) -> EnvSettings:
    return EnvSettings(
        _env_file=None,
        integrator_address=ha_report["recipient_address"],
        data_dir=tmp_path / "data",
        state_db=tmp_path / "data" / "state.sqlite3",
        poll_interval_seconds=600,
        network_config_file=tmp_path / "network.yaml",
        senders_config_file=tmp_path / "senders.yaml",
    )


@pytest.fixture
def network_config() -> NetworkConfig:
    return NetworkConfig.model_validate(
        {
            "network": "polkadot",
            "wss": {
                "polkadot": ["wss://polkadot.rpc.robonomics.network/"],
                "kusama": ["wss://kusama.rpc.robonomics.network/"],
            },
            "ipfs_gateways": ["https://gateway.pinata.cloud/"],
            "timeouts": {"datalog_request_seconds": 15, "ipfs_download_seconds": 60},
            "retries": {
                "datalog_request_max_attempts": 3,
                "ipfs_download_max_attempts": 3,
                "retry_backoff_seconds": 2,
            },
        }
    )


@pytest.fixture
def sender_registry() -> SenderRegistryConfig:
    return registry_for(("home-a", ADDRESS_1), ("home-b", ADDRESS_2))


@pytest.fixture
def ha_registry(sender_address) -> SenderRegistryConfig:
    return registry_for(("ha-home", sender_address))


@pytest.fixture
def reader() -> FakeReader:
    return FakeReader()


@pytest.fixture
def run(env_settings, network_config, sender_registry, reader, recipient_account):
    def run(registry=None, load_account=None, download=unavailable_download):
        return run_once(
            env_settings,
            network_config,
            registry or sender_registry,
            reader,
            load_account=load_account or (lambda: recipient_account),
            download=download,
        )

    return run


def open_store(env_settings: EnvSettings) -> StateStore:
    return StateStore(create_session_factory(create_db_engine(env_settings.state_db)))


def entries_for(store: StateStore, status: DatalogStatus) -> list[tuple[str, int]]:
    return [
        (entry.cid or entry.raw_payload or "", entry.datalog_index)
        for entry in store.list_datalog_entry_records_by_status(status)
    ]


def only_entry(store: StateStore):
    entries = [
        entry
        for status in DatalogStatus
        for entry in store.list_datalog_entry_records_by_status(status)
    ]
    assert len(entries) == 1
    return entries[0]


# Collection


def test_first_run_stores_only_latest_record_and_sets_cursor(
    run, env_settings, reader
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    reader.publish(ADDRESS_1, 1, TIMESTAMP_2, CID_2)

    result = run()

    store = open_store(env_settings)
    assert entries_for(store, DatalogStatus.NEW) == [(CID_2, 1)]
    assert result.new_events == 1
    assert result.processed == 2
    assert result.exit_code == 0
    sender = store.get_sender_record_by_address(ADDRESS_1)
    assert sender is not None
    assert datetime_to_ms(sender.last_scanned_datalog_timestamp) == TIMESTAMP_2
    assert sender.last_scanned_datalog_index == 1


def test_next_run_stores_records_after_cursor(run, env_settings, reader) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    run()
    reader.publish(ADDRESS_1, 1, TIMESTAMP_2, CID_2)
    reader.publish(ADDRESS_1, 2, TIMESTAMP_3, CID_3)

    result = run()

    store = open_store(env_settings)
    assert entries_for(store, DatalogStatus.NEW) == [(CID_1, 0), (CID_2, 1), (CID_3, 2)]
    assert result.new_events == 2
    assert result.known_events == 1
    sender = store.get_sender_record_by_address(ADDRESS_1)
    assert sender is not None
    assert datetime_to_ms(sender.last_scanned_datalog_timestamp) == TIMESTAMP_3


def test_rerun_without_new_records_changes_nothing(run, env_settings, reader) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    run()

    result = run()

    assert result.new_events == 0
    assert result.known_events == 1
    assert entries_for(open_store(env_settings), DatalogStatus.NEW) == [(CID_1, 0)]


def test_reused_ring_slot_is_a_new_event(run, env_settings, reader) -> None:
    reader.publish(ADDRESS_1, 5, TIMESTAMP_1, CID_1)
    run()
    reader.publish(ADDRESS_1, 5, TIMESTAMP_2, CID_2)

    result = run()

    assert result.new_events == 1
    assert entries_for(open_store(env_settings), DatalogStatus.NEW) == [
        (CID_1, 5),
        (CID_2, 5),
    ]


def test_non_cid_payload_is_stored_as_ignored(run, env_settings, reader) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, '{"archive": "not a plain cid"}')

    result = run()

    store = open_store(env_settings)
    ignored = store.list_datalog_entry_records_by_status(DatalogStatus.IGNORED)
    assert result.ignored_events == 1
    assert len(ignored) == 1
    assert ignored[0].raw_payload == '{"archive": "not a plain cid"}'
    assert ignored[0].cid is None
    assert ignored[0].error_message == NOT_A_REPORT_MESSAGE


def test_failing_sender_does_not_block_others_or_move_its_cursor(
    run, env_settings, reader
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    reader.publish(ADDRESS_2, 0, TIMESTAMP_2, CID_2)
    reader.failing_addresses.add(ADDRESS_1)

    result = run()

    store = open_store(env_settings)
    assert result.failed == 1
    assert result.processed == 1
    assert result.exit_code == 3
    assert entries_for(store, DatalogStatus.NEW) == [(CID_2, 0)]
    failing_sender = store.get_sender_record_by_address(ADDRESS_1)
    assert failing_sender is not None
    assert failing_sender.last_scanned_datalog_timestamp is None


def test_overwritten_cursor_is_reported_as_gap(run, reader, caplog) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    run()
    reader.publish(ADDRESS_1, 1, TIMESTAMP_2, CID_2)
    reader.gap_addresses.add(ADDRESS_1)

    with caplog.at_level(logging.WARNING):
        result = run()

    assert result.senders_with_gaps == 1
    assert "no longer holds records up to the cursor" in caplog.text


# Report processing


def test_report_is_downloaded_and_decrypted(
    run, env_settings, reader, ha_registry, ha_report, ha_report_archive, sender_address
) -> None:
    reader.publish(sender_address, 0, TIMESTAMP_1, CID_1)

    result = run(registry=ha_registry, download=ArchiveDownloader(ha_report_archive))

    assert result.reports_processed == 1
    assert result.exit_code == 0
    store = open_store(env_settings)
    entry = only_entry(store)
    assert entry.status == DatalogStatus.PROCESSED
    assert entry.error_message is None

    artifact = store.get_report_artifact_record(entry.id)
    assert artifact is not None
    expected_dir = (
        env_settings.data_dir / "reports" / "ha-home" / f"datalog_0_{TIMESTAMP_1}"
    )
    assert artifact.archive_path == str(expected_dir / "archive.zip")
    assert artifact.decrypted_dir == str(expected_dir / "decrypted")
    assert artifact.processed_at is not None
    for name, expected in ha_report["files"].items():
        data = (expected_dir / "decrypted" / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == expected["sha256"]
    reports_root = env_settings.data_dir / "reports"
    assert stat.S_IMODE(reports_root.stat().st_mode) == 0o700


def test_processed_report_is_not_downloaded_again(
    run, reader, ha_registry, ha_report_archive, sender_address
) -> None:
    reader.publish(sender_address, 0, TIMESTAMP_1, CID_1)
    downloader = ArchiveDownloader(ha_report_archive)

    run(registry=ha_registry, download=downloader)
    result = run(registry=ha_registry, download=downloader)

    assert downloader.calls == [CID_1]
    assert result.reports_processed == 0


def test_download_failure_keeps_report_pending_for_next_run(
    run, env_settings, reader, ha_registry, ha_report_archive, sender_address
) -> None:
    reader.publish(sender_address, 0, TIMESTAMP_1, CID_1)
    downloader = ArchiveDownloader(ha_report_archive, failures=1)

    first = run(registry=ha_registry, download=downloader)

    entry = only_entry(open_store(env_settings))
    assert first.reports_pending == 1
    assert first.exit_code == 0
    assert entry.status == DatalogStatus.NEW
    assert entry.error_message.startswith("download: gateway timeout")

    second = run(registry=ha_registry, download=downloader)

    assert second.reports_processed == 1
    assert only_entry(open_store(env_settings)).status == DatalogStatus.PROCESSED


def test_too_large_archive_is_a_permanent_failure(
    run, env_settings, reader, ha_registry, ha_report_archive, sender_address
) -> None:
    reader.publish(sender_address, 0, TIMESTAMP_1, CID_1)
    downloader = ArchiveDownloader(
        ha_report_archive, failures=1, error=ReportTooLargeError("too large")
    )

    result = run(registry=ha_registry, download=downloader)
    run(registry=ha_registry, download=downloader)

    entry = only_entry(open_store(env_settings))
    assert result.reports_failed == 1
    assert entry.status == DatalogStatus.FAILED
    assert entry.error_message == "download: too large"
    assert downloader.calls == [CID_1]


def test_decryption_failure_marks_report_failed_without_retry(
    run, env_settings, reader, ha_report_archive
) -> None:
    # ADDRESS_1 is not the key that encrypted the golden archive.
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    downloader = ArchiveDownloader(ha_report_archive)

    result = run(download=downloader)
    run(download=downloader)

    entry = only_entry(open_store(env_settings))
    assert result.reports_failed == 1
    assert result.exit_code == 0
    assert entry.status == DatalogStatus.FAILED
    assert entry.error_message.startswith("decrypt: ")
    assert downloader.calls == [CID_1]


def test_unavailable_integrator_key_leaves_reports_pending(
    run, env_settings, reader, ha_registry, ha_report_archive, sender_address
) -> None:
    reader.publish(sender_address, 0, TIMESTAMP_1, CID_1)
    downloader = ArchiveDownloader(ha_report_archive)

    def missing_key():
        raise SecretUnavailableError("pass-cli session expired")

    result = run(registry=ha_registry, load_account=missing_key, download=downloader)

    assert result.integrator_key_unavailable is True
    assert result.reports_pending == 1
    assert result.exit_code == 3
    assert only_entry(open_store(env_settings)).status == DatalogStatus.NEW
    assert downloader.calls == []


def test_integrator_key_is_not_loaded_without_pending_reports(run) -> None:
    loads: list[bool] = []

    result = run(load_account=lambda: loads.append(True))

    assert loads == []
    assert result.exit_code == 0


def test_interrupted_report_resumes_from_downloaded_archive(
    run, env_settings, reader, ha_registry, ha_report_archive, sender_address
) -> None:
    reader.publish(sender_address, 0, TIMESTAMP_1, CID_1)
    run(registry=ha_registry)
    store = open_store(env_settings)
    entry = only_entry(store)
    sender = store.get_sender_record_by_id(entry.sender_id)
    target = report_dir(env_settings.data_dir, sender, entry)
    target.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ha_report_archive, target / "archive.zip")
    store.mark_datalog_entry_status(entry.id, DatalogStatus.DECRYPTING)
    downloader = ArchiveDownloader(ha_report_archive)

    result = run(registry=ha_registry, download=downloader)

    assert downloader.calls == []
    assert result.reports_processed == 1
    assert only_entry(open_store(env_settings)).status == DatalogStatus.PROCESSED


# Helpers


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (CID_1, CID_1),
        (f"  {CID_1}\n", CID_1),
        (
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
        ),
        ("bafyshort", None),
        ("Qm-not-a-cid", None),
        ('{"archive": "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"}', None),
        ("", None),
    ],
)
def test_extract_report_cid(payload: str, expected: str | None) -> None:
    assert extract_report_cid(payload) == expected


def test_timestamp_conversion_round_trip() -> None:
    value = ms_to_datetime(TIMESTAMP_1)

    assert value.tzinfo == UTC
    assert datetime_to_ms(value) == TIMESTAMP_1
    assert datetime_to_ms(value.replace(tzinfo=None)) == TIMESTAMP_1
    assert datetime_to_ms(None) is None
    assert ms_to_datetime(0) == datetime(1970, 1, 1, tzinfo=UTC)
