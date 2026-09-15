import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rrs_connector.config import EnvSettings, NetworkConfig, SenderRegistryConfig
from rrs_connector.pipeline import (
    NOT_A_REPORT_MESSAGE,
    datetime_to_ms,
    extract_report_cid,
    ms_to_datetime,
    run_once,
)
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


@pytest.fixture
def env_settings(tmp_path: Path) -> EnvSettings:
    return EnvSettings(
        _env_file=None,
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
    return SenderRegistryConfig.model_validate(
        {
            "senders": [
                {
                    "client_id": "home-a",
                    "robonomics_address": ADDRESS_1,
                    "description": "Home A",
                    "enabled": True,
                },
                {
                    "client_id": "home-b",
                    "robonomics_address": ADDRESS_2,
                    "description": "Home B",
                    "enabled": True,
                },
            ]
        }
    )


@pytest.fixture
def reader() -> FakeReader:
    return FakeReader()


def open_store(env_settings: EnvSettings) -> StateStore:
    return StateStore(create_session_factory(create_db_engine(env_settings.state_db)))


def entries_for(store: StateStore, status: DatalogStatus) -> list[tuple[str, int]]:
    return [
        (entry.cid or entry.raw_payload or "", entry.datalog_index)
        for entry in store.list_datalog_entry_records_by_status(status)
    ]


def test_first_run_stores_only_latest_record_and_sets_cursor(
    env_settings, network_config, sender_registry, reader
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    reader.publish(ADDRESS_1, 1, TIMESTAMP_2, CID_2)

    result = run_once(env_settings, network_config, sender_registry, reader)

    store = open_store(env_settings)
    assert entries_for(store, DatalogStatus.NEW) == [(CID_2, 1)]
    assert result.new_events == 1
    assert result.processed == 2
    assert result.exit_code == 0
    sender = store.get_sender_record_by_address(ADDRESS_1)
    assert sender is not None
    assert datetime_to_ms(sender.last_scanned_datalog_timestamp) == TIMESTAMP_2
    assert sender.last_scanned_datalog_index == 1


def test_next_run_stores_records_after_cursor(
    env_settings, network_config, sender_registry, reader
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    run_once(env_settings, network_config, sender_registry, reader)
    reader.publish(ADDRESS_1, 1, TIMESTAMP_2, CID_2)
    reader.publish(ADDRESS_1, 2, TIMESTAMP_3, CID_3)

    result = run_once(env_settings, network_config, sender_registry, reader)

    store = open_store(env_settings)
    assert entries_for(store, DatalogStatus.NEW) == [(CID_1, 0), (CID_2, 1), (CID_3, 2)]
    assert result.new_events == 2
    assert result.known_events == 1
    sender = store.get_sender_record_by_address(ADDRESS_1)
    assert sender is not None
    assert datetime_to_ms(sender.last_scanned_datalog_timestamp) == TIMESTAMP_3


def test_rerun_without_new_records_changes_nothing(
    env_settings, network_config, sender_registry, reader
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    run_once(env_settings, network_config, sender_registry, reader)

    result = run_once(env_settings, network_config, sender_registry, reader)

    assert result.new_events == 0
    assert result.known_events == 1
    assert entries_for(open_store(env_settings), DatalogStatus.NEW) == [(CID_1, 0)]


def test_reused_ring_slot_is_a_new_event(
    env_settings, network_config, sender_registry, reader
) -> None:
    reader.publish(ADDRESS_1, 5, TIMESTAMP_1, CID_1)
    run_once(env_settings, network_config, sender_registry, reader)
    reader.publish(ADDRESS_1, 5, TIMESTAMP_2, CID_2)

    result = run_once(env_settings, network_config, sender_registry, reader)

    assert result.new_events == 1
    assert entries_for(open_store(env_settings), DatalogStatus.NEW) == [
        (CID_1, 5),
        (CID_2, 5),
    ]


def test_non_cid_payload_is_stored_as_ignored(
    env_settings, network_config, sender_registry, reader
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, '{"archive": "not a plain cid"}')

    result = run_once(env_settings, network_config, sender_registry, reader)

    store = open_store(env_settings)
    ignored = store.list_datalog_entry_records_by_status(DatalogStatus.IGNORED)
    assert result.ignored_events == 1
    assert len(ignored) == 1
    assert ignored[0].raw_payload == '{"archive": "not a plain cid"}'
    assert ignored[0].cid is None
    assert ignored[0].error_message == NOT_A_REPORT_MESSAGE


def test_failing_sender_does_not_block_others_or_move_its_cursor(
    env_settings, network_config, sender_registry, reader
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    reader.publish(ADDRESS_2, 0, TIMESTAMP_2, CID_2)
    reader.failing_addresses.add(ADDRESS_1)

    result = run_once(env_settings, network_config, sender_registry, reader)

    store = open_store(env_settings)
    assert result.failed == 1
    assert result.processed == 1
    assert result.exit_code == 3
    assert entries_for(store, DatalogStatus.NEW) == [(CID_2, 0)]
    failing_sender = store.get_sender_record_by_address(ADDRESS_1)
    assert failing_sender is not None
    assert failing_sender.last_scanned_datalog_timestamp is None


def test_overwritten_cursor_is_reported_as_gap(
    env_settings, network_config, sender_registry, reader, caplog
) -> None:
    reader.publish(ADDRESS_1, 0, TIMESTAMP_1, CID_1)
    run_once(env_settings, network_config, sender_registry, reader)
    reader.publish(ADDRESS_1, 1, TIMESTAMP_2, CID_2)
    reader.gap_addresses.add(ADDRESS_1)

    with caplog.at_level(logging.WARNING):
        result = run_once(env_settings, network_config, sender_registry, reader)

    assert result.senders_with_gaps == 1
    assert "no longer holds records up to the cursor" in caplog.text


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
