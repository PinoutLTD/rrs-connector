from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from rrs_connector.config import SenderConfig, SenderRegistryConfig
from rrs_connector.state.db import (
    create_db_engine,
    create_session_factory,
    initialize_database,
)
from rrs_connector.state.models import (
    DatalogEntryRecord,
    DatalogStatus,
    SenderRecord,
)
from rrs_connector.state.store import StateStore

ADDRESS_1 = "4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN"
ADDRESS_2 = "4Ff5w7XuzrfnuMT25GYijtu3w2DoRbjNSBgym4TRgPSh279f"
ADDRESS_3 = "4CrE2aFEXYeBrNJePkTPxF363NqYYvXC1PhWsSZWtbWy3PzJ"

CID_1 = "QmRHvtsEViqHFN6Mt66p9o5MvvzB2H5uvfMTi8maAnLmfi"
CID_2 = "QmUbQTQknKLuDB8SmJF9pUhkTPdJbXp5ghDwp7oXwwDb9V"

DATALOG_TS_1 = datetime.fromtimestamp(1779171696000 / 1000.0, UTC)
DATALOG_TS_2 = datetime.fromtimestamp(1779156696000 / 1000.0, UTC)


@pytest.fixture
def sender_configs() -> list[SenderConfig]:
    senders_data = {
        "senders": [
            {
                "client_id": "1",
                "robonomics_address": ADDRESS_1,
                "description": "test_description1",
                "enabled": True,
            },
            {
                "client_id": "2",
                "robonomics_address": ADDRESS_2,
                "description": "test_description2",
                "enabled": True,
            },
            {
                "client_id": "3",
                "robonomics_address": ADDRESS_3,
                "description": "test_description3",
                "enabled": False,
            },
        ]
    }
    return SenderRegistryConfig.model_validate(senders_data).senders


@pytest.fixture
def session_factory(tmp_path: Path) -> sessionmaker[Session]:
    db_path = tmp_path / "state.sqlite3"
    engine = create_db_engine(db_path)
    initialize_database(engine)
    return create_session_factory(engine)


@pytest.fixture
def store(session_factory: sessionmaker[Session]) -> StateStore:
    return StateStore(session_factory)


@pytest.fixture
def synced_store(
    store: StateStore,
    sender_configs: list[SenderConfig],
) -> StateStore:
    store.sync_senders(sender_configs)
    return store


def get_sender_record(
    session: Session,
    address: str,
) -> SenderRecord | None:
    return session.scalar(
        select(SenderRecord).where(SenderRecord.robonomics_address == address)
    )


def get_required_sender_record(
    store: StateStore,
    address: str = ADDRESS_1,
) -> SenderRecord:
    sender = store.get_sender_record_by_address(address)
    assert sender is not None
    return sender


def get_required_datalog_entry_record(
    store: StateStore,
    sender_id: int,
    datalog_index: int = 1,
    datalog_timestamp: datetime = DATALOG_TS_1,
) -> DatalogEntryRecord:
    entry = store.get_datalog_entry_record(sender_id, datalog_index, datalog_timestamp)
    assert entry is not None
    return entry


def add_datalog_entry_record(
    store: StateStore,
    sender_id: int,
    datalog_index: int = 1,
    raw_payload: str | None = None,
    cid: str | None = CID_1,
    status: DatalogStatus = DatalogStatus.NEW,
    datalog_timestamp: datetime = DATALOG_TS_1,
    error_message: str | None = None,
) -> DatalogEntryRecord:
    if raw_payload is None:
        raw_payload = f"Test Payload: {cid}"

    is_added = store.add_datalog_entry(
        sender_id=sender_id,
        datalog_index=datalog_index,
        datalog_timestamp=datalog_timestamp,
        raw_payload=raw_payload,
        cid=cid,
        status=status,
        error_message=error_message,
    )
    assert is_added is True

    return get_required_datalog_entry_record(
        store, sender_id, datalog_index, datalog_timestamp
    )


def add_datalog_entry_for_sender_address(
    store: StateStore,
    address: str = ADDRESS_1,
    datalog_index: int = 1,
    raw_payload: str | None = None,
    cid: str | None = CID_1,
    status: DatalogStatus = DatalogStatus.NEW,
    datalog_timestamp: datetime = DATALOG_TS_1,
    error_message: str | None = None,
) -> tuple[SenderRecord, DatalogEntryRecord]:
    sender = get_required_sender_record(store, address)
    entry = add_datalog_entry_record(
        store=store,
        sender_id=sender.id,
        datalog_index=datalog_index,
        raw_payload=raw_payload,
        cid=cid,
        status=status,
        datalog_timestamp=datalog_timestamp,
        error_message=error_message,
    )
    return sender, entry


def assert_sender_matches_config(
    sender_record: SenderRecord | None,
    sender_config: SenderConfig,
) -> None:
    assert sender_record is not None
    assert sender_record.client_id == sender_config.client_id
    assert sender_record.robonomics_address == sender_config.robonomics_address
    assert sender_record.description == sender_config.description
    assert sender_record.enabled == sender_config.enabled
    assert sender_record.created_at is not None
    assert sender_record.updated_at is not None


def test_sync_senders_creates_senders(
    store: StateStore,
    session_factory: sessionmaker[Session],
    sender_configs: list[SenderConfig],
) -> None:
    store.sync_senders(sender_configs)

    with session_factory() as session:
        sender_records = session.scalars(select(SenderRecord)).all()

        assert len(sender_records) == len(sender_configs)

        for sender_config in sender_configs:
            sender_record = get_sender_record(
                session,
                sender_config.robonomics_address,
            )
            assert_sender_matches_config(sender_record, sender_config)


def test_sync_senders_updates_existing_senders(
    store: StateStore,
    session_factory: sessionmaker[Session],
    sender_configs: list[SenderConfig],
) -> None:
    store.sync_senders(sender_configs)

    sender_configs[0].description = "test_description1_new"
    sender_configs[0].enabled = False

    store.sync_senders(sender_configs)

    with session_factory() as session:
        sender_records = session.scalars(select(SenderRecord)).all()
        updated_sender = get_sender_record(session, ADDRESS_1)

        assert len(sender_records) == len(sender_configs)
        assert_sender_matches_config(updated_sender, sender_configs[0])


def test_sync_senders_preserves_sender_cursor(
    synced_store: StateStore,
    sender_configs: list[SenderConfig],
) -> None:
    sender = get_required_sender_record(synced_store)

    synced_store.mark_sender_scanned(sender.id, 42, DATALOG_TS_1)

    sender_configs[0].description = "test_description1_new"
    synced_store.sync_senders(sender_configs)

    updated = synced_store.get_sender_record_by_address(ADDRESS_1)
    assert updated is not None
    assert updated.description == "test_description1_new"
    assert updated.last_scanned_datalog_timestamp == DATALOG_TS_1.replace(tzinfo=None)
    assert updated.last_scanned_datalog_index == 42
    assert updated.last_scanned_at is not None


def test_get_enabled_sender_records(synced_store: StateStore) -> None:
    sender_records = synced_store.get_enabled_sender_records()

    assert [record.robonomics_address for record in sender_records] == [
        ADDRESS_1,
        ADDRESS_2,
    ]


def test_get_sender_record_by_address(synced_store: StateStore) -> None:
    sender = synced_store.get_sender_record_by_address(ADDRESS_1)

    assert sender is not None
    assert sender.robonomics_address == ADDRESS_1


def test_mark_sender_scanned_updates_cursor(synced_store: StateStore) -> None:
    sender = get_required_sender_record(synced_store)

    synced_store.mark_sender_scanned(sender.id, 42, DATALOG_TS_1)

    updated = synced_store.get_sender_record_by_address(ADDRESS_1)
    assert updated is not None
    assert updated.last_scanned_datalog_timestamp == DATALOG_TS_1.replace(tzinfo=None)
    assert updated.last_scanned_datalog_index == 42
    assert updated.last_scanned_at is not None


def test_mark_sender_scanned_raises_for_unknown_sender(
    store: StateStore,
) -> None:
    with pytest.raises(ValueError, match="Sender not found"):
        store.mark_sender_scanned(999, 42, DATALOG_TS_1)


def test_sync_senders_disables_missing_senders(
    store: StateStore,
    sender_configs: list[SenderConfig],
) -> None:
    store.sync_senders(sender_configs)

    store.sync_senders(sender_configs[1:])

    missing_sender = store.get_sender_record_by_address(ADDRESS_1)

    assert missing_sender is not None
    assert missing_sender.enabled is False


def test_add_datalog_entry_creates_entry(synced_store: StateStore) -> None:
    sender = get_required_sender_record(synced_store)

    is_added = synced_store.add_datalog_entry(
        sender_id=sender.id,
        datalog_index=1,
        raw_payload=f"Test Payload: {CID_1}",
        cid=CID_1,
        status=DatalogStatus.NEW,
        datalog_timestamp=DATALOG_TS_1,
    )

    assert is_added is True

    entry = get_required_datalog_entry_record(synced_store, sender.id)
    assert entry.sender_id == sender.id
    assert entry.datalog_index == 1
    assert entry.raw_payload == f"Test Payload: {CID_1}"
    assert entry.cid == CID_1
    assert entry.status == DatalogStatus.NEW
    assert entry.datalog_timestamp == DATALOG_TS_1.replace(tzinfo=None)
    assert entry.error_message is None
    assert entry.first_seen_at is not None
    assert entry.updated_at is not None
    assert entry.processed_at is None


def test_add_datalog_entry_returns_false_for_duplicate_event(
    synced_store: StateStore,
) -> None:
    sender, _ = add_datalog_entry_for_sender_address(synced_store)

    is_added = synced_store.add_datalog_entry(
        sender_id=sender.id,
        datalog_index=1,
        datalog_timestamp=DATALOG_TS_1,
        raw_payload=f"Test Payload: {CID_1}",
        cid=CID_1,
        status=DatalogStatus.NEW,
    )

    assert is_added is False


def test_add_datalog_entry_allows_reused_ring_slot_with_new_timestamp(
    synced_store: StateStore,
) -> None:
    sender, _ = add_datalog_entry_for_sender_address(synced_store)

    is_added = synced_store.add_datalog_entry(
        sender_id=sender.id,
        datalog_index=1,
        datalog_timestamp=DATALOG_TS_2,
        raw_payload=f"Test Payload: {CID_2}",
        cid=CID_2,
        status=DatalogStatus.NEW,
    )

    assert is_added is True
    entry = get_required_datalog_entry_record(synced_store, sender.id, 1, DATALOG_TS_2)
    assert entry.cid == CID_2


def test_add_datalog_entry_allows_same_cid_with_different_index(
    synced_store: StateStore,
) -> None:
    sender, _ = add_datalog_entry_for_sender_address(synced_store)

    is_added = synced_store.add_datalog_entry(
        sender_id=sender.id,
        datalog_index=2,
        raw_payload=f"Test Payload: {CID_1}",
        cid=CID_1,
        status=DatalogStatus.NEW,
        datalog_timestamp=DATALOG_TS_2,
    )

    assert is_added is True


def test_add_datalog_entry_raises_for_unknown_sender(
    store: StateStore,
) -> None:
    with pytest.raises(ValueError, match="Sender not found"):
        store.add_datalog_entry(
            sender_id=1,
            datalog_index=1,
            raw_payload=f"Test Payload: {CID_1}",
            cid=CID_1,
            status=DatalogStatus.NEW,
            datalog_timestamp=DATALOG_TS_1,
        )


def test_add_datalog_entry_creates_ignored_entry(
    synced_store: StateStore,
) -> None:
    sender = get_required_sender_record(synced_store)

    is_added = synced_store.add_datalog_entry(
        sender_id=sender.id,
        datalog_index=1,
        raw_payload="not a report cid",
        cid=None,
        status=DatalogStatus.IGNORED,
        datalog_timestamp=DATALOG_TS_1,
        error_message="Payload is not a report CID",
    )

    assert is_added is True

    entry = get_required_datalog_entry_record(synced_store, sender.id)
    assert entry.raw_payload == "not a report cid"
    assert entry.cid is None
    assert entry.status == DatalogStatus.IGNORED
    assert entry.error_message == "Payload is not a report CID"


def test_get_datalog_entry_record_by_id(synced_store: StateStore) -> None:
    _, entry = add_datalog_entry_for_sender_address(synced_store)

    entry_by_id = synced_store.get_datalog_entry_record_by_id(entry.id)

    assert entry_by_id is not None
    assert entry_by_id.id == entry.id


def test_list_datalog_entry_records_by_status(synced_store: StateStore) -> None:
    add_datalog_entry_for_sender_address(synced_store, ADDRESS_1)
    add_datalog_entry_for_sender_address(
        synced_store,
        ADDRESS_2,
        cid=CID_2,
        datalog_timestamp=DATALOG_TS_2,
    )

    datalog_entry_records = synced_store.list_datalog_entry_records_by_status(
        DatalogStatus.NEW
    )

    assert [record.cid for record in datalog_entry_records] == [CID_1, CID_2]
    assert [record.status for record in datalog_entry_records] == [
        DatalogStatus.NEW,
        DatalogStatus.NEW,
    ]


def test_mark_datalog_entry_status(synced_store: StateStore) -> None:
    sender, datalog_entry = add_datalog_entry_for_sender_address(synced_store)

    synced_store.mark_datalog_entry_status(
        datalog_entry.id,
        DatalogStatus.FETCHING,
    )
    datalog_entry = get_required_datalog_entry_record(synced_store, sender.id)
    assert datalog_entry.status == DatalogStatus.FETCHING

    synced_store.mark_datalog_entry_status(
        datalog_entry.id,
        DatalogStatus.FAILED,
        error_message="Test error",
    )
    datalog_entry = get_required_datalog_entry_record(synced_store, sender.id)
    assert datalog_entry.status == DatalogStatus.FAILED
    assert datalog_entry.error_message == "Test error"

    synced_store.mark_datalog_entry_status(datalog_entry.id, DatalogStatus.PROCESSED)
    datalog_entry = get_required_datalog_entry_record(synced_store, sender.id)
    assert datalog_entry.status == DatalogStatus.PROCESSED
    assert datalog_entry.processed_at is not None
    assert datalog_entry.error_message is None


def test_mark_datalog_entry_status_raises_for_unknown_entry(
    store: StateStore,
) -> None:
    with pytest.raises(ValueError, match="Datalog entry not found"):
        store.mark_datalog_entry_status(1, DatalogStatus.FETCHING)


def test_upsert_report_artifact(
    synced_store: StateStore,
    tmp_path: Path,
) -> None:
    _, datalog_entry = add_datalog_entry_for_sender_address(synced_store)

    synced_store.upsert_report_artifact(datalog_entry.id)

    artifact_record = synced_store.get_report_artifact_record(datalog_entry.id)
    assert artifact_record is not None

    synced_store.upsert_report_artifact(
        datalog_entry.id,
        archive_path=tmp_path / "test_archive.zip",
    )

    artifact_record_archive = synced_store.get_report_artifact_record(datalog_entry.id)
    assert artifact_record_archive is not None
    assert artifact_record.id == artifact_record_archive.id

    synced_store.upsert_report_artifact(
        datalog_entry.id,
        archive_path=None,
        raw_dir=tmp_path / "raw_dir",
    )

    artifact_record = synced_store.get_report_artifact_record(datalog_entry.id)
    assert artifact_record is not None
    assert artifact_record.archive_path == str(tmp_path / "test_archive.zip")
    assert artifact_record.raw_dir == str(tmp_path / "raw_dir")


def test_get_report_artifact_record_returns_none_without_artifact(
    synced_store: StateStore,
) -> None:
    _, datalog_entry = add_datalog_entry_for_sender_address(synced_store)

    assert synced_store.get_report_artifact_record(datalog_entry.id) is None


def test_get_report_artifact_record_raises_for_unknown_entry(
    store: StateStore,
) -> None:
    with pytest.raises(ValueError, match="Datalog entry not found"):
        store.get_report_artifact_record(42)


def test_upsert_report_artifact_raises_for_unknown_entry(
    store: StateStore,
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="Datalog entry not found"):
        store.upsert_report_artifact(42, archive_path=tmp_path / "test_archive.zip")
