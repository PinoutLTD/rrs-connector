import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from rrs_connector.config import (
    EnvSettings,
    NetworkConfig,
    SenderRegistryConfig,
)
from rrs_connector.robonomics.datalog_reader import DatalogReader, DatalogScan
from rrs_connector.state.db import (
    create_db_engine,
    create_session_factory,
    initialize_database,
)
from rrs_connector.state.models import DatalogStatus, SenderRecord
from rrs_connector.state.store import StateStore

LOGGER = logging.getLogger(__name__)

# CIDv0 (base58 "Qm...") and CIDv1 in base32 ("b..."), as returned by Pinata.
CID_PATTERN = re.compile(r"^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{58,})$")
NOT_A_REPORT_MESSAGE = "Payload is not a report CID"


class DatalogSource(Protocol):
    def list_new_records(
        self, sender_address: str, cursor_timestamp_ms: int | None
    ) -> DatalogScan: ...


@dataclass
class SenderCollectResult:
    new: int = 0
    ignored: int = 0
    known: int = 0
    gap: bool = False


@dataclass
class RunOnceResult:
    processed: int
    enabled: int
    failed: int
    skipped: int
    new_events: int = 0
    ignored_events: int = 0
    known_events: int = 0
    senders_with_gaps: int = 0

    @property
    def exit_code(self) -> int:
        return 0 if self.failed == 0 else 3


def ms_to_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC)


def datetime_to_ms(value: datetime | None) -> int | None:
    if value is None:
        return None
    # SQLite returns naive datetimes; everything is stored in UTC.
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return round(value.timestamp() * 1000)


def extract_report_cid(payload: str) -> str | None:
    candidate = payload.strip()
    return candidate if CID_PATTERN.match(candidate) else None


def collect_sender_events(
    store: StateStore,
    reader: DatalogSource,
    sender: SenderRecord,
) -> SenderCollectResult:
    result = SenderCollectResult()
    cursor_ms = datetime_to_ms(sender.last_scanned_datalog_timestamp)

    scan = reader.list_new_records(sender.robonomics_address, cursor_ms)

    if cursor_ms is not None and not scan.reached_cursor:
        result.gap = True
        LOGGER.warning(
            "Sender %s: the datalog ring buffer no longer holds records up to "
            "the cursor %s; older reports were overwritten before being read",
            sender.client_id,
            ms_to_datetime(cursor_ms).isoformat(),
        )

    for record in scan.records:
        cid = extract_report_cid(record.payload)
        is_added = store.add_datalog_entry(
            sender_id=sender.id,
            datalog_index=record.datalog_index,
            datalog_timestamp=ms_to_datetime(record.timestamp_ms),
            raw_payload=record.payload,
            cid=cid,
            status=DatalogStatus.NEW if cid else DatalogStatus.IGNORED,
            error_message=None if cid else NOT_A_REPORT_MESSAGE,
        )
        if not is_added:
            result.known += 1
        elif cid:
            result.new += 1
        else:
            result.ignored += 1

    # The cursor moves only after every record of this scan is stored, so a
    # failure above leaves it in place and the next run re-reads the records.
    if scan.records:
        last_record = scan.records[-1]
        store.mark_sender_scanned(
            sender.id,
            last_record.datalog_index,
            ms_to_datetime(last_record.timestamp_ms),
        )

    return result


def create_datalog_reader(network_config: NetworkConfig) -> DatalogReader:
    endpoints = getattr(network_config.wss, network_config.network)
    return DatalogReader(
        wss_endpoints=[str(endpoint) for endpoint in endpoints],
        request_timeout_seconds=network_config.timeouts.datalog_request_seconds,
    )


def run_once(
    env_settings: EnvSettings,
    network_config: NetworkConfig,
    sender_registry: SenderRegistryConfig,
    reader: DatalogSource | None = None,
) -> RunOnceResult:
    LOGGER.info("Starting run-once pass")

    engine = create_db_engine(env_settings.state_db)
    initialize_database(engine)
    session_factory = create_session_factory(engine)

    store = StateStore(session_factory)
    store.sync_senders(sender_registry.senders)

    if reader is None:
        reader = create_datalog_reader(network_config)

    sender_records = store.get_enabled_sender_records()
    result = RunOnceResult(
        processed=0,
        enabled=len(sender_records),
        failed=0,
        skipped=len(sender_registry.senders) - len(sender_records),
    )

    for sender in sender_records:
        try:
            sender_result = collect_sender_events(store, reader, sender)
        except Exception:
            LOGGER.exception(
                "Error during processing sender %s (%s)",
                sender.client_id,
                sender.robonomics_address,
            )
            result.failed += 1
            continue

        result.processed += 1
        result.new_events += sender_result.new
        result.ignored_events += sender_result.ignored
        result.known_events += sender_result.known
        result.senders_with_gaps += int(sender_result.gap)
        LOGGER.info(
            "Sender %s: new=%d ignored=%d already_known=%d%s",
            sender.client_id,
            sender_result.new,
            sender_result.ignored,
            sender_result.known,
            " gap=yes" if sender_result.gap else "",
        )

    LOGGER.info(
        "Run once is completed: senders processed=%d/%d failed=%d skipped=%d; "
        "events new=%d ignored=%d already_known=%d; senders with gaps=%d",
        result.processed,
        result.enabled,
        result.failed,
        result.skipped,
        result.new_events,
        result.ignored_events,
        result.known_events,
        result.senders_with_gaps,
    )
    return result
