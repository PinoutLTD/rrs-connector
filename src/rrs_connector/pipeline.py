import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from robonomicsinterface import Account

from rrs_connector.config import (
    EnvSettings,
    NetworkConfig,
    SenderRegistryConfig,
)
from rrs_connector.proton_pass import load_integrator_account
from rrs_connector.reports.decryptor import ReportDecryptionError, decrypt_archive
from rrs_connector.reports.fetcher import (
    DownloadSettings,
    ReportDownloadError,
    ReportTooLargeError,
    download_report,
)
from rrs_connector.robonomics.datalog_reader import DatalogReader, DatalogScan
from rrs_connector.state.db import (
    create_db_engine,
    create_session_factory,
    initialize_database,
)
from rrs_connector.state.models import DatalogEntryRecord, DatalogStatus, SenderRecord
from rrs_connector.state.store import StateStore

LOGGER = logging.getLogger(__name__)

# CIDv0 (base58 "Qm...") and CIDv1 in base32 ("b..."), as returned by Pinata.
CID_PATTERN = re.compile(r"^(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{58,})$")
NOT_A_REPORT_MESSAGE = "Payload is not a report CID"

# Statuses of reports that still have to be downloaded or decrypted; FETCHING
# and DECRYPTING are resumed if a previous run stopped midway.
PENDING_STATUSES = (
    DatalogStatus.NEW,
    DatalogStatus.FETCHING,
    DatalogStatus.FETCHED,
    DatalogStatus.DECRYPTING,
)
REPORTS_DIR_NAME = "reports"
ARCHIVE_FILE_NAME = "archive.zip"
DECRYPTED_DIR_NAME = "decrypted"
# Decrypted reports are logs from clients' homes: owner-only access.
PRIVATE_DIR_MODE = 0o700

AccountLoader = Callable[[], Account]
ReportDownloader = Callable[[str, Path, DownloadSettings], int]


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
class ReportProcessResult:
    processed: int = 0
    pending: int = 0
    failed: int = 0
    key_unavailable: bool = False


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
    reports_processed: int = 0
    reports_pending: int = 0
    reports_failed: int = 0
    integrator_key_unavailable: bool = False

    @property
    def exit_code(self) -> int:
        return 3 if self.failed or self.integrator_key_unavailable else 0


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


def safe_path_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip()).strip("._-")
    return cleaned or "unknown"


def report_dir(data_dir: Path, sender: SenderRecord, entry: DatalogEntryRecord) -> Path:
    client = safe_path_part(sender.client_id or sender.robonomics_address)
    timestamp_ms = datetime_to_ms(entry.datalog_timestamp)
    return (
        data_dir
        / REPORTS_DIR_NAME
        / client
        / f"datalog_{entry.datalog_index}_{timestamp_ms}"
    )


def prepare_reports_root(data_dir: Path) -> None:
    reports_root = data_dir / REPORTS_DIR_NAME
    reports_root.mkdir(parents=True, exist_ok=True)
    reports_root.chmod(PRIVATE_DIR_MODE)


def process_report(
    store: StateStore,
    entry: DatalogEntryRecord,
    sender: SenderRecord,
    data_dir: Path,
    account: Account,
    download_settings: DownloadSettings,
    download: ReportDownloader,
) -> DatalogStatus:
    """Download and decrypt one report; returns the status it ends in."""

    if not entry.cid:
        store.mark_datalog_entry_status(
            entry.id, DatalogStatus.FAILED, "report event has no CID"
        )
        return DatalogStatus.FAILED

    target = report_dir(data_dir, sender, entry)
    target.mkdir(parents=True, exist_ok=True)
    archive_path = target / ARCHIVE_FILE_NAME

    if entry.status in (DatalogStatus.NEW, DatalogStatus.FETCHING) or not (
        archive_path.exists()
    ):
        store.mark_datalog_entry_status(entry.id, DatalogStatus.FETCHING)
        try:
            download(entry.cid, archive_path, download_settings)
        except ReportTooLargeError as e:
            store.mark_datalog_entry_status(
                entry.id, DatalogStatus.FAILED, f"download: {e}"
            )
            return DatalogStatus.FAILED
        except ReportDownloadError as e:
            store.mark_datalog_entry_status(
                entry.id, DatalogStatus.NEW, f"download: {e}"
            )
            return DatalogStatus.NEW
        store.upsert_report_artifact(entry.id, archive_path=archive_path)
        store.mark_datalog_entry_status(entry.id, DatalogStatus.FETCHED)

    store.mark_datalog_entry_status(entry.id, DatalogStatus.DECRYPTING)
    decrypted_dir = target / DECRYPTED_DIR_NAME
    try:
        files = decrypt_archive(
            archive_path, decrypted_dir, account, sender.robonomics_address
        )
    except ReportDecryptionError as e:
        store.mark_datalog_entry_status(entry.id, DatalogStatus.FAILED, f"decrypt: {e}")
        return DatalogStatus.FAILED

    store.upsert_report_artifact(
        entry.id, decrypted_dir=decrypted_dir, processed_at=datetime.now(UTC)
    )
    store.mark_datalog_entry_status(entry.id, DatalogStatus.PROCESSED)
    LOGGER.info(
        "Sender %s datalog #%d: decrypted %s",
        sender.client_id,
        entry.datalog_index,
        ", ".join(f"{file.name} ({file.size} B)" for file in files),
    )
    return DatalogStatus.PROCESSED


def process_reports(
    store: StateStore,
    data_dir: Path,
    download_settings: DownloadSettings,
    load_account: AccountLoader,
    download: ReportDownloader,
) -> ReportProcessResult:
    result = ReportProcessResult()
    entries = sorted(
        (
            entry
            for status in PENDING_STATUSES
            for entry in store.list_datalog_entry_records_by_status(status)
        ),
        key=lambda entry: (entry.sender_id, entry.datalog_timestamp),
    )
    if not entries:
        return result

    try:
        account = load_account()
    except Exception as e:
        LOGGER.error(
            "Cannot load the integrator key, %d report(s) left pending: %s",
            len(entries),
            e,
        )
        result.key_unavailable = True
        result.pending = len(entries)
        return result

    prepare_reports_root(data_dir)
    senders: dict[int, SenderRecord] = {}

    for entry in entries:
        sender = senders.get(entry.sender_id) or store.get_sender_record_by_id(
            entry.sender_id
        )
        if sender is None:
            raise ValueError(f"Sender not found: {entry.sender_id}")
        senders[entry.sender_id] = sender

        try:
            status = process_report(
                store, entry, sender, data_dir, account, download_settings, download
            )
        except Exception as e:
            # Unexpected (e.g. disk) errors: keep the report for the next run.
            LOGGER.exception(
                "Unexpected error processing datalog #%d of sender %s",
                entry.datalog_index,
                sender.client_id,
            )
            store.mark_datalog_entry_status(
                entry.id, DatalogStatus.NEW, f"unexpected: {e}"
            )
            status = DatalogStatus.NEW

        if status is DatalogStatus.PROCESSED:
            result.processed += 1
        elif status is DatalogStatus.FAILED:
            result.failed += 1
        else:
            result.pending += 1

    return result


def create_download_settings(network_config: NetworkConfig) -> DownloadSettings:
    return DownloadSettings(
        gateways=[str(gateway) for gateway in network_config.ipfs_gateways],
        timeout_seconds=network_config.timeouts.ipfs_download_seconds,
        max_attempts=network_config.retries.ipfs_download_max_attempts,
        backoff_seconds=network_config.retries.retry_backoff_seconds,
    )


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
    load_account: AccountLoader | None = None,
    download: ReportDownloader | None = None,
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

    report_result = process_reports(
        store,
        env_settings.data_dir,
        create_download_settings(network_config),
        load_account
        or (
            lambda: load_integrator_account(
                env_settings.integrator_address, env_settings.pass_vault
            )
        ),
        download or download_report,
    )
    result.reports_processed = report_result.processed
    result.reports_pending = report_result.pending
    result.reports_failed = report_result.failed
    result.integrator_key_unavailable = report_result.key_unavailable

    LOGGER.info(
        "Run once is completed: senders processed=%d/%d failed=%d skipped=%d; "
        "events new=%d ignored=%d already_known=%d; senders with gaps=%d; "
        "reports processed=%d pending=%d failed=%d%s",
        result.processed,
        result.enabled,
        result.failed,
        result.skipped,
        result.new_events,
        result.ignored_events,
        result.known_events,
        result.senders_with_gaps,
        result.reports_processed,
        result.reports_pending,
        result.reports_failed,
        "; integrator key unavailable" if result.integrator_key_unavailable else "",
    )
    return result
