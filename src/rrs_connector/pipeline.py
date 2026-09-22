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
from rrs_connector.reports.manifest import (
    build_manifest,
    describe_files,
    write_manifest,
)
from rrs_connector.reports.permissions import PRIVATE, ArtifactModes, artifact_modes
from rrs_connector.reports.recipients import RecipientKeys
from rrs_connector.reports.retention import apply_retention
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

# Loads the key for one recipient address; raises when it cannot.
AccountLoader = Callable[[str], Account]


class RecipientKeyUnavailable(RuntimeError):
    """The report needs a key that cannot be loaded right now."""


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
    reports_removed: int = 0
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
    history_from: datetime | None = None,
) -> SenderCollectResult:
    result = SenderCollectResult()
    cursor_ms = datetime_to_ms(sender.last_scanned_datalog_timestamp)
    # A site not scanned yet starts from its configured history point, if any:
    # every record since then is read, and a site silent since then yields none.
    history_ms = datetime_to_ms(history_from) if history_from is not None else None
    start_ms = cursor_ms if cursor_ms is not None else history_ms

    scan = reader.list_new_records(sender.robonomics_address, start_ms)

    if cursor_ms is not None and not scan.reached_cursor:
        result.gap = True
        LOGGER.warning(
            "Sender %s: the datalog ring buffer no longer holds records up to "
            "the cursor %s; older reports were overwritten before being read",
            sender.client_id,
            ms_to_datetime(cursor_ms).isoformat(),
        )

    for record in scan.records:
        before_history = history_ms is not None and record.timestamp_ms < history_ms
        if cursor_ms is None and before_history:
            continue
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


def client_key(sender: SenderRecord) -> str:
    """Identity of a client in paths and in the manifest contract."""

    return safe_path_part(sender.client_id or sender.robonomics_address)


def report_dir(data_dir: Path, sender: SenderRecord, entry: DatalogEntryRecord) -> Path:
    timestamp_ms = datetime_to_ms(entry.datalog_timestamp)
    return (
        data_dir
        / REPORTS_DIR_NAME
        / client_key(sender)
        / f"datalog_{entry.datalog_index}_{timestamp_ms}"
    )


def prepare_reports_root(data_dir: Path, modes: ArtifactModes = PRIVATE) -> None:
    reports_root = data_dir / REPORTS_DIR_NAME
    reports_root.mkdir(parents=True, exist_ok=True)
    reports_root.chmod(modes.dir_mode)


def process_report(
    store: StateStore,
    entry: DatalogEntryRecord,
    sender: SenderRecord,
    data_dir: Path,
    keys: RecipientKeys,
    download_settings: DownloadSettings,
    download: ReportDownloader,
    modes: ArtifactModes = PRIVATE,
) -> DatalogStatus:
    """Download and decrypt one report; returns the status it ends in.

    Raises RecipientKeyUnavailable when the key the report needs cannot be
    loaded: the downloaded archive is kept and the report resumes next run.
    """

    if not entry.cid:
        store.mark_datalog_entry_status(
            entry.id, DatalogStatus.FAILED, "report event has no CID"
        )
        return DatalogStatus.FAILED

    target = report_dir(data_dir, sender, entry)
    target.mkdir(parents=True, exist_ok=True)
    # The client directory and the report directory, as created above.
    for directory in (target.parent, target):
        directory.chmod(modes.dir_mode)
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
        archive_path.chmod(modes.file_mode)
        store.upsert_report_artifact(entry.id, archive_path=archive_path)
        store.mark_datalog_entry_status(entry.id, DatalogStatus.FETCHED)

    # The envelope names the addresses the report is encrypted for; that
    # decides which of our keys is needed.
    try:
        recipient = keys.choose(archive_path)
    except ReportDecryptionError as e:
        store.mark_datalog_entry_status(entry.id, DatalogStatus.FAILED, f"decrypt: {e}")
        return DatalogStatus.FAILED

    try:
        account = keys.account(recipient)
    except Exception as e:
        store.mark_datalog_entry_status(
            entry.id, DatalogStatus.FETCHED, f"key {recipient}: {e}"
        )
        raise RecipientKeyUnavailable(recipient) from e

    store.mark_datalog_entry_status(entry.id, DatalogStatus.DECRYPTING)
    decrypted_dir = target / DECRYPTED_DIR_NAME
    try:
        files = decrypt_archive(
            archive_path, decrypted_dir, account, sender.robonomics_address, modes
        )
    except ReportDecryptionError as e:
        store.mark_datalog_entry_status(entry.id, DatalogStatus.FAILED, f"decrypt: {e}")
        return DatalogStatus.FAILED

    # The manifest is written last: its presence means the report is complete
    # and may be read by the admin layer.
    processed_at = datetime.now(UTC)
    manifest_path = write_manifest(
        target,
        build_manifest(
            client_id=client_key(sender),
            sender_address=sender.robonomics_address,
            datalog_index=entry.datalog_index,
            datalog_timestamp=ms_to_datetime(datetime_to_ms(entry.datalog_timestamp)),
            cid=entry.cid,
            directory=target,
            archive_path=archive_path,
            decrypted_dir=decrypted_dir,
            files=describe_files(files, target),
            processed_at=processed_at,
        ),
        file_mode=modes.file_mode,
    )

    store.upsert_report_artifact(
        entry.id,
        decrypted_dir=decrypted_dir,
        meta_path=manifest_path,
        processed_at=processed_at,
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
    recipient_addresses: list[str],
    load_account: AccountLoader,
    download: ReportDownloader,
    modes: ArtifactModes = PRIVATE,
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

    # Keys are loaded lazily: only those the pending reports actually need.
    keys = RecipientKeys(recipient_addresses, load_account)
    prepare_reports_root(data_dir, modes)
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
                store,
                entry,
                sender,
                data_dir,
                keys,
                download_settings,
                download,
                modes,
            )
        except RecipientKeyUnavailable as e:
            LOGGER.error(
                "Cannot load recipient key %s, datalog #%d of %s left pending: %s",
                e,
                entry.datalog_index,
                sender.client_id,
                e.__cause__,
            )
            result.key_unavailable = True
            status = DatalogStatus.FETCHED
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
        max_attempts=network_config.retries.datalog_request_max_attempts,
        backoff_seconds=network_config.retries.retry_backoff_seconds,
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

    history_from = {s.client_id: s.history_from for s in sender_registry.senders}
    for sender in sender_records:
        try:
            sender_result = collect_sender_events(
                store, reader, sender, history_from.get(sender.client_id)
            )
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
        env_settings.integrator_addresses,
        load_account
        or (lambda address: load_integrator_account(address, env_settings.pass_vault)),
        download or download_report,
        artifact_modes(env_settings.artifact_group_readable),
    )
    result.reports_processed = report_result.processed
    result.reports_pending = report_result.pending
    result.reports_failed = report_result.failed
    result.integrator_key_unavailable = report_result.key_unavailable

    # Cleanup runs last: a failure here must not cost us the reports we just
    # collected, so it is reported and does not fail the run.
    try:
        retention = apply_retention(
            store,
            env_settings.keep_decrypted_days,
            env_settings.keep_archive_days,
        )
        result.reports_removed = retention.reports_removed
    except Exception:
        LOGGER.exception("Retention pass failed")

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
