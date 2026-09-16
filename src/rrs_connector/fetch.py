"""One-off download and decryption of a report, outside the pipeline.

The scheduled run only looks forward from its cursor and deletes artifacts by
age, which is right for a service and useless when a person asks about a
specific report: "it broke on Thursday", or a report that retention already
removed but IPFS still holds. This command answers that without touching the
state database or the artifacts the pipeline manages.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

from robonomicsinterface import Account

from rrs_connector.config import EnvSettings, NetworkConfig
from rrs_connector.pipeline import (
    ARCHIVE_FILE_NAME,
    DECRYPTED_DIR_NAME,
    AccountLoader,
    ReportDownloader,
    create_datalog_reader,
    create_download_settings,
    extract_report_cid,
    safe_path_part,
)
from rrs_connector.proton_pass import load_integrator_account
from rrs_connector.reports.decryptor import (
    DecryptedFile,
    ReportDecryptionError,
    decrypt_archive,
)
from rrs_connector.reports.fetcher import ReportDownloadError, download_report
from rrs_connector.reports.permissions import PRIVATE
from rrs_connector.robonomics.datalog_reader import DatalogReader

LOGGER = logging.getLogger(__name__)

FETCHED_DIR_NAME = "fetched"


@dataclass(frozen=True)
class RequestedReport:
    """A report to fetch, and the directory name it will be stored under."""

    cid: str
    name: str


@dataclass
class FetchResult:
    fetched: list[Path] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        return 3 if self.failed or not self.fetched else 0


def requested_from_datalog(
    reader: DatalogReader, sender_address: str, count: int
) -> list[RequestedReport]:
    """The last `count` reports a sender published, oldest first."""

    requested: list[RequestedReport] = []
    for record in reader.list_last_records(sender_address, count):
        cid = extract_report_cid(record.payload)
        if cid is None:
            LOGGER.warning(
                "Datalog #%d is not a report CID, skipping: %.80s",
                record.datalog_index,
                record.payload,
            )
            continue
        requested.append(
            RequestedReport(
                cid, f"datalog_{record.datalog_index}_{record.timestamp_ms}"
            )
        )
    return requested


def fetch_report(
    report: RequestedReport,
    target: Path,
    sender_address: str,
    account: Account,
    download_settings,
    download: ReportDownloader,
) -> list[DecryptedFile]:
    target.mkdir(parents=True, exist_ok=True)
    target.chmod(PRIVATE.dir_mode)
    archive_path = target / ARCHIVE_FILE_NAME

    if not archive_path.exists():
        download(report.cid, archive_path, download_settings)
        archive_path.chmod(PRIVATE.file_mode)

    return decrypt_archive(
        archive_path, target / DECRYPTED_DIR_NAME, account, sender_address, PRIVATE
    )


def fetch(
    env_settings: EnvSettings,
    network_config: NetworkConfig,
    sender_address: str,
    cids: list[str] | None = None,
    last: int | None = None,
    output_dir: Path | None = None,
    reader: DatalogReader | None = None,
    load_account: AccountLoader | None = None,
    download: ReportDownloader | None = None,
) -> FetchResult:
    result = FetchResult()

    if cids:
        requested = [RequestedReport(cid, safe_path_part(cid)) for cid in cids]
    else:
        reader = reader or create_datalog_reader(network_config)
        requested = requested_from_datalog(reader, sender_address, last or 1)

    if not requested:
        LOGGER.warning("Nothing to fetch for %s", sender_address)
        return result

    account = (
        load_account
        or (
            lambda: load_integrator_account(
                env_settings.integrator_address, env_settings.pass_vault
            )
        )
    )()

    root = output_dir or (
        env_settings.data_dir / FETCHED_DIR_NAME / safe_path_part(sender_address)
    )
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(PRIVATE.dir_mode)
    download_settings = create_download_settings(network_config)
    downloader = download or download_report

    for report in requested:
        target = root / report.name
        try:
            files = fetch_report(
                report,
                target,
                sender_address,
                account,
                download_settings,
                downloader,
            )
        except (ReportDownloadError, ReportDecryptionError) as e:
            LOGGER.error("Report %s: %s", report.cid, e)
            result.failed.append((report.cid, str(e)))
            continue

        result.fetched.append(target)
        LOGGER.info(
            "Report %s decrypted into %s: %s",
            report.cid,
            target,
            ", ".join(f"{file.name} ({file.size} B)" for file in files),
        )

    LOGGER.warning(
        "These files are plaintext logs from a client's home: delete them when done"
    )
    return result
