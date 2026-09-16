import stat
from pathlib import Path

import pytest
from test_pipeline import ArchiveDownloader, unavailable_download

from rrs_connector.fetch import fetch
from rrs_connector.robonomics.datalog_reader import DatalogRecord

CID_1 = "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"
CID_2 = "QmUqNnzdZnic61UYTuKT9EzBNzMW6jc5uHSFk4Xzd3iM93"
TIMESTAMP_1 = 1780065423000
TIMESTAMP_2 = 1780072623000


class FakeLastRecords:
    def __init__(self, records: list[DatalogRecord]) -> None:
        self.records = records
        self.requested: list[int] = []

    def list_last_records(self, sender_address: str, count: int):
        self.requested.append(count)
        return self.records[-count:]


@pytest.fixture
def run_fetch(env_settings, network_config, recipient_account, ha_report_archive):
    def run(sender_address, **kwargs):
        kwargs.setdefault("download", ArchiveDownloader(ha_report_archive))
        kwargs.setdefault("load_account", lambda: recipient_account)
        return fetch(env_settings, network_config, sender_address, **kwargs)

    return run


def test_report_is_fetched_by_cid(
    run_fetch, env_settings, sender_address, ha_report
) -> None:
    result = run_fetch(sender_address, cids=[CID_1])

    assert result.exit_code == 0
    (directory,) = result.fetched
    assert directory == env_settings.data_dir / "fetched" / sender_address / CID_1
    for name in ha_report["files"]:
        assert (directory / "decrypted" / name).exists()
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE((directory / "archive.zip").stat().st_mode) == 0o600


def test_several_cids_are_fetched(run_fetch, sender_address) -> None:
    result = run_fetch(sender_address, cids=[CID_1, CID_2])

    assert [path.name for path in result.fetched] == [CID_1, CID_2]


def test_output_directory_is_honoured(run_fetch, sender_address, tmp_path) -> None:
    target = tmp_path / "support-request"

    result = run_fetch(sender_address, cids=[CID_1], output_dir=target)

    assert result.fetched == [target / CID_1]


def test_last_reports_come_from_the_datalog(run_fetch, sender_address) -> None:
    reader = FakeLastRecords(
        [
            DatalogRecord(sender_address, 96, TIMESTAMP_1, CID_1),
            DatalogRecord(sender_address, 97, TIMESTAMP_2, CID_2),
        ]
    )

    result = run_fetch(sender_address, last=2, reader=reader)

    assert reader.requested == [2]
    assert [path.name for path in result.fetched] == [
        f"datalog_96_{TIMESTAMP_1}",
        f"datalog_97_{TIMESTAMP_2}",
    ]


def test_payload_that_is_not_a_cid_is_skipped(run_fetch, sender_address) -> None:
    reader = FakeLastRecords(
        [
            DatalogRecord(sender_address, 96, TIMESTAMP_1, "not a report"),
            DatalogRecord(sender_address, 97, TIMESTAMP_2, CID_1),
        ]
    )

    result = run_fetch(sender_address, last=2, reader=reader)

    assert [path.name for path in result.fetched] == [f"datalog_97_{TIMESTAMP_2}"]


def test_nothing_to_fetch_is_reported(run_fetch, sender_address) -> None:
    result = run_fetch(sender_address, last=1, reader=FakeLastRecords([]))

    assert result.fetched == []
    assert result.exit_code == 3


def test_download_failure_is_reported_per_report(
    run_fetch, sender_address, ha_report_archive
) -> None:
    result = run_fetch(
        sender_address,
        cids=[CID_1, CID_2],
        download=ArchiveDownloader(ha_report_archive, failures=1),
    )

    assert len(result.failed) == 1
    assert len(result.fetched) == 1
    assert result.exit_code == 3


def test_wrong_recipient_is_a_decryption_failure(
    run_fetch, sender_address, recipient_account
) -> None:
    # ADDRESS_1 did not encrypt the golden archive for us.
    result = run_fetch("4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN", cids=[CID_1])

    assert result.fetched == []
    assert "decrypt" in result.failed[0][1] or result.failed[0][1]


def test_state_database_is_never_touched(
    run_fetch, env_settings, sender_address
) -> None:
    run_fetch(sender_address, cids=[CID_1])

    assert not Path(env_settings.state_db).exists()


def test_existing_archive_is_not_downloaded_again(
    run_fetch, env_settings, sender_address, ha_report_archive
) -> None:
    downloader = ArchiveDownloader(ha_report_archive)

    run_fetch(sender_address, cids=[CID_1], download=downloader)
    run_fetch(sender_address, cids=[CID_1], download=downloader)

    assert downloader.calls == [CID_1]


def test_key_is_not_loaded_when_there_is_nothing_to_fetch(
    run_fetch, sender_address
) -> None:
    loaded: list[bool] = []

    fetched = run_fetch(
        sender_address,
        last=1,
        reader=FakeLastRecords([]),
        load_account=lambda: loaded.append(True),
        download=unavailable_download,
    )

    assert loaded == []
    assert fetched.fetched == []
