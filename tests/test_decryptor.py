import hashlib
import json
import stat
from pathlib import Path
from zipfile import ZipFile

import pytest
from robonomicsinterface import Keypair, generate_mnemonic

from rrs_connector.reports import decryptor
from rrs_connector.reports.decryptor import (
    ReportDecryptionError,
    decrypt_archive,
    parse_decrypted,
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stranger_account() -> Keypair:
    return Keypair.from_mnemonic(generate_mnemonic())


def make_zip(path: Path, members: dict[str, str]) -> Path:
    with ZipFile(path, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return path


def test_decrypts_archive_produced_by_ha_integration(
    tmp_path, ha_report, ha_report_archive, recipient_account, sender_address
) -> None:
    output_dir = tmp_path / "decrypted"

    files = decrypt_archive(
        ha_report_archive, output_dir, recipient_account, sender_address
    )

    assert [file.name for file in files] == sorted(ha_report["files"])
    for file in files:
        expected = ha_report["files"][file.name]
        assert file.path == output_dir / file.name
        assert file.size == expected["size"]
        assert sha256(file.path) == expected["sha256"]
        assert stat.S_IMODE(file.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(output_dir.stat().st_mode) == 0o700
    issue = json.loads((output_dir / "issue_description.json").read_text("utf-8"))
    assert issue["type"] == "accumulated_system_log_problems"


def test_rejects_package_not_encrypted_for_recipient(
    tmp_path, ha_report_archive, sender_address
) -> None:
    output_dir = tmp_path / "decrypted"

    with pytest.raises(ReportDecryptionError, match="not addressed to"):
        decrypt_archive(
            ha_report_archive, output_dir, stranger_account(), sender_address
        )

    assert not output_dir.exists()
    assert not (tmp_path / "decrypted.partial").exists()


def test_rejects_wrong_sender_address(
    tmp_path, ha_report_archive, recipient_account
) -> None:
    wrong_sender = stranger_account().address

    with pytest.raises(ReportDecryptionError, match="cannot unwrap the secret key"):
        decrypt_archive(
            ha_report_archive, tmp_path / "out", recipient_account, wrong_sender
        )


def test_failure_keeps_previous_output_untouched(
    tmp_path, ha_report_archive, recipient_account, sender_address
) -> None:
    output_dir = tmp_path / "decrypted"
    decrypt_archive(ha_report_archive, output_dir, recipient_account, sender_address)
    before = sorted(path.name for path in output_dir.iterdir())

    with pytest.raises(ReportDecryptionError):
        decrypt_archive(
            ha_report_archive, output_dir, stranger_account(), sender_address
        )

    assert sorted(path.name for path in output_dir.iterdir()) == before


@pytest.mark.parametrize("member", ["../evil.enc", "nested/file.enc", "dir/"])
def test_rejects_unexpected_archive_entries(
    tmp_path, member, recipient_account, sender_address
) -> None:
    archive = make_zip(tmp_path / "bad.zip", {member: "{}"})

    with pytest.raises(ReportDecryptionError, match="unexpected archive entry"):
        decrypt_archive(archive, tmp_path / "out", recipient_account, sender_address)


def test_rejects_non_zip_file(tmp_path, recipient_account, sender_address) -> None:
    archive = tmp_path / "not.zip"
    archive.write_text("definitely not a zip", "utf-8")

    with pytest.raises(ReportDecryptionError, match="not a valid zip archive"):
        decrypt_archive(archive, tmp_path / "out", recipient_account, sender_address)


def test_rejects_empty_archive(tmp_path, recipient_account, sender_address) -> None:
    archive = make_zip(tmp_path / "empty.zip", {})

    with pytest.raises(ReportDecryptionError, match="archive is empty"):
        decrypt_archive(archive, tmp_path / "out", recipient_account, sender_address)


def test_rejects_too_many_members(
    tmp_path, monkeypatch, recipient_account, sender_address
) -> None:
    monkeypatch.setattr(decryptor, "MAX_ARCHIVE_MEMBERS", 2)
    archive = make_zip(tmp_path / "many.zip", {f"{i}.enc": "{}" for i in range(3)})

    with pytest.raises(ReportDecryptionError, match="limit is 2"):
        decrypt_archive(archive, tmp_path / "out", recipient_account, sender_address)


def test_rejects_invalid_package(tmp_path, recipient_account, sender_address) -> None:
    archive = make_zip(tmp_path / "bad.zip", {"a.enc": "not json"})

    with pytest.raises(
        ReportDecryptionError, match="a.enc: not an encryption package"
    ):
        decrypt_archive(archive, tmp_path / "out", recipient_account, sender_address)


def test_parse_decrypted_splits_payload_and_meta() -> None:
    text = json.dumps({"payload": "data", "meta": {"orig_file_name": "x.log"}})

    assert parse_decrypted(text) == ("data", {"orig_file_name": "x.log"})
    assert parse_decrypted("plain text") == ("plain text", None)
    assert parse_decrypted('{"other": 1}') == ('{"other": 1}', None)


@pytest.mark.parametrize(
    ("meta", "expected"),
    [
        ({"orig_file_name": "home-assistant.log"}, "home-assistant.log"),
        ({"orig_file_name": ".storage/trace.saved_traces"}, "trace.saved_traces"),
        ({"orig_file_name": "../../etc/passwd"}, "passwd"),
        (None, "tmpabc.txt"),
    ],
)
def test_output_file_name_uses_only_the_base_name(meta, expected) -> None:
    assert decryptor.output_file_name(meta, "tmpabc.enc") == expected
