"""Multi-envelope decryption of Home Assistant report archives.

The archive layout and envelope format mirror rrs-ha-integration
(`utils/file_handler.py`, `utils/encrypt_tools.py`): a zip of separately
encrypted files with random names, whose original names are carried in the
encrypted metadata.
"""

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from zipfile import BadZipFile, ZipFile, ZipInfo

from nacl.secret import SecretBox
from robonomicsinterface import Account
from substrateinterface import Keypair, KeypairType

from rrs_connector.reports.permissions import PRIVATE, ArtifactModes

MAX_ARCHIVE_MEMBERS = 32
# HA sends at most 3 MiB of plaintext per file; hex encoding doubles it.
MAX_MEMBER_BYTES = 32 * 1024 * 1024


class ReportDecryptionError(RuntimeError):
    pass


@dataclass(frozen=True)
class DecryptedFile:
    name: str
    path: Path
    size: int


def decrypt_msg(
    encrypted_msg: str,
    sender_public_key: bytes,
    recipient_keypair: Keypair,
) -> bytes:
    if encrypted_msg[:2] == "0x":
        encrypted_msg = encrypted_msg[2:]
    return recipient_keypair.decrypt_message(
        bytes.fromhex(encrypted_msg), sender_public_key
    )


def multi_envelope_decrypt_data(
    encryption_package: str,
    recipient_account: Account,
    sender_address: str,
) -> str:
    """Unwrap the symmetric key for the recipient, then decrypt the data."""

    try:
        package = json.loads(encryption_package)
        encrypted_keys = package["keys"]
        encrypted_data_hex = package["data"]
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        raise ReportDecryptionError("invalid encryption package") from e

    if not isinstance(encrypted_keys, dict) or not isinstance(encrypted_data_hex, str):
        raise ReportDecryptionError("invalid encryption package structure")

    encrypted_key = encrypted_keys.get(recipient_account.get_address())
    if not encrypted_key:
        raise ReportDecryptionError("package is not encrypted for this recipient")

    try:
        sender_keypair = Keypair(
            ss58_address=sender_address, crypto_type=KeypairType.ED25519
        )
        secret_key = decrypt_msg(
            encrypted_key, sender_keypair.public_key, recipient_account.keypair
        )
    except Exception as e:
        raise ReportDecryptionError("cannot unwrap the secret key") from e

    try:
        if encrypted_data_hex[:2] == "0x":
            encrypted_data_hex = encrypted_data_hex[2:]
        data = SecretBox(secret_key).decrypt(bytes.fromhex(encrypted_data_hex))
        return data.decode("utf-8")
    except Exception as e:
        raise ReportDecryptionError("cannot decrypt the payload") from e


def parse_decrypted(text: str) -> tuple[str, dict | None]:
    """Split `{"payload": ..., "meta": ...}` or return plain data without meta."""

    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return text, None
    if isinstance(obj, dict) and "payload" in obj:
        meta = obj.get("meta")
        return obj["payload"], meta if isinstance(meta, dict) else None
    return text, None


def output_file_name(meta: dict | None, member_name: str) -> str:
    original = meta.get("orig_file_name") if meta else None
    name = Path(str(original)).name if original else f"{Path(member_name).stem}.txt"
    if name in ("", ".", ".."):
        raise ReportDecryptionError(f"{member_name}: invalid original file name")
    return name


def validate_member(info: ZipInfo) -> None:
    if info.is_dir() or "/" in info.filename or "\\" in info.filename:
        raise ReportDecryptionError(f"unexpected archive entry: {info.filename!r}")
    if info.file_size > MAX_MEMBER_BYTES:
        raise ReportDecryptionError(
            f"{info.filename}: {info.file_size} bytes exceeds {MAX_MEMBER_BYTES}"
        )


def decrypt_archive(
    archive_path: Path,
    output_dir: Path,
    recipient_account: Account,
    sender_address: str,
    modes: ArtifactModes = PRIVATE,
) -> list[DecryptedFile]:
    """Decrypt every archive member into `output_dir`, all or nothing.

    Encrypted members are read in memory and never extracted, so archive entry
    names are never used as paths. Files are written to a staging directory
    that replaces `output_dir` only after every member was decrypted.
    """

    staging_dir = output_dir.with_name(output_dir.name + ".partial")
    shutil.rmtree(staging_dir, ignore_errors=True)
    staging_dir.mkdir(parents=True)
    # mkdir's mode argument is masked and drops setgid, so set it explicitly.
    staging_dir.chmod(modes.dir_mode)

    try:
        files = _decrypt_members(
            archive_path, staging_dir, recipient_account, sender_address, modes
        )
        if output_dir.exists():
            shutil.rmtree(output_dir)
        staging_dir.rename(output_dir)
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    return [
        DecryptedFile(file.name, output_dir / file.name, file.size) for file in files
    ]


def _decrypt_members(
    archive_path: Path,
    staging_dir: Path,
    recipient_account: Account,
    sender_address: str,
    modes: ArtifactModes,
) -> list[DecryptedFile]:
    try:
        archive = ZipFile(archive_path)
    except (BadZipFile, OSError) as e:
        raise ReportDecryptionError(f"not a valid zip archive: {e}") from e

    with archive:
        members = archive.infolist()
        if not members:
            raise ReportDecryptionError("archive is empty")
        if len(members) > MAX_ARCHIVE_MEMBERS:
            raise ReportDecryptionError(
                f"archive has {len(members)} entries, limit is {MAX_ARCHIVE_MEMBERS}"
            )
        for info in members:
            validate_member(info)

        files: list[DecryptedFile] = []
        for info in sorted(members, key=lambda member: member.filename):
            try:
                package = archive.read(info).decode("utf-8")
                payload, meta = parse_decrypted(
                    multi_envelope_decrypt_data(
                        package, recipient_account, sender_address
                    )
                )
            except ReportDecryptionError as e:
                raise ReportDecryptionError(f"{info.filename}: {e}") from e
            except (BadZipFile, UnicodeDecodeError, OSError) as e:
                raise ReportDecryptionError(f"{info.filename}: {e}") from e

            name = output_file_name(meta, info.filename)
            path = staging_dir / name
            if path.exists():
                raise ReportDecryptionError(f"duplicate file name in archive: {name}")
            data = str(payload).encode("utf-8")
            path.write_bytes(data)
            path.chmod(modes.file_mode)
            files.append(DecryptedFile(name, path, len(data)))

    return sorted(files, key=lambda file: file.name)
