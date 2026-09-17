"""Choosing which integrator key opens a report.

A site encrypts its report for the addresses it was configured with, and the
envelope lists those addresses in the clear. With more than one recipient key
in service — the old one that existing sites still use, and a fresh one for
new installs — the report itself says which key is needed, so nothing is
tried blindly.

Only addresses from our own configuration are ever looked up in Proton Pass.
The envelope comes from outside; it chooses among our keys, it never names
new ones.
"""

import json
import logging
from collections.abc import Callable
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from robonomicsinterface import Account

from rrs_connector.reports.decryptor import (
    MAX_ARCHIVE_MEMBERS,
    MAX_MEMBER_BYTES,
    ReportDecryptionError,
)

LOGGER = logging.getLogger(__name__)

KeyLoader = Callable[[str], Account]


class NotAddressedToUsError(ReportDecryptionError):
    """The report was encrypted for addresses we hold no key for."""


def archive_recipients(archive_path: Path) -> set[str]:
    """Addresses every file of the archive is encrypted for."""

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

        common: set[str] | None = None
        for info in members:
            if info.file_size > MAX_MEMBER_BYTES:
                raise ReportDecryptionError(
                    f"{info.filename}: {info.file_size} bytes exceeds "
                    f"{MAX_MEMBER_BYTES}"
                )
            try:
                keys = json.loads(archive.read(info))["keys"]
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError) as e:
                raise ReportDecryptionError(
                    f"{info.filename}: invalid encryption package"
                ) from e
            if not isinstance(keys, dict):
                raise ReportDecryptionError(
                    f"{info.filename}: invalid encryption package structure"
                )
            addresses = set(keys)
            common = addresses if common is None else common & addresses

    return common or set()


class RecipientKeys:
    """Our recipient keys, loaded from Proton Pass only when a report needs one."""

    def __init__(self, addresses: list[str], load: KeyLoader) -> None:
        if not addresses:
            raise ValueError("at least one recipient address is required")
        self.addresses = list(addresses)
        self._load = load
        self._accounts: dict[str, Account] = {}

    def choose(self, archive_path: Path) -> str:
        """Our address the archive is encrypted for, in configuration order."""

        recipients = archive_recipients(archive_path)
        for address in self.addresses:
            if address in recipients:
                return address
        raise NotAddressedToUsError(
            "encrypted for "
            + (", ".join(sorted(recipients)) or "nobody")
            + "; none of them is a configured recipient key"
        )

    def account(self, address: str) -> Account:
        """Load a key once per run. Errors propagate: the report stays pending."""

        if address not in self.addresses:
            raise ValueError(f"{address} is not a configured recipient key")
        if address not in self._accounts:
            LOGGER.info("Loading recipient key %s from Proton Pass", address)
            self._accounts[address] = self._load(address)
        return self._accounts[address]
