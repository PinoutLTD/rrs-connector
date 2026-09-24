import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from robonomicsinterface import ROBONOMICS_GENESIS_HASH, DatalogItem, RobonomicsSync

from rrs_connector.robonomics.retry import with_retries

LOGGER = logging.getLogger(__name__)

# The library checks the genesis of every node it connects to and knows only
# Robonomics on Polkadot; Kusama is ours to name.
ROBONOMICS_KUSAMA_GENESIS_HASH = (
    "0x631ccc82a078481584041656af292834e1ae6daab61d2875b4dd0c14bb9b17bc"
)
GENESIS_HASHES = {
    "polkadot": ROBONOMICS_GENESIS_HASH,
    "kusama": ROBONOMICS_KUSAMA_GENESIS_HASH,
}


@dataclass(frozen=True)
class DatalogRecord:
    sender_address: str
    datalog_index: int
    timestamp_ms: int
    payload: str


@dataclass(frozen=True)
class DatalogScan:
    """Records at or after the cursor, oldest to newest.

    `reached_cursor` is False when a cursor was given but the ring buffer no
    longer holds a record at or before it: older records were overwritten
    before they could be read.
    """

    records: list[DatalogRecord]
    reached_cursor: bool


class DatalogItems(Protocol):
    def items(self, address: str) -> list[DatalogItem]: ...


class DatalogClient(Protocol):
    """The part of `RobonomicsSync` the reader uses."""

    datalog: DatalogItems

    def close(self) -> None: ...


def as_record(sender_address: str, item: DatalogItem) -> DatalogRecord | None:
    """A datalog item as our record, or None if it cannot be one of ours.

    Sites publish text — a CID or a heartbeat's JSON — so a record that is not
    UTF-8, is empty or has no timestamp was not written by the integration.
    """

    payload = item.text
    if item.timestamp_ms == 0 or not payload:
        LOGGER.debug(
            "Skipping datalog #%d of %s: not a text record", item.index, sender_address
        )
        return None
    return DatalogRecord(sender_address, item.index, item.timestamp_ms, payload)


class DatalogReader:
    """Reads a site's datalog: every live record in one request per site.

    The ring buffer holds at most 127 records of at most 512 bytes, so reading
    all of them at once costs less than walking the slots one by one, and the
    cursor is applied here rather than on the chain.
    """

    def __init__(
        self,
        wss_endpoints: Sequence[str],
        request_timeout_seconds: int,
        max_attempts: int = 1,
        backoff_seconds: float = 0,
        genesis_hash: str | None = ROBONOMICS_GENESIS_HASH,
        client: DatalogClient | None = None,
    ) -> None:
        if not wss_endpoints:
            raise ValueError("At least one WSS endpoint is required")

        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        # Reading chain state is public, so no keypair is needed here. The
        # library does not retry on its own (retries=0): `with_retries` repeats
        # a failed read after a pause, and each attempt connects to the first
        # endpoint that answers — which is what a failed TLS handshake on the
        # first connection of a run needs.
        self.client: DatalogClient = client or RobonomicsSync(
            list(wss_endpoints),
            timeout=request_timeout_seconds,
            retries=0,
            genesis_hash=genesis_hash,
        )

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "DatalogReader":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def read_records(self, sender_address: str) -> list[DatalogRecord]:
        """Every live record of the site, oldest first."""

        items = with_retries(
            lambda: self.client.datalog.items(sender_address),
            what=f"Reading the datalog of {sender_address}",
            max_attempts=self.max_attempts,
            backoff_seconds=self.backoff_seconds,
        )
        records = (as_record(sender_address, item) for item in items)
        return [record for record in records if record is not None]

    def list_last_records(self, sender_address: str, count: int) -> list[DatalogRecord]:
        """The newest `count` records still held by the ring, oldest first."""

        if count < 1:
            raise ValueError("count must be at least 1")

        return self.read_records(sender_address)[-count:]

    def list_new_records(
        self,
        sender_address: str,
        cursor_timestamp_ms: int | None,
    ) -> DatalogScan:
        """Read records not older than the cursor.

        Without a cursor only the latest record is returned. Records with the
        cursor timestamp itself are included, so callers must store them
        idempotently; this keeps records published in the same block safe.
        """
        records = self.read_records(sender_address)

        if cursor_timestamp_ms is None or not records:
            return DatalogScan(records=records[-1:], reached_cursor=True)

        new = [r for r in records if r.timestamp_ms >= cursor_timestamp_ms]
        # Records are written in time order, so the oldest one decides whether
        # anything at or before the cursor is still held.
        reached_cursor = records[0].timestamp_ms <= cursor_timestamp_ms
        return DatalogScan(records=new, reached_cursor=reached_cursor)
