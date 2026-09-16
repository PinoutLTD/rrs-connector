from collections.abc import Sequence
from dataclasses import dataclass

from robonomicsinterface import Account, Datalog
from substrateinterface import SubstrateInterface


@dataclass(frozen=True)
class DatalogRecord:
    sender_address: str
    datalog_index: int
    timestamp_ms: int
    payload: str


@dataclass(frozen=True)
class DatalogIndexRange:
    start: int
    end: int


@dataclass(frozen=True)
class DatalogScan:
    """Records at or after the cursor, oldest to newest.

    `reached_cursor` is False when a cursor was given but the ring buffer no
    longer holds a record at or before it: older records were overwritten
    before they could be read.
    """

    records: list[DatalogRecord]
    reached_cursor: bool


def ring_buffer_indices(index_range: DatalogIndexRange, window_size: int) -> list[int]:
    """Datalog slots from oldest to newest.

    The datalog pallet keeps the last `window_size - 1` records per account and
    reuses slots once full, so `end < start` means the buffer has wrapped.
    """
    start, end = index_range.start, index_range.end
    count = end - start if start <= end else window_size + end - start
    return [(start + offset) % window_size for offset in range(count)]


class DatalogReader:
    def __init__(
        self,
        wss_endpoints: Sequence[str],
        request_timeout_seconds: int,
    ) -> None:
        self.wss_endpoints = list(wss_endpoints)

        if not self.wss_endpoints:
            raise ValueError("At least one WSS endpoint is required")

        self.current_wss: str = self.wss_endpoints[0]
        # Reading chain state is public, so no keypair is needed here.
        self.datalog = Datalog(Account(remote_ws=self.current_wss))
        self._window_size: int | None = None

    def get_window_size(self) -> int:
        if self._window_size is None:
            with SubstrateInterface(url=self.current_wss) as substrate:
                constant = substrate.get_constant("Datalog", "WindowSize")
                self._window_size = int(constant.value)
        return self._window_size

    def get_index_range(self, sender_address: str) -> DatalogIndexRange:
        index_info = self.datalog.get_index(sender_address)
        start = int(index_info["start"])
        end = int(index_info["end"])
        return DatalogIndexRange(start, end)

    def get_item(self, sender_address: str, datalog_index: int) -> DatalogRecord | None:
        # robonomicsinterface.get_item(index=0) treats 0 as "latest";
        # query storage directly so explicit datalog indices stay exact.
        record = self.datalog._service_functions.chainstate_query(
            "Datalog",
            "DatalogItem",
            [sender_address, datalog_index],
        )

        if record is None:
            return None

        timestamp, datalog_content = record

        if timestamp == 0 or datalog_content is None:
            return None

        return DatalogRecord(
            sender_address,
            datalog_index,
            int(timestamp),
            payload=str(datalog_content),
        )

    def list_last_records(self, sender_address: str, count: int) -> list[DatalogRecord]:
        """The newest `count` records still held by the ring, oldest first."""

        if count < 1:
            raise ValueError("count must be at least 1")

        indices = ring_buffer_indices(
            self.get_index_range(sender_address), self.get_window_size()
        )
        newest_first: list[DatalogRecord] = []

        for index in reversed(indices):
            record = self.get_item(sender_address, index)
            if record is not None:
                newest_first.append(record)
            if len(newest_first) == count:
                break

        return list(reversed(newest_first))

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
        indices = ring_buffer_indices(
            self.get_index_range(sender_address), self.get_window_size()
        )

        if not indices:
            return DatalogScan(records=[], reached_cursor=True)

        if cursor_timestamp_ms is None:
            for index in reversed(indices):
                record = self.get_item(sender_address, index)
                if record is not None:
                    return DatalogScan(records=[record], reached_cursor=True)
            return DatalogScan(records=[], reached_cursor=True)

        newest_first: list[DatalogRecord] = []
        reached_cursor = False

        for index in reversed(indices):
            record = self.get_item(sender_address, index)
            if record is None:
                continue
            if record.timestamp_ms <= cursor_timestamp_ms:
                reached_cursor = True
            if record.timestamp_ms < cursor_timestamp_ms:
                break
            newest_first.append(record)

        return DatalogScan(
            records=list(reversed(newest_first)), reached_cursor=reached_cursor
        )
