from collections.abc import Sequence
from dataclasses import dataclass

from robonomicsinterface import Account, Datalog
from substrateinterface import KeypairType


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


class DatalogReader:
    def __init__(
        self,
        recipient_seed: str,
        wss_endpoints: Sequence[str],
        request_timeout_seconds: int,
    ) -> None:
        self.wss_endpoints = list(wss_endpoints)

        if not self.wss_endpoints:
            raise ValueError("At least one WSS endpoint is required")

        self.current_wss: str = self.wss_endpoints[0]
        self.recipient_account: Account = Account(
            recipient_seed,
            crypto_type=KeypairType.ED25519,
            remote_ws=self.current_wss,
        )

        self.datalog = Datalog(
            self.recipient_account, rws_sub_owner=self.recipient_account.get_address()
        )

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

    def list_new_records(
        self,
        sender_address: str,
        last_scanned_datalog_index: int | None,
    ) -> list[DatalogRecord]:

        index_range = self.get_index_range(sender_address)

        if index_range.end <= index_range.start:
            return []

        if last_scanned_datalog_index is None:
            first_index = max(index_range.start, index_range.end - 1)
        else:
            first_index = max(index_range.start, last_scanned_datalog_index + 1)

        records: list[DatalogRecord] = []

        for index in range(first_index, index_range.end):
            record = self.get_item(sender_address, index)
            if record is not None:
                records.append(record)

        return records
