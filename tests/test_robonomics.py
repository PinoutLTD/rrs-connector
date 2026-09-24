import pytest
from robonomicsinterface import (
    ROBONOMICS_GENESIS_HASH,
    ConnectionLost,
    DatalogItem,
    RpcError,
)

from rrs_connector.pipeline import create_datalog_reader
from rrs_connector.robonomics.datalog_reader import (
    ROBONOMICS_KUSAMA_GENESIS_HASH,
    DatalogReader,
    DatalogRecord,
    DatalogScan,
)

ADDRESS_1 = "4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN"
CID_1 = "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"
CID_2 = "QmUqNnzdZnic61UYTuKT9EzBNzMW6jc5uHSFk4Xzd3iM93"
CID_3 = "QmZK64M7M31mkMsDd8yQa1dfX4a4KeDCyaUMsTuzsKq6LC"

TIMESTAMP_1 = 1780065423000
TIMESTAMP_2 = 1780072623000
TIMESTAMP_3 = 1780079823000


def item(index: int, timestamp_ms: int, data: str | bytes) -> DatalogItem:
    raw = data.encode() if isinstance(data, str) else data
    return DatalogItem(index, timestamp_ms, raw)


LINEAR = [
    item(0, TIMESTAMP_1, CID_1),
    item(1, TIMESTAMP_2, CID_2),
    item(2, TIMESTAMP_3, CID_3),
]
# A full buffer that has wrapped: the library returns live slots oldest first,
# across the end of the window.
WRAPPED = [
    item(127, TIMESTAMP_1, CID_1),
    item(93, TIMESTAMP_2, CID_2),
    item(94, TIMESTAMP_3, CID_3),
]


class FakeDatalog:
    def __init__(self, items: list[DatalogItem], failures: list[Exception]) -> None:
        self._items = items
        self.failures = failures
        self.calls = 0

    def items(self, address: str) -> list[DatalogItem]:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return list(self._items)


class FakeClient:
    def __init__(
        self, items: list[DatalogItem], failures: list[Exception] | None = None
    ) -> None:
        self.datalog = FakeDatalog(items, list(failures or []))
        self.closed = False

    def close(self) -> None:
        self.closed = True


def reader_for(
    items: list[DatalogItem],
    failures: list[Exception] | None = None,
    attempts: int = 1,
) -> DatalogReader:
    return DatalogReader(
        ["wss://node/"],
        request_timeout_seconds=15,
        max_attempts=attempts,
        client=FakeClient(items, failures),
    )


def test_records_are_read_oldest_first() -> None:
    records = reader_for(LINEAR).read_records(ADDRESS_1)

    assert records == [
        DatalogRecord(ADDRESS_1, 0, TIMESTAMP_1, CID_1),
        DatalogRecord(ADDRESS_1, 1, TIMESTAMP_2, CID_2),
        DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3),
    ]


def test_slot_zero_is_an_ordinary_record() -> None:
    # robonomics-interface 2.x read index 0 as "the latest"; 3.0 does not.
    records = reader_for([item(0, TIMESTAMP_1, CID_1)]).read_records(ADDRESS_1)

    assert records == [DatalogRecord(ADDRESS_1, 0, TIMESTAMP_1, CID_1)]


def test_records_that_are_not_ours_are_skipped() -> None:
    reader = reader_for(
        [
            item(0, TIMESTAMP_1, CID_1),
            item(1, 0, CID_2),  # no timestamp
            item(2, TIMESTAMP_2, b""),  # empty
            item(3, TIMESTAMP_2, b"\xff\xfe"),  # not UTF-8
            item(4, TIMESTAMP_3, CID_3),
        ]
    )

    assert [r.datalog_index for r in reader.read_records(ADDRESS_1)] == [0, 4]


def test_a_heartbeat_comes_through_as_its_text() -> None:
    heartbeat = '{"t":"hb","v":"1.1.0-beta.4","ha":"2026.7.0","ts":1789905600}'

    (record,) = reader_for([item(5, TIMESTAMP_1, heartbeat)]).read_records(ADDRESS_1)

    assert record.payload == heartbeat


def test_list_last_records_returns_the_newest_oldest_first() -> None:
    reader = reader_for(LINEAR)

    newest = reader.list_last_records(ADDRESS_1, 2)

    assert [r.payload for r in newest] == [CID_2, CID_3]
    assert len(reader.list_last_records(ADDRESS_1, 10)) == 3
    with pytest.raises(ValueError):
        reader.list_last_records(ADDRESS_1, 0)


def test_list_new_records_reads_latest_for_empty_cursor() -> None:
    scan = reader_for(LINEAR).list_new_records(ADDRESS_1, None)

    assert scan == DatalogScan([DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3)], True)


def test_list_new_records_reads_records_not_older_than_cursor() -> None:
    scan = reader_for(LINEAR).list_new_records(ADDRESS_1, TIMESTAMP_2)

    assert scan == DatalogScan(
        [
            DatalogRecord(ADDRESS_1, 1, TIMESTAMP_2, CID_2),
            DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3),
        ],
        reached_cursor=True,
    )


def test_list_new_records_after_the_newest_record_is_empty() -> None:
    scan = reader_for(LINEAR).list_new_records(ADDRESS_1, TIMESTAMP_3 + 1)

    assert scan == DatalogScan([], reached_cursor=True)


def test_list_new_records_handles_wrapped_buffer_without_cursor() -> None:
    scan = reader_for(WRAPPED).list_new_records(ADDRESS_1, None)

    assert scan.records == [DatalogRecord(ADDRESS_1, 94, TIMESTAMP_3, CID_3)]


def test_list_new_records_crosses_the_wrap_point_in_order() -> None:
    scan = reader_for(WRAPPED).list_new_records(ADDRESS_1, TIMESTAMP_1)

    assert [record.datalog_index for record in scan.records] == [127, 93, 94]
    assert scan.reached_cursor is True


def test_list_new_records_reports_gap_when_cursor_was_overwritten() -> None:
    scan = reader_for(WRAPPED).list_new_records(ADDRESS_1, TIMESTAMP_1 - 1000)

    assert [record.datalog_index for record in scan.records] == [127, 93, 94]
    assert scan.reached_cursor is False


def test_list_new_records_returns_empty_scan_for_empty_buffer() -> None:
    reader = reader_for([])

    assert reader.list_new_records(ADDRESS_1, None) == DatalogScan([], True)
    assert reader.list_new_records(ADDRESS_1, TIMESTAMP_1) == DatalogScan([], True)


def test_datalog_reader_raises_for_empty_wss_endpoints() -> None:
    with pytest.raises(ValueError, match="At least one WSS endpoint is required"):
        DatalogReader(wss_endpoints=[], request_timeout_seconds=60)


def test_a_dropped_connection_is_retried(monkeypatch) -> None:
    monkeypatch.setattr("rrs_connector.robonomics.retry.time.sleep", lambda _: None)
    reader = reader_for(LINEAR, failures=[ConnectionLost("closed")], attempts=3)

    assert len(reader.read_records(ADDRESS_1)) == 3
    assert reader.client.datalog.calls == 2


def test_an_error_answered_by_the_node_is_not_retried() -> None:
    error = RpcError("state_queryStorageAt", -32000, "bad params")
    reader = reader_for(LINEAR, failures=[error], attempts=3)

    with pytest.raises(RpcError):
        reader.read_records(ADDRESS_1)
    assert reader.client.datalog.calls == 1


def test_the_reader_closes_its_client() -> None:
    reader = reader_for(LINEAR)

    with reader:
        reader.read_records(ADDRESS_1)

    assert reader.client.closed is True


@pytest.mark.parametrize(
    ("network", "genesis"),
    [
        ("polkadot", ROBONOMICS_GENESIS_HASH),
        ("kusama", ROBONOMICS_KUSAMA_GENESIS_HASH),
    ],
)
def test_the_reader_checks_the_genesis_of_its_network(
    network_config, network, genesis
) -> None:
    config = network_config.model_copy(update={"network": network})

    # Nothing connects until the first read.
    with create_datalog_reader(config) as reader:
        assert reader.client.client.genesis_hash == genesis
        assert reader.client.client.retries == 0
        assert reader.client.client.timeout == 15
