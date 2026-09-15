import pytest

from rrs_connector.robonomics.datalog_reader import (
    DatalogIndexRange,
    DatalogReader,
    DatalogRecord,
    DatalogScan,
    ring_buffer_indices,
)

ADDRESS_1 = "4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN"
CID_1 = "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"
CID_2 = "QmUqNnzdZnic61UYTuKT9EzBNzMW6jc5uHSFk4Xzd3iM93"
CID_3 = "QmZK64M7M31mkMsDd8yQa1dfX4a4KeDCyaUMsTuzsKq6LC"

TIMESTAMP_1 = 1780065423000
TIMESTAMP_2 = 1780072623000
TIMESTAMP_3 = 1780079823000

WINDOW_SIZE = 128


class FakeDatalog:
    def __init__(
        self,
        start: int = 0,
        end: int = 3,
        items: dict[int, tuple[int, str | None]] | None = None,
    ) -> None:
        self.start = start
        self.end = end
        self.items = (
            items
            if items is not None
            else {
                0: (TIMESTAMP_1, CID_1),
                1: (TIMESTAMP_2, CID_2),
                2: (TIMESTAMP_3, CID_3),
            }
        )
        self._service_functions = FakeServiceFunctions(self.items)

    def get_index(self, sender_address: str) -> dict[str, int]:
        return {"start": self.start, "end": self.end}


class FakeServiceFunctions:
    def __init__(self, items: dict[int, tuple[int, str | None]]) -> None:
        self.items = items
        self.queried_indices: list[int] = []

    def chainstate_query(
        self,
        module: str,
        storage_function: str,
        params: list[str | int],
    ) -> tuple[int, str | None] | None:
        assert module == "Datalog"
        assert storage_function == "DatalogItem"
        self.queried_indices.append(int(params[1]))
        return self.items.get(int(params[1]))


class FakeDatalogWithBuggyPublicGetItem(FakeDatalog):
    def get_item(self, addr: str, index: int) -> tuple[int, str | None] | None:
        if index == 0:
            return self.items[2]
        return self.items.get(index)


def make_datalog_reader(fake_datalog: FakeDatalog) -> DatalogReader:
    reader = DatalogReader.__new__(DatalogReader)
    reader.datalog = fake_datalog
    reader._window_size = WINDOW_SIZE
    return reader


@pytest.fixture
def datalog_reader() -> DatalogReader:
    return make_datalog_reader(FakeDatalog())


def wrapped_datalog() -> FakeDatalog:
    """A full buffer that has wrapped, like a live sender with start=96, end=95."""
    return FakeDatalog(
        start=96,
        end=95,
        items={
            127: (TIMESTAMP_1, CID_1),
            93: (TIMESTAMP_2, CID_2),
            94: (TIMESTAMP_3, CID_3),
        },
    )


def test_ring_buffer_indices_for_empty_buffer() -> None:
    assert ring_buffer_indices(DatalogIndexRange(3, 3), WINDOW_SIZE) == []


def test_ring_buffer_indices_for_linear_buffer() -> None:
    assert ring_buffer_indices(DatalogIndexRange(0, 3), WINDOW_SIZE) == [0, 1, 2]


def test_ring_buffer_indices_for_wrapped_buffer() -> None:
    indices = ring_buffer_indices(DatalogIndexRange(96, 95), WINDOW_SIZE)

    assert len(indices) == WINDOW_SIZE - 1
    assert indices[:2] == [96, 97]
    assert indices[31:33] == [127, 0]
    assert indices[-2:] == [93, 94]
    assert 95 not in indices


def test_get_index_range(datalog_reader: DatalogReader) -> None:
    index_range = datalog_reader.get_index_range(ADDRESS_1)

    assert index_range == DatalogIndexRange(0, 3)


def test_get_item(datalog_reader: DatalogReader) -> None:
    record = datalog_reader.get_item(ADDRESS_1, 0)

    assert record == DatalogRecord(ADDRESS_1, 0, TIMESTAMP_1, CID_1)


def test_get_item_reads_zero_index_directly() -> None:
    reader = make_datalog_reader(FakeDatalogWithBuggyPublicGetItem())

    record = reader.get_item(ADDRESS_1, 0)

    assert record == DatalogRecord(ADDRESS_1, 0, TIMESTAMP_1, CID_1)


def test_get_item_returns_none_for_missing_item(
    datalog_reader: DatalogReader,
) -> None:
    assert datalog_reader.get_item(ADDRESS_1, 42) is None


def test_get_item_returns_none_for_zero_timestamp() -> None:
    reader = make_datalog_reader(FakeDatalog(items={0: (0, CID_1)}))

    assert reader.get_item(ADDRESS_1, 0) is None


def test_get_item_returns_none_for_empty_payload() -> None:
    reader = make_datalog_reader(FakeDatalog(items={0: (TIMESTAMP_1, None)}))

    assert reader.get_item(ADDRESS_1, 0) is None


def test_list_new_records_reads_latest_for_empty_cursor(
    datalog_reader: DatalogReader,
) -> None:
    scan = datalog_reader.list_new_records(ADDRESS_1, None)

    assert scan == DatalogScan([DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3)], True)


def test_list_new_records_reads_records_not_older_than_cursor(
    datalog_reader: DatalogReader,
) -> None:
    scan = datalog_reader.list_new_records(ADDRESS_1, TIMESTAMP_2)

    assert scan == DatalogScan(
        [
            DatalogRecord(ADDRESS_1, 1, TIMESTAMP_2, CID_2),
            DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3),
        ],
        reached_cursor=True,
    )


def test_list_new_records_stops_at_first_record_older_than_cursor() -> None:
    fake_datalog = FakeDatalog()
    reader = make_datalog_reader(fake_datalog)

    reader.list_new_records(ADDRESS_1, TIMESTAMP_3)

    assert fake_datalog._service_functions.queried_indices == [2, 1]


def test_list_new_records_handles_wrapped_buffer_without_cursor() -> None:
    reader = make_datalog_reader(wrapped_datalog())

    scan = reader.list_new_records(ADDRESS_1, None)

    assert scan.records == [DatalogRecord(ADDRESS_1, 94, TIMESTAMP_3, CID_3)]


def test_list_new_records_handles_wrapped_buffer_after_cursor() -> None:
    reader = make_datalog_reader(wrapped_datalog())

    scan = reader.list_new_records(ADDRESS_1, TIMESTAMP_2)

    assert scan == DatalogScan(
        [
            DatalogRecord(ADDRESS_1, 93, TIMESTAMP_2, CID_2),
            DatalogRecord(ADDRESS_1, 94, TIMESTAMP_3, CID_3),
        ],
        reached_cursor=True,
    )


def test_list_new_records_crosses_the_wrap_point_in_order() -> None:
    reader = make_datalog_reader(wrapped_datalog())

    scan = reader.list_new_records(ADDRESS_1, TIMESTAMP_1)

    assert [record.datalog_index for record in scan.records] == [127, 93, 94]
    assert scan.reached_cursor is True


def test_list_new_records_reports_gap_when_cursor_was_overwritten() -> None:
    reader = make_datalog_reader(wrapped_datalog())

    scan = reader.list_new_records(ADDRESS_1, TIMESTAMP_1 - 1000)

    assert [record.datalog_index for record in scan.records] == [127, 93, 94]
    assert scan.reached_cursor is False


def test_list_new_records_returns_empty_scan_for_empty_buffer() -> None:
    reader = make_datalog_reader(FakeDatalog(start=3, end=3, items={}))

    assert reader.list_new_records(ADDRESS_1, None) == DatalogScan([], True)
    assert reader.list_new_records(ADDRESS_1, TIMESTAMP_1) == DatalogScan([], True)


def test_list_new_records_skips_empty_items() -> None:
    reader = make_datalog_reader(
        FakeDatalog(
            items={
                0: (TIMESTAMP_1, CID_1),
                1: (0, CID_2),
                2: (TIMESTAMP_3, CID_3),
            },
        ),
    )

    scan = reader.list_new_records(ADDRESS_1, TIMESTAMP_1)

    assert scan.records == [
        DatalogRecord(ADDRESS_1, 0, TIMESTAMP_1, CID_1),
        DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3),
    ]


def test_datalog_reader_raises_for_empty_wss_endpoints() -> None:
    with pytest.raises(ValueError, match="At least one WSS endpoint is required"):
        DatalogReader(wss_endpoints=[], request_timeout_seconds=60)
