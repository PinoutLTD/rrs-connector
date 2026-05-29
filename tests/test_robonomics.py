import pytest

from rrs_connector.robonomics.datalog_reader import (
    DatalogIndexRange,
    DatalogReader,
    DatalogRecord,
)

ADDRESS_1 = "4DVyLjBGM99Np9XBhADqkbTw9JGn2LgnFpHAQ8TBSjGPZ5fN"
CID_1 = "QmWue3YfuZvuRvgcNb4vZuheX9TaZ9E1b8aCdxSoaGTbVN"
CID_2 = "QmUqNnzdZnic61UYTuKT9EzBNzMW6jc5uHSFk4Xzd3iM93"
CID_3 = "QmZK64M7M31mkMsDd8yQa1dfX4a4KeDCyaUMsTuzsKq6LC"

TIMESTAMP_1 = 1780065423000
TIMESTAMP_2 = 1780072623000
TIMESTAMP_3 = 1780079823000


class FakeDatalog:
    def __init__(
        self,
        start: int = 0,
        end: int = 3,
        items: dict[int, tuple[int, str | None]] | None = None,
    ) -> None:
        self.start = start
        self.end = end
        self.items = items or {
            0: (TIMESTAMP_1, CID_1),
            1: (TIMESTAMP_2, CID_2),
            2: (TIMESTAMP_3, CID_3),
        }

    def get_index(self, sender_address: str) -> dict[str, int]:
        return {"start": self.start, "end": self.end}

    def get_item(self, addr: str, index: int) -> tuple[int, str | None] | None:
        return self.items.get(index)


def make_datalog_reader(fake_datalog: FakeDatalog) -> DatalogReader:
    reader = DatalogReader.__new__(DatalogReader)
    reader.datalog = fake_datalog
    return reader


@pytest.fixture
def datalog_reader() -> DatalogReader:
    return make_datalog_reader(FakeDatalog())


def test_get_index_range(datalog_reader: DatalogReader) -> None:
    index_range = datalog_reader.get_index_range(ADDRESS_1)

    assert index_range == DatalogIndexRange(0, 3)


def test_get_item(datalog_reader: DatalogReader) -> None:
    record = datalog_reader.get_item(ADDRESS_1, 0)

    assert record == DatalogRecord(ADDRESS_1, 0, TIMESTAMP_1, CID_1)


def test_get_item_returns_none_for_missing_item(
    datalog_reader: DatalogReader,
) -> None:
    record = datalog_reader.get_item(ADDRESS_1, 42)

    assert record is None


def test_get_item_returns_none_for_zero_timestamp() -> None:
    reader = make_datalog_reader(FakeDatalog(items={0: (0, CID_1)}))

    record = reader.get_item(ADDRESS_1, 0)

    assert record is None


def test_get_item_returns_none_for_empty_payload() -> None:
    reader = make_datalog_reader(FakeDatalog(items={0: (TIMESTAMP_1, None)}))

    record = reader.get_item(ADDRESS_1, 0)

    assert record is None


def test_list_new_records_reads_latest_for_empty_cursor(
    datalog_reader: DatalogReader,
) -> None:
    records = datalog_reader.list_new_records(ADDRESS_1, None)

    assert records == [DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3)]


def test_list_new_records_reads_records_after_cursor(
    datalog_reader: DatalogReader,
) -> None:
    records = datalog_reader.list_new_records(ADDRESS_1, 0)

    assert records == [
        DatalogRecord(ADDRESS_1, 1, TIMESTAMP_2, CID_2),
        DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3),
    ]


def test_list_new_records_starts_from_available_range_start() -> None:
    reader = make_datalog_reader(
        FakeDatalog(
            start=5,
            end=8,
            items={
                5: (TIMESTAMP_1, CID_1),
                6: (TIMESTAMP_2, CID_2),
                7: (TIMESTAMP_3, CID_3),
            },
        ),
    )

    records = reader.list_new_records(ADDRESS_1, 2)

    assert records == [
        DatalogRecord(ADDRESS_1, 5, TIMESTAMP_1, CID_1),
        DatalogRecord(ADDRESS_1, 6, TIMESTAMP_2, CID_2),
        DatalogRecord(ADDRESS_1, 7, TIMESTAMP_3, CID_3),
    ]


def test_list_new_records_returns_empty_list_for_empty_range() -> None:
    reader = make_datalog_reader(FakeDatalog(start=3, end=3, items={}))

    records = reader.list_new_records(ADDRESS_1, None)

    assert records == []


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

    records = reader.list_new_records(ADDRESS_1, 0)

    assert records == [DatalogRecord(ADDRESS_1, 2, TIMESTAMP_3, CID_3)]


def test_datalog_reader_raises_for_empty_wss_endpoints() -> None:
    with pytest.raises(ValueError, match="At least one WSS endpoint is required"):
        DatalogReader(
            recipient_seed="test seed",
            wss_endpoints=[],
            request_timeout_seconds=60,
        )
