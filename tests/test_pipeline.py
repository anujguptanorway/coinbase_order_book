from marketdata.pipeline import Pipeline
from marketdata.silver.normalize import FeedRecord, parse_message


def _record(side: str, price: float, size: float, timestamp: float) -> FeedRecord:
    return FeedRecord("BTC-USD", side, price, size, timestamp)


def test_mid_price_average_excludes_time_when_book_has_no_two_sides() -> None:
    pipeline = Pipeline()
    pipeline.ingest(
        "snapshot",
        [_record("bid", 100.0, 1.0, 0.0), _record("ask", 102.0, 1.0, 0.0)],
    )
    pipeline.ingest("l2update", [_record("ask", 102.0, 0.0, 60.0)])
    pipeline.ingest("l2update", [_record("ask", 104.0, 1.0, 180.0)])
    pipeline.ingest("l2update", [_record("bid", 100.0, 2.0, 240.0)])

    assert pipeline.rows()[6][1] == "101.50"


def test_empty_snapshot_preserves_its_timestamp_as_a_gap_marker() -> None:
    pipeline = Pipeline()
    parsed = parse_message(
        {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "time": "1970-01-01T00:00:30Z",
            "bids": [],
            "asks": [],
        }
    )

    assert parsed is not None
    pipeline.ingest(*parsed)

    assert pipeline.mid_prices[-1].timestamp == 30.0
    assert pipeline.mid_prices[-1].mid_price is None
