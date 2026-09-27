from marketdata.gold.spread_metrics import spread, track_widest_spread
from marketdata.silver.normalize import FeedRecord
from marketdata.silver.order_book import OrderBook


def _record(side: str, price: float, size: float, ts: float = 1.0) -> FeedRecord:
    return FeedRecord("BTC-USD", side, price, size, ts)


def test_spread_is_none_on_an_empty_book() -> None:
    assert spread(OrderBook()) is None


def test_spread_is_lowest_ask_minus_highest_bid() -> None:
    book = OrderBook()
    book.apply_records("snapshot", [_record("bid", 100.0, 1.0), _record("ask", 103.0, 1.0)])

    assert spread(book) == 3.0


def test_first_observation_becomes_the_widest_spread() -> None:
    widest = track_widest_spread(None, spread=1.0, timestamp=100.0, bid_price=10.0, ask_price=11.0)

    assert widest.spread == 1.0
    assert widest.timestamp == 100.0


def test_a_bigger_spread_replaces_the_current_widest() -> None:
    widest = track_widest_spread(None, 1.0, 100.0, 10.0, 11.0)
    widest = track_widest_spread(widest, spread=2.0, timestamp=200.0, bid_price=9.0, ask_price=11.0)

    assert widest.spread == 2.0
    assert widest.timestamp == 200.0


def test_a_smaller_or_equal_spread_does_not_replace_the_current_widest() -> None:
    widest = track_widest_spread(None, 2.0, 100.0, 10.0, 12.0)

    smaller = track_widest_spread(widest, spread=1.0, timestamp=200.0, bid_price=10.0, ask_price=11.0)
    equal = track_widest_spread(widest, spread=2.0, timestamp=300.0, bid_price=10.0, ask_price=12.0)

    assert smaller is widest
    assert equal is widest
