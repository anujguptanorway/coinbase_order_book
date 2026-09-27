from collections import deque

from pytest import approx

from marketdata.gold.mid_price_metrics import (
    LONGEST_WINDOW,
    add_mid_price_sample,
    mid_price,
    mid_price_window_averages,
)
from marketdata.silver.normalize import FeedRecord
from marketdata.silver.order_book import OrderBook


def _record(side: str, price: float, size: float, ts: float = 1.0) -> FeedRecord:
    return FeedRecord("BTC-USD", side, price, size, ts)


def test_mid_price_is_none_on_an_empty_book() -> None:
    assert mid_price(OrderBook()) is None


def test_mid_price_is_none_with_only_one_side() -> None:
    book = OrderBook()
    book.apply_records("snapshot", [_record("bid", 100.0, 1.0)])

    assert mid_price(book) is None


def test_mid_price_is_the_midpoint_of_best_bid_and_ask() -> None:
    book = OrderBook()
    book.apply_records("snapshot", [_record("bid", 100.0, 1.0), _record("ask", 103.0, 1.0)])

    assert mid_price(book) == 101.5


def test_empty_samples_yield_none_for_every_window() -> None:
    assert mid_price_window_averages(deque()) == {"1m": None, "5m": None, "15m": None}


def test_average_over_a_single_sample_is_that_sample() -> None:
    samples = deque()
    add_mid_price_sample(samples, timestamp=100.0, mid_price=50.0)

    averages = mid_price_window_averages(samples)
    assert averages["1m"] == 50.0
    assert averages["5m"] == 50.0
    assert averages["15m"] == 50.0


def test_window_average_weights_prices_by_their_duration() -> None:
    samples = deque()
    add_mid_price_sample(samples, timestamp=0.0, mid_price=10.0)  # outside the 1m window
    add_mid_price_sample(samples, timestamp=100.0, mid_price=20.0)  # 20s before "now"
    add_mid_price_sample(samples, timestamp=120.0, mid_price=30.0)  # "now"

    averages = mid_price_window_averages(samples)
    assert averages["1m"] == approx(40 / 3)
    assert averages["5m"] == approx(35 / 3)


def test_five_minute_average_is_time_weighted_for_irregular_intervals() -> None:
    samples = deque()
    add_mid_price_sample(samples, timestamp=0.0, mid_price=100.0)
    add_mid_price_sample(samples, timestamp=240.0, mid_price=110.0)
    add_mid_price_sample(samples, timestamp=300.0, mid_price=110.0)

    averages = mid_price_window_averages(samples)

    assert averages["5m"] == 102.0


def test_previous_sample_is_retained_to_cover_the_window_boundary() -> None:
    samples = deque()
    add_mid_price_sample(samples, timestamp=0.0, mid_price=10.0)
    add_mid_price_sample(samples, timestamp=LONGEST_WINDOW + 1, mid_price=20.0)

    assert len(samples) == 2
    assert mid_price_window_averages(samples)["15m"] == 10.0
