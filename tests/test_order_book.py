from marketdata.silver.normalize import FeedRecord
from marketdata.silver.order_book import OrderBook


def _record(side: str, price: float, size: float, ts: float = 1.0) -> FeedRecord:
    return FeedRecord("BTC-USD", side, price, size, ts)


def test_empty_book_has_no_best_bid_or_ask() -> None:
    book = OrderBook()
    assert book.best_bid is None
    assert book.best_ask is None


def test_snapshot_populates_book_and_picks_best_levels() -> None:
    book = OrderBook()
    book.apply_records(
        "snapshot",
        [
            _record("bid", 100.0, 1.0),
            _record("bid", 101.0, 2.0),
            _record("ask", 102.0, 3.0),
            _record("ask", 103.0, 4.0),
        ],
    )

    assert book.best_bid.price == 101.0
    assert book.best_bid.quantity == 2.0
    assert book.best_ask.price == 102.0
    assert book.best_ask.quantity == 3.0


def test_l2update_upserts_a_price_level() -> None:
    book = OrderBook()
    book.apply_records("snapshot", [_record("bid", 100.0, 1.0), _record("ask", 102.0, 3.0)])
    book.apply_records("l2update", [_record("bid", 100.0, 5.0)])  # overwrite existing level

    assert book.best_bid.quantity == 5.0


def test_zero_size_removes_the_price_level() -> None:
    book = OrderBook()
    book.apply_records("snapshot", [_record("bid", 100.0, 1.0), _record("bid", 99.0, 2.0)])
    book.apply_records("l2update", [_record("bid", 100.0, 0.0)])  # remove the best bid

    assert book.best_bid.price == 99.0


def test_removing_a_level_that_never_existed_is_a_no_op() -> None:
    book = OrderBook()
    book.apply_records("l2update", [_record("bid", 100.0, 0.0)])

    assert book.best_bid is None


def test_second_snapshot_wipes_the_first_entirely() -> None:
    book = OrderBook()
    book.apply_records("snapshot", [_record("bid", 100.0, 1.0), _record("ask", 102.0, 1.0)])
    book.apply_records("l2update", [_record("bid", 100.0, 5.0)])

    book.apply_records("snapshot", [_record("bid", 50.0, 9.0), _record("ask", 60.0, 9.0)])

    assert book.best_bid.price == 50.0
    assert book.best_ask.price == 60.0
