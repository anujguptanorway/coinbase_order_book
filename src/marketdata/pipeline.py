"""The whole transform, start to finish.

Reading top to bottom:

  1. Pipeline -- ties everything together; the calls cli.py makes

`bronze/` holds the raw feed connector (`feed.py`) and archiver. `silver/`
holds the two genuinely stateful pieces: `normalize.py` (raw dict -> flat
records) and `order_book.py` (records -> current book, exposing only facts
that literally exist in the book -- price levels, best bid/ask). Every
*derived* value (mid-price, spread, rolling averages, forecasting) is gold
and lives under `gold/` as pure functions over that silver state. This
module just wires the two together.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from .gold.forecast_metrics import (
    ForecastErrors,
    PendingForecasts,
    forecast_error_window_averages,
    update_forecasts,
)
from .gold.mid_price_metrics import (
    MidPriceSamples,
    add_mid_price_sample,
    mid_price,
    mid_price_window_averages,
)
from .gold.spread_metrics import WidestSpread, spread, track_widest_spread
from .silver.order_book import OrderBook

if TYPE_CHECKING:
    from .silver.normalize import FeedRecord

DISPLAY_TZ = ZoneInfo("Europe/Oslo")


@dataclass
class Counters:
    """Makes the feed -> book -> metrics flow observable at runtime."""

    feed_messages: int = 0
    snapshots: int = 0
    updates: int = 0
    records_applied: int = 0
    last_snapshot_records: int = 0


def _fmt(value: float | None, digits: int = 2) -> str:
    return f"{value:.{digits}f}" if value is not None else "n/a"


def _ts(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=DISPLAY_TZ).strftime("%H:%M:%S.%f")[:-3]


class Pipeline:
    def __init__(self) -> None:
        self.book = OrderBook()
        self.widest_spread: WidestSpread | None = None
        self.mid_prices: MidPriceSamples = deque()
        self.pending_forecasts: PendingForecasts = deque()
        self.forecast_errors: ForecastErrors = deque()
        self.last_forecast: float | None = None
        self.counters = Counters()

    def count_feed_message(self) -> None:
        self.counters.feed_messages += 1

    def ingest(self, msg_type: str, records: list[FeedRecord], event_time: float | None = None) -> None:
        """Records -> book state -> observed stats."""
        self.book.apply_records(msg_type, records)

        if msg_type == "snapshot":
            self.counters.snapshots += 1
            self.counters.last_snapshot_records = len(records)
        else:
            self.counters.updates += 1
        self.counters.records_applied += len(records)

        if not records and msg_type != "snapshot":
            return  # nothing to derive stats from (e.g. an empty l2update)
        event_time = event_time if event_time is not None else records[0].time if records else None
        bid, ask = self.book.best_bid, self.book.best_ask
        mid, spr = mid_price(self.book), spread(self.book)
        if bid is None or ask is None or mid is None or spr is None:
            if event_time is not None:
                add_mid_price_sample(self.mid_prices, event_time, None)
            self.pending_forecasts.clear()
            self.last_forecast = None
            return  # one side of the book is still empty (e.g. before the first snapshot)

        if event_time is None:
            return
        self.widest_spread = track_widest_spread(self.widest_spread, spr, event_time, bid.price, ask.price)
        add_mid_price_sample(self.mid_prices, event_time, mid)

        self.last_forecast = update_forecasts(
            self.pending_forecasts, self.forecast_errors, self.mid_prices, event_time, mid
        )

    def rows(self) -> list[tuple[str, str] | None]:
        """Display rows; `None` marks a section break."""
        # read fresh at call time -- highest bid / lowest ask / mid-price are "as of now",
        # unlike widest_spread and the mid-price averages below, which accumulate over time
        bid, ask, mid = self.book.best_bid, self.book.best_ask, mid_price(self.book)
        widest = self.widest_spread

        rows: list[tuple[str, str] | None] = [
            ("Highest bid (price / qty)", f"{_fmt(bid.price)} / {_fmt(bid.quantity, 6)}" if bid else "n/a"),
            ("Lowest ask (price / qty)", f"{_fmt(ask.price)} / {_fmt(ask.quantity, 6)}" if ask else "n/a"),
            ("Mid-price", _fmt(mid) if mid is not None else "n/a"),
            ("Biggest spread seen so far", _fmt(widest.spread) if widest else "n/a"),
            (
                "  ...seen at (bid / ask)",
                f"{_ts(widest.timestamp)} ({_fmt(widest.bid_price)} / {_fmt(widest.ask_price)})"
                if widest
                else "n/a",
            ),
        ]
        rows += [
            (f"Avg mid-price (last {label})", _fmt(value))
            for label, value in mid_price_window_averages(self.mid_prices).items()
        ]
        rows.append(("Forecasted mid-price (+60s)", _fmt(self.last_forecast)))
        rows += [
            (f"Avg forecast error (last {label})", _fmt(value))
            for label, value in forecast_error_window_averages(self.forecast_errors).items()
        ]
        rows += [
            None,
            ("Feed messages received", str(self.counters.feed_messages)),
            ("Snapshot / Update", f"{self.counters.snapshots} / {self.counters.updates}"),
            ("Last snapshot records", str(self.counters.last_snapshot_records)),
            ("Records applied to book", str(self.counters.records_applied)),
        ]
        return rows
