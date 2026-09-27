"""Current mid-price and elapsed-time-weighted rolling averages."""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from ..silver.order_book import OrderBook

WINDOWS = {"1m": 60, "5m": 300, "15m": 900}
LONGEST_WINDOW = max(WINDOWS.values())


def mid_price(book: OrderBook) -> float | None:
    """Mid-point between best bid and best ask; `None` until both sides exist."""
    bid, ask = book.best_bid, book.best_ask
    if bid is None or ask is None:
        return None
    return (bid.price + ask.price) / 2


class MidPriceSample(NamedTuple):
    timestamp: float
    mid_price: float | None


# Timestamp-ordered samples; the oldest retained sample may precede the
# longest window so its price can be carried up to that window's boundary.
MidPriceSamples = deque[MidPriceSample]


def add_mid_price_sample(samples: MidPriceSamples, timestamp: float, mid_price: float | None) -> None:
    """Append one sample and discard history no longer needed by any window."""
    samples.append(MidPriceSample(timestamp, mid_price))
    cutoff = timestamp - LONGEST_WINDOW
    while len(samples) > 1 and samples[1].timestamp <= cutoff:
        samples.popleft()


def mid_price_window_averages(samples: MidPriceSamples) -> dict[str, float | None]:
    """One elapsed-time-weighted average for each trailing window."""
    if not samples:
        return {label: None for label in WINDOWS}

    now = samples[-1].timestamp  # "now" = the latest sample's own event time, not wall clock
    return {label: _average_since(samples, now - seconds) for label, seconds in WINDOWS.items()}


def _average_since(samples: MidPriceSamples, cutoff: float) -> float | None:
    now = samples[-1].timestamp
    weighted_total = 0.0
    observed_seconds = 0.0

    for index, sample in enumerate(samples):
        next_timestamp = samples[index + 1].timestamp if index + 1 < len(samples) else now
        duration = min(next_timestamp, now) - max(sample.timestamp, cutoff)
        if duration > 0 and sample.mid_price is not None:
            weighted_total += sample.mid_price * duration
            observed_seconds += duration

    if observed_seconds == 0:
        return samples[-1].mid_price
    return weighted_total / observed_seconds
