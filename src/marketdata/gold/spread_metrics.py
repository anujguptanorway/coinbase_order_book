"""Everything about spread: the current point-in-time value, and the widest
bid/ask spread seen so far -- a single running max since process start.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from ..silver.order_book import OrderBook


def spread(book: OrderBook) -> float | None:
    """Standard market-spread definition: lowest ask minus highest bid."""
    bid, ask = book.best_bid, book.best_ask
    if bid is None or ask is None:
        return None
    return ask.price - bid.price


class WidestSpread(NamedTuple):
    """Enough context to audit a surprising value against the archived feed."""

    spread: float
    timestamp: float
    bid_price: float
    ask_price: float


def track_widest_spread(
    current: WidestSpread | None, spread: float, timestamp: float, bid_price: float, ask_price: float
) -> WidestSpread:
    """Return the new widest-spread record if `spread` beats `current`, else `current` unchanged."""
    if current is None or spread > current.spread:
        return WidestSpread(spread, timestamp, bid_price, ask_price)
    return current
