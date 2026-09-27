"""In-memory limit order book for a single product -- silver layer only.

Built from Coinbase `level2_batch` `snapshot` and `l2update` messages. Bids and
asks are kept in separate sorted maps so the best bid/ask is always an O(1)
lookup and applying an update is O(log n). This class only exposes facts that
literally exist in the book (price levels, best bid/ask); any *derived* value
(mid-price, spread, ...) is a gold-layer computation and lives in `gold/`
instead -- see `gold/mid_price_metrics.py` and `gold/spread_metrics.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from sortedcontainers import SortedDict

if TYPE_CHECKING:
    # only needed for the type hint below; keeps this module import-free at runtime
    from .normalize import FeedRecord

Side = Literal["bid", "ask"]


@dataclass(frozen=True)
class BookLevel:
    price: float
    quantity: float


class OrderBook:
    """Tracks current bid/ask price levels for one product.

    Both `_bids` and `_asks` are kept sorted ascending by price. The "best"
    price is therefore at opposite ends: the highest bid is the *last* item,
    the lowest ask is the *first* item.
    """

    def __init__(self) -> None:
        self._bids: SortedDict[float, float] = SortedDict()
        self._asks: SortedDict[float, float] = SortedDict()

    def apply_change(self, side: Side, price: float, size: float) -> None:
        """Upsert one price level. `size <= 0` means "remove this level" --
        the single rule every snapshot/l2update record follows.
        """
        book = self._bids if side == "bid" else self._asks
        if size <= 0:
            book.pop(price, None)  # pop(..., None): no error if the level is already gone
        else:
            book[price] = size  # insert if new, overwrite if the price level already existed

    def apply_records(self, msg_type: str, records: list[FeedRecord]) -> None:
        """Turn flat feed records into book state.

        A "snapshot" wipes the book first; an "l2update" doesn't. Either way,
        every record then goes through the same apply_change upsert-or-remove.
        """
        if msg_type == "snapshot":
            # a snapshot is a full replacement of book state, not a diff
            self._bids.clear()
            self._asks.clear()
        for r in records:
            self.apply_change(r.side, r.price, r.size)

    @property
    def best_bid(self) -> BookLevel | None:
        """Highest bid: last item, since `_bids` is sorted ascending."""
        if not self._bids:
            return None  # book is empty on either side until the first snapshot arrives
        price, qty = self._bids.peekitem(-1)  # O(1): no scan needed, the dict is already sorted
        return BookLevel(price, qty)

    @property
    def best_ask(self) -> BookLevel | None:
        """Lowest ask: first item, since `_asks` is sorted ascending."""
        if not self._asks:
            return None
        price, qty = self._asks.peekitem(0)
        return BookLevel(price, qty)
