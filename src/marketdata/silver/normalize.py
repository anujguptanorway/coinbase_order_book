"""Silver: normalize raw Coinbase feed messages into flat, typed `FeedRecord`s.

This is deliberately separate from `feed.py` (which only talks to the
websocket): parsing is a pure "raw dict -> typed record" transformation with
no network/connection concerns, so it doesn't need to move with the feed
connector if that ever becomes its own deployable service. Anything that
already has raw message dicts -- the live feed, archived data, a future
Event Hub/Kafka consumer -- can reuse this without depending on `feed.py`.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

# Coinbase calls the two sides "buy"/"sell" on the wire; the challenge's
# schema calls them "bid"/"ask". This is the one place that mapping happens.
_SIDE_MAP = {"buy": "bid", "sell": "ask"}


@dataclass(frozen=True)
class FeedRecord:
    """One order-book row: exactly the challenge's schema (side, price,
    quantity) plus product_id and time for context. One record per price
    level -- callers can treat a snapshot and an l2update identically.
    """

    product_id: str
    side: str  # "bid" or "ask"
    price: float
    size: float
    time: float  # epoch seconds, exchange event time when available


def resolve_event_timestamp(msg: dict, fallback: float) -> float:
    """Prefer the exchange's own event time (`msg["time"]`) over local arrival
    time; falls back only if a message is ever missing that field.
    """
    time_str = msg.get("time")
    if not time_str:
        return fallback  # message has no "time" field at all
    try:
        return datetime.fromisoformat(time_str).timestamp()
    except ValueError:
        return fallback  # malformed timestamp string, don't crash the feed over it


def normalize_snapshot(msg: dict, event_time: float | None = None) -> list[FeedRecord]:
    """snapshot -> one flat FeedRecord per bid/ask price level."""
    product_id = msg.get("product_id", "")
    ts = event_time if event_time is not None else resolve_event_timestamp(msg, time.time())
    # Coinbase sends bids/asks as [[price_str, size_str], ...] pairs -- cast both to float here
    records = [
        FeedRecord(product_id, "bid", float(price), float(size), ts)
        for price, size in msg.get("bids", [])
    ]
    records += [
        FeedRecord(product_id, "ask", float(price), float(size), ts)
        for price, size in msg.get("asks", [])
    ]
    return records


def normalize_update(msg: dict, event_time: float | None = None) -> list[FeedRecord]:
    """l2update -> one flat FeedRecord per changed price level; unknown sides dropped."""
    product_id = msg.get("product_id", "")
    ts = event_time if event_time is not None else resolve_event_timestamp(msg, time.time())
    records: list[FeedRecord] = []
    # each change is ["buy"|"sell", price_str, size_str]; size "0" means remove that price level
    for side_raw, price, size in msg.get("changes", []):
        side = _SIDE_MAP.get(side_raw)
        if side is None:
            continue  # not "buy"/"sell" -- shouldn't happen, but don't let it crash the feed
        records.append(FeedRecord(product_id, side, float(price), float(size), ts))
    return records


def parse_message(msg: dict) -> tuple[str, list[FeedRecord], float] | None:
    """One raw message in -> (msg_type, records, event_time), or None to skip it.

    A plain function so any caller with a raw message dict -- live feed,
    archived data, a future non-websocket source -- can share the exact
    same parsing logic without depending on how that dict was obtained.
    """
    msg_type = msg.get("type")
    if msg_type in {"snapshot", "l2update"}:
        event_time = resolve_event_timestamp(msg, time.time())
    else:
        event_time = None
    if msg_type == "snapshot":
        return "snapshot", normalize_snapshot(msg, event_time), event_time
    if msg_type == "l2update":
        return "l2update", normalize_update(msg, event_time), event_time
    if msg_type == "error":
        logger.error("Feed error message: %s", msg)
        # fall through and return None -- an error message carries no book records
    return None  # subscription acks and other message types are ignored
