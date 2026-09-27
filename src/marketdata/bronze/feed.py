"""Coinbase Exchange public `level2_batch` feed client (the "feeder") -- bronze layer only.

Why this channel: `level2_batch` delivers the same guaranteed-delivery
order-book snapshot + incremental updates as `level2`, but does not require
authentication -- no Coinbase API key is needed to run this tool. See
https://docs.cdp.coinbase.com/exchange/websocket-feed/channels

Design: this module's only job is talking to Coinbase and yielding the
untouched raw message dicts. It does not know about `FeedRecord`, parsing,
`OrderBook`, MinIO, or anything else in this app. That keeps it
independently deployable: if this ever needs to become its own container
(e.g. the "feed connector" in the Part 2 cloud architecture), nothing else
needs to move with it -- not even the parsing logic, which lives in
`normalize.py` instead. Turning raw dicts into `FeedRecord`s happens there;
applying them to an order book happens in `order_book.py`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

import websockets

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-feed.exchange.coinbase.com"
RECONNECT_DELAY_SECONDS = 2


class CoinbaseFeed:
    """Connects to level2_batch for one product and streams raw message dicts.

    Consume it with `async for msg in feed.stream(): ...`. Maintains no book
    state and does no parsing itself -- callers decide what to do with each
    raw message (e.g. `normalize.parse_message`, archive it, or both), which
    is what keeps this class deployable on its own.
    """

    def __init__(self, product: str) -> None:
        self.product = product

    async def stream(self) -> AsyncIterator[dict]:
        """Yield every raw message dict received on the websocket."""
        subscribe_msg = json.dumps(
            {
                "type": "subscribe",
                "product_ids": [self.product],
                "channels": ["level2_batch"],
            }
        )
        # outer loop: reconnect forever on any connection drop, so the caller
        # never has to know the websocket died and came back
        while True:
            try:
                # snapshot messages for busy products can exceed the default 1MB frame cap
                async with websockets.connect(WS_URL, ping_interval=20, max_size=32 * 1024 * 1024) as ws:
                    await ws.send(subscribe_msg)
                    async for raw in ws:
                        yield json.loads(raw)
            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                # reconnect after a short backoff; a fresh connection triggers a
                # fresh "snapshot" from Coinbase, so the caller's book self-heals
                logger.warning(
                    "Feed connection lost (%s); reconnecting in %ss",
                    exc,
                    RECONNECT_DELAY_SECONDS,
                )
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)
