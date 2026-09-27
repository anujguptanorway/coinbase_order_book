"""CLI entrypoint: streams a Coinbase product's order book and prints insight
metrics every 5 seconds.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Callable, Iterator
from typing import Any

from dotenv import find_dotenv, load_dotenv
from rich.console import Console
from rich.live import Live
from rich.table import Table

from .bronze.archiver import MinioArchiver
from .bronze.feed import CoinbaseFeed
from .pipeline import Pipeline
from .silver.normalize import parse_message

PRINT_INTERVAL_SECONDS = 5
CONSUMER_POLL_SECONDS = 4
CONSUMER_OBJECT_BATCH_SIZE = 8


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coinbase order-book insight tool")
    parser.add_argument(
        "--product", default="BTC-USD", help="Product id to track, e.g. BTC-USD, ETH-EUR"
    )
    parser.add_argument(
        "--archive-to-minio",
        action="store_true",
        help="Don't run the live display at all; instead act as a headless producer that "
        "connects to the feed and archives raw messages to MinIO for a separate "
        "--consume-from-minio process to read",
    )
    parser.add_argument(
        "--consume-from-minio",
        action="store_true",
        help="Don't connect to the feed at all; instead poll MinIO every 4s for messages "
        "archived by a separate producer process (running with --archive-to-minio) and render "
        "a live table from them -- a fully decoupled consumer",
    )
    return parser.parse_args(argv)


def build_table(product: str, pipeline: Pipeline) -> Table:
    table = Table(title=f"{product} order book insights")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for row in pipeline.rows():
        if row is None:
            table.add_section()
        else:
            table.add_row(*row)
    return table


def _decode_archive_record(
    record: dict, processed_event_ids: set[str]
) -> tuple[str | None, dict | None]:
    """Unwrap an identified envelope, or pass through a legacy raw message."""
    event_id = record.get("event_id")
    payload = record.get("payload")
    if event_id is None or not isinstance(payload, dict):
        return None, record
    event_id = str(event_id)
    if event_id in processed_event_ids:
        return event_id, None
    return event_id, payload


async def _read_archive_batch(
    read_object: Callable[[str], Iterator[dict[str, Any]]], object_names: list[str]
) -> list[list[dict[str, Any]]]:
    return list(
        await asyncio.gather(
            *(
                asyncio.to_thread(lambda name=name: list(read_object(name)))
                for name in object_names
            )
        )
    )


async def _find_latest_snapshot(
    read_object: Callable[[str], Iterator[dict[str, Any]]], object_names: list[str]
) -> tuple[int, int] | None:
    """Return the object and record indexes of the newest archived snapshot."""
    for end in range(len(object_names), 0, -CONSUMER_OBJECT_BATCH_SIZE):
        start = max(0, end - CONSUMER_OBJECT_BATCH_SIZE)
        batch_names = list(reversed(object_names[start:end]))
        batch_messages = await _read_archive_batch(read_object, batch_names)
        for batch_index, messages in enumerate(batch_messages):
            for record_index in range(len(messages) - 1, -1, -1):
                _, message = _decode_archive_record(messages[record_index], set())
                if message is not None and message.get("type") == "snapshot":
                    object_index = end - 1 - batch_index
                    return object_index, record_index
    return None


async def live_display(product: str, pipeline: Pipeline) -> None:
    """Redraw the table in place every 5s; `Live` handles the cursor work so
    the terminal doesn't flicker or fill with scrollback.
    """
    with Live(build_table(product, pipeline), refresh_per_second=4) as live:
        while True:
            await asyncio.sleep(PRINT_INTERVAL_SECONDS)
            live.update(build_table(product, pipeline))

#CONSUMES DATA FROM LIVE FEED AND DISPLAYS LIVE TABLE
async def run(product: str) -> None:
    """Live display mode: connects directly to the feed and renders the table
    every 5s. No archiving -- use --archive-to-minio for a headless producer
    that a separate --consume-from-minio process can read from instead.
    """
    console = Console()
    pipeline = Pipeline()
    feed = CoinbaseFeed(product)

    async def consume_feed() -> None:
        async for msg in feed.stream():
            pipeline.count_feed_message()
            parsed = parse_message(msg)
            if parsed is not None:
                pipeline.ingest(*parsed)

    console.print(f"Connecting to Coinbase level2_batch feed for {product}...")
    # runs forever: consume_feed() never returns, so this only exits on Ctrl+C/error
    await asyncio.gather(
        consume_feed(),
        live_display(product, pipeline),
    )

#STORES DATA TO MINIO BUCKET
async def run_producer(product: str) -> None:
    """Headless producer: connects to the feed and archives raw messages to
    MinIO in one-second batches. No local metrics computation or table -- a separate
    --consume-from-minio process is what shows the insights.
    """
    console = Console()
    feed = CoinbaseFeed(product)
    archiver = MinioArchiver(product)
    await archiver.start()

    message_count = 0

    async def consume_feed() -> None:
        nonlocal message_count
        async for msg in feed.stream():
            message_count += 1
            await archiver.record(msg)

    async def status_loop() -> None:
        while True:
            await asyncio.sleep(PRINT_INTERVAL_SECONDS)
            console.print(f"Archived {message_count} raw messages so far...")

    console.print(f"Archiving raw messages to MinIO bucket '{archiver.bucket}'...")
    console.print(f"Connecting to Coinbase level2_batch feed for {product}...")
    await asyncio.gather(consume_feed(), status_loop())

#CONSUMES DATA FROM MINIO BUCKET AND DISPLAYS LIVE TABLE, LOOKS FOR LATEST SNAPSHOT FIRST THEN REPLAYS SUBSEQUENT RECORDS
async def run_consumer(product: str) -> None:
    """Find the newest snapshot, replay subsequent records, then poll MinIO
    every 4s for new objects. No websocket connection is opened.
    """
    console = Console()
    pipeline = Pipeline()
    archiver = MinioArchiver(product)
    seen: set[str] = set()
    processed_event_ids: set[str] = set()
    snapshot_search_cursor = 0
    bootstrapped = False

    console.print(
        f"Polling MinIO bucket '{archiver.bucket}' for {product} messages "
        f"every {CONSUMER_POLL_SECONDS}s..."
    )
    with Live(build_table(product, pipeline), refresh_per_second=4) as live:
        while True:
            # list_objects/get_object are blocking calls -- keep them off the event loop
            names = await asyncio.to_thread(archiver.list_object_names)
            if not bootstrapped:
                search_start = snapshot_search_cursor
                start = await _find_latest_snapshot(
                    archiver.read_object, names[search_start:]
                )
                snapshot_search_cursor = len(names)
                if start is None:
                    live.update(build_table(product, pipeline))
                    await asyncio.sleep(CONSUMER_POLL_SECONDS)
                    continue
                snapshot_object_index, snapshot_record_index = start
                snapshot_object_index += search_start
                snapshot_object_name = names[snapshot_object_index]
                seen.update(names[:snapshot_object_index])
                pending_names = names[snapshot_object_index:]
                bootstrapped = True
            else:
                pending_names = [name for name in names if name not in seen]
            for start in range(0, len(pending_names), CONSUMER_OBJECT_BATCH_SIZE):
                batch_names = pending_names[start : start + CONSUMER_OBJECT_BATCH_SIZE]
                batch_messages = await _read_archive_batch(archiver.read_object, batch_names)
                for name, messages in zip(batch_names, batch_messages):
                    seen.add(name)
                    first_record = snapshot_record_index if name == snapshot_object_name else 0
                    for record in messages[first_record:]:
                        event_id, msg = _decode_archive_record(record, processed_event_ids)
                        if msg is None:
                            continue
                        pipeline.count_feed_message()
                        parsed = parse_message(msg)
                        if parsed is not None:
                            pipeline.ingest(*parsed)
                        if event_id is not None:
                            processed_event_ids.add(event_id)
                live.update(build_table(product, pipeline))
            await asyncio.sleep(CONSUMER_POLL_SECONDS)


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    load_dotenv(find_dotenv(usecwd=True))
    args = parse_args()
    try:
        if args.consume_from_minio:
            asyncio.run(run_consumer(args.product))
        elif args.archive_to_minio:
            asyncio.run(run_producer(args.product))
        else:
            asyncio.run(run(args.product))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
