"""Optional archival of raw feed messages to a MinIO (S3-compatible) bucket.

Messages are buffered in memory and flushed as a single batched
newline-delimited-JSON object on a fixed interval, via a background asyncio
task. This is a side effect only: it never blocks or slows down the live
order book / 5-second metrics loop, which always reads from in-memory state.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from minio import Minio

logger = logging.getLogger(__name__)

FLUSH_INTERVAL_SECONDS = 1


class MinioArchiver:
    def __init__(self, product: str, client: Minio | None = None) -> None:
        self.product = product
        self.bucket = os.environ.get("MINIO_BUCKET", "coinbase-raw")
        self._client = client or Minio(
            os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        )
        self._buffer: list[str] = []
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Make sure the bucket exists, then start the background flush timer."""
        await asyncio.to_thread(self._ensure_bucket)  # bucket_exists/make_bucket are blocking calls
        asyncio.create_task(self._flush_loop())  # fire-and-forget: runs for the life of the process

    def _ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self.bucket):
            self._client.make_bucket(self.bucket)

    async def record(self, message: dict[str, Any]) -> None:
        """Buffer one identified raw message until the next flush."""
        envelope = {"event_id": uuid4().hex, "payload": message}
        async with self._lock:
            self._buffer.append(json.dumps(envelope))

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            await self.flush()

    async def flush(self) -> None:
        """Swap out the buffer and upload it as one batched object; a no-op if empty."""
        async with self._lock:
            if not self._buffer:
                return
            # swap-then-release-the-lock so record() isn't blocked during the upload below
            batch, self._buffer = self._buffer, []
        payload = ("\n".join(batch) + "\n").encode("utf-8")
        key = self._object_key()
        try:
            await asyncio.to_thread(
                self._client.put_object,
                self.bucket,
                key,
                io.BytesIO(payload),
                length=len(payload),
                content_type="application/x-ndjson",
            )
        except Exception:
            # Retain the serialized envelopes so retries keep their event IDs.
            async with self._lock:
                self._buffer = batch + self._buffer
            logger.exception("Failed to flush %d messages to MinIO", len(batch))

    def _object_key(self) -> str:
        now = datetime.now(UTC)
        # nanosecond suffix guarantees uniqueness even for back-to-back flushes
        return f"{self.product}/{now:%Y/%m/%d/%H}/{now:%Y%m%dT%H%M%S}-{time.time_ns()}.jsonl"

    def list_object_names(self) -> list[str]:
        """All archived object names for this product, oldest first (names
        embed the flush timestamp, so sorting by name is sorting by time).
        """
        objects = self._client.list_objects(self.bucket, prefix=f"{self.product}/", recursive=True)
        return sorted(obj.object_name for obj in objects)

    def read_object(self, object_name: str) -> Iterator[dict[str, Any]]:
        """Read back every archived envelope or legacy raw message."""
        response = self._client.get_object(self.bucket, object_name)
        try:
            for line in response.read().decode("utf-8").splitlines():
                if line:  # skip the trailing blank line from the batch's final "\n"
                    yield json.loads(line)
        finally:
            # both calls are required by minio's client to release the underlying connection
            response.close()
            response.release_conn()

