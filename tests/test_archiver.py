import asyncio
import json
from collections.abc import Iterator

from marketdata.bronze.archiver import MinioArchiver
from marketdata.cli import (
    _decode_archive_record,
    _find_latest_snapshot,
    _read_archive_batch,
)


class _FakeMinio:
    def __init__(self) -> None:
        self.fail_next_upload = True
        self.uploads: list[str] = []

    def put_object(self, bucket, key, data, **kwargs) -> None:
        if self.fail_next_upload:
            self.fail_next_upload = False
            raise OSError("temporary upload failure")
        self.uploads.append(data.read().decode("utf-8"))


def test_record_wraps_raw_message_with_event_id() -> None:
    client = _FakeMinio()
    archiver = MinioArchiver("BTC-USD", client=client)
    message = {"type": "l2update", "product_id": "BTC-USD", "changes": []}

    asyncio.run(archiver.record(message))

    envelope = json.loads(archiver._buffer[0])
    assert envelope["event_id"]
    assert envelope["payload"] == message


def test_failed_upload_requeues_same_event_ids_for_retry() -> None:
    client = _FakeMinio()
    archiver = MinioArchiver("BTC-USD", client=client)
    asyncio.run(archiver.record({"type": "snapshot", "product_id": "BTC-USD"}))
    original_event_id = json.loads(archiver._buffer[0])["event_id"]

    asyncio.run(archiver.flush())
    assert json.loads(archiver._buffer[0])["event_id"] == original_event_id

    asyncio.run(archiver.flush())

    uploaded = json.loads(client.uploads[0].strip())
    assert uploaded["event_id"] == original_event_id


def test_archive_record_decoder_deduplicates_envelopes_and_accepts_legacy() -> None:
    processed_ids = {"event-1"}
    payload = {"type": "l2update", "product_id": "BTC-USD"}

    assert _decode_archive_record(
        {"event_id": "event-1", "payload": payload}, processed_ids
    ) == ("event-1", None)
    assert _decode_archive_record(
        {"event_id": "event-2", "payload": payload}, processed_ids
    ) == ("event-2", payload)
    assert _decode_archive_record(payload, processed_ids) == (None, payload)


def test_archive_batch_results_keep_object_order() -> None:
    def read_object(name: str) -> Iterator[dict]:
        yield {"object": name}

    messages = asyncio.run(_read_archive_batch(read_object, ["older", "newer"]))

    assert messages == [[{"object": "older"}], [{"object": "newer"}]]


def test_latest_snapshot_search_returns_exact_object_and_record_offset() -> None:
    objects = {
        "older": [
            {"event_id": "old-snapshot", "payload": {"type": "snapshot"}},
            {"event_id": "old-update", "payload": {"type": "l2update"}},
        ],
        "newer": [
            {"event_id": "new-update-1", "payload": {"type": "l2update"}},
            {"event_id": "new-snapshot", "payload": {"type": "snapshot"}},
            {"event_id": "new-update-2", "payload": {"type": "l2update"}},
        ],
    }

    result = asyncio.run(_find_latest_snapshot(objects.__getitem__, ["older", "newer"]))

    assert result == (1, 1)