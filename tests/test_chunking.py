from __future__ import annotations

import io
import unittest

from minidrive.chunking import Chunk, iter_chunks


class ChunkingTests(unittest.TestCase):
    # WHAT: Verify that an empty stream produces no chunks.
    # WHY: An empty file should not create metadata rows or phantom chunk records, which would complicate upload orchestration and cleanup logic.
    # TRADE-OFF: This checks the simplest boundary only; it does not exercise downstream storage behavior.
    def test_empty_stream_returns_no_chunks(self) -> None:
        chunks = list(iter_chunks(io.BytesIO(b""), chunk_size=4))
        self.assertEqual(chunks, [])

    # WHAT: Verify that exact chunk boundaries preserve chunk count, order, and contents.
    # WHY: Boundary-aligned files are the easiest place for off-by-one errors, and this confirms that the final read stops cleanly instead of emitting an empty tail chunk.
    # TRADE-OFF: The test uses a tiny chunk size for clarity rather than the production default.
    def test_exact_chunk_boundaries_preserve_order(self) -> None:
        payload = b"abcdefgh"

        chunks = list(iter_chunks(io.BytesIO(payload), chunk_size=4))

        self.assertEqual(
            chunks,
            [
                Chunk(index=0, data=b"abcd"),
                Chunk(index=1, data=b"efgh"),
            ],
        )

    # WHAT: Verify that a final partial chunk is emitted instead of being dropped.
    # WHY: Dropping the tail bytes would silently corrupt downloads, and this test catches that failure mode with a non-multiple file size.
    # TRADE-OFF: The test covers correctness of the final slice, not the performance characteristics of large files.
    def test_partial_tail_chunk_is_retained(self) -> None:
        payload = b"abcdefg"

        chunks = list(iter_chunks(io.BytesIO(payload), chunk_size=4))

        self.assertEqual(
            chunks,
            [
                Chunk(index=0, data=b"abcd"),
                Chunk(index=1, data=b"efg"),
            ],
        )

    # WHAT: Verify that invalid chunk sizes fail fast.
    # WHY: A zero or negative read size would either loop incorrectly or raise from the stream implementation later, so rejecting it early gives a clearer failure mode.
    # TRADE-OFF: This adds one explicit guard branch in exchange for safer caller feedback.
    def test_invalid_chunk_size_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            list(iter_chunks(io.BytesIO(b"abc"), chunk_size=0))


if __name__ == "__main__":
    unittest.main()
