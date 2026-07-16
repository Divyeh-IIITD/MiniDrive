from __future__ import annotations

import hashlib
import unittest

from fastapi.testclient import TestClient

from storage_node.main import STORAGE_DIR, app, chunk_path_for_hash


client = TestClient(app)


class StorageNodeTests(unittest.TestCase):
    # WHAT: Compute the SHA-256 digest for a payload.
    # WHY: Tests need the exact content hash that the API expects so they can verify
    #      dedupe and retrieval semantics without reimplementing the server path logic.
    # TRADE-OFF: This helper is test-only and duplicates the algorithm used by the app.
    def sha256_hex(self, payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    # WHAT: Remove a stored test object if it exists.
    # WHY: Tests should clean up after themselves so repeated runs stay deterministic.
    # TRADE-OFF: Best-effort cleanup is enough for these unit tests; production would
    #            need stronger lifecycle management.
    def remove_chunk_if_present(self, chunk_hash: str) -> None:
        path = chunk_path_for_hash(chunk_hash)
        if path.exists():
            path.unlink()
        parent = path.parent
        while parent != STORAGE_DIR and parent.exists():
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent

    def test_store_get_delete_chunk_round_trip(self) -> None:
        payload = b"hello-minidrive"
        chunk_hash = self.sha256_hex(payload)
        self.remove_chunk_if_present(chunk_hash)

        response = client.post("/chunks", content=payload, headers={"X-Chunk-Hash": chunk_hash})
        self.assertEqual(response.status_code, 201)

        stored_path = chunk_path_for_hash(chunk_hash)
        self.assertTrue(stored_path.exists())

        get_response = client.get(f"/chunks/{chunk_hash}")
        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(get_response.content, payload)

        delete_response = client.delete(f"/chunks/{chunk_hash}")
        self.assertEqual(delete_response.status_code, 200)
        self.assertFalse(stored_path.exists())

    def test_duplicate_upload_is_deduplicated(self) -> None:
        payload = b"duplicate-me"
        chunk_hash = self.sha256_hex(payload)
        self.remove_chunk_if_present(chunk_hash)

        first = client.post("/chunks", content=payload, headers={"X-Chunk-Hash": chunk_hash})
        second = client.post("/chunks", content=payload, headers={"X-Chunk-Hash": chunk_hash})

        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json()["status"], "deduplicated")

        self.remove_chunk_if_present(chunk_hash)

    def test_hash_mismatch_is_rejected(self) -> None:
        payload = b"corrupt"
        wrong_hash = self.sha256_hex(b"different")
        self.remove_chunk_if_present(wrong_hash)

        response = client.post("/chunks", content=payload, headers={"X-Chunk-Hash": wrong_hash})
        self.assertEqual(response.status_code, 400)
