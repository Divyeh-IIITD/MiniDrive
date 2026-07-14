from __future__ import annotations

import unittest
from unittest.mock import patch, MagicMock

from minidrive.chunking import Chunk
from minidrive.replication import Replicator, ReplicationError


class ReplicationTests(unittest.TestCase):
    def test_successful_replication_two_nodes(self) -> None:
        nodes = ["http://n1/chunk", "http://n2/chunk"]
        r = Replicator(nodes, replication_factor=2)

        chunk = Chunk(index=0, data=b"data")

        mock_resp = MagicMock()
        mock_resp.getcode.return_value = 200
        mock_ctx = MagicMock()
        mock_ctx.__enter__.return_value = mock_resp

        with patch("urllib.request.urlopen", return_value=mock_ctx) as m:
            successes = r.replicate_chunk(chunk)

        self.assertEqual(set(successes), set(nodes))
        self.assertEqual(m.call_count, 2)

    def test_single_node_failure_then_alternate_succeeds(self) -> None:
        nodes = ["http://a/chunk", "http://b/chunk", "http://c/chunk"]
        r = Replicator(nodes, replication_factor=2)

        chunk = Chunk(index=2, data=b"x")

        # Simulate: first candidate (primary) raises, next two succeed
        def side_effect(req, timeout=...):
            url = req.full_url
            if url.startswith("http://c"):
                raise urllib.error.URLError("simulated network error")
            mock_resp = MagicMock()
            mock_resp.getcode.return_value = 200
            mock_ctx = MagicMock()
            mock_ctx.__enter__.return_value = mock_resp
            return mock_ctx

        with patch("urllib.request.urlopen", side_effect=side_effect) as m:
            successes = r.replicate_chunk(chunk)

        self.assertEqual(len(successes), 2)

    def test_total_failure_raises(self) -> None:
        nodes = ["http://1", "http://2"]
        r = Replicator(nodes, replication_factor=2)

        chunk = Chunk(index=1, data=b"y")

        def fail_all(req, timeout=...):
            raise urllib.error.URLError("down")

        with patch("urllib.request.urlopen", side_effect=fail_all):
            with self.assertRaises(ReplicationError):
                r.replicate_chunk(chunk)


if __name__ == "__main__":
    unittest.main()
