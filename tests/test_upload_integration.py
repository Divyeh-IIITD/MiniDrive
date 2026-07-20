from __future__ import annotations

import hashlib
import importlib.util
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from coordinator import main as coordinator_main
from minidrive.chunking import iter_chunks
from minidrive.distribution import RoundRobinDistributor
from minidrive.replication import Replicator


class _UrlOpenResponse:
    def __init__(self, status_code: int):
        self._status_code = status_code

    def getcode(self) -> int:
        return self._status_code

    def __enter__(self) -> "_UrlOpenResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class UploadIntegrationTests(unittest.TestCase):
    @staticmethod
    def _load_storage_node_module(module_name: str):
        module_path = Path(__file__).resolve().parents[1] / "storage_node" / "main.py"
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("failed to load storage node module")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)

        self.node_clients = {}
        self.node_urls = []

        for index in range(2):
            module = self._load_storage_node_module(f"test_storage_node_{index}")
            node_dir = root / f"node_{index}"
            node_dir.mkdir(parents=True, exist_ok=True)
            module.STORAGE_DIR = node_dir
            client = TestClient(module.app)
            node_url = f"http://test-node-{index}.local/chunks"

            self.node_clients[node_url] = client
            self.node_urls.append(node_url)

        coordinator_main.DISTRIBUTOR = RoundRobinDistributor(self.node_urls)
        coordinator_main.REPLICATOR = Replicator(self.node_urls, replication_factor=2)
        self.coordinator_client = TestClient(coordinator_main.app)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _fake_urlopen(self, request, timeout=10):
        url = request.full_url if hasattr(request, "full_url") else str(request)
        target = self.node_clients[url]
        headers = dict(request.headers)
        response = target.post("/chunks", content=request.data, headers=headers)
        return _UrlOpenResponse(response.status_code)

    def test_upload_routes_chunks_to_storage_nodes(self) -> None:
        payload = (b"mini-drive-integration-test-" * 90000)[:2_600_000]
        self.assertGreater(len(payload), 1_048_576)

        expected_chunks = []
        for chunk in iter_chunks(io.BytesIO(payload)):
            chunk_hash = hashlib.sha256(chunk.data).hexdigest()
            expected_chunks.append((chunk.index, chunk_hash, chunk.data))

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            response = self.coordinator_client.post(
                "/upload",
                files={"file": ("integration.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["total_chunks"], len(expected_chunks))
        self.assertEqual(len(body["chunks"]), len(expected_chunks))

        for chunk_result, (expected_index, expected_hash, expected_data) in zip(body["chunks"], expected_chunks):
            self.assertEqual(chunk_result["index"], expected_index)
            self.assertEqual(chunk_result["hash"], expected_hash)
            self.assertEqual(chunk_result["status"], "stored")
            self.assertEqual(set(chunk_result["stored_nodes"]), set(self.node_urls))

            for node_url in chunk_result["stored_nodes"]:
                node_client = self.node_clients[node_url]
                get_response = node_client.get(f"/chunks/{expected_hash}")
                self.assertEqual(get_response.status_code, 200)
                self.assertEqual(get_response.content, expected_data)
