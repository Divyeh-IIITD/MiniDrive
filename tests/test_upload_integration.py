from __future__ import annotations

import hashlib
import importlib.util
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from coordinator import main as coordinator_main
from coordinator.db import Base
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


class _DownloadResponse:
    def __init__(self, payload: bytes, status_code: int = 200):
        self._payload = payload
        self._status_code = status_code
        self.status_code = status_code
        self.content = payload
        self._offset = 0

    def getcode(self) -> int:
        return self._status_code

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._payload) - self._offset
        if self._offset >= len(self._payload):
            return b""
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def __enter__(self) -> "_DownloadResponse":
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

        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        coordinator_main.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)

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

    def _fake_download_urlopen(self, request, timeout=10):
        url = request.full_url if hasattr(request, "full_url") else str(request)
        base_url = url.rsplit("/", 1)[0]
        target = self.node_clients[base_url]
        response = target.get(url[url.find("/chunks/") :])
        status_code = getattr(response, "status_code", 200)
        if status_code >= 400:
            raise HTTPError(url, status_code, getattr(response, "text", "error"), hdrs=None, fp=None)
        return _DownloadResponse(response.content, status_code)

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

    def test_upload_then_download_round_trips_exact_bytes(self) -> None:
        payload = (b"mini-drive-round-trip-" * 90000)[:2_600_000]
        expected_hash = hashlib.sha256(payload).hexdigest()

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            upload_response = self.coordinator_client.post(
                "/upload",
                files={"file": ("roundtrip.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(upload_response.status_code, 200)
        file_id = upload_response.json()["file_id"]

        with patch("urllib.request.urlopen", side_effect=self._fake_download_urlopen):
            download_response = self.coordinator_client.get(f"/files/{file_id}/download")

        self.assertEqual(download_response.status_code, 200)
        self.assertEqual(download_response.content, payload)
        self.assertEqual(hashlib.sha256(download_response.content).hexdigest(), expected_hash)
        self.assertEqual(download_response.headers["content-disposition"], 'attachment; filename="roundtrip.bin"')

    def test_download_falls_back_to_replica_when_primary_fails(self) -> None:
        payload = (b"mini-drive-failover-" * 90000)[:2_600_000]

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            upload_response = self.coordinator_client.post(
                "/upload",
                files={"file": ("failover.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(upload_response.status_code, 200)
        file_id = upload_response.json()["file_id"]

        original_get = self.node_clients[self.node_urls[0]].get

        def failing_primary_get(path, *args, **kwargs):
            if path.startswith("/chunks/"):
                return type("_FailResponse", (), {"status_code": 500, "text": "simulated failure"})()
            return original_get(path, *args, **kwargs)

        self.node_clients[self.node_urls[0]].get = failing_primary_get  # type: ignore[assignment]
        try:
            with patch("urllib.request.urlopen", side_effect=self._fake_download_urlopen):
                download_response = self.coordinator_client.get(f"/files/{file_id}/download")
        finally:
            self.node_clients[self.node_urls[0]].get = original_get  # type: ignore[assignment]

        self.assertEqual(download_response.status_code, 200)
        self.assertEqual(download_response.content, payload)

    def test_download_falls_back_to_replica_when_primary_bytes_fail_hash_check(self) -> None:
        payload = (b"mini-drive-corrupt-primary-" * 90000)[:2_600_000]

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            upload_response = self.coordinator_client.post(
                "/upload",
                files={"file": ("corrupt.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(upload_response.status_code, 200)
        file_id = upload_response.json()["file_id"]

        original_get = self.node_clients[self.node_urls[0]].get

        def corrupt_primary_get(path, *args, **kwargs):
            if path.startswith("/chunks/"):
                return _DownloadResponse(b"corrupt-primary-bytes")
            return original_get(path, *args, **kwargs)

        self.node_clients[self.node_urls[0]].get = corrupt_primary_get  # type: ignore[assignment]
        try:
            with patch("urllib.request.urlopen", side_effect=self._fake_download_urlopen):
                download_response = self.coordinator_client.get(f"/files/{file_id}/download")
        finally:
            self.node_clients[self.node_urls[0]].get = original_get  # type: ignore[assignment]

        self.assertEqual(download_response.status_code, 200)
        self.assertEqual(download_response.content, payload)

    def test_download_sets_etag_and_supports_conditional_get(self) -> None:
        payload = (b"etag-test-" * 90000)[:2_600_000]

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            upload_response = self.coordinator_client.post(
                "/upload",
                files={"file": ("etag.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(upload_response.status_code, 200)
        file_id = upload_response.json()["file_id"]

        with patch("urllib.request.urlopen", side_effect=self._fake_download_urlopen):
            first_download = self.coordinator_client.get(f"/files/{file_id}/download")

        self.assertEqual(first_download.status_code, 200)
        etag = first_download.headers["etag"]
        self.assertTrue(etag)
        self.assertEqual(first_download.content, payload)

        with patch("urllib.request.urlopen", side_effect=self._fake_download_urlopen):
            second_download = self.coordinator_client.get(
                f"/files/{file_id}/download",
                headers={"If-None-Match": etag},
            )

        self.assertEqual(second_download.status_code, 304)
        self.assertEqual(second_download.content, b"")
