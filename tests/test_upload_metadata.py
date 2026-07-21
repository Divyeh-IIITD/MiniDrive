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
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from coordinator import main as coordinator_main
from coordinator.db import Base, ChunkLocationRecord, ChunkRecord, FileRecord
from minidrive.chunking import Chunk, iter_chunks
from minidrive.distribution import RoundRobinDistributor
from minidrive.replication import Replicator, ReplicationError


class _FakeReplicator:
    def __init__(self, nodes: list[str], fail_after: int | None = None):
        self.nodes = nodes
        self.fail_after = fail_after
        self.calls = 0
        self.replication_factor = len(nodes)

    def replicate_chunk(self, chunk: Chunk) -> list[str]:
        self.calls += 1
        if self.fail_after is not None and self.calls > self.fail_after:
            raise ReplicationError("simulated replication failure", successes=self.nodes[:1], errors=[])
        return list(self.nodes)


class _UrlOpenResponse:
    def __init__(self, status_code: int):
        self._status_code = status_code

    def getcode(self) -> int:
        return self._status_code

    def __enter__(self) -> "_UrlOpenResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class MetadataUploadTests(unittest.TestCase):
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
        self.root = Path(self.temp_dir.name)

        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        coordinator_main.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)

        self.node_clients = {}
        self.node_urls = []

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _attach_in_process_nodes(self, node_count: int = 2) -> None:
        self.node_clients.clear()
        self.node_urls.clear()

        for index in range(node_count):
            module = self._load_storage_node_module(f"metadata_storage_node_{index}")
            node_dir = self.root / f"node_{index}"
            node_dir.mkdir(parents=True, exist_ok=True)
            module.STORAGE_DIR = node_dir
            client = TestClient(module.app)
            node_url = f"http://metadata-node-{index}.local/chunks"
            self.node_clients[node_url] = client
            self.node_urls.append(node_url)

        coordinator_main.DISTRIBUTOR = RoundRobinDistributor(self.node_urls)

    def _fake_urlopen(self, request, timeout=10):
        url = request.full_url if hasattr(request, "full_url") else str(request)
        client = self.node_clients[url]
        response = client.post("/chunks", content=request.data, headers=dict(request.headers))
        return _UrlOpenResponse(response.status_code)

    def _session(self):
        return coordinator_main.SessionLocal()

    def test_successful_upload_commits_file_chunks_and_locations(self) -> None:
        self._attach_in_process_nodes(2)
        coordinator_main.REPLICATOR = _FakeReplicator(self.node_urls)
        client = TestClient(coordinator_main.app)

        payload = b"metadata-success-case-" * 100000
        expected_chunks = list(iter_chunks(io.BytesIO(payload)))

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            response = client.post(
                "/upload",
                files={"file": ("metadata.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(response.status_code, 200)

        with self._session() as session:
            file_row = session.scalar(select(FileRecord))
            self.assertIsNotNone(file_row)
            self.assertEqual(file_row.status, "committed")
            self.assertEqual(file_row.size_bytes, len(payload))

            chunk_rows = session.scalars(select(ChunkRecord).order_by(ChunkRecord.chunk_index)).all()
            self.assertEqual(len(chunk_rows), len(expected_chunks))

            location_rows = session.scalars(select(ChunkLocationRecord)).all()
            self.assertEqual(len(location_rows), len(expected_chunks) * len(self.node_urls))

            chunk_counts = session.execute(
                select(ChunkRecord.chunk_index, func.count(ChunkLocationRecord.id))
                .join(ChunkLocationRecord, ChunkLocationRecord.chunk_id == ChunkRecord.id)
                .group_by(ChunkRecord.chunk_index)
                .order_by(ChunkRecord.chunk_index)
            ).all()
            self.assertEqual([count for _, count in chunk_counts], [len(self.node_urls)] * len(expected_chunks))

    def test_failed_upload_marks_failed_and_never_exposes_committed_state(self) -> None:
        self._attach_in_process_nodes(2)
        node_urls = list(self.node_urls)

        class FailingReplicator:
            def __init__(self):
                self.calls = 0
                self.replication_factor = len(node_urls)

            def replicate_chunk(self, chunk: Chunk) -> list[str]:
                self.calls += 1
                if self.calls == 1:
                    return list(node_urls)
                raise ReplicationError("simulated failure", successes=node_urls[:1], errors=[])

        coordinator_main.REPLICATOR = FailingReplicator()
        client = TestClient(coordinator_main.app)

        payload = b"metadata-failure-case-" * 120000

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            response = client.post(
                "/upload",
                files={"file": ("failure.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(response.status_code, 500)

        with self._session() as session:
            file_row = session.scalar(select(FileRecord))
            self.assertIsNotNone(file_row)
            self.assertEqual(file_row.status, "failed")
            committed_visible = session.scalar(select(FileRecord).where(FileRecord.status == "committed"))
            self.assertIsNone(committed_visible)
            self.assertEqual(session.scalar(select(func.count(ChunkRecord.id))), 0)

    def test_integration_upload_persists_db_records_matching_storage_nodes(self) -> None:
        self._attach_in_process_nodes(2)
        coordinator_main.REPLICATOR = Replicator(self.node_urls, replication_factor=2)
        client = TestClient(coordinator_main.app)

        payload = (b"day-four-integration-" * 90000)[:2_300_000]
        expected_chunks = [
            (chunk.index, hashlib.sha256(chunk.data).hexdigest(), chunk.data)
            for chunk in iter_chunks(io.BytesIO(payload))
        ]

        with patch("urllib.request.urlopen", side_effect=self._fake_urlopen):
            response = client.post(
                "/upload",
                files={"file": ("integration.bin", payload, "application/octet-stream")},
            )

        self.assertEqual(response.status_code, 200)

        with self._session() as session:
            file_row = session.scalar(select(FileRecord))
            self.assertEqual(file_row.status, "committed")

            chunk_rows = session.scalars(select(ChunkRecord).order_by(ChunkRecord.chunk_index)).all()
            self.assertEqual(len(chunk_rows), len(expected_chunks))

            for chunk_row, (_, expected_hash, _) in zip(chunk_rows, expected_chunks):
                self.assertEqual(chunk_row.hash, expected_hash)

            persisted_locations = session.scalars(select(ChunkLocationRecord)).all()
            self.assertEqual(len(persisted_locations), len(expected_chunks) * len(self.node_urls))

        for _, expected_hash, expected_data in expected_chunks:
            for node_url in self.node_urls:
                get_response = self.node_clients[node_url].get(f"/chunks/{expected_hash}")
                self.assertEqual(get_response.status_code, 200)
                self.assertEqual(get_response.content, expected_data)

    def test_download_rejects_non_committed_files(self) -> None:
        client = TestClient(coordinator_main.app)

        with self._session() as session:
            uploading_file = FileRecord(filename="uploading.bin", size_bytes=123, status="uploading")
            failed_file = FileRecord(filename="failed.bin", size_bytes=456, status="failed")
            session.add(uploading_file)
            session.add(failed_file)
            session.commit()

        for file_id in (uploading_file.id, failed_file.id):
            response = client.get(f"/files/{file_id}/download")
            self.assertEqual(response.status_code, 409)
