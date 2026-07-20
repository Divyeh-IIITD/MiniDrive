from __future__ import annotations

import hashlib
import math
from typing import List

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

from coordinator.db import ChunkLocationRecord, ChunkRecord, FileRecord, SessionLocal
from minidrive.config import REPLICATION_FACTOR, STORAGE_NODE_URLS
from minidrive.chunking import iter_chunks, DEFAULT_CHUNK_SIZE
from minidrive.distribution import RoundRobinDistributor
from minidrive.replication import Replicator, ReplicationError


app = FastAPI(title="MiniDrive Coordinator")

DISTRIBUTOR = RoundRobinDistributor(STORAGE_NODE_URLS)
REPLICATOR = Replicator(STORAGE_NODE_URLS, replication_factor=REPLICATION_FACTOR)


def _replica_targets_for_chunk(chunk_index: int) -> List[str]:
    primary_node = DISTRIBUTOR.assign_by_index(chunk_index)
    primary_position = DISTRIBUTOR.nodes.index(primary_node)
    replication_factor = min(
        getattr(REPLICATOR, "replication_factor", len(DISTRIBUTOR.nodes)),
        len(DISTRIBUTOR.nodes),
    )
    return [
        DISTRIBUTOR.nodes[(primary_position + offset) % len(DISTRIBUTOR.nodes)]
        for offset in range(replication_factor)
    ]


def _estimate_total_chunks(file_size_bytes: int | None, chunk_size: int) -> int | None:
    if file_size_bytes is None:
        return None
    if file_size_bytes == 0:
        return 0
    return math.ceil(file_size_bytes / chunk_size)


def _create_upload_record(filename: str, size_bytes: int) -> int:
    with SessionLocal() as session:
        with session.begin():
            file_row = FileRecord(filename=filename, size_bytes=size_bytes, status="uploading")
            session.add(file_row)
            session.flush()
            return file_row.id


def _mark_upload_failed(file_id: int, size_bytes: int) -> None:
    with SessionLocal() as session:
        with session.begin():
            file_row = session.get(FileRecord, file_id)
            if file_row is None:
                raise RuntimeError(f"upload file {file_id} was not found")
            file_row.size_bytes = size_bytes
            file_row.status = "failed"


def _commit_upload_metadata(file_id: int, size_bytes: int, chunk_results: List[dict]) -> None:
    with SessionLocal() as session:
        with session.begin():
            file_row = session.get(FileRecord, file_id)
            if file_row is None:
                raise RuntimeError(f"upload file {file_id} was not found")

            pending_chunks: list[ChunkRecord] = []
            for chunk_result in chunk_results:
                chunk_row = ChunkRecord(
                    file_id=file_id,
                    chunk_index=chunk_result["index"],
                    hash=chunk_result["hash"],
                    size_bytes=chunk_result["size_bytes"],
                )
                session.add(chunk_row)
                pending_chunks.append(chunk_row)

            session.flush()

            for chunk_row, chunk_result in zip(pending_chunks, chunk_results):
                for node_url in chunk_result["stored_nodes"]:
                    session.add(
                        ChunkLocationRecord(
                            chunk_id=chunk_row.id,
                            node_url=node_url,
                            is_primary=node_url == chunk_result["primary_node"],
                        )
                    )

            file_row.size_bytes = size_bytes
            file_row.status = "committed"


# WHAT: Accept a file upload and stream it into fixed-size chunk files on disk.
# WHY: Streaming via `iter_chunks` keeps the memory footprint at O(chunk_size)
#      instead of O(file_size). This avoids spiking memory for large uploads and
#      lets us begin processing (replication, hashing, metadata writes) per chunk
#      without waiting for the entire file.
# TRADE-OFF: This single-pass streaming simplifies memory/IO behavior but means
#            we cannot randomly re-read later chunks without reopening or
#            rewinding the original upload stream.
@app.post("/upload")
async def upload(file: UploadFile = File(...), chunk_size: int = DEFAULT_CHUNK_SIZE) -> JSONResponse:
    # Use the underlying binary file object for streaming reads.
    stream = file.file

    file_size_hint = getattr(file, "size", None)
    file_id = _create_upload_record(file.filename or "upload", file_size_hint or 0)
    chunk_results: List[dict] = []
    total_bytes = 0

    # Crash boundary 1: the files row is already committed before chunking begins.
    # If the process dies here, the database contains an 'uploading' file with no
    # chunk metadata, which is visible as in-progress rather than falsely committed.
    for chunk in iter_chunks(stream, chunk_size=chunk_size):
        chunk_size_bytes = len(chunk.data)
        total_bytes += chunk_size_bytes
        chunk_hash = hashlib.sha256(chunk.data).hexdigest()
        primary_node = DISTRIBUTOR.assign_by_index(chunk.index)
        target_nodes = _replica_targets_for_chunk(chunk.index)

        try:
            landed_nodes = REPLICATOR.replicate_chunk(chunk)
            status = "stored"
            error_message = None
        except ReplicationError as exc:
            landed_nodes = list(exc.successes)
            status = "failed"
            error_message = str(exc)

            # Crash/cleanup note: the storage nodes may already contain some chunks
            # from earlier iterations. We only mark the file as failed here; node-side
            # cleanup is a later compensation/GC task.
            _mark_upload_failed(file_id, file_size_hint or total_bytes)

        chunk_results.append(
            {
                "index": chunk.index,
                "hash": chunk_hash,
                "size_bytes": chunk_size_bytes,
                "primary_node": primary_node,
                "target_nodes": target_nodes,
                "stored_nodes": landed_nodes,
                "status": status,
                "error": error_message,
            }
        )

        if status == "failed":
            return JSONResponse(
                {
                    "file_id": file_id,
                    "filename": file.filename,
                    "status": "failed",
                    "total_chunks": _estimate_total_chunks(file_size_hint, chunk_size) or len(chunk_results),
                    "chunks": chunk_results,
                },
                status_code=500,
            )

    # Crash boundary 2: all chunk metadata and the committed status are written in
    # one transaction. If the process dies before commit, PostgreSQL rolls back the
    # chunk rows and the status flip together, so no caller can ever observe a
    # 'committed' file without the full chunk set recorded.
    _commit_upload_metadata(file_id, file_size_hint or total_bytes, chunk_results)

    return JSONResponse(
        {
            "file_id": file_id,
            "filename": file.filename,
            "status": "committed",
            "total_chunks": _estimate_total_chunks(file_size_hint, chunk_size) or len(chunk_results),
            "chunks": chunk_results,
        }
    )


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})
