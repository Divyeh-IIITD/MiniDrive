from __future__ import annotations

import io
import hashlib
import math
import urllib.error
import urllib.request
from typing import List

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

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


def _load_download_manifest(file_id: int) -> tuple[str, int, List[dict]]:
    with SessionLocal() as session:
        file_row = session.get(FileRecord, file_id)
        if file_row is None:
            raise HTTPException(status_code=404, detail="file not found")

        if file_row.status != "committed":
            raise HTTPException(status_code=409, detail=f"file {file_id} is not ready for download")

        chunk_rows = (
            session.query(ChunkRecord)
            .filter(ChunkRecord.file_id == file_id)
            .order_by(ChunkRecord.chunk_index.asc())
            .all()
        )

        manifest: List[dict] = []
        for chunk_row in chunk_rows:
            ordered_locations = sorted(
                list(chunk_row.locations),
                key=lambda location: (not location.is_primary, location.id),
            )
            if not ordered_locations:
                raise HTTPException(
                    status_code=502,
                    detail=f"chunk {chunk_row.chunk_index} has no stored locations",
                )

            manifest.append(
                {
                    "index": chunk_row.chunk_index,
                    "hash": chunk_row.hash,
                    "size_bytes": chunk_row.size_bytes,
                    "locations": [location.node_url for location in ordered_locations],
                }
            )

        return file_row.filename, file_row.size_bytes, manifest


def _is_retryable_download_error(error: Exception) -> bool:
    if isinstance(error, urllib.error.HTTPError):
        return error.code == 404 or error.code >= 500
    return isinstance(error, (urllib.error.URLError, TimeoutError, OSError, ValueError))


def _fetch_verified_chunk(node_url: str, chunk_hash: str, expected_size_bytes: int) -> bytes:
    request = urllib.request.Request(f"{node_url.rstrip('/')}/{chunk_hash}", method="GET")
    hasher = hashlib.sha256()
    buffer = io.BytesIO()

    with urllib.request.urlopen(request, timeout=10) as response:
        while True:
            data = response.read(64 * 1024)
            if not data:
                break
            hasher.update(data)
            buffer.write(data)

    chunk_bytes = buffer.getvalue()
    if len(chunk_bytes) != expected_size_bytes:
        raise ValueError(
            f"chunk {chunk_hash} size mismatch: expected {expected_size_bytes}, got {len(chunk_bytes)}"
        )

    actual_hash = hasher.hexdigest()
    if actual_hash != chunk_hash:
        raise ValueError(f"chunk {chunk_hash} failed verification: got {actual_hash}")

    return chunk_bytes


def _download_chunk_with_failover(chunk: dict) -> bytes:
    last_error: Exception | None = None

    for node_url in chunk["locations"]:
        try:
            return _fetch_verified_chunk(node_url, chunk["hash"], chunk["size_bytes"])
        except Exception as error:
            if not _is_retryable_download_error(error):
                last_error = error
                break
            last_error = error

    raise HTTPException(
        status_code=502,
        detail=f"failed to download chunk {chunk['index']} after failover attempts: {last_error}",
    )


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


# WHAT: Reassemble a committed file by streaming its verified chunks back in order.
# WHY: This is the point where replication pays off. The coordinator can treat multiple chunk copies as interchangeable fallbacks, which turns a single-node outage into a normal retry instead of a user-visible failure.
# TRADE-OFF: We verify and buffer one chunk at a time before yielding it, which preserves bounded memory usage while still allowing failover and integrity checks; a mid-stream failure can still truncate an already-started response, but it cannot contaminate later chunks with unverified bytes.
@app.get("/files/{file_id}/download")
async def download_file(file_id: int):
    filename, file_size_bytes, chunk_manifest = _load_download_manifest(file_id)

    def chunk_stream():
        for chunk in chunk_manifest:
            yield _download_chunk_with_failover(chunk)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(file_size_bytes),
        "X-File-Filename": filename,
    }

    return StreamingResponse(chunk_stream(), media_type="application/octet-stream", headers=headers)


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})
