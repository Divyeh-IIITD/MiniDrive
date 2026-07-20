from __future__ import annotations

import hashlib
import os
import uuid
from typing import List

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

from minidrive.config import REPLICATION_FACTOR, STORAGE_NODE_URLS
from minidrive.chunking import iter_chunks, DEFAULT_CHUNK_SIZE
from minidrive.distribution import RoundRobinDistributor
from minidrive.replication import Replicator, ReplicationError


app = FastAPI(title="MiniDrive Coordinator")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

DISTRIBUTOR = RoundRobinDistributor(STORAGE_NODE_URLS)
REPLICATOR = Replicator(STORAGE_NODE_URLS, replication_factor=REPLICATION_FACTOR)


def _replica_targets_for_chunk(chunk_index: int) -> List[str]:
    primary_node = DISTRIBUTOR.assign_by_index(chunk_index)
    primary_position = DISTRIBUTOR.nodes.index(primary_node)
    return [
        DISTRIBUTOR.nodes[(primary_position + offset) % len(DISTRIBUTOR.nodes)]
        for offset in range(REPLICATOR.replication_factor)
    ]


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

    file_id = uuid.uuid4().hex
    chunk_results: List[dict] = []
    for chunk in iter_chunks(stream, chunk_size=chunk_size):
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

        chunk_results.append(
            {
                "index": chunk.index,
                "hash": chunk_hash,
                "primary_node": primary_node,
                "target_nodes": target_nodes,
                "stored_nodes": landed_nodes,
                "status": status,
                "error": error_message,
            }
        )

    return JSONResponse(
        {
            "file_id": file_id,
            "filename": file.filename,
            "total_chunks": len(chunk_results),
            "chunks": chunk_results,
        }
    )


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})
