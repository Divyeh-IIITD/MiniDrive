from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse


app = FastAPI(title="MiniDrive Storage Node")

STORAGE_DIR = Path(__file__).resolve().parent / "data"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

HEX_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
HASH_PREFIX_LEN = 2


# WHAT: Validate and normalize a chunk hash so the storage layout stays predictable.
# WHY: Content-addressed storage relies on stable lowercase hex keys; validating early prevents path traversal, malformed filenames, and accidental non-determinism. This is O(length of hash), which is trivial compared with disk I/O.
# TRADE-OFF: The implementation currently assumes SHA-256 hex strings only; supporting multiple algorithms would require a tagged hash format and a slightly more complex directory layout.
def normalize_chunk_hash(chunk_hash: str) -> str:
    normalized_hash = chunk_hash.strip().lower()
    if not HEX_HASH_RE.match(normalized_hash):
        raise HTTPException(status_code=400, detail="chunk hash must be a 64-character lowercase hex SHA-256 digest")
    return normalized_hash


# WHAT: Map a chunk hash to a git-style sharded path under the node's storage directory.
# WHY: Prefix sharding keeps directories from getting too large, which avoids slow directory listings and filesystem hot spots while preserving O(1) lookup by hash.
# TRADE-OFF: Hash-prefix sharding adds one more directory lookup, but it greatly reduces filesystem scaling problems versus dumping every object into one folder.
def chunk_path_for_hash(chunk_hash: str) -> Path:
    normalized_hash = normalize_chunk_hash(chunk_hash)
    return STORAGE_DIR / normalized_hash[:HASH_PREFIX_LEN] / normalized_hash[HASH_PREFIX_LEN:]


# WHAT: Remove empty parent directories after deleting a chunk.
# WHY: This keeps the sharded directory tree tidy without scanning the whole storage tree. Cleanup is bounded to the chunk's own prefix path, so it remains cheap.
# TRADE-OFF: Directory cleanup is best-effort; concurrent writes/deletes may leave empty directories behind, which is harmless.
def cleanup_empty_parents(path: Path) -> None:
    parent = path.parent
    while parent != STORAGE_DIR and parent.exists():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


# WHAT: Store a raw chunk body under its content hash and deduplicate identical uploads.
# WHY: We stream the request body through a SHA-256 hasher while writing to a temp file, so memory stays O(1) with respect to the chunk size. Using the hash as the filename gives deduplication for free and lets us verify integrity by comparing the computed digest to the supplied digest.
# TRADE-OFF: This is sequential disk I/O, so it is slower than an in-memory buffer for tiny chunks; however, it scales safely to larger chunks and preserves correctness under load.
@app.post("/chunks")
async def store_chunk(request: Request, x_chunk_hash: str = Header(alias="X-Chunk-Hash")) -> JSONResponse:
    expected_hash = normalize_chunk_hash(x_chunk_hash)
    final_path = chunk_path_for_hash(expected_hash)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    hasher = hashlib.sha256()
    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=final_path.parent)
    temp_path = Path(temp_file.name)

    try:
        with temp_file:
            async for body_chunk in request.stream():
                if not body_chunk:
                    continue
                hasher.update(body_chunk)
                temp_file.write(body_chunk)

        actual_hash = hasher.hexdigest()
        if actual_hash != expected_hash:
            temp_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail="chunk hash mismatch")

        if final_path.exists():
            temp_path.unlink(missing_ok=True)
            return JSONResponse({"status": "deduplicated", "hash": expected_hash, "path": str(final_path)})

        os.replace(temp_path, final_path)
        return JSONResponse({"status": "stored", "hash": expected_hash, "path": str(final_path)}, status_code=201)
    except HTTPException:
        raise
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"failed to store chunk: {exc}") from exc


# WHAT: Stream the bytes for a chunk addressed by its hash.
# WHY: Returning the file via a streaming response avoids loading the chunk into memory and preserves a simple raw-bytes download path. The caller can verify the hash after download if desired.
# TRADE-OFF: We return raw bytes only; metadata headers like ETag/Last-Modified are intentionally deferred to a later step.
@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str) -> Response:
    path = chunk_path_for_hash(chunk_hash)
    if not path.exists():
        raise HTTPException(status_code=404, detail="chunk not found")

    def file_iterator():
        with path.open("rb") as fh:
            while True:
                data = fh.read(64 * 1024)
                if not data:
                    break
                yield data

    return StreamingResponse(file_iterator(), media_type="application/octet-stream")


# WHAT: Delete a chunk from disk using its content hash as the lookup key.
# WHY: Hash-addressed deletion is O(1) path resolution plus O(1) unlink; it also allows clean dedupe semantics because deleting one chunk does not depend on sequential IDs or references from other objects.
# TRADE-OFF: If multiple higher-level files reference the same chunk, a production system would need reference counting or GC; this Day 2 API deletes the object immediately for simplicity.
@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str) -> JSONResponse:
    path = chunk_path_for_hash(chunk_hash)
    if not path.exists():
        raise HTTPException(status_code=404, detail="chunk not found")

    path.unlink()
    cleanup_empty_parents(path)
    return JSONResponse({"status": "deleted", "hash": normalize_chunk_hash(chunk_hash)})


# WHAT: Report that the storage node is running.
# WHY: A health check gives the coordinator and tests a cheap liveness probe with O(1) work.
# TRADE-OFF: This only checks process reachability, not disk space or hash-index integrity.
@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})
