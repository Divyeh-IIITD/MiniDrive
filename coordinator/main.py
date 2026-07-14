from __future__ import annotations

import os
from typing import List

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

from minidrive.chunking import iter_chunks, DEFAULT_CHUNK_SIZE


app = FastAPI(title="MiniDrive Coordinator")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)


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

    saved_chunks: List[str] = []
    for chunk in iter_chunks(stream, chunk_size=chunk_size):
        chunk_path = os.path.join(DATA_DIR, f"{file.filename}.chunk{chunk.index}")
        # write each chunk to disk immediately (small files in data/ for Day 1)
        with open(chunk_path, "wb") as fh:
            fh.write(chunk.data)
        saved_chunks.append(chunk_path)

    return JSONResponse({"filename": file.filename, "chunks": len(saved_chunks)})


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})
