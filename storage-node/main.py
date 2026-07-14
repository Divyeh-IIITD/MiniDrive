from __future__ import annotations

import os

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse


app = FastAPI(title="MiniDrive Storage Node")

STORAGE_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(STORAGE_DIR, exist_ok=True)


# WHAT: Store an incoming chunk to local disk under a predictable filename.
# WHY: A simple HTTP endpoint keeps node responsibilities minimal (store/serve
#      raw bytes). Writing to disk immediately ensures durability on the node.
# TRADE-OFF: This accepts raw bytes in the request body and writes synchronously;
#            it does not yet implement checksums, partial-write recovery, or
#            versioning — those are Day 2 concerns.
@app.post("/chunks/{file_id}/{chunk_index}")
async def store_chunk(file_id: str, chunk_index: int, request: Request) -> JSONResponse:
    body = await request.body()
    path = os.path.join(STORAGE_DIR, f"{file_id}.chunk{chunk_index}")
    with open(path, "wb") as fh:
        fh.write(body)
    return JSONResponse({"status": "ok", "path": path})


# WHAT: Stream a stored chunk back to the caller.
# WHY: Streaming the file avoids loading the chunk entirely into memory on the
#      node for large chunks and allows the coordinator/frontend to handle
#      conditional GETs or partial content later.
# TRADE-OFF: No ETag/Last-Modified logic yet; this is a raw stream endpoint.
@app.get("/chunks/{file_id}/{chunk_index}")
def get_chunk(file_id: str, chunk_index: int) -> Response:
    path = os.path.join(STORAGE_DIR, f"{file_id}.chunk{chunk_index}")
    if not os.path.exists(path):
        return JSONResponse({"error": "not found"}, status_code=404)

    def file_iterator():
        with open(path, "rb") as fh:
            while True:
                data = fh.read(64 * 1024)
                if not data:
                    break
                yield data

    return StreamingResponse(file_iterator(), media_type="application/octet-stream")
