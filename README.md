# MiniDrive

This workspace contains a small, interview-focused prototype of a distributed
file store.

## Repository Layout

- `coordinator/` — FastAPI app for upload orchestration and chunking.
- `storage-node/` — FastAPI launcher for the storage node server.
- `storage_node/` — Importable Python package containing the storage-node API.
- `frontend/` — Vite + React scaffold (basic placeholder).
- `minidrive/` — Shared Python utilities, including chunking and distribution.
- `tests/` — Unit tests for chunking, distribution, replication, and storage-node behavior.

## Implemented So Far

- Day 1: streaming file chunking with a generator, round-robin distribution,
  replication scaffolding, coordinator scaffold, and frontend scaffold.
- Day 2: storage-node API with content-addressed storage:
  - `POST /chunks` stores chunk bytes under a hash-based path.
  - `GET /chunks/{hash}` returns raw chunk bytes.
  - `DELETE /chunks/{hash}` deletes a stored chunk.

The storage node uses hash-prefix directories similar to git object storage.
That keeps directories from getting too large, supports deduplication, and makes
integrity verification straightforward.

Python requirements (for coordinator/storage-node): see `requirements.txt`.

To run the tests for the Python library:

```bash
python -m unittest discover -s tests
```

To run the coordinator locally (example):

```bash
pip install -r requirements.txt
uvicorn coordinator.main:app --reload --port 8000
```

To run the storage node locally:

```bash
uvicorn storage_node.main:app --reload --port 8001
```

To run the storage-node launcher from the legacy path:

```bash
python storage-node/main.py
```

Example storage-node API usage:

```bash
curl -X POST http://127.0.0.1:8001/chunks \
  -H "X-Chunk-Hash: <sha256-hex-digest>" \
  --data-binary @chunk.bin

curl http://127.0.0.1:8001/chunks/<sha256-hex-digest> -o chunk.bin

curl -X DELETE http://127.0.0.1:8001/chunks/<sha256-hex-digest>
```
