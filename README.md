# MiniDrive — Day 1

This workspace contains a small, interview-focused prototype of a distributed
file store. Day 1 scaffolding includes:

- `coordinator/` — FastAPI app with a streaming upload endpoint that splits
  incoming files into 1MB chunks using a generator (see `minidrive/chunking.py`).
- `storage-node/` — FastAPI app that stores raw chunks to local disk.
- `frontend/` — Vite + React scaffold (basic placeholder).

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
