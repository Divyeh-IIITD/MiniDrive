# MiniDrive

MiniDrive is a compact distributed file-store prototype with a Python/FastAPI coordinator, replicated storage nodes, a SQL metadata layer, and a Vite + React frontend.

## What It Does

- Streams uploads into fixed-size chunks.
- Replicates chunks across storage nodes.
- Persists file, chunk, and replica metadata atomically.
- Streams downloads back through the coordinator with failover, hash verification, and conditional GET support.
- Shows upload progress in the browser and polls file status instead of keeping a persistent socket open.
- Highlights duplicate files in the file list and lets you delete server-side entries from the UI.

## UI

- Minimal pastel landing layout with floating status cards.
- Upload panel with byte-level progress bar.
- File ledger with duplicate highlighting, download, delete, and refresh actions.

## Repository Layout

- `coordinator/` - FastAPI control plane for uploads, file listing, status, downloads, and rename operations.
- `storage_node/` - FastAPI storage-node implementation.
- `storage-node/` - Legacy launcher for the storage node.
- `frontend/` - Vite + React UI for uploads, polling, file listing, and downloads.
- `minidrive/` - Shared chunking, placement, configuration, and replication utilities.
- `migrations/` - Alembic schema migrations.
- `scripts/` - Utility scripts, including the rename race demo and load test.
- `docs/` - Design, milestone, debugging, architecture, and function-rationale notes.
- `tests/` - Unit and integration tests.

## Setup

Python 3.13 is the validated interpreter in this workspace. Install backend dependencies with:

```bash
pip install -r requirements.txt
```

For the frontend:

```bash
cd frontend
npm install
```

## Run

Fastest local start on Windows:

```powershell
./start-dev.ps1
```

That launcher installs dependencies if needed, creates a local SQLite DB, starts three isolated storage nodes, starts the coordinator, and opens the frontend dev server in separate PowerShell windows.

The frontend uses `http://127.0.0.1:8000` by default, so the upload UI works immediately after the launcher starts.

If you want to skip reinstalling packages, use:

```powershell
./start-dev.ps1 -SkipInstall
```

Start the coordinator:

```bash
python -m uvicorn coordinator.main:app --reload --port 8000
```

Start the storage node:

```bash
python -m uvicorn storage_node.main:app --reload --port 8001
```

Start the frontend:

```bash
cd frontend
npm run dev
```

If your frontend is served on the default Vite port, it will talk to `http://127.0.0.1:8000` unless you override `VITE_API_BASE_URL`.

## Test

Run the Python test suite:

```bash
python -m unittest discover -s tests
```

Run the focused load test:

```bash
python scripts/load_test.py --files 24 --workers 8
```

## Notes

- The frontend uses polling for upload status because it is simpler than maintaining a WebSocket connection for this scale.
- The storage node uses hash-prefix sharding similar to git object storage to keep directories manageable.
- The coordinator exposes `GET /files`, `GET /files/{file_id}/status`, and `GET /files/{file_id}/download` for the browser UI.
- For local development, `start-dev.ps1` launches a SQLite-backed coordinator plus three isolated storage nodes on ports 8001, 8002, and 8003.

## API Examples

```bash
curl -X POST http://127.0.0.1:8001/chunks \
  -H "X-Chunk-Hash: <sha256-hex-digest>" \
  --data-binary @chunk.bin

curl http://127.0.0.1:8001/chunks/<sha256-hex-digest> -o chunk.bin

curl -X DELETE http://127.0.0.1:8001/chunks/<sha256-hex-digest>
```
