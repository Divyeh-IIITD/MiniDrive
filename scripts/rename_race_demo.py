from __future__ import annotations

import argparse
import concurrent.futures
import json
import tempfile
import threading
import time
import sys
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from coordinator import main as coordinator_main
from coordinator.db import Base, FileRecord


def build_environment(db_path: Path):
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    coordinator_main.SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return engine


def seed_file(filename: str) -> int:
    with coordinator_main.SessionLocal() as session:
        with session.begin():
            file_row = FileRecord(filename=filename, size_bytes=1, status="committed")
            session.add(file_row)
            session.flush()
            return file_row.id


def run_race(file_id: int, strategy: str) -> dict:
    client = TestClient(coordinator_main.app)
    rename_names = [f"rename-{index}.txt" for index in range(10)]
    barrier = threading.Barrier(len(rename_names))

    original_naive = coordinator_main._rename_file_naive

    def delayed_naive(target_file_id: int, new_name: str):
        with coordinator_main.SessionLocal() as session:
            with session.begin():
                file_row = session.get(FileRecord, target_file_id)
                if file_row is None:
                    raise RuntimeError("missing file")
                previous_name = file_row.filename
                time.sleep(0.05)
                file_row.filename = new_name
                file_row.version = file_row.version + 1
                session.flush()
                session.refresh(file_row)
                return file_row, previous_name

    try:
        if strategy == "naive":
            coordinator_main._rename_file_naive = delayed_naive

        def send_rename(new_name: str):
            barrier.wait()
            response = client.put(
                f"/files/{file_id}/rename",
                json={"new_name": new_name, "strategy": strategy},
            )
            return {"status_code": response.status_code, "body": response.json() if response.content else None}

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(rename_names)) as executor:
            responses = list(executor.map(send_rename, rename_names))

        with coordinator_main.SessionLocal() as session:
            file_row = session.get(FileRecord, file_id)
            final_state = {
                "file_id": file_id,
                "filename": file_row.filename if file_row else None,
                "version": file_row.version if file_row else None,
            }

        return {
            "strategy": strategy,
            "requested_names": rename_names,
            "responses": responses,
            "final_state": final_state,
        }
    finally:
        coordinator_main._rename_file_naive = original_naive


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a 10-way concurrent rename race against the coordinator.")
    parser.add_argument("--strategy", choices=["naive", "optimistic", "pessimistic"], default="naive")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "rename_race.sqlite3"
        engine = build_environment(db_path)
        try:
            file_id = seed_file("original.txt")
            result = run_race(file_id, args.strategy)
            print(json.dumps(result, indent=2, sort_keys=True))
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()