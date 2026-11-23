"""Single-host durable queue with atomic claims and bounded retained payloads."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from uuid import uuid4

TERMINAL = ("completed", "failed", "cancelled", "timed_out")
PUBLIC_COLUMNS = (
    "id,filename,profile,artifact_sha256,state,created_at,started_at,finished_at,"
    "cancel_requested,error,(report IS NOT NULL) AS has_report,evaluation_succeeded"
)


class QueueFull(ValueError):
    pass


class JobStore:
    def __init__(self, directory: Path):
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / "jobs.sqlite3"
        with self.connection() as db:
            if self.path.stat().st_uid == os.geteuid():
                self.path.chmod(0o660)
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY, filename TEXT NOT NULL, profile TEXT NOT NULL,
                  artifact_sha256 TEXT NOT NULL, state TEXT NOT NULL,
                  created_at REAL NOT NULL, started_at REAL, finished_at REAL,
                  cancel_requested INTEGER NOT NULL DEFAULT 0, error TEXT,
                  request TEXT, report TEXT, evaluation_succeeded INTEGER);
                CREATE TABLE IF NOT EXISTS worker (id INTEGER PRIMARY KEY CHECK(id=1), updated_at REAL, profiles TEXT);
                CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT);
            """)
            db.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('deployment', ?)",
                (uuid4().hex,),
            )

    @contextmanager
    def connection(self):
        with sqlite3.connect(self.path, timeout=10) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=10000")
            yield db

    def deployment_id(self):
        with self.connection() as db:
            return db.execute(
                "SELECT value FROM metadata WHERE name='deployment'"
            ).fetchone()[0]

    def readiness(self):
        with self.connection() as db:
            row = db.execute("SELECT * FROM worker WHERE id=1").fetchone()
        return (
            json.loads(row["profiles"])
            if row and time.time() - row["updated_at"] < 15
            else []
        )

    def heartbeat(self, profiles):
        with self.connection() as db:
            db.execute(
                "INSERT OR REPLACE INTO worker VALUES (1, ?, ?)",
                (time.time(), json.dumps(profiles)),
            )

    def submit(self, request: dict):
        encoded = json.dumps(request, allow_nan=False)
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            count, pending, size = db.execute(
                "SELECT count(*), sum(state IN ('queued','running')), coalesce(sum(length(request)+coalesce(length(report),0)),0) FROM jobs"
            ).fetchone()
            if (
                count >= 100
                or (pending or 0) >= 8
                or size + len(encoded) > 128 * 1024 * 1024
            ):
                raise QueueFull(
                    "Job storage or queue capacity reached; remove completed jobs"
                )
            identity = uuid4().hex
            digest = hashlib.sha256(request["source"].encode("utf-8")).hexdigest()
            db.execute(
                "INSERT INTO jobs (id,filename,profile,artifact_sha256,state,created_at,request) VALUES (?,?,?,?,?,?,?)",
                (
                    identity,
                    request["filename"],
                    request["profile"],
                    digest,
                    "queued",
                    time.time(),
                    encoded,
                ),
            )
        return self.get(identity)

    @staticmethod
    def public(row):
        if row is None:
            return None
        value = dict(row)
        value["has_report"] = bool(value["has_report"])
        value["cancel_requested"] = bool(value["cancel_requested"])
        value["evaluation_succeeded"] = (
            bool(value["evaluation_succeeded"])
            if value["evaluation_succeeded"] is not None
            else None
        )
        return value

    def get(self, identity):
        with self.connection() as db:
            return self.public(
                db.execute(
                    f"SELECT {PUBLIC_COLUMNS} FROM jobs WHERE id=?", (identity,)
                ).fetchone()
            )

    def list(self):
        with self.connection() as db:
            return [
                self.public(row)
                for row in db.execute(
                    f"SELECT {PUBLIC_COLUMNS} FROM jobs ORDER BY created_at DESC LIMIT 100"
                )
            ]

    def claim(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM jobs WHERE state='queued' ORDER BY created_at LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            db.execute(
                "UPDATE jobs SET state='running',started_at=? WHERE id=?",
                (time.time(), row["id"]),
            )
            return dict(row)

    def cancel(self, identity):
        with self.connection() as db:
            db.execute(
                "UPDATE jobs SET cancel_requested=1 WHERE id=? AND state IN ('queued','running')",
                (identity,),
            )
            db.execute(
                "UPDATE jobs SET state='cancelled',finished_at=?,request=NULL WHERE id=? AND state='queued'",
                (time.time(), identity),
            )
        return self.get(identity)

    def finish(self, identity, *, state, error=None, report=None):
        encoded = json.dumps(report, allow_nan=False) if report is not None else None
        if encoded and len(encoded) > 8 * 1024 * 1024:
            state, error, encoded = "failed", "report_too_large", None
        succeeded = None
        if encoded is not None:
            runtime = (report.get("energy", {}).get("measurements") or {}).get(
                "runtime"
            ) or {}
            succeeded = not report.get("errors") and (
                not runtime or runtime.get("status") == "ok"
            )
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            size = db.execute(
                "SELECT coalesce(sum(coalesce(length(request),0)+coalesce(length(report),0)),0) FROM jobs WHERE id != ?",
                (identity,),
            ).fetchone()[0]
            current = db.execute(
                "SELECT coalesce(length(request),0) FROM jobs WHERE id=?", (identity,)
            ).fetchone()
            if (
                encoded
                and size + (current[0] if current else 0) + len(encoded)
                > 128 * 1024 * 1024
            ):
                state, error, encoded, succeeded = (
                    "failed",
                    "job_storage_limit_exceeded",
                    None,
                    None,
                )
            row = db.execute(
                "SELECT cancel_requested FROM jobs WHERE id=?", (identity,)
            ).fetchone()
            if row and row[0]:
                state, error, encoded, succeeded = "cancelled", None, None, None
            db.execute(
                "UPDATE jobs SET state=?,error=?,report=?,finished_at=?,evaluation_succeeded=? WHERE id=? AND state='running'",
                (state, error, encoded, time.time(), succeeded, identity),
            )

    def report(self, identity):
        with self.connection() as db:
            row = db.execute(
                "SELECT report FROM jobs WHERE id=?", (identity,)
            ).fetchone()
            return json.loads(row[0]) if row and row[0] else None

    def source(self, identity):
        with self.connection() as db:
            row = db.execute(
                "SELECT request FROM jobs WHERE id=?", (identity,)
            ).fetchone()
            request = json.loads(row[0]) if row and row[0] else None
            return (
                {"filename": request["filename"], "source": request["source"]}
                if request
                else None
            )

    def delete(self, identity):
        with self.connection() as db:
            row = db.execute(
                "SELECT state FROM jobs WHERE id=?", (identity,)
            ).fetchone()
            if row and row[0] not in TERMINAL:
                raise ValueError("Cancel the active job before deleting it")
            db.execute("DELETE FROM jobs WHERE id=?", (identity,))
            return row is not None

    def recover(self):
        with self.connection() as db:
            db.execute(
                "UPDATE jobs SET state='failed',error='worker_restarted',finished_at=? WHERE state='running'",
                (time.time(),),
            )
