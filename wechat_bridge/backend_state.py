"""Private durable jobs; transactions are short ownership, never execution leases."""
import contextlib
import hashlib
import json
import os
import sqlite3
import time


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode("utf-8")).hexdigest()


class BackendState:
    def __init__(self, store, config, owner_id, bot_id):
        self.path = os.path.join(store.state_dir, "backend_jobs.sqlite3")
        self.binding = {"version": 1, "owner": owner_id, "bot": bot_id,
                        "config_digest": digest(config), "base": os.path.realpath(store.base)}
        connection = sqlite3.connect(self.path, timeout=2)
        try:
            os.chmod(self.path, 0o600)
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript('''
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, sender TEXT NOT NULL,
                    status TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS inputs (id TEXT PRIMARY KEY, sender TEXT NOT NULL,
                    kind TEXT NOT NULL, job_id TEXT, record TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS approvals (id TEXT PRIMARY KEY, status TEXT NOT NULL,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS outputs (id TEXT PRIMARY KEY, phase TEXT NOT NULL,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS routes (sender TEXT PRIMARY KEY, backend TEXT NOT NULL,
                    revision INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS sessions (key TEXT PRIMARY KEY, session_id TEXT NOT NULL);
            ''')
            row = connection.execute("SELECT value FROM metadata WHERE key='binding'").fetchone()
            if row and json.loads(row[0]) != self.binding:
                raise ValueError("managed state identity/config changed; use its original profile or a fresh bot/state")
            if not row:
                connection.execute("INSERT INTO metadata VALUES ('binding',?)", (encode(self.binding),))
            connection.commit()
        finally:
            connection.close()

    @contextlib.contextmanager
    def transaction(self):
        # External provider operations must occur AFTER leaving this block.
        # No refresh or approval wait extends this short transaction/ownership.
        connection = sqlite3.connect(self.path, timeout=2)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        started = time.monotonic()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            if time.monotonic() - started >= 90:
                raise TimeoutError("short state transaction exceeded ownership deadline")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def get_job(connection, job_id):
        row = connection.execute("SELECT payload FROM jobs WHERE id=?", (job_id,)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def put_job(connection, job):
        connection.execute("INSERT OR REPLACE INTO jobs VALUES (?,?,?,?)", (
            job['execution_id'], job['sender_key'], job['status'], encode(job)))

    def recover(self, now):
        with self.transaction() as connection:
            for row in connection.execute("SELECT payload FROM jobs WHERE status IN "
                    "('running','waiting_approval','cancelling')").fetchall():
                job = json.loads(row[0])
                job.update(status="unknown", error_code="restart_requires_reconciliation", updated_at=now)
                self.put_job(connection, job)
            connection.execute("UPDATE approvals SET status='invalidated' WHERE status IN ('pending','deciding')")
            # A crash between intent and filesystem publication cannot prove send state.
            connection.execute("UPDATE outputs SET phase='unknown' WHERE phase='publishing'")
