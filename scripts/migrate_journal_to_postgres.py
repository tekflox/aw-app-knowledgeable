#!/usr/bin/env python3
"""One-shot: import the bulk-ingest journal's SQLite rows into the
workspace Postgres table `app__knowledgeable__bulk_ingest_files`
(docs/design/aw-knowledgeable-sqlite-exit.md §4 step 5), then delete the
SQLite file — but only once the counts have been verified.

Lives in `scripts/`, outside `knowledgeable_app/`, so the card's own check
hint (`grep sqlite3.connect`) stays at zero even before this script's own
one-time job is done.

Runs INSIDE the workspace container (this app is Tier-1/in-process, sharing
that Python environment) — it imports `src.apps.db_tables` the same way
`src/cli/local_client.py`'s own callers do, which only resolves with
`/opt/aw-workspace` (or `$AW_WORKSPACE_CONTAINER_DIR`) on `sys.path` and the
server's own env vars (`AW_WORKSPACE_SCHEMA`, the workspace DB URL) already
set in this process — i.e. run this via the workspace server's own process,
not a bare `python3` with a hand-picked venv.

Run only while no ingest tick is active — check
`GET /api/apps/knowledgeable/bulk-ingest/status` (or
`aw-workspace-cli knowledgeable-ingest status`) first; a tick writing to the
SQLite file while this script reads it would race the import.

Usage:
    python3 /opt/aw-workspace/repos/aw-app-knowledgeable/scripts/migrate_journal_to_postgres.py
"""
from __future__ import annotations

import os
import sqlite3
import sys

sys.path.insert(0, os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace"))

APP_ID = "knowledgeable"
TABLE = "app__knowledgeable__bulk_ingest_files"

_COLUMNS = (
    "relpath", "sha256", "bucket", "status", "external_id",
    "canonical_relpath", "server_deduplicated", "error", "scanned_at", "updated_at",
)


def _workspace_home() -> str:
    home = os.environ.get("AW_WORKSPACE_HOME")
    if home:
        return home
    root = os.path.realpath(os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace"))
    return os.path.join(root, ".aw-workspace")


def _journal_path() -> str:
    return os.path.join(_workspace_home(), "data", "knowledgeable", "bulk_ingest.sqlite")


def _read_sqlite_rows(path: str) -> list[tuple]:
    if not os.path.exists(path):
        return []
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(f"SELECT {', '.join(_COLUMNS)} FROM files").fetchall()
        return [tuple(row[c] for c in _COLUMNS) for row in rows]
    finally:
        conn.close()


def _status_counts(rows: list[tuple]) -> dict[str, int]:
    status_idx = _COLUMNS.index("status")
    counts: dict[str, int] = {}
    for row in rows:
        counts[row[status_idx]] = counts.get(row[status_idx], 0) + 1
    return counts


def main() -> int:
    from src.apps.db_tables import DbTables

    journal_path = _journal_path()
    sqlite_rows = _read_sqlite_rows(journal_path)
    sqlite_count = len(sqlite_rows)
    sqlite_status_counts = _status_counts(sqlite_rows)

    db = DbTables()
    db.create(
        APP_ID,
        TABLE,
        "relpath TEXT PRIMARY KEY, sha256 TEXT NOT NULL, bucket TEXT NOT NULL, "
        "status TEXT NOT NULL, external_id TEXT, canonical_relpath TEXT, "
        "server_deduplicated INTEGER NOT NULL DEFAULT 0, error TEXT, "
        "scanned_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL",
    )
    db.execute(APP_ID, TABLE, "CREATE INDEX IF NOT EXISTS ix_bulk_ingest_files_status ON {table} (status)")
    db.execute(APP_ID, TABLE, "CREATE INDEX IF NOT EXISTS ix_bulk_ingest_files_bucket ON {table} (bucket)")
    db.execute(APP_ID, TABLE, "CREATE INDEX IF NOT EXISTS ix_bulk_ingest_files_sha256 ON {table} (sha256)")

    for row in sqlite_rows:
        params = dict(zip(_COLUMNS, row))
        db.execute(
            APP_ID,
            TABLE,
            """
            INSERT INTO {table}
                (relpath, sha256, bucket, status, external_id, canonical_relpath,
                 server_deduplicated, error, scanned_at, updated_at)
            VALUES
                (:relpath, :sha256, :bucket, :status, :external_id, :canonical_relpath,
                 :server_deduplicated, :error, :scanned_at, :updated_at)
            ON CONFLICT (relpath) DO NOTHING
            """,
            params,
        )

    pg_rows = db.execute(APP_ID, TABLE, "SELECT status FROM {table}")
    pg_count = len(pg_rows)
    pg_status_counts: dict[str, int] = {}
    for row in pg_rows:
        pg_status_counts[row[0]] = pg_status_counts.get(row[0], 0) + 1

    print(f"sqlite_count={sqlite_count}")
    print(f"pg_count={pg_count}")
    print(f"sqlite_status_counts={sqlite_status_counts}")
    print(f"pg_status_counts={pg_status_counts}")

    if sqlite_count != pg_count or sqlite_status_counts != pg_status_counts:
        print(
            "ABORT: counts do not match — the SQLite file is NOT deleted. "
            "Re-run once resolved; the import is idempotent (ON CONFLICT DO NOTHING).",
            file=sys.stderr,
        )
        return 1

    if os.path.exists(journal_path):
        os.remove(journal_path)
        print(f"OK: counts match ({sqlite_count} total) — deleted {journal_path}")
    else:
        print(f"OK: counts match ({sqlite_count} total) — no SQLite file existed to delete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
