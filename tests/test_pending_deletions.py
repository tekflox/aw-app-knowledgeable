"""Card `Connector: agent-facing pending-deletion tools` — the MCP tool
bodies in `pending_deletions.py`, exercised directly: a plain `_run` helper
driving coroutines, `client`'s async HTTP functions monkeypatched rather
than `httpx` itself, same style as `test_fs_sync.py`/`test_bulk_ingest.py`
(and the same `_FakeDb` double, duplicated per that file's own stated
convention)."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest, pending_deletions  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeDb:
    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    def create(self, name: str, columns_sql: str) -> str:
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({columns_sql})")
        self._conn.commit()
        return name

    def execute(self, name: str, sql: str, params: dict | None = None):
        stmt = sql.replace("{table}", name)
        cur = self._conn.execute(stmt, params or {})
        if stmt.strip().lower().startswith("select"):
            rows = cur.fetchall()
            self._conn.commit()
            return rows
        self._conn.commit()
        return cur


@pytest.fixture()
def db():
    return _FakeDb()


@pytest.fixture(autouse=True)
def _fresh_table_flag(monkeypatch):
    monkeypatch.setattr(pending_deletions, "_table_ensured", False)
    # Journal-reconcile tests below call through to `bulk_ingest._ensure_
    # table` on this same module-level-guarded table — reset its flag too,
    # same reason `test_fs_sync.py`/`test_bulk_ingest.py` each reset it for
    # their own module-level `db` fixture (a fresh in-memory sqlite
    # connection per test; the flag must not survive past the connection
    # it was set for).
    monkeypatch.setattr(bulk_ingest, "_table_ensured", False)


def _audit_rows(db):
    try:
        rows = db.execute(pending_deletions.TABLE, "SELECT * FROM {table} ORDER BY ts")
    except sqlite3.OperationalError:
        return []  # a validation failure before any write never creates the table
    return [dict(r) for r in rows]


_DOC_A = {"id": "doc-a", "source_path": "notion/kanban/a.md"}
_DOC_B = {"id": "doc-b", "source_path": "notion/kanban/b.md"}
_DOC_C = {"id": "doc-c", "source_path": "cli_reference/c.md"}
_DOC_ROOT = {"id": "doc-root", "source_path": "top-level.md"}


def _pending(docs):
    async def fake(bucket=None):
        return {"documents": list(docs)}, None

    return fake


def _seed_journal_row(db, relpath: str, external_id: str) -> None:
    """A pending-delete fs-sync journal row, same shape `test_fs_sync.py`'s
    own `_seed_uploaded` seeds — this file doesn't import that one (two
    separate test modules by this repo's own stated convention), so a
    trimmed local equivalent lives here instead."""
    bulk_ingest._ensure_table(db)
    db.execute(
        bulk_ingest.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, external_id, scanned_at, updated_at, "
        "missing_scans, pending_delete) "
        "VALUES (:relpath, 'abc', 'main', :status, :external_id, 0, 0, 2, 1)",
        {"relpath": relpath, "status": bulk_ingest.STATUS_UPLOADED, "external_id": external_id},
    )


def _journal_row(db, relpath: str):
    rows = db.execute(
        bulk_ingest.TABLE, "SELECT * FROM {table} WHERE relpath = :relpath", {"relpath": relpath}
    )
    rows = list(rows)
    return dict(rows[0]) if rows else None


# ---------------------------------------------------------------------------
# list_pending_deletions
# ---------------------------------------------------------------------------


def test_list_pending_deletions_groups_by_immediate_parent_dir(monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B, _DOC_C, _DOC_ROOT]))
    text, is_error = _run(pending_deletions.list_pending_deletions({}))
    assert is_error is False
    payload = json.loads(text)
    assert payload["count"] == 4
    assert payload["prefix_breakdown"] == [
        {"dir": "notion/kanban/", "count": 2},
        {"dir": "cli_reference/", "count": 1},
        {"dir": "(root)", "count": 1},
    ]


def test_list_pending_deletions_surfaces_a_backend_error(monkeypatch):
    async def fake(bucket=None):
        return None, "HTTP 503: degraded"

    monkeypatch.setattr(client, "get_documents_pending_delete", fake)
    text, is_error = _run(pending_deletions.list_pending_deletions({}))
    assert is_error is True
    assert "HTTP 503" in text


# ---------------------------------------------------------------------------
# approve_pending_deletions — scoped, counted, no blanket
# ---------------------------------------------------------------------------


def test_approve_requires_a_bucket(db):
    text, is_error = _run(pending_deletions.approve_pending_deletions({}, db, "actor"))
    assert is_error is True
    assert "bucket is required" in text
    assert _audit_rows(db) == []


def test_approve_requires_a_scope(db):
    text, is_error = _run(
        pending_deletions.approve_pending_deletions({"bucket": "main", "expected_count": 0}, db, "actor")
    )
    assert is_error is True
    assert "no blanket" in text
    assert _audit_rows(db) == []


def test_approve_requires_expected_count(db):
    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "path_prefix": "notion/"}, db, "actor"
        )
    )
    assert is_error is True
    assert "expected_count is required" in text
    assert _audit_rows(db) == []


def test_approve_rejects_on_count_mismatch_and_deletes_nothing(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B]))
    deleted = []

    async def fake_delete(node_id, *, bucket):
        deleted.append(node_id)
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "path_prefix": "notion/kanban/", "expected_count": 1}, db, "agent:coder run=42"
        )
    )
    assert is_error is True
    assert "expected_count mismatch" in text
    assert "scope matched 2" in text
    assert deleted == [], "a rejected call must not delete anything"

    rows = _audit_rows(db)
    assert len(rows) == 1
    assert rows[0]["action"] == "approve"
    assert rows[0]["actor"] == "agent:coder run=42"
    assert rows[0]["outcome"] == "rejected_count_mismatch"
    assert rows[0]["matched_count"] == 2
    assert rows[0]["succeeded_count"] == 0
    assert rows[0]["expected_count"] == 1


def test_approve_deletes_the_matched_scope_and_writes_an_audit_row(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B, _DOC_C]))
    deleted = []

    async def fake_delete(node_id, *, bucket):
        assert bucket == "main"
        deleted.append(node_id)
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "path_prefix": "notion/kanban/", "expected_count": 2}, db, "agent:coder run=42"
        )
    )
    assert is_error is False
    payload = json.loads(text)
    assert payload["outcome"] == "ok"
    assert sorted(payload["approved"]) == ["doc-a", "doc-b"]
    assert payload["failed"] == []
    assert sorted(deleted) == ["doc-a", "doc-b"]
    assert "doc-c" not in deleted, "out-of-scope document must not be touched"

    rows = _audit_rows(db)
    assert len(rows) == 1
    assert rows[0]["action"] == "approve"
    assert rows[0]["outcome"] == "ok"
    assert rows[0]["matched_count"] == 2
    assert rows[0]["succeeded_count"] == 2
    assert rows[0]["failed_count"] == 0


def test_approve_by_explicit_node_ids(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B, _DOC_C]))
    deleted = []

    async def fake_delete(node_id, *, bucket):
        deleted.append(node_id)
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "node_ids": ["doc-a", "doc-c"], "expected_count": 2}, db, "actor"
        )
    )
    assert is_error is False
    assert sorted(deleted) == ["doc-a", "doc-c"]


def test_approve_reports_a_partial_failure_as_an_error(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B]))

    async def fake_delete(node_id, *, bucket):
        if node_id == "doc-a":
            return True, None
        return False, "HTTP 404: Node not found"

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "path_prefix": "notion/kanban/", "expected_count": 2}, db, "actor"
        )
    )
    assert is_error is True, "a partial failure must not read as a clean success"
    payload = json.loads(text)
    assert payload["outcome"] == "partial"
    assert payload["approved"] == ["doc-a"]
    assert payload["failed"] == [{"id": "doc-b", "source_path": "notion/kanban/b.md", "error": "HTTP 404: Node not found"}]

    rows = _audit_rows(db)
    assert rows[0]["succeeded_count"] == 1
    assert rows[0]["failed_count"] == 1
    assert rows[0]["outcome"] == "partial"


# ---------------------------------------------------------------------------
# approve_pending_deletions reconciles this connector's own fs-sync
# journal for every document it just deleted, in the same operation —
# rather than leaving a dangling `pending_delete=1` row for a tick's
# `fs_sync._gc_dead_pending_delete_rows` that may never promptly run.
# ---------------------------------------------------------------------------


def test_approve_deletes_the_journal_row_for_every_succeeded_document(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B]))
    _seed_journal_row(db, _DOC_A["source_path"], _DOC_A["id"])
    _seed_journal_row(db, _DOC_B["source_path"], _DOC_B["id"])

    async def fake_delete(node_id, *, bucket):
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "path_prefix": "notion/kanban/", "expected_count": 2}, db, "actor"
        )
    )
    assert is_error is False
    payload = json.loads(text)
    assert payload["journal_reconciled"] == 2
    assert _journal_row(db, _DOC_A["source_path"]) is None
    assert _journal_row(db, _DOC_B["source_path"]) is None


def test_approve_only_reconciles_the_journal_for_documents_that_actually_succeeded(db, monkeypatch):
    """The partial-failure sibling of the test above — a failed delete must
    leave that document's journal row exactly as it was, still
    `pending_delete=1`, so a later tick's GC (or a retried approve) still
    has something to act on."""
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B]))
    _seed_journal_row(db, _DOC_A["source_path"], _DOC_A["id"])
    _seed_journal_row(db, _DOC_B["source_path"], _DOC_B["id"])

    async def fake_delete(node_id, *, bucket):
        if node_id == _DOC_A["id"]:
            return True, None
        return False, "HTTP 404: Node not found"

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "path_prefix": "notion/kanban/", "expected_count": 2}, db, "actor"
        )
    )
    assert is_error is True  # the partial failure itself
    payload = json.loads(text)
    assert payload["journal_reconciled"] == 1
    assert _journal_row(db, _DOC_A["source_path"]) is None
    assert _journal_row(db, _DOC_B["source_path"]) is not None


def test_approve_leaves_a_journal_row_alone_when_a_different_document_has_since_reclaimed_its_path(
    db, monkeypatch
):
    """A race the `(relpath, external_id)` match guards against: between
    this call's own listing and its delete, some other writer re-created
    a document at the same `source_path` (e.g. the fs-sync upload pass
    picking the file back up). That new document's `external_id` differs
    from the one this call just deleted, so the journal row — now
    correctly describing the NEW claimant — must not be swept away with
    the old one."""
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A]))
    _seed_journal_row(db, _DOC_A["source_path"], "doc-a-reclaimed")

    async def fake_delete(node_id, *, bucket):
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "node_ids": [_DOC_A["id"]], "expected_count": 1}, db, "actor"
        )
    )
    assert is_error is False
    payload = json.loads(text)
    assert payload["journal_reconciled"] == 0
    row = _journal_row(db, _DOC_A["source_path"])
    assert row is not None
    assert row["external_id"] == "doc-a-reclaimed"


def test_approve_with_no_journal_row_at_all_still_succeeds(db, monkeypatch):
    """Not every approved document necessarily has a connector journal row
    (e.g. one marked some other way) — reconciliation must be a no-op for
    those, not an error."""
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A]))

    async def fake_delete(node_id, *, bucket):
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    text, is_error = _run(
        pending_deletions.approve_pending_deletions(
            {"bucket": "main", "node_ids": [_DOC_A["id"]], "expected_count": 1}, db, "actor"
        )
    )
    assert is_error is False
    payload = json.loads(text)
    assert payload["journal_reconciled"] == 0


# ---------------------------------------------------------------------------
# decline_pending_deletions — may be blanket, expected_count optional
# ---------------------------------------------------------------------------


def test_decline_requires_a_bucket(db):
    text, is_error = _run(pending_deletions.decline_pending_deletions({}, db, "actor"))
    assert is_error is True
    assert "bucket is required" in text
    assert _audit_rows(db) == []


def test_decline_with_no_scope_declines_every_pending_document(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B, _DOC_C]))
    restored = []

    async def fake_restore(node_id, *, bucket):
        restored.append(node_id)
        return {"id": node_id}, None

    monkeypatch.setattr(client, "restore_document", fake_restore)

    text, is_error = _run(pending_deletions.decline_pending_deletions({"bucket": "main"}, db, "actor"))
    assert is_error is False
    payload = json.loads(text)
    assert payload["outcome"] == "ok"
    assert sorted(restored) == ["doc-a", "doc-b", "doc-c"]

    rows = _audit_rows(db)
    assert rows[0]["action"] == "decline"
    assert rows[0]["path_prefix"] is None
    assert rows[0]["node_ids"] is None
    assert rows[0]["expected_count"] is None
    assert rows[0]["matched_count"] == 3


def test_decline_optional_expected_count_mismatch_restores_nothing(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B]))
    restored = []

    async def fake_restore(node_id, *, bucket):
        restored.append(node_id)
        return {"id": node_id}, None

    monkeypatch.setattr(client, "restore_document", fake_restore)

    text, is_error = _run(
        pending_deletions.decline_pending_deletions({"bucket": "main", "expected_count": 1}, db, "actor")
    )
    assert is_error is True
    assert "nothing was restored" in text
    assert restored == []

    rows = _audit_rows(db)
    assert rows[0]["outcome"] == "rejected_count_mismatch"


def test_decline_scoped_by_path_prefix(db, monkeypatch):
    monkeypatch.setattr(client, "get_documents_pending_delete", _pending([_DOC_A, _DOC_B, _DOC_C]))
    restored = []

    async def fake_restore(node_id, *, bucket):
        restored.append(node_id)
        return {"id": node_id}, None

    monkeypatch.setattr(client, "restore_document", fake_restore)

    text, is_error = _run(
        pending_deletions.decline_pending_deletions(
            {"bucket": "main", "path_prefix": "cli_reference/"}, db, "actor"
        )
    )
    assert is_error is False
    assert restored == ["doc-c"]
