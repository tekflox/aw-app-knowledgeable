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

from knowledgeable_app import pending_deletions  # noqa: E402
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
