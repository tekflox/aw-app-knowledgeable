"""`orphan_reconcile.py` — the cleanup pass for the journal-orphaned
duplicate documents the two-identity gap stranded (card `3fa5bf3b-9510-812c-
a439-d7fcc773f2bd`).

Three properties carry the risk here and each has its own test: it marks
only documents that provably have a live sibling claiming their path, it
never calls anything but `mark-delete` (no `force`, no `DELETE`), and it
caps a single pass at the §7 valve's own ceiling even though it runs
outside the tick the valve guards.

Same conventions as the sibling modules: plain ``_run``, ``client``'s async
functions monkeypatched, ``_FakeDb`` duplicated rather than shared (see
``test_bulk_ingest.py``'s module docstring).
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest, fs_sync, orphan_reconcile, pending_deletions  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeDb:
    """Double for ``ctx.db`` — see ``test_bulk_ingest.py``'s identical class
    for the full rationale."""

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


@pytest.fixture(autouse=True)
def _workspace_home(tmp_path, monkeypatch):
    monkeypatch.setenv("AW_WORKSPACE_HOME", str(tmp_path))


@pytest.fixture(autouse=True)
def _fresh_table_flags(monkeypatch):
    monkeypatch.setattr(bulk_ingest, "_table_ensured", False)
    monkeypatch.setattr(pending_deletions, "_table_ensured", False)


@pytest.fixture(autouse=True)
def _no_other_writes(monkeypatch):
    """Every destructive call this module must NEVER make, wired to blow up
    rather than silently succeed: the reconciler marks, and marking is the
    only thing it is allowed to do."""

    async def forbidden(*a, **k):  # pragma: no cover - the point is it never runs
        raise AssertionError("the reconciler must only ever call mark_delete")

    monkeypatch.setattr(client, "delete_document", forbidden)
    monkeypatch.setattr(client, "mark_delete_by_path", forbidden)
    monkeypatch.setattr(client, "restore_document", forbidden)


@pytest.fixture()
def db() -> _FakeDb:
    return _FakeDb()


@pytest.fixture()
def marks(monkeypatch) -> list[dict]:
    """Records every `mark-delete` call, in order."""
    calls: list[dict] = []

    async def fake_mark_delete(node_id, *, bucket, reason=None):
        calls.append({"id": node_id, "bucket": bucket, "reason": reason})
        return {"id": node_id, "sync_status": "pending_delete"}, None

    monkeypatch.setattr(client, "mark_delete", fake_mark_delete)
    return calls


def _documents(monkeypatch, documents: list[dict], err: str | None = None):
    async def fake_get_documents(bucket=None):
        if err:
            return None, err
        return {"documents": documents}, None

    monkeypatch.setattr(client, "get_documents", fake_get_documents)


def _seed(db: _FakeDb, relpath: str, external_id: str | None) -> None:
    bulk_ingest._ensure_table(db)
    now = time.time()
    db.execute(
        bulk_ingest.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, external_id, scanned_at, updated_at) "
        "VALUES (:relpath, 'h', 'main', :status, :external_id, :now, :now)",
        {
            "relpath": relpath, "status": bulk_ingest.STATUS_UPLOADED,
            "external_id": external_id, "now": now,
        },
    )


def _doc(doc_id: str, source_path: str, *, slug: str | None = None, sync_status: str = "") -> dict:
    return {
        "id": doc_id, "source_path": source_path, "workspace_slug": slug,
        "sync_status": sync_status,
    }


def _audit_rows(db: _FakeDb) -> list[dict]:
    """``[]`` also covers "the audit table was never even created" — which
    is what a dry run should leave behind, since `_record_audit` is the only
    thing in this path that creates it."""
    try:
        rows = db.execute(pending_deletions.TABLE, "SELECT * FROM {table}")
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# classify — the join rule, with no db and no HTTP in the way
# ---------------------------------------------------------------------------


def test_classify_separates_the_four_cases():
    claims = {
        "memory/a.md": "doc-live",      # live claimant, orphan beside it
        "memory/b.md": "doc-b",         # the journal names this doc itself
        "memory/d.md": None,            # adopted/never-converged → not a claim
    }
    claims = {k: v for k, v in claims.items() if v}  # `_journal_claims` skips NULLs
    claimed_ids = set(claims.values())
    documents = [
        _doc("doc-live", "memory/a.md", slug="aw"),
        _doc("doc-orphan", "memory/a.md"),                            # eligible
        _doc("doc-marked", "memory/a.md", sync_status="pending_delete"),  # already marked
        _doc("doc-b", "memory/b.md", slug="aw"),                      # referenced
        _doc("doc-blind", "memory/c.md"),                             # no journal row
        _doc("doc-blind2", "memory/d.md"),                            # row, but no external_id
    ]
    classes = orphan_reconcile.classify(documents, claims, claimed_ids)
    assert [d["id"] for d in classes["eligible"]] == ["doc-orphan"]
    assert [d["id"] for d in classes["already_marked"]] == ["doc-marked"]
    assert sorted(d["id"] for d in classes["no_live_claimant"]) == ["doc-blind", "doc-blind2"]


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------


def test_dry_run_is_the_default_and_marks_nothing(db, marks, monkeypatch):
    _seed(db, "memory/a.md", "doc-live")
    _documents(monkeypatch, [
        _doc("doc-live", "memory/a.md", slug="aw"),
        _doc("doc-orphan", "memory/a.md"),
    ])

    report = _run(orphan_reconcile.reconcile(db))

    assert report["ok"] is True
    assert report["applied"] is False
    assert report["eligible"] == 1
    assert report["graph_documents"] == 2
    assert marks == [], "a dry run must not touch the server"
    assert _audit_rows(db) == []
    assert report["sample"][0]["live_claimant"] == "doc-live"


def test_apply_marks_only_the_orphan_and_only_through_mark_delete(db, marks, monkeypatch):
    _seed(db, "memory/a.md", "doc-live")
    _documents(monkeypatch, [
        _doc("doc-live", "memory/a.md", slug="aw"),
        _doc("doc-orphan", "memory/a.md"),
    ])

    report = _run(orphan_reconcile.reconcile(db, apply=True, actor="coder run=1"))

    assert report["applied"] is True
    assert report["marked"] == 1
    assert report["marked_ids"] == ["doc-orphan"]
    assert report["remaining"] == 0
    assert [m["id"] for m in marks] == ["doc-orphan"]
    assert marks[0]["bucket"] == "main", (
        "an omitted bucket 400s as ambiguous the moment a second writable bucket exists "
        "(memory fs-sync-writes-need-an-explicit-bucket)"
    )
    assert "3fa5bf3b" in marks[0]["reason"]
    # The live document is never touched, whatever else happens.
    assert "doc-live" not in [m["id"] for m in marks]


def test_apply_writes_one_audit_row_naming_what_it_marked(db, marks, monkeypatch):
    _seed(db, "memory/a.md", "doc-live")
    _documents(monkeypatch, [
        _doc("doc-live", "memory/a.md", slug="aw"),
        _doc("doc-orphan", "memory/a.md"),
    ])

    _run(orphan_reconcile.reconcile(db, apply=True, actor="coder run=7"))

    rows = _audit_rows(db)
    assert len(rows) == 1
    assert rows[0]["action"] == orphan_reconcile.AUDIT_ACTION
    assert rows[0]["actor"] == "coder run=7"
    assert rows[0]["outcome"] == "ok"
    assert json.loads(rows[0]["node_ids"]) == ["doc-orphan"]
    assert rows[0]["succeeded_count"] == 1


def test_a_document_already_pending_delete_is_reported_not_re_marked(db, marks, monkeypatch):
    _seed(db, "memory/a.md", "doc-live")
    _documents(monkeypatch, [
        _doc("doc-live", "memory/a.md", slug="aw"),
        _doc("doc-old", "memory/a.md", sync_status="pending_delete"),
    ])

    report = _run(orphan_reconcile.reconcile(db, apply=True))

    assert report["already_pending_delete"] == 1
    assert report["eligible"] == 0
    assert marks == []


def test_an_orphan_with_no_live_claimant_is_never_marked(db, marks, monkeypatch):
    """§17's journal-blind class: nothing else claims the path, so there is
    no evidence this is a stale duplicate rather than the only copy. Out of
    scope by design — counted, never marked."""
    _seed(db, "memory/adopted.md", None)
    _documents(monkeypatch, [
        _doc("doc-blind", "memory/never-journalled.md"),
        _doc("doc-blind2", "memory/adopted.md"),
    ])

    report = _run(orphan_reconcile.reconcile(db, apply=True))

    assert report["no_live_claimant"] == 2
    assert report["eligible"] == 0
    assert marks == []


def test_one_pass_can_never_exceed_the_valve_ceiling(db, marks, monkeypatch):
    """The §7 valve cannot see this job (it runs outside the tick), so the
    cap is re-imposed here from the valve's OWN formula. 300 tracked journal
    rows ⇒ ceiling 30, and a caller asking for 1,000 still gets 30."""
    documents = []
    for i in range(300):
        relpath = f"notion/kanban/card-{i}.md"
        _seed(db, relpath, f"doc-live-{i}")
        documents.append(_doc(f"doc-live-{i}", relpath, slug="aw"))
        documents.append(_doc(f"doc-orphan-{i}", relpath))
    _documents(monkeypatch, documents)

    report = _run(orphan_reconcile.reconcile(db, apply=True, limit=1000))

    ceiling = fs_sync.mass_delete_threshold(300)
    assert ceiling == 30
    assert report["cap"] == {
        "requested": 1000, "batch": 30, "valve_ceiling": 30, "tracked_rows": 300,
        "formula": f"max({fs_sync.MASS_DELETE_FLOOR}, 10% of 300)",
    }
    assert report["eligible"] == 300
    assert report["marked"] == 30
    assert report["remaining"] == 270
    assert len(marks) == 30
    # Stable, path-ordered batches — so repeated passes drain the backlog
    # instead of re-shuffling it.
    assert [m["id"] for m in marks] == [
        d["id"] for d in sorted(
            (d for d in documents if d["id"].startswith("doc-orphan")),
            key=lambda d: (d["source_path"], d["id"]),
        )[:30]
    ]


def test_the_default_batch_is_smaller_than_the_ceiling(db, marks, monkeypatch):
    """A default pass stays well inside the valve's ceiling rather than
    riding it — the backlog drains in reviewable chunks."""
    documents = []
    for i in range(2000):
        relpath = f"notion/kanban/card-{i}.md"
        _seed(db, relpath, f"doc-live-{i}")
        documents.append(_doc(f"doc-live-{i}", relpath, slug="aw"))
        documents.append(_doc(f"doc-orphan-{i}", relpath))
    _documents(monkeypatch, documents)

    report = _run(orphan_reconcile.reconcile(db, apply=True))

    assert fs_sync.mass_delete_threshold(2000) == 200
    assert orphan_reconcile.DEFAULT_MARK_LIMIT == 100
    assert report["marked"] == 100
    assert report["remaining"] == 1900


def test_a_listing_failure_reports_the_error_and_marks_nothing(db, marks, monkeypatch):
    _seed(db, "memory/a.md", "doc-live")
    _documents(monkeypatch, [], err="HTTP 500: boom")

    report = _run(orphan_reconcile.reconcile(db, apply=True))

    assert report["ok"] is False
    assert "HTTP 500" in report["error"]
    assert marks == []


def test_a_failed_mark_is_reported_as_partial_and_audited(db, monkeypatch):
    _seed(db, "memory/a.md", "doc-live")
    _seed(db, "memory/b.md", "doc-live-b")
    _documents(monkeypatch, [
        _doc("doc-live", "memory/a.md", slug="aw"),
        _doc("doc-orphan-a", "memory/a.md"),
        _doc("doc-live-b", "memory/b.md", slug="aw"),
        _doc("doc-orphan-b", "memory/b.md"),
    ])

    async def fake_mark_delete(node_id, *, bucket, reason=None):
        if node_id == "doc-orphan-b":
            return None, "HTTP 404: Node not found"
        return {"id": node_id}, None

    monkeypatch.setattr(client, "mark_delete", fake_mark_delete)

    report = _run(orphan_reconcile.reconcile(db, apply=True))

    assert report["outcome"] == "partial"
    assert report["marked_ids"] == ["doc-orphan-a"]
    assert report["failed"][0]["id"] == "doc-orphan-b"
    audit = _audit_rows(db)[0]
    assert audit["outcome"] == "partial"
    assert audit["failed_count"] == 1


def test_prefix_breakdown_tells_an_operator_where_the_orphans_are(db, marks, monkeypatch):
    documents = []
    for prefix, count in (("notion", 3), ("skills", 2)):
        for i in range(count):
            relpath = f"{prefix}/f{i}.md"
            _seed(db, relpath, f"doc-live-{prefix}-{i}")
            documents.append(_doc(f"doc-live-{prefix}-{i}", relpath, slug="aw"))
            documents.append(_doc(f"doc-orphan-{prefix}-{i}", relpath))
    _documents(monkeypatch, documents)

    report = _run(orphan_reconcile.reconcile(db))

    assert report["eligible_by_prefix"] == {"notion": 3, "skills": 2}
