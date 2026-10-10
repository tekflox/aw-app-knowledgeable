"""The §13 bulk-ingest driver's engine — scan/canonicalize, the bounded
tick, and the report, exercised directly (no FastAPI, no real network, no
pytest-asyncio — same style as ``test_ingest_key_push.py``: a plain ``_run``
helper driving coroutines, and ``client``'s own async functions monkeypatched
rather than ``httpx`` itself, since what's under test here is the engine's
own decisions, not the HTTP shaping (that's ``test_client.py``'s job).

``_FakeDb``/``_FakeLease`` are minimal doubles for ``ctx.db``/``ctx.state.
lease`` (docs/design/aw-knowledgeable-sqlite-exit.md) — real enough to
exercise the actual SQL ``bulk_ingest.py`` sends (named ``:param`` binding,
``{table}`` substitution, the Postgres ``ON CONFLICT ... DO UPDATE SET
... excluded.col`` upsert, which stdlib sqlite3 also accepts since 3.24) so a
SQL typo still fails a test, without a live workspace Postgres.
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeDb:
    """Double for ``ctx.db`` — in-memory sqlite, kept open for the test's
    duration like the real lazy-singleton Postgres engine. ``{table}`` is
    substituted with the literal table name (no schema qualification needed
    here); name-prefix validation is the real facade's job, not this
    module's, so it is not reproduced here."""

    def __init__(self) -> None:
        # check_same_thread=False: production `run_tick` dispatches each sync
        # db call to a threadpool worker via `run_in_threadpool` — the real
        # SQLAlchemy engine hands each thread its own pooled connection, but
        # this double is one shared sqlite3.Connection, so it needs the same
        # opt-out tenant_store.py's own sqlite test engine uses.
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


class _FakeLease:
    """Double for ``ctx.state.lease`` — single-process, non-blocking,
    exclusive-by-flag (the real one is flock-backed; this test never needs
    more than "is somebody already holding this name")."""

    def __init__(self) -> None:
        self._held = False

    def claim(self, name: str) -> bool:
        if self._held:
            return False
        self._held = True
        return True

    def release(self, name: str) -> None:
        self._held = False


@pytest.fixture(autouse=True)
def _workspace_home(tmp_path, monkeypatch):
    monkeypatch.setenv("AW_WORKSPACE_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture(autouse=True)
def _buckets_already_exist(monkeypatch):
    """§13.1's precondition, satisfied by default in every test — the one
    test that cares about a bucket-creation failure overrides this itself."""

    async def fake_create_bucket(name):
        return {"bucket": name, "already_existed": True}, None

    monkeypatch.setattr(client, "create_bucket", fake_create_bucket)


@pytest.fixture(autouse=True)
def _fresh_table_flag(monkeypatch):
    """`_ensure_table`'s guard is a module-level flag — each test gets a
    fresh `_FakeDb` (a fresh, empty sqlite backing), so the flag must reset
    too or a test after the first would skip CREATE TABLE against a
    connection that was never given one."""
    monkeypatch.setattr(bulk_ingest, "_table_ensured", False)


@pytest.fixture()
def db() -> _FakeDb:
    return _FakeDb()


@pytest.fixture()
def lease() -> _FakeLease:
    return _FakeLease()


def _write(root: Path, relpath: str, content: str) -> None:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _ok_status(claiming: bool = False, pending: int = 0):
    async def fake(bucket=None):
        return {"extraction": {"claiming": claiming}, "processing": {"pending": pending}}, None

    return fake


def _journal_rows(db: _FakeDb) -> list[dict]:
    return [dict(r) for r in db.execute(bulk_ingest.TABLE, "SELECT * FROM {table}")]


# ---------------------------------------------------------------------------
# _priority — the canonical-copy rule (§13.2)
# ---------------------------------------------------------------------------


def test_priority_curated_subtree_beats_mapped_folders():
    assert bulk_ingest._priority("crispal/notes.md") < bulk_ingest._priority(
        "mapped_folders/repos/crispal/notes.md"
    )


def test_priority_real_repo_beats_monolith_checkout():
    assert bulk_ingest._priority("mapped_folders/repos/crispal/notes.md") < bulk_ingest._priority(
        "mapped_folders/repos/agentic-workspace/docs/knowledge_base/crispal/notes.md"
    )


def test_priority_real_repo_beats_apps_slug_mirror():
    assert bulk_ingest._priority("mapped_folders/repos/aw-app-crispal/notes.md") < bulk_ingest._priority(
        "mapped_folders/apps/crispal/notes.md"
    )


# ---------------------------------------------------------------------------
# scan — hashing, canonicalization, journaling
# ---------------------------------------------------------------------------


def test_scan_finds_files_across_all_six_subtrees_into_one_bucket(_workspace_home, db):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "crispal body")
    _write(root, "memory/b.md", "memory body")
    _write(root, "notion/c.md", "notion body")
    _write(root, "cli_reference/restart.md", "cli reference body")
    _write(root, "skills/aw-demo.md", "skill body")
    _write(root, "mapped_folders/docs/design/d.md", "mapped design body")

    counts = bulk_ingest.scan(db)
    assert counts["scanned"] == 6
    assert counts["canonical"] == 6
    assert counts["alias"] == 0

    status = bulk_ingest.status(db)
    assert status["bucket"] == "main"
    assert status["by_subtree"] == {
        "crispal": {"pending": 1},
        "memory": {"pending": 1},
        "notion": {"pending": 1},
        "cli_reference": {"pending": 1},
        "skills": {"pending": 1},
        "mapped_folders": {"pending": 1},
    }


def test_scan_never_walks_the_skipped_code_map_subtrees(_workspace_home, db):
    """The §8 skip rule, as a property of the WALK. These two subtrees are
    generated code maps codegraph answers better (Frederico, 2026-10-10) and
    were 79% of the ingested corpus before the collapse.

    MUTATION: drop the `is_skipped(relpath)` guard in `scan()` and this goes
    red on `scanned == 1` — the journal would hold the two code-map paths as
    `pending`, which is exactly how the next tick re-creates documents the
    migration deleted.
    """
    root = bulk_ingest.kb_root()
    _write(root, "mapped_folders/docs/design/keep.md", "hand-written design")
    _write(root, "mapped_folders/repos/aw-stack/scripts/drop.md", "code map")
    _write(root, "mapped_folders/aw-workspace/src/also-drop.md", "code map")

    counts = bulk_ingest.scan(db)
    assert counts["scanned"] == 1
    relpaths = sorted(r["relpath"] for r in _journal_rows(db))
    assert relpaths == ["mapped_folders/docs/design/keep.md"]


def test_scan_canonicalizes_duplicate_content_across_prefixes(_workspace_home, db):
    """§13.2's cross-prefix canonicalization: the same content reachable from
    more than one place, uploaded exactly once.

    The three prefixes this originally measured (`mapped_folders/repos/
    agentic-workspace/`, a real repo, and `mapped_folders/apps/`) are mostly
    gone with the §8 skip rule — `mapped_folders/repos/` is never walked —
    so the case is re-pinned on prefixes that still exist. The rule itself is
    unchanged: curated subtrees (priority 0) beat anything under
    `mapped_folders/`, and `apps/` loses to everything.
    """
    root = bulk_ingest.kb_root()
    same = "duplicated across three prefixes"
    _write(root, "crispal/atendimento/notes.md", same)
    _write(root, "mapped_folders/docs/design/notes.md", same)
    _write(root, "mapped_folders/apps/crispal/docs/knowledge_base/notes.md", same)

    counts = bulk_ingest.scan(db)
    assert counts["scanned"] == 3
    assert counts["canonical"] == 1
    assert counts["alias"] == 2

    canonical = db.execute(
        bulk_ingest.TABLE, "SELECT relpath FROM {table} WHERE status = :status",
        {"status": bulk_ingest.STATUS_PENDING},
    )
    assert [r["relpath"] for r in canonical] == ["crispal/atendimento/notes.md"]
    aliases = db.execute(
        bulk_ingest.TABLE,
        "SELECT relpath, canonical_relpath FROM {table} WHERE status = :status ORDER BY relpath",
        {"status": bulk_ingest.STATUS_ALIAS},
    )
    assert [r["canonical_relpath"] for r in aliases] == [
        "crispal/atendimento/notes.md",
        "crispal/atendimento/notes.md",
    ]


def test_scan_skips_oversized_files_with_a_reason(_workspace_home, db):
    root = bulk_ingest.kb_root()
    big = "x" * (bulk_ingest.MAX_FILE_BYTES + 1)
    _write(root, "memory/huge.md", big)

    counts = bulk_ingest.scan(db)
    assert counts["skipped"] == 1

    rows = db.execute(
        bulk_ingest.TABLE, "SELECT status, error FROM {table} WHERE relpath = :relpath",
        {"relpath": "memory/huge.md"},
    )
    row = rows[0]
    assert row["status"] == bulk_ingest.STATUS_SKIPPED
    assert "10MB" in row["error"]


def test_rescan_does_not_duplicate_or_regress_an_uploaded_row(_workspace_home, db, lease, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "stable content")
    bulk_ingest.scan(db)

    uploaded = {"calls": 0}

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        uploaded["calls"] += 1
        return {"id": "doc-1", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)
    result = _run(bulk_ingest.run_tick(db, lease))
    assert result["uploaded"] == 1
    assert uploaded["calls"] == 1

    # Re-scan: the uploaded row must not flip back to pending.
    bulk_ingest.scan(db)
    status = bulk_ingest.status(db)
    assert status["by_subtree"]["crispal"] == {"uploaded": 1}


# ---------------------------------------------------------------------------
# run_tick — the ignition guard, the per-tick ceiling, the backlog ceiling
# ---------------------------------------------------------------------------


def test_run_tick_with_nothing_pending_is_a_noop(_workspace_home, db, lease, monkeypatch):
    calls = []
    monkeypatch.setattr(client, "get_ingest_status", lambda bucket=None: calls.append(bucket))
    result = _run(bulk_ingest.run_tick(db, lease))
    assert result == {
        "blocked": False,
        "uploaded": 0,
        "deduplicated": 0,
        "failed": 0,
        "note": "nothing pending — run scan() first, or the pass is already complete",
    }
    assert calls == [], "must not even check ingest status with nothing to upload"


def test_run_tick_ignition_guard_blocks_when_extraction_is_claiming(_workspace_home, db, lease, monkeypatch):
    """§13.5.2 — the non-negotiable: never upload while extraction is
    claiming work. MUTATION CHECK: dropping this check would let the next
    assertion's upload fake get called — it asserts zero calls."""
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content")
    bulk_ingest.scan(db)

    upload_calls = []

    async def fake_upload(*a, **k):
        upload_calls.append((a, k))
        return {"id": "doc-1", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status(claiming=True))
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(db, lease))

    assert result["blocked"] is True
    assert "claiming" in result["reason"]
    assert upload_calls == []


def test_run_tick_blocks_when_a_bucket_cannot_be_ensured(_workspace_home, db, lease, monkeypatch):
    """§13.1 — the four buckets must exist before anything else touches
    them. Found live against production (2026-10-03): the buckets had never
    been created, and every subsequent call 404'd on 'bucket not found'
    until this guard existed."""
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content")
    bulk_ingest.scan(db)

    async def fake_create_bucket(name):
        return None, "HTTP 500: internal error"

    upload_calls = []

    async def fake_upload(*a, **k):
        upload_calls.append((a, k))
        return {"id": "doc-1", "deduplicated": False}, None

    monkeypatch.setattr(client, "create_bucket", fake_create_bucket)
    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(db, lease))

    assert result["blocked"] is True
    assert "could not ensure bucket" in result["reason"]
    assert upload_calls == []


def test_run_tick_blocks_when_status_check_itself_fails(_workspace_home, db, lease, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content")
    bulk_ingest.scan(db)

    async def fake_status(bucket=None):
        return None, "could not reach aw-knowledgeable"

    monkeypatch.setattr(client, "get_ingest_status", fake_status)
    result = _run(bulk_ingest.run_tick(db, lease))
    assert result["blocked"] is True


def test_run_tick_refuses_to_overlap_a_tick_already_in_progress(_workspace_home, db, lease, monkeypatch):
    """Found live 2026-10-08: a slow tick still running server-side after its
    caller gave up (`httpx.ReadTimeout`) held its sqlite write transaction
    open long enough that a second tick's own first `UPDATE` hit
    `sqlite3.OperationalError: database is locked`, surfaced as a bare 500.
    MUTATION CHECK: dropping the lease check would let the second call's
    `fake_upload` get called too — it asserts exactly one call."""
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content")
    bulk_ingest.scan(db)

    first_may_finish = asyncio.Event()
    upload_calls = []

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        upload_calls.append(source_path)
        await first_may_finish.wait()
        return {"id": "doc-1", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    async def scenario():
        first = asyncio.ensure_future(bulk_ingest.run_tick(db, lease))
        await asyncio.sleep(0)  # let the first tick claim the lease and start uploading
        second_result = await bulk_ingest.run_tick(db, lease)
        first_may_finish.set()
        first_result = await first
        return first_result, second_result

    first_result, second_result = _run(scenario())

    assert second_result["blocked"] is True
    assert "already in progress" in second_result["reason"]
    assert first_result["uploaded"] == 1
    assert upload_calls == ["crispal/a.md"], "the second tick must never reach upload_bytes"


def test_run_tick_respects_the_backlog_ceiling(_workspace_home, db, lease, monkeypatch):
    """§13.5.3 — the bucket at/over the backlog ceiling is not pushed further
    into backlog. One bucket now, so this is the whole tick rather than a
    per-bucket skip.

    It reports `blocked: False`, deliberately: `blocked` is what the CLI
    turns into exit 1 and the scheduled task escalates to an agent, and a
    full backlog is the ceiling doing its job. Pre-collapse this was a
    per-bucket `skipped_reason` with exit 0 — pinned here so collapsing to
    one bucket cannot quietly turn backpressure into a page.
    """
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    bulk_ingest.scan(db)

    uploaded = []

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        uploaded.append(bucket)
        return {"id": f"doc-{len(uploaded)}", "deduplicated": False}, None

    monkeypatch.setattr(
        client, "get_ingest_status", _ok_status(pending=bulk_ingest.MAX_PENDING_BACKLOG)
    )
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(db, lease))

    assert uploaded == []
    assert result["blocked"] is False
    assert "backlog" in result["skipped_reason"]


def test_run_tick_uploads_every_subtree_into_the_one_bucket(_workspace_home, db, lease, monkeypatch):
    """The collapse, as a behavioural assertion: six subtrees, one `bucket=`
    argument, and the subtree carried by `source_path` instead.

    MUTATION: pass `bucket=subtree` to `upload_bytes` and this goes red on
    the bucket set — which is the shape that would re-create per-folder
    buckets one upload at a time.
    """
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "a")
    _write(root, "notion/b.md", "b")
    _write(root, "mapped_folders/docs/c.md", "c")
    bulk_ingest.scan(db)

    seen = []

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        seen.append((bucket, source_path))
        return {"id": f"doc-{len(seen)}", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(db, lease))

    assert result["uploaded"] == 3
    assert result["bucket"] == "main"
    assert {bucket for bucket, _path in seen} == {"main"}
    assert sorted(path for _bucket, path in seen) == [
        "crispal/a.md",
        "mapped_folders/docs/c.md",
        "notion/b.md",
    ]
    assert set(result["per_subtree"]) == {"crispal", "notion", "mapped_folders"}


def test_run_tick_respects_the_max_uploads_ceiling_across_buckets(_workspace_home, db, lease, monkeypatch):
    root = bulk_ingest.kb_root()
    for i in range(3):
        _write(root, f"crispal/c{i}.md", f"content {i}")
    for i in range(3):
        _write(root, f"memory/m{i}.md", f"other content {i}")
    bulk_ingest.scan(db)

    uploaded = []

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        uploaded.append(source_path)
        return {"id": f"doc-{len(uploaded)}", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(db, lease, max_uploads=4))

    assert result["uploaded"] == 4
    assert len(uploaded) == 4
    # §13.3 order survives the collapse, now over subtrees: crispal's 3
    # pending go first, then 1 from memory.
    assert uploaded[:3] == ["crispal/c0.md", "crispal/c1.md", "crispal/c2.md"]
    assert uploaded[3] == "memory/m0.md"


def test_run_tick_counts_a_server_dedup_hit_separately_from_a_fresh_upload(_workspace_home, db, lease, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    _write(root, "crispal/b.md", "content b")
    bulk_ingest.scan(db)

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        dedup = source_path == "crispal/b.md"
        return {"id": "doc-shared" if dedup else "doc-a", "deduplicated": dedup}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(db, lease))

    assert result["uploaded"] == 1
    assert result["deduplicated"] == 1
    assert result["failed"] == 0


def test_run_tick_marks_an_upload_error_as_failed_and_keeps_going(_workspace_home, db, lease, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    _write(root, "crispal/b.md", "content b")
    bulk_ingest.scan(db)

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        if source_path == "crispal/a.md":
            return None, "HTTP 500: internal error"
        return {"id": "doc-b", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(db, lease))

    assert result["failed"] == 1
    assert result["uploaded"] == 1

    rows = db.execute(
        bulk_ingest.TABLE, "SELECT status, error FROM {table} WHERE relpath = :relpath",
        {"relpath": "crispal/a.md"},
    )
    row = rows[0]
    assert row["status"] == bulk_ingest.STATUS_FAILED
    assert row["error"] == "HTTP 500: internal error"


# ---------------------------------------------------------------------------
# report — the §13.0 deliverable
# ---------------------------------------------------------------------------


def test_report_tallies_scanned_uploaded_deduplicated_and_pending(_workspace_home, db, lease, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    _write(root, "crispal/b.md", "content b")
    _write(root, "mapped_folders/docs/c.md", "content a")  # alias of a.md's content
    bulk_ingest.scan(db)

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        return {"id": "doc-a", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)
    _run(bulk_ingest.run_tick(db, lease, max_uploads=1))

    report = bulk_ingest.report(db)
    assert report["bucket"] == "main"
    crispal = report["subtrees"]["crispal"]
    assert crispal["scanned"] == 2
    assert crispal["uploaded_new"] == 1
    assert crispal["pending"] == 1
    mapped = report["subtrees"]["mapped_folders"]
    assert mapped["driver_deduplicated_alias"] == 1
    assert report["totals"]["scanned"] == 3
