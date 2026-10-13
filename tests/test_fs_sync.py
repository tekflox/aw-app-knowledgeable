"""docs/design/aw-knowledgeable-fs-sync.md §3/§4/§6/§7 (card B) — the
fs-sync upsync engine, exercised directly: a plain ``_run`` helper driving
coroutines, ``client``'s async functions monkeypatched rather than
``httpx`` itself, same style as ``test_bulk_ingest.py`` (and the same
``_FakeDb``/``_FakeLease`` doubles, duplicated rather than shared — this
repo's existing convention, see that file's own module docstring).

The §7 mass-delete valve is tested FIRST and in isolation (card
instruction: "build and test this first") — before any scan/upload/
delete-marking test touches the lease/client plumbing around it.
"""
from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest, fs_sync  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeDb:
    """Double for ``ctx.db`` — see ``test_bulk_ingest.py``'s identical
    class for the full rationale (named ``:param`` binding, ``{table}``
    substitution, the Postgres-style upsert sqlite also accepts)."""

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


class _FakeLease:
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
def _fresh_table_flag(monkeypatch):
    monkeypatch.setattr(bulk_ingest, "_table_ensured", False)


@pytest.fixture(autouse=True)
def _healthy_ingest_status(monkeypatch):
    """§13.5.2/§13.5.3's ignition guard and backlog ceiling, satisfied by
    default (same convention as `test_bulk_ingest.py`'s `_ok_status`) — the
    handful of tests that care about this guard override it themselves."""

    async def fake_get_ingest_status(bucket=None):
        return {"extraction": {"claiming": False}, "processing": {"pending": 0}}, None

    monkeypatch.setattr(client, "get_ingest_status", fake_get_ingest_status)


@pytest.fixture(autouse=True)
def _no_adoptable_paths(monkeypatch):
    """§17.2 — every tick now runs an adoption pass unconditionally, so
    without this default every pre-amendment test would also fire a real
    (failing) HTTP call to `GET /sync-paths`. Default: nothing to adopt.
    The tests that exercise adoption itself override this explicitly."""

    async def fake_get_sync_paths(workspace_slug, after=None, limit=500):
        return {"paths": [], "cursor": None}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_get_sync_paths)


@pytest.fixture(autouse=True)
def _no_pending_changes(monkeypatch):
    """§3.3 step 2 — every tick now also runs the pull pass unconditionally,
    so without this default every pre-card-C test would fire a real
    (failing) HTTP call to `GET /changes`. Default: nothing new on the
    feed. The tests that exercise the pull pass itself override this
    explicitly."""

    async def fake_get_document_changes(since=None, limit=200):
        return {"changes": [], "cursor": since}, None

    monkeypatch.setattr(client, "get_document_changes", fake_get_document_changes)


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


def _journal_rows(db: _FakeDb) -> list[dict]:
    return [dict(r) for r in db.execute(fs_sync.TABLE, "SELECT * FROM {table}")]


def _seed_uploaded(db: _FakeDb, relpath: str, *, sha256: str = "abc", external_id: str = "doc-1",
                   last_synced_hash: str | None = None, missing_scans: int = 0,
                   pending_delete: int = 0, restore_suppressed: int = 0,
                   scanned_at: float | None = None) -> None:
    """Seed one already-synced journal row directly — the no-wipe
    bootstrap's shape (§8's 2,046 rows): `status=uploaded`, a real
    `external_id`, and (after the §3.2 backfill) `last_synced_hash`."""
    bulk_ingest._ensure_table(db)
    now = scanned_at if scanned_at is not None else time.time()
    db.execute(
        fs_sync.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, external_id, scanned_at, updated_at, "
        "missing_scans, pending_delete, last_synced_hash, restore_suppressed) "
        "VALUES (:relpath, :sha256, :bucket, :status, :external_id, :now, :now, :missing, :pending, "
        ":last_synced, :suppressed)",
        {
            "relpath": relpath, "sha256": sha256, "bucket": "main", "status": bulk_ingest.STATUS_UPLOADED,
            "external_id": external_id, "now": now, "missing": missing_scans, "pending": pending_delete,
            "last_synced": last_synced_hash if last_synced_hash is not None else sha256,
            "suppressed": restore_suppressed,
        },
    )


def _seed_adopted(db: _FakeDb, relpath: str, *, missing_scans: int = 0, pending_delete: int = 0) -> None:
    """Seed one §17.2 adopted row directly — `origin='adopted'`,
    `external_id` NULL, `sha256=''` (never read off disk): the exact shape
    `_adopt_paths` itself inserts for a server-claimed path the journal
    lacked, without going through a fake `GET /sync-paths` response."""
    bulk_ingest._ensure_table(db)
    now = time.time()
    db.execute(
        fs_sync.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, origin, scanned_at, updated_at, "
        "missing_scans, pending_delete) "
        "VALUES (:relpath, '', :bucket, :status, :origin, :now, :now, :missing, :pending)",
        {
            "relpath": relpath, "bucket": "main", "status": bulk_ingest.STATUS_PENDING,
            "origin": fs_sync.ORIGIN_ADOPTED, "now": now, "missing": missing_scans, "pending": pending_delete,
        },
    )


# ---------------------------------------------------------------------------
# §7 — the mass-delete valve, built and tested FIRST.
# ---------------------------------------------------------------------------


def test_valve_guard1_trips_on_an_empty_tree_and_marks_nothing(_workspace_home, db):
    """The card's own acceptance gate, verbatim: a simulated empty tree
    produces zero marks, a declared flag/reason, and (via the CLI's own
    exit-code mapping) a non-zero exit — `blocked: True` is that signal
    here."""
    root = bulk_ingest.kb_root()
    root.mkdir(parents=True, exist_ok=True)  # exists, but genuinely empty
    _seed_uploaded(db, "notion/a.md")
    _seed_uploaded(db, "notion/b.md")

    result = _run(fs_sync.run_tick(db, _FakeLease(), workspace_slug="aw"))

    assert result["blocked"] is True
    assert result["valve_tripped"] is True
    assert result["deleted_marked"] == 0
    rows = _journal_rows(db)
    assert all(r["missing_scans"] == 0 for r in rows)
    assert all(r["pending_delete"] == 0 for r in rows)


def test_valve_guard1_trips_when_kb_root_itself_is_missing(_workspace_home, db):
    """Distinct from "exists but empty": `kb_root()` never created at all
    (a fresh install, or a mapped-folder unmount) must trip the same
    guard, not crash on a missing directory."""
    _seed_uploaded(db, "notion/a.md")

    result = _run(fs_sync.run_tick(db, _FakeLease(), workspace_slug="aw"))

    assert result["blocked"] is True
    assert result["valve_tripped"] is True
    assert _journal_rows(db)[0]["missing_scans"] == 0


def test_valve_guard2_trips_past_the_mass_delete_threshold(_workspace_home, db, monkeypatch):
    """max(25, 10% of uploaded rows). Seed 30 already-synced rows (so the
    floor of 25 is what governs: max(25, 3) == 25), remove 26 of them from
    disk — one past the threshold — and confirm the whole tick aborts with
    zero marks, never calling `mark_delete` at all."""
    root = bulk_ingest.kb_root()
    for i in range(30):
        _seed_uploaded(db, f"notion/{i}.md", sha256=f"h{i}", external_id=f"doc-{i}")
    # Only 4 files survive on disk — 26 are "missing", past max(25, 3)=25.
    for i in range(4):
        _write(root, f"notion/{i}.md", f"body {i}")

    calls = []

    async def fake_mark_delete_by_path(source_path, workspace_slug, reason=None):
        calls.append(source_path)
        return {"source_path": source_path, "marked": ["doc-x"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark_delete_by_path)

    result = _run(fs_sync.run_tick(db, _FakeLease(), workspace_slug="aw"))

    assert result["blocked"] is True
    assert result["valve_tripped"] is True
    assert result["missing_count"] == 26
    assert result["threshold"] == 25
    assert calls == []
    rows = {r["relpath"]: r for r in _journal_rows(db)}
    assert all(rows[f"notion/{i}.md"]["missing_scans"] == 0 for i in range(30))


def test_valve_guard2_threshold_formula():
    assert fs_sync.mass_delete_threshold(0) == 25
    assert fs_sync.mass_delete_threshold(100) == 25
    assert fs_sync.mass_delete_threshold(2046) == 204
    assert fs_sync.mass_delete_threshold(1000) == 100


def test_valve_clear_when_missing_count_is_under_threshold(_workspace_home, db, monkeypatch):
    """The valve must not fire on the card's own measured steady state:
    2,046 uploaded, 122 missing — comfortably under 204."""
    root = bulk_ingest.kb_root()
    for i in range(50):
        _seed_uploaded(db, f"notion/{i}.md", sha256=f"h{i}", external_id=f"doc-{i}",
                       scanned_at=time.time() - 100)
    for i in range(45):  # 5 missing, under max(25, 5)=25
        _write(root, f"notion/{i}.md", f"body {i}")

    monkeypatch.setattr(
        client, "mark_delete_by_path",
        lambda *a, **k: _async({"source_path": a[0] if a else "", "marked": ["doc-x"], "count": 1}, None),
    )

    result = _run(fs_sync.run_tick(db, _FakeLease(), workspace_slug="aw"))

    assert result["blocked"] is False
    assert "valve_tripped" not in result or not result.get("valve_tripped")


async def _async(data, err):
    return data, err


# ---------------------------------------------------------------------------
# §6 — workspace_slug refusal
# ---------------------------------------------------------------------------


def test_run_tick_refuses_with_empty_workspace_slug(_workspace_home, db, lease):
    result = _run(fs_sync.run_tick(db, lease, workspace_slug=""))
    assert result["blocked"] is True
    assert "AW_WORKSPACE" in result["reason"]


# ---------------------------------------------------------------------------
# §3.2 — journal ALTERs + bootstrap backfill, idempotent
# ---------------------------------------------------------------------------


def test_journal_gains_the_six_sync_columns_and_backfills_last_synced_hash(_workspace_home, db):
    bulk_ingest._ensure_table(db)
    db.execute(
        bulk_ingest.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, external_id, scanned_at, updated_at) "
        "VALUES ('notion/a.md', 'hash-a', 'main', :status, 'doc-1', 0, 0)",
        {"status": bulk_ingest.STATUS_UPLOADED},
    )
    # Re-run _ensure_table (a fresh process boot, module flag reset) — must
    # not crash on columns that already exist, and must not clobber an
    # already-set last_synced_hash.
    import knowledgeable_app.bulk_ingest as bi
    bi._table_ensured = False
    bulk_ingest._ensure_table(db)
    bi._table_ensured = False
    bulk_ingest._ensure_table(db)

    row = _journal_rows(db)[0]
    assert row["last_synced_hash"] == "hash-a"
    assert row["missing_scans"] == 0
    assert row["pending_delete"] == 0
    assert row["restore_suppressed"] == 0


def test_backfill_never_overwrites_an_already_synced_hash(_workspace_home, db):
    """A row fs-sync already advanced past bootstrap (last_synced_hash
    differs from the frozen sha256, e.g. after an upstream edit landed via
    a later card) must not be reset back to sha256 on a later boot."""
    bulk_ingest._ensure_table(db)
    db.execute(
        bulk_ingest.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, external_id, scanned_at, updated_at, "
        "last_synced_hash) VALUES ('notion/a.md', 'new-hash', 'main', :status, 'doc-1', 0, 0, 'new-hash')",
        {"status": bulk_ingest.STATUS_UPLOADED},
    )
    import knowledgeable_app.bulk_ingest as bi
    bi._table_ensured = False
    bulk_ingest._ensure_table(db)

    assert _journal_rows(db)[0]["last_synced_hash"] == "new-hash"


# ---------------------------------------------------------------------------
# Edit test — stable external_id, rev advances (via the server's own
# response contract, which this engine journals verbatim).
# ---------------------------------------------------------------------------


def test_edit_after_debounce_puts_by_path_with_base_hash_and_keeps_external_id(
    _workspace_home, db, lease, monkeypatch
):
    root = bulk_ingest.kb_root()
    _write(root, "authored/note.md", "new body")
    # The PREVIOUS tick already observed this exact (new) content and
    # recorded it in `sha256` — `last_synced_hash` is still the stale,
    # server-confirmed value, and `scanned_at` is far enough in the past
    # that THIS tick's debounce check passes.
    current_digest = hashlib.sha256(b"new body").hexdigest()
    _seed_uploaded(
        db, "authored/note.md", sha256=current_digest, external_id="doc-42",
        last_synced_hash="old-hash", scanned_at=time.time() - 100,
    )

    calls = []

    async def fake_put(filename, raw, *, source_path, workspace_slug, base_hash=None, title=None):
        calls.append({"source_path": source_path, "workspace_slug": workspace_slug, "base_hash": base_hash})
        return {"id": "doc-42", "rev": 2, "content_hash": "new-hash-value", "outcome": "updated"}, None

    monkeypatch.setattr(client, "put_document_by_path", fake_put)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["updated"] == 1
    assert result["created"] == 0
    assert len(calls) == 1
    assert calls[0]["base_hash"] == "old-hash"
    row = _journal_rows(db)[0]
    assert row["external_id"] == "doc-42"  # unchanged — no duplicate minted
    assert row["last_synced_hash"] == "new-hash-value"


def test_fresh_edit_is_not_uploaded_before_the_debounce_window_elapses(
    _workspace_home, db, lease, monkeypatch
):
    """§3.3 step 4 — a hash change observed THIS tick waits; only a
    STABLE hash observed ≥30s ago actually uploads."""
    root = bulk_ingest.kb_root()
    _write(root, "authored/note.md", "edited body")
    _seed_uploaded(
        db, "authored/note.md", sha256="old-hash", external_id="doc-42",
        last_synced_hash="old-hash", scanned_at=time.time(),  # just scanned, hash still "old-hash" in db
    )

    called = []
    monkeypatch.setattr(
        client, "put_document_by_path",
        lambda *a, **k: called.append(1) or _async({"id": "doc-42", "outcome": "updated", "content_hash": "x"}, None),
    )

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["updated"] == 0
    assert called == []  # no upload yet — this tick only recorded the new hash
    row = _journal_rows(db)[0]
    assert row["sha256"] == hashlib.sha256(b"edited body").hexdigest()
    assert row["last_synced_hash"] == "old-hash"  # not advanced yet


# ---------------------------------------------------------------------------
# Delete test — not marked on scan 1, marked on scan 2.
# ---------------------------------------------------------------------------


def test_missing_file_is_not_marked_on_the_first_scan(_workspace_home, db, lease, monkeypatch):
    bulk_ingest.kb_root().mkdir(parents=True, exist_ok=True)  # empty tree would trip the valve, so...
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")  # ...keep one real file present
    _seed_uploaded(db, "notion/gone.md", external_id="doc-99")

    called = []
    monkeypatch.setattr(client, "mark_delete_by_path", lambda *a, **k: called.append(1) or _async({}, None))

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["deleted_marked"] == 0
    assert called == []
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/gone.md")
    assert row["missing_scans"] == 1
    assert row["pending_delete"] == 0


def test_missing_file_is_marked_on_the_second_consecutive_scan(_workspace_home, db, lease, monkeypatch):
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/gone.md", external_id="doc-99", missing_scans=1)

    called = []

    async def fake_mark_delete_by_path(source_path, workspace_slug, reason=None):
        called.append(source_path)
        return {"source_path": source_path, "marked": ["doc-99"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark_delete_by_path)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["deleted_marked"] == 1
    assert called == ["notion/gone.md"]
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/gone.md")
    assert row["missing_scans"] == 2
    assert row["pending_delete"] == 1


def test_missing_scans_resets_when_the_file_reappears(_workspace_home, db, lease):
    root = bulk_ingest.kb_root()
    _write(root, "notion/back.md", "I'm back")
    _seed_uploaded(db, "notion/back.md", sha256="h", last_synced_hash="h", missing_scans=1, external_id="doc-7")

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    row = _journal_rows(db)[0]
    assert row["missing_scans"] == 0


# ---------------------------------------------------------------------------
# Echo/no-op — a tick immediately after a successful tick does nothing.
# ---------------------------------------------------------------------------


def test_steady_state_tick_is_a_noop(_workspace_home, db, lease):
    root = bulk_ingest.kb_root()
    _write(root, "notion/a.md", "stable body")
    import hashlib
    digest = hashlib.sha256(b"stable body").hexdigest()
    _seed_uploaded(db, "notion/a.md", sha256=digest, last_synced_hash=digest, external_id="doc-1")

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["created"] == 0
    assert result["updated"] == 0
    assert result["deleted_marked"] == 0
    assert result["unchanged"] == 1


# ---------------------------------------------------------------------------
# Two-worker lease test — exactly one tick runs.
# ---------------------------------------------------------------------------


def test_second_tick_is_blocked_while_the_first_holds_the_lease(_workspace_home, db, lease):
    """Two processes, exactly one tick runs: simulate a second worker by
    having `lease` already held before `run_tick` ever gets to claim it —
    the same non-blocking, flock-backed contract the real lease has."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/a.md", "body")
    lease.claim(fs_sync.LEASE_NAME)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is True
    assert "already in progress" in result["reason"]


# ---------------------------------------------------------------------------
# §13.5.2/§13.5.3 — the ignition guard and backlog ceiling, kept as-is.
# ---------------------------------------------------------------------------


def test_ignition_guard_blocks_the_whole_tick_when_extraction_is_claiming(
    _workspace_home, db, lease, monkeypatch
):
    _write(bulk_ingest.kb_root(), "notion/a.md", "body")

    async def fake_claiming(bucket=None):
        return {"extraction": {"claiming": True}, "processing": {"pending": 0}}, None

    monkeypatch.setattr(client, "get_ingest_status", fake_claiming)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is True
    assert "ignition guard" in result["reason"]


def test_backlog_ceiling_skips_uploads_but_still_tracks_deletes(
    _workspace_home, db, lease, monkeypatch
):
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep")  # present, not missing
    current_digest = hashlib.sha256(b"edited").hexdigest()
    _seed_uploaded(
        db, "authored/note.md", sha256=current_digest, last_synced_hash="old-hash",
        external_id="doc-1", scanned_at=time.time() - 100,
    )
    _write(root, "authored/note.md", "edited")
    _seed_uploaded(db, "notion/gone.md", external_id="doc-99", missing_scans=1)

    async def fake_full_backlog(bucket=None):
        return {"extraction": {"claiming": False}, "processing": {"pending": 999}}, None

    monkeypatch.setattr(client, "get_ingest_status", fake_full_backlog)
    called = []
    monkeypatch.setattr(
        client, "put_document_by_path",
        lambda *a, **k: called.append(1) or _async({"id": "doc-1", "outcome": "updated", "content_hash": "x"}, None),
    )

    async def fake_mark_delete_by_path(source_path, workspace_slug, reason=None):
        return {"source_path": source_path, "marked": ["doc-99"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark_delete_by_path)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert "skipped_reason" in result
    assert called == []  # upload budget was zero — the edit waits
    assert result["deleted_marked"] == 1  # delete-marking is unaffected


# ---------------------------------------------------------------------------
# §17 (card G2) — path-keyed marking, journal adoption, catch-up, valve
# widening. docs/design/aw-knowledgeable-fs-sync.md §17.7's acceptance gates,
# one test (or group) per bullet.
# ---------------------------------------------------------------------------


def test_adopted_row_is_not_skipped_by_the_old_external_id_guard(_workspace_home, db, lease, monkeypatch):
    """The gate, stated precisely: the PRE-amendment guard was
    `if relpath in found or not row["external_id"]: continue` — for an
    adopted row (external_id NULL, by construction) that `continue` fires
    unconditionally, so `missing_scans` would stay 0 FOREVER and the path
    could never reach a second miss. Proving the guard no longer excludes
    it means proving `missing_scans` actually advances on the first miss."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_adopted(db, "notion/gone.md")

    called = []
    monkeypatch.setattr(client, "mark_delete_by_path", lambda *a, **k: called.append(1) or _async({}, None))

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/gone.md")
    assert row["missing_scans"] == 1  # the old guard would have left this at 0 forever
    assert row["pending_delete"] == 0
    assert called == []  # not yet the second miss


def test_adopted_row_is_marked_via_by_path_on_the_second_miss(_workspace_home, db, lease, monkeypatch):
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_adopted(db, "notion/gone.md", missing_scans=1)

    calls = []

    async def fake_mark(source_path, workspace_slug, reason=None):
        calls.append((source_path, workspace_slug))
        return {"source_path": source_path, "marked": ["doc-x"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["deleted_marked"] == 1
    assert calls == [("notion/gone.md", "aw")]
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/gone.md")
    assert row["missing_scans"] == 2
    assert row["pending_delete"] == 1


def test_adoption_excludes_skipped_paths(_workspace_home, db, lease, monkeypatch):
    """§17.3 exclusion 1 — `is_skipped()` paths may well have a live file
    the scan deliberately never looks at; adopting one would lie about
    "missing from source tree."
    """
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return {
            "paths": [
                {"source_path": "mapped_folders/repos/foo.md", "claimant_count": 1},
                {"source_path": "notion/real.md", "claimant_count": 1},
            ],
            "cursor": None,
        }, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["adopted"] == 1  # only notion/real.md
    relpaths = {r["relpath"] for r in _journal_rows(db)}
    assert "mapped_folders/repos/foo.md" not in relpaths
    assert "notion/real.md" in relpaths


def test_adoption_no_longer_excludes_authored_paths_now_that_the_pull_pass_exists(
    _workspace_home, db, lease, monkeypatch
):
    """§17.6 point 4 — card C removes the pre-downsync tripwire
    deliberately: an `authored/` document whose file was genuinely deleted
    on disk must be adopted (and, eventually, delete-detected) exactly
    like any other path now that the pull pass exists to keep a
    legitimately file-less `authored/` document's journal row written
    before anything downstream could see it as missing."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return {
            "paths": [
                {"source_path": "authored/note.md", "claimant_count": 1},
                {"source_path": "notion/real.md", "claimant_count": 1},
            ],
            "cursor": None,
        }, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["adopted"] == 2
    relpaths = {r["relpath"] for r in _journal_rows(db)}
    assert "authored/note.md" in relpaths
    assert "notion/real.md" in relpaths


def test_adoption_requests_sync_paths_scoped_to_this_workspace_slug(_workspace_home, db, lease, monkeypatch):
    """§17.3 exclusion 3 — a foreign-`workspace_slug` claim is excluded
    SERVER-side by `GET /sync-paths`' own slug-or-NULL filter (§17.2); the
    only thing the connector itself can get wrong is which slug it asks
    for, so the regression this test actually guards is "always the
    caller's OWN slug, never omitted, never a different one."
    """
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")

    calls = []

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        calls.append(workspace_slug)
        return {"paths": [], "cursor": None}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)

    _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert calls == ["aw"]


def test_adopted_path_whose_file_exists_converges_to_a_normal_synced_row(
    _workspace_home, db, lease, monkeypatch
):
    """A pleasant consequence of adopting PATHS, not documents (§17.2): if
    the file turns out to exist, the row heals itself through the SAME
    debounce+upload machinery as any other edit — no special-case code."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/adopted.md", "adopted body")
    _seed_adopted(db, "notion/adopted.md")

    # Tick 1 — the adopted row's sha256 ('') differs from the real file's
    # digest for the first time; this just restarts the debounce window,
    # same as any fresh edit.
    result1 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result1["blocked"] is False
    row1 = next(r for r in _journal_rows(db) if r["relpath"] == "notion/adopted.md")
    assert row1["sha256"] == hashlib.sha256(b"adopted body").hexdigest()
    assert row1["external_id"] is None

    # Elapse the debounce window, then tick 2 uploads — `external_id`/
    # `last_synced_hash` land from the PUT response, never a second read.
    db.execute(
        fs_sync.TABLE, "UPDATE {table} SET scanned_at = :t WHERE relpath = :r",
        {"t": time.time() - 100, "r": "notion/adopted.md"},
    )

    async def fake_put(filename, raw, *, source_path, workspace_slug, base_hash=None, title=None):
        return {"id": "doc-new", "rev": 1, "content_hash": "hash-after-put", "outcome": "created"}, None

    monkeypatch.setattr(client, "put_document_by_path", fake_put)

    result2 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result2["blocked"] is False
    assert result2["created"] == 1
    row2 = next(r for r in _journal_rows(db) if r["relpath"] == "notion/adopted.md")
    assert row2["external_id"] == "doc-new"
    assert row2["last_synced_hash"] == "hash-after-put"


def test_valve_guard2_counts_adopted_rows_in_both_numerator_and_denominator(
    _workspace_home, db, lease
):
    """§17.5 — a NUMERATOR-ONLY widening (adopted rows become eligible to
    be counted missing, but the threshold's own base stays the OLD
    `status='uploaded'`-only count) would make this exact tick wrongly
    trip the valve. 200 real synced rows + 100 adopted rows = 300 tracked;
    losing 29 of the adopted ones is 29/300, comfortably under
    max(25, 30%)=30. A denominator that only sees the 200 real rows
    computes max(25, 20)=25 instead, and 29 > 25 would wrongly abort the
    whole tick — so this test fails under a numerator-only fix and passes
    only when both sides of guard 2 widen together."""
    root = bulk_ingest.kb_root()
    for i in range(200):
        _seed_uploaded(db, f"notion/real-{i}.md", sha256=f"h{i}", external_id=f"doc-{i}",
                       scanned_at=time.time() - 100)
        _write(root, f"notion/real-{i}.md", f"body {i}")
    for i in range(100):
        _seed_adopted(db, f"notion/adopted-{i}.md")
        if i >= 29:  # adopted-0..28 (29 paths) are missing; the other 71 are present
            _write(root, f"notion/adopted-{i}.md", f"adopted body {i}")

    assert fs_sync._tracked_row_count(db) == 300  # the denominator must see all 300

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert not result.get("valve_tripped")


def test_in_pass_document_ceiling_stops_marking_sets_the_flag_and_exits_non_zero(
    _workspace_home, db, lease, monkeypatch
):
    """§17.5's third guard: by-path marking fans out (one path can mark
    several claimant documents at once), invisible to guard 2's PATH
    count. Seed 10 missing paths — well under guard 2's own path
    threshold of max(25, 10% of 100)=25 — where each mark call reports 4
    duplicate claimants: 40 documents on the table, past the same 25."""
    root = bulk_ingest.kb_root()
    for i in range(90):
        _seed_uploaded(db, f"notion/keep-{i}.md", sha256=f"h{i}", external_id=f"doc-keep-{i}")
        _write(root, f"notion/keep-{i}.md", f"body {i}")
    for i in range(10):
        _seed_uploaded(db, f"notion/gone-{i}.md", external_id=f"doc-gone-{i}", missing_scans=1)

    assert fs_sync._tracked_row_count(db) == 100  # max(25, 10% of 100) == 25

    calls = []

    async def fake_mark(source_path, workspace_slug, reason=None):
        calls.append(source_path)
        return {
            "source_path": source_path,
            "marked": [f"{source_path}#{n}" for n in range(4)],
            "count": 4,
        }, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is True
    assert result["valve_tripped"] is True
    assert result["document_ceiling"] == 25
    assert result["marked_documents"] > 25
    # the ceiling actually STOPPED marking mid-pass — not every one of the
    # 10 candidate paths got a call.
    assert len(calls) < 10
    assert result["deleted_marked"] == len(calls)


def test_catchup_reissues_mark_delete_by_path_once_then_stays_off(
    _workspace_home, db, lease, monkeypatch
):
    """§17.4 — the live-35 heal: every row already `pending_delete=1` gets
    its by-path mark re-issued exactly once (idempotent on the server —
    G1's own gate), gated by a row in `FLAGS_TABLE` rather than any
    in-memory flag ("idempotent across restarts" — nothing here is
    per-process state to lose on a restart)."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/already-marked.md", external_id="doc-1", pending_delete=1)

    calls = []

    async def fake_mark(source_path, workspace_slug, reason=None):
        calls.append(source_path)
        return {"source_path": source_path, "marked": ["doc-1", "doc-1-sibling"], "count": 2}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    result1 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result1["blocked"] is False
    assert result1["catchup_marked"] == 1
    assert calls == ["notion/already-marked.md"]
    assert fs_sync._catchup_done(db) is True

    result2 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result2["catchup_marked"] == 0
    assert calls == ["notion/already-marked.md"]  # unchanged — no second call


def test_valve_guard1_aborts_before_adoption_or_catchup_touch_anything(
    _workspace_home, db, lease, monkeypatch
):
    """The zero-file-tree gate, explicitly: guard 1 stays FIRST — adoption
    and the one-time catch-up must not run at all this tick, not merely
    "run and have nothing to do."
    """
    # kb_root() never created — guard 1's "missing or unreadable" branch.
    _seed_uploaded(db, "notion/already-marked.md", external_id="doc-1", pending_delete=1)

    sync_paths_calls = []
    mark_calls = []

    async def spy_sync_paths(workspace_slug, after=None, limit=500):
        sync_paths_calls.append(1)
        return {"paths": [], "cursor": None}, None

    async def spy_mark(source_path, workspace_slug, reason=None):
        mark_calls.append(source_path)
        return {"source_path": source_path, "marked": [], "count": 0}, None

    monkeypatch.setattr(client, "get_sync_paths", spy_sync_paths)
    monkeypatch.setattr(client, "mark_delete_by_path", spy_mark)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is True
    assert result["valve_tripped"] is True
    assert sync_paths_calls == []
    assert mark_calls == []
    # Catch-up never even got as far as creating its own flags table —
    # proof it was never reached, not just that it had nothing to do.
    with pytest.raises(Exception):
        fs_sync._catchup_done(db)


# ---------------------------------------------------------------------------
# §3.3 steps 2-3 (card C) — the pull pass: journal-before-file, conflict
# resolution, remote_rev-only drift tracking, and the decline/restore gap.
# ---------------------------------------------------------------------------


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def test_pull_writes_the_journal_row_before_the_file_the_echo_loop_breaker(
    _workspace_home, db, lease, monkeypatch
):
    """The card's own single most important invariant, made explicit: if
    this were reversed, a scan could observe the new bytes on disk before
    `last_synced_hash` reflects them, PUT them straight back up, and the
    two directions would loop forever."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")  # a real file so guard 1 doesn't trip

    remote_content = b"pulled from the server"
    remote_hash = hashlib.sha256(remote_content).hexdigest()

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-99", "source_path": "authored/note.md",
                "rev": 1, "content_hash": remote_hash, "sync_status": None,
                "updated_at": _iso(time.time()),
            }],
            "cursor": "cursor-1",
        }, None

    async def fake_content(external_id):
        assert external_id == "doc-99"
        return remote_content, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)

    events: list[str] = []
    real_execute = db.execute

    def spy_execute(name, sql, params=None):
        if name == fs_sync.TABLE and (params or {}).get("relpath") == "authored/note.md" \
                and "last_synced_hash" in sql:
            events.append("journal")
        return real_execute(name, sql, params)

    monkeypatch.setattr(db, "execute", spy_execute)

    real_write_bytes = Path.write_bytes

    def spy_write_bytes(self, data):
        if self.name == "note.md":
            events.append("file")
        return real_write_bytes(self, data)

    monkeypatch.setattr(Path, "write_bytes", spy_write_bytes)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["authored_written"] == 1
    assert events == ["journal", "file"]  # journal committed strictly before the file
    assert (root / "authored/note.md").read_bytes() == remote_content
    row = next(r for r in _journal_rows(db) if r["relpath"] == "authored/note.md")
    assert row["last_synced_hash"] == remote_hash
    assert row["remote_rev"] == 1
    assert row["external_id"] == "doc-99"


def test_pull_updates_remote_rev_only_outside_authored_never_touches_disk(
    _workspace_home, db, lease, monkeypatch
):
    """§3.3 step 2's mirror-image rule: a change outside `authored/`
    advances `remote_rev` so drift is visible in `report()`, but the file
    on disk is never touched and the content is never even fetched."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(
        db, "notion/keep.md", external_id="doc-1",
        last_synced_hash=hashlib.sha256(b"keep me").hexdigest(),
    )

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-1", "source_path": "notion/keep.md",
                "rev": 5, "content_hash": "server-side-only-hash", "sync_status": None,
                "updated_at": _iso(time.time()),
            }],
            "cursor": "cursor-1",
        }, None

    content_calls = []

    async def fake_content(external_id):
        content_calls.append(external_id)
        return b"must never be fetched for a non-authored path", None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["remote_rev_only"] == 1
    assert result["authored_written"] == 0
    assert content_calls == []
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/keep.md")
    assert row["remote_rev"] == 5
    assert (root / "notion/keep.md").read_text(encoding="utf-8") == "keep me"  # untouched


def test_pull_conflict_remote_wins_on_newer_mtime_overwrites_the_local_edit(
    _workspace_home, db, lease, monkeypatch
):
    """§3.3 step 3 — both sides changed; the remote side is clearly newer
    (well past the ±5s epsilon), so it wins and the local edit is
    overwritten."""
    import os

    root = bulk_ingest.kb_root()
    local_content = b"a stale local edit"
    _write(root, "authored/note.md", local_content.decode())
    old_time = time.time() - 1000
    os.utime(root / "authored/note.md", (old_time, old_time))
    _seed_uploaded(
        db, "authored/note.md", sha256=hashlib.sha256(local_content).hexdigest(),
        last_synced_hash="base-hash", external_id="doc-42",
    )

    remote_content = b"the newer remote edit"
    remote_hash = hashlib.sha256(remote_content).hexdigest()

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-42", "source_path": "authored/note.md",
                "rev": 3, "content_hash": remote_hash, "sync_status": None,
                "updated_at": _iso(time.time()),  # now — far newer than old_time
            }],
            "cursor": "cursor-1",
        }, None

    async def fake_content(external_id):
        return remote_content, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["authored_written"] == 1
    assert result["conflicts_local_won"] == 0
    assert (root / "authored/note.md").read_bytes() == remote_content
    row = next(r for r in _journal_rows(db) if r["relpath"] == "authored/note.md")
    assert row["last_synced_hash"] == remote_hash
    assert row["remote_rev"] == 3


def test_pull_conflict_filesystem_wins_leaves_the_file_and_defers_the_loss_to_the_next_upload(
    _workspace_home, db, lease, monkeypatch
):
    """§3.3 step 3 — both sides changed, the filesystem is newer (or ties
    within the ±5s epsilon): the file is left alone and the loser's
    content is never even fetched. `last_synced_hash` is deliberately NOT
    advanced to the losing remote hash — a test must assert the loser is
    recorded, not just that the winner won: leaving the stale base means
    the NEXT scan's own upload PUTs with a `base_hash` that no longer
    matches the server's current (remote) content, and the server records
    that remote content as `conflict_overwrote` on its own upsert path —
    nothing here silently merges or drops it."""
    root = bulk_ingest.kb_root()
    local_content = b"the newer local edit"
    _write(root, "authored/note.md", local_content.decode())
    _seed_uploaded(
        db, "authored/note.md", sha256=hashlib.sha256(local_content).hexdigest(),
        last_synced_hash="base-hash", external_id="doc-42",
        scanned_at=time.time(),
    )

    old_remote_iso = _iso(time.time() - 1000)  # remote change is far OLDER

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-42", "source_path": "authored/note.md",
                "rev": 3, "content_hash": "the-losing-remote-hash", "sync_status": None,
                "updated_at": old_remote_iso,
            }],
            "cursor": "cursor-1",
        }, None

    content_calls = []

    async def fake_content(external_id):
        content_calls.append(external_id)
        return b"should never be written to disk", None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert content_calls == []  # filesystem won — the loser's bytes were never even fetched
    assert (root / "authored/note.md").read_bytes() == local_content  # untouched
    row = next(r for r in _journal_rows(db) if r["relpath"] == "authored/note.md")
    assert row["last_synced_hash"] == "base-hash"  # deliberately NOT advanced
    assert row["remote_rev"] == 3  # this remote revision won't be re-flagged next tick


def test_pull_conflict_tie_within_skew_epsilon_goes_to_filesystem(
    _workspace_home, db, lease, monkeypatch
):
    """§3.3 step 3's tie rule, made to actually bite: the remote change is
    nominally newer than the file, but by less than the ±5s skew epsilon,
    so the design says the tie still goes to the filesystem. The two
    tests above only exercise "remote far newer" and "local far newer" —
    both would stay green even if the tie direction were silently
    inverted, since neither puts the two mtimes anywhere near each
    other. This is the case that actually proves the direction."""
    import os

    root = bulk_ingest.kb_root()
    local_content = b"local content, nominally a bit older but inside the window"
    _write(root, "authored/note.md", local_content.decode())
    local_time = time.time() - 500
    os.utime(root / "authored/note.md", (local_time, local_time))
    _seed_uploaded(
        db, "authored/note.md", sha256=hashlib.sha256(local_content).hexdigest(),
        last_synced_hash="base-hash", external_id="doc-42",
    )

    remote_time = local_time + 3.0  # nominally newer, but inside ±CONFLICT_SKEW_SECONDS
    assert remote_time - local_time < fs_sync.CONFLICT_SKEW_SECONDS

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-42", "source_path": "authored/note.md",
                "rev": 3, "content_hash": "the-losing-remote-hash", "sync_status": None,
                "updated_at": _iso(remote_time),
            }],
            "cursor": "cursor-1",
        }, None

    content_calls = []

    async def fake_content(external_id):
        content_calls.append(external_id)
        return b"should never be written to disk", None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["conflicts_local_won"] == 1
    assert content_calls == []  # inside the window — filesystem wins, remote content never fetched
    assert (root / "authored/note.md").read_bytes() == local_content
    row = next(r for r in _journal_rows(db) if r["relpath"] == "authored/note.md")
    assert row["last_synced_hash"] == "base-hash"
    assert row["remote_rev"] == 3


def test_pull_conflict_at_exact_skew_boundary_goes_to_filesystem(
    _workspace_home, db, lease, monkeypatch
):
    """The boundary itself: remote exactly `CONFLICT_SKEW_SECONDS` newer
    than the file. The design's comparison is inclusive (`>=`, see
    `fs_sync._run_pull_pass`), so this exact point is still a tie and
    still goes to the filesystem — a `>` instead of `>=` would flip only
    this one point without affecting the "clearly inside"/"clearly
    outside" cases above and below."""
    import os

    root = bulk_ingest.kb_root()
    local_content = b"local content exactly at the skew boundary"
    _write(root, "authored/note.md", local_content.decode())
    # Whole seconds, deliberately: the mtime/iso round trip loses sub-second
    # precision in ways that make the exact fractional boundary flaky to
    # pin down — a whole-second boundary is exact on both sides and still
    # genuinely exercises the `>=` in `fs_sync._run_pull_pass`.
    local_time = float(int(time.time())) - 500
    os.utime(root / "authored/note.md", (local_time, local_time))
    _seed_uploaded(
        db, "authored/note.md", sha256=hashlib.sha256(local_content).hexdigest(),
        last_synced_hash="base-hash", external_id="doc-42",
    )

    remote_time = local_time + fs_sync.CONFLICT_SKEW_SECONDS  # exactly on the boundary

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-42", "source_path": "authored/note.md",
                "rev": 3, "content_hash": "the-losing-remote-hash", "sync_status": None,
                "updated_at": _iso(remote_time),
            }],
            "cursor": "cursor-1",
        }, None

    content_calls = []

    async def fake_content(external_id):
        content_calls.append(external_id)
        return b"should never be written to disk", None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["conflicts_local_won"] == 1
    assert content_calls == []  # boundary included — filesystem still wins
    assert (root / "authored/note.md").read_bytes() == local_content
    row = next(r for r in _journal_rows(db) if r["relpath"] == "authored/note.md")
    assert row["last_synced_hash"] == "base-hash"


def test_pull_conflict_just_outside_skew_epsilon_remote_wins(
    _workspace_home, db, lease, monkeypatch
):
    """One step past the boundary: remote more than `CONFLICT_SKEW_SECONDS`
    newer than the file. Pairs with the exact-boundary test above to
    prove the ±5s window has a real edge in both directions, not just a
    direction with no limit."""
    import os

    root = bulk_ingest.kb_root()
    local_content = b"a stale local edit, just past the skew window"
    _write(root, "authored/note.md", local_content.decode())
    local_time = time.time() - 500
    os.utime(root / "authored/note.md", (local_time, local_time))
    _seed_uploaded(
        db, "authored/note.md", sha256=hashlib.sha256(local_content).hexdigest(),
        last_synced_hash="base-hash", external_id="doc-42",
    )

    remote_content = b"the remote edit that wins just past the window"
    remote_hash = hashlib.sha256(remote_content).hexdigest()
    remote_time = local_time + fs_sync.CONFLICT_SKEW_SECONDS + 0.5

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-42", "source_path": "authored/note.md",
                "rev": 3, "content_hash": remote_hash, "sync_status": None,
                "updated_at": _iso(remote_time),
            }],
            "cursor": "cursor-1",
        }, None

    async def fake_content(external_id):
        return remote_content, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["conflicts_local_won"] == 0
    assert (root / "authored/note.md").read_bytes() == remote_content
    row = next(r for r in _journal_rows(db) if r["relpath"] == "authored/note.md")
    assert row["last_synced_hash"] == remote_hash
    assert row["remote_rev"] == 3


def _server_conflict_overwrote(*, old_hash: str, base_hash: str | None, new_hash: str) -> str | None:
    """Mirrors `upsert_document_by_path`'s Cypher in
    repos/aw-knowledgeable/backend/app/core/graph.py (§1.2) exactly:

        changed = NOT created AND old_hash <> $content_hash
        conflict_overwrote = CASE
            WHEN changed AND $base_hash IS NOT NULL AND $base_hash <> old_hash
            THEN old_hash ELSE d.conflict_overwrote END

    Defined here, not imported, because this card is scoped to
    repos/aw-app-knowledgeable only — but the connector's entire
    contribution to that server-side audit trail is the `base_hash` value
    it sends, so mirroring the formula is the only way to assert the
    actual recorded value from this repo rather than merely trusting that
    some non-empty `base_hash` was passed."""
    changed = old_hash != new_hash
    if changed and base_hash is not None and base_hash != old_hash:
        return old_hash
    return None


def test_filesystem_wins_conflict_then_upload_sends_the_stale_base_hash_that_records_the_losing_remote_hash(
    _workspace_home, db, lease, monkeypatch
):
    """Gap 2 from the mutation-testing card: no test asserted that the
    losing side's hash is actually recorded in `conflict_overwrote`.
    That field is written server-side (out of scope for this card — see
    `_server_conflict_overwrote` above), but the connector's own half of
    the promise is sending the STALE pre-conflict `base_hash` on the
    upload that follows a filesystem-won conflict (this module's own
    docstring on `_run_pull_pass`). Debounce is pre-satisfied
    (`scanned_at` far in the past) so the conflict and the deferred
    upload both land in this one tick, same as production: the pull pass
    leaves `last_synced_hash` stale, and the scan/upload loop later in
    the SAME tick finds `digest != last_synced_hash` and uploads
    immediately with that stale hash as `base_hash`."""
    root = bulk_ingest.kb_root()
    local_content = b"the newer local edit, uploaded this same tick"
    _write(root, "authored/note.md", local_content.decode())
    local_digest = hashlib.sha256(local_content).hexdigest()
    _seed_uploaded(
        db, "authored/note.md", sha256=local_digest, last_synced_hash="base-hash",
        external_id="doc-42", scanned_at=time.time() - 100,  # debounce already satisfied
    )

    remote_hash = "the-losing-remote-hash"
    old_remote_iso = _iso(time.time() - 1000)  # far older — filesystem wins the pull conflict

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-42", "source_path": "authored/note.md",
                "rev": 3, "content_hash": remote_hash, "sync_status": None,
                "updated_at": old_remote_iso,
            }],
            "cursor": "cursor-1",
        }, None

    content_calls = []

    async def fake_content(external_id):
        content_calls.append(external_id)
        return b"should never be fetched", None

    put_calls = []

    async def fake_put(filename, raw, *, source_path, workspace_slug, base_hash=None, title=None):
        put_calls.append({"base_hash": base_hash, "raw": raw})
        return {"id": "doc-42", "rev": 4, "content_hash": local_digest, "outcome": "updated"}, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)
    monkeypatch.setattr(client, "put_document_by_path", fake_put)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["conflicts_local_won"] == 1
    assert content_calls == []  # filesystem won the pull-side conflict
    assert len(put_calls) == 1
    assert put_calls[0]["base_hash"] == "base-hash"  # the stale pre-conflict hash — not the winner's

    recorded = _server_conflict_overwrote(
        old_hash=remote_hash, base_hash=put_calls[0]["base_hash"], new_hash=local_digest,
    )
    assert recorded == remote_hash  # the LOSING remote hash — not the local winner's, not None


def test_remote_wins_conflict_never_uploads_the_discarded_local_edit(
    _workspace_home, db, lease, monkeypatch
):
    """Mirror image of the test above: when the remote side wins, the
    discarded local edit must never be pushed back up at all — there is
    no `base_hash` to send and no server-side loser to record, because
    the local bytes are simply overwritten on disk. A copy-paste of the
    filesystem-wins branch's deferred-upload behaviour into this branch
    would silently re-upload the very content that just lost, so this
    asserts zero `put_document_by_path` calls, not just that the file
    ends up with the remote content (already covered by
    `test_pull_conflict_remote_wins_on_newer_mtime_overwrites_the_local_edit`,
    which never previously confirmed upload was skipped — it just never
    mocked `put_document_by_path`, so a real network call would have
    raised or errored silently and still been invisible to that test)."""
    import os

    root = bulk_ingest.kb_root()
    local_content = b"a stale local edit about to be discarded"
    _write(root, "authored/note.md", local_content.decode())
    old_time = time.time() - 1000
    os.utime(root / "authored/note.md", (old_time, old_time))
    _seed_uploaded(
        db, "authored/note.md", sha256=hashlib.sha256(local_content).hexdigest(),
        last_synced_hash="base-hash", external_id="doc-42", scanned_at=old_time,
    )

    remote_content = b"the newer remote edit that wins"
    remote_hash = hashlib.sha256(remote_content).hexdigest()

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-42", "source_path": "authored/note.md",
                "rev": 3, "content_hash": remote_hash, "sync_status": None,
                "updated_at": _iso(time.time()),  # now — far newer than old_time
            }],
            "cursor": "cursor-1",
        }, None

    async def fake_content(external_id):
        return remote_content, None

    put_calls = []

    async def fake_put(filename, raw, *, source_path, workspace_slug, base_hash=None, title=None):
        put_calls.append({"base_hash": base_hash})
        return {"id": "doc-42", "rev": 99, "content_hash": "should-never-be-used", "outcome": "updated"}, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "get_document_content", fake_content)
    monkeypatch.setattr(client, "put_document_by_path", fake_put)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["conflicts_local_won"] == 0
    assert put_calls == []  # the discarded local edit is never uploaded
    assert (root / "authored/note.md").read_bytes() == remote_content
    row = next(r for r in _journal_rows(db) if r["relpath"] == "authored/note.md")
    assert row["last_synced_hash"] == remote_hash


def test_decline_restore_arriving_on_the_feed_clears_the_stale_journal_mark(
    _workspace_home, db, lease, monkeypatch
):
    """§17.6 point 5's full round trip: mark, decline (restored
    server-side, so the feed's `sync_status` is no longer `pending_delete`),
    tick — the journal's stale `pending_delete=1` must be cleared rather
    than left to swallow every future miss at this path forever, but the
    declined path also must NOT be instantly re-marked just because it is
    still, separately, missing from disk."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/declined.md", external_id="doc-7", missing_scans=2, pending_delete=1)

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-7", "source_path": "notion/declined.md",
                "rev": 1, "content_hash": "h", "sync_status": None,  # restored
                "updated_at": _iso(time.time()),
            }],
            "cursor": "cursor-1",
        }, None

    mark_calls = []

    async def fake_mark(source_path, workspace_slug, reason=None):
        mark_calls.append(source_path)
        return {"source_path": source_path, "marked": [], "count": 0}, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["restored"] == 1
    assert mark_calls == []  # not re-marked this tick
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/declined.md")
    assert row["pending_delete"] == 0
    assert row["restore_suppressed"] == 1  # §18.2 — held until the file reappears


def test_pull_pass_persists_its_cursor_across_ticks(_workspace_home, db, lease, monkeypatch):
    """The cursor must survive the tick boundary — a second poll starts
    from where the first left off, not from scratch. Matches the real
    server's contract (`document_changes`'s own `cursor = ... if rows
    else since`): the cursor only ever advances alongside an actual row,
    so the fake only advances it on the page that carries one."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(
        db, "notion/keep.md", external_id="doc-1",
        last_synced_hash=hashlib.sha256(b"keep me").hexdigest(),
    )

    since_values = []
    calls = {"n": 0}

    async def fake_changes(since=None, limit=200):
        since_values.append(since)
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "changes": [{
                    "external_id": "doc-1", "source_path": "notion/keep.md",
                    "rev": 1, "content_hash": "h", "sync_status": None,
                    "updated_at": _iso(time.time()),
                }],
                "cursor": "cursor-after-tick-1",
            }, None
        return {"changes": [], "cursor": since}, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)

    _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    # The pull pass pages until the cursor stops advancing, so tick 1 makes
    # a second, same-tick call with the new cursor before it is satisfied
    # there is nothing left — every call after the first real page echoes
    # that same persisted cursor back, including tick 2's very first call.
    assert since_values[0] is None
    assert len(since_values) >= 2
    assert all(v == "cursor-after-tick-1" for v in since_values[1:])


# ---------------------------------------------------------------------------
# §18 (card H2) — restore suppression, GC of journal-rows-whose-claimant-
# is-gone, the §7 valve numerator narrowing, and the one-time
# restore_reconcile_catchup_v1. docs/design/aw-knowledgeable-fs-sync.md
# §18.7's acceptance gates, one test (or group) per bullet.
# ---------------------------------------------------------------------------


def test_restore_suppression_holds_across_further_ticks_while_the_file_stays_missing(
    _workspace_home, db, lease, monkeypatch
):
    """§18.2's whole point: a restore must not be a one-tick reprieve. The
    file is STILL missing — two more ticks pass, each would reach the
    old ≥2-scans threshold under reset-only behaviour, and neither
    re-marks."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/declined.md", external_id="doc-7", missing_scans=2, pending_delete=1)

    async def fake_changes(since=None, limit=200):
        return {
            "changes": [{
                "external_id": "doc-7", "source_path": "notion/declined.md",
                "rev": 1, "content_hash": "h", "sync_status": None,  # restored
                "updated_at": _iso(time.time()),
            }],
            "cursor": "cursor-1",
        }, None

    mark_calls = []

    async def fake_mark(source_path, workspace_slug, reason=None):
        mark_calls.append(source_path)
        return {"source_path": source_path, "marked": [], "count": 0}, None

    monkeypatch.setattr(client, "get_document_changes", fake_changes)
    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    result1 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result1["restored"] == 1
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/declined.md")
    assert row["restore_suppressed"] == 1
    assert row["pending_delete"] == 0
    assert row["missing_scans"] == 1  # this tick's own miss, post-restore

    # Two further ticks — the feed has nothing new now; the file is STILL
    # missing from disk both times.
    monkeypatch.setattr(
        client, "get_document_changes",
        lambda since=None, limit=200: _async({"changes": [], "cursor": since}, None),
    )

    _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert mark_calls == []  # never re-marked despite missing_scans climbing past 2
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/declined.md")
    assert row["pending_delete"] == 0
    assert row["restore_suppressed"] == 1
    assert row["missing_scans"] >= 2  # would have tripped the old threshold


def test_restore_suppression_clears_on_reappearance_then_re_marks_after_normal_two_scans(
    _workspace_home, db, lease, monkeypatch
):
    """§17.6-5 closed, loop-free: once the file genuinely comes back, the
    flag clears unconditionally and a LATER disappearance is marked again
    through the normal ≥2-scans clock — suppression is not a permanent
    exemption."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/declined.md", external_id="doc-7", missing_scans=3,
                   pending_delete=0, restore_suppressed=1)

    mark_calls = []

    async def fake_mark(source_path, workspace_slug, reason=None):
        mark_calls.append(source_path)
        return {"source_path": source_path, "marked": ["doc-7"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    # Tick 1 — the file reappears.
    _write(root, "notion/declined.md", "back again")
    result1 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result1["blocked"] is False
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/declined.md")
    assert row["restore_suppressed"] == 0
    assert row["missing_scans"] == 0
    assert mark_calls == []

    # Tick 2 — the file disappears again; first miss, not yet marked.
    (root / "notion/declined.md").unlink()
    result2 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result2["blocked"] is False
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/declined.md")
    assert row["missing_scans"] == 1
    assert mark_calls == []

    # Tick 3 — second consecutive miss: marked, exactly like any other path.
    result3 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result3["blocked"] is False
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/declined.md")
    assert row["missing_scans"] == 2
    assert row["pending_delete"] == 1
    assert mark_calls == ["notion/declined.md"]


def test_gc_removes_a_dead_pending_delete_row_absent_from_a_full_sync_paths_enumeration(
    _workspace_home, db, lease, monkeypatch
):
    """§18.3 — an approval is structurally invisible to the feed even after
    H1 (the node is gone, no tombstone). Absence from a full, successful
    sync-paths enumeration is the server-truth signal that the last
    claimant at that path is gone; the dead journal row gets GC'd."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/gone.md", external_id="doc-1", pending_delete=1)

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return {"paths": [{"source_path": "notion/keep.md", "claimant_count": 1}], "cursor": None}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["gc_removed"] == 1
    relpaths = {r["relpath"] for r in _journal_rows(db)}
    assert "notion/gone.md" not in relpaths
    assert "notion/keep.md" in relpaths


def test_gc_leaves_a_dead_row_untouched_when_the_enumeration_errors(
    _workspace_home, db, lease, monkeypatch
):
    """The zero-trust guard, exercised: an errored enumeration must not be
    read as "nobody claims this path any more" — GC must touch nothing
    rather than risk treating a transient failure as mass approval."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/gone.md", external_id="doc-1", pending_delete=1)

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return None, "HTTP 500"

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["gc_removed"] == 0
    relpaths = {r["relpath"] for r in _journal_rows(db)}
    assert "notion/gone.md" in relpaths


def test_gc_leaves_a_dead_row_untouched_when_the_enumeration_is_empty(
    _workspace_home, db, lease
):
    """Distinct from the errored case: a SUCCESSFUL but empty enumeration
    is still unusable for GC — on a real corpus, "nobody claims anything"
    is itself a suspect signal, not evidence every pending-delete row was
    just approved. The autouse `_no_adoptable_paths` fixture already
    returns an empty, error-free page."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/gone.md", external_id="doc-1", pending_delete=1)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["gc_removed"] == 0
    relpaths = {r["relpath"] for r in _journal_rows(db)}
    assert "notion/gone.md" in relpaths


def test_gc_never_touches_a_live_row_with_an_absent_claimant(
    _workspace_home, db, lease, monkeypatch
):
    """A live (`pending_delete=0`) row whose claimant vanished from
    sync-paths is the §17 journal-blind family, not deletion evidence —
    GC must never touch it; it stays for the ordinary missing-scans clock
    to pick up."""
    _write(bulk_ingest.kb_root(), "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/orphan.md", external_id="doc-2")  # pending_delete=0, not on disk

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return {"paths": [{"source_path": "notion/keep.md", "claimant_count": 1}], "cursor": None}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["gc_removed"] == 0
    relpaths = {r["relpath"] for r in _journal_rows(db)}
    assert "notion/orphan.md" in relpaths


def test_valve_denominator_must_not_narrow_with_the_numerator(_workspace_home, db, lease):
    """§18.5 point 5's rejected alternative, proven the other direction:
    narrowing the DENOMINATOR alongside the numerator would make the valve
    MORE trigger-happy as dead/suppressed rows accumulate — punishing
    exactly the corpus state the operator just blessed. 300 tracked rows
    (125 dead `pending_delete=1`, 7 `restore_suppressed=1`, 168 mark-
    eligible of which 28 are missing this tick): the correct, UNCHANGED
    denominator gives threshold max(25, 30%)=30 and 28 < 30 stays clear; a
    denominator narrowed to the 168 eligible rows gives max(25, 16.8%)=25
    and 28 > 25 would wrongly trip. Fails under a narrowed denominator,
    passes only when the denominator stays the full tracked set."""
    root = bulk_ingest.kb_root()
    for i in range(125):
        _seed_uploaded(db, f"notion/dead-{i}.md", sha256=f"d{i}", external_id=f"doc-dead-{i}", pending_delete=1)
    for i in range(7):
        _seed_uploaded(db, f"notion/suppressed-{i}.md", sha256=f"s{i}", external_id=f"doc-sup-{i}",
                       restore_suppressed=1)
    for i in range(140):
        _seed_uploaded(db, f"notion/present-{i}.md", sha256=f"p{i}", external_id=f"doc-present-{i}",
                       scanned_at=time.time() - 100)
        _write(root, f"notion/present-{i}.md", f"body {i}")
    for i in range(28):
        _seed_uploaded(db, f"notion/missing-{i}.md", sha256=f"m{i}", external_id=f"doc-missing-{i}")

    assert fs_sync._tracked_row_count(db) == 300  # the denominator must see ALL 300

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert not result.get("valve_tripped")


def test_valve_headroom_restored_on_a_simulated_stuck_corpus(_workspace_home, db, lease):
    """QA's own measured regression (pre-H2): 132 of 206 tracked rows
    permanently consumed guard 2's numerator — 107 dead (§18.3 GC's job),
    18 declined-but-suppressed, 7 legitimately `pending_delete`. Simulates
    that exact split plus 74 live rows and confirms the valve clears on a
    perfectly quiet tree: the near-zero headroom §18.3 promises, not the
    132-of-206 permanently consumed before it."""
    root = bulk_ingest.kb_root()
    for i in range(107):
        _seed_uploaded(db, f"notion/dead-{i}.md", sha256=f"d{i}", external_id=f"doc-dead-{i}", pending_delete=1)
    for i in range(18):
        _seed_uploaded(db, f"notion/declined-{i}.md", sha256=f"c{i}", external_id=f"doc-decl-{i}",
                       restore_suppressed=1)
    for i in range(7):
        _seed_uploaded(db, f"notion/pending-{i}.md", sha256=f"e{i}", external_id=f"doc-pend-{i}", pending_delete=1)
    for i in range(74):
        _seed_uploaded(db, f"notion/live-{i}.md", sha256=f"l{i}", external_id=f"doc-live-{i}",
                       scanned_at=time.time() - 100)
        _write(root, f"notion/live-{i}.md", f"body {i}")

    assert fs_sync._tracked_row_count(db) == 206

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert not result.get("valve_tripped")


def test_restore_reconcile_catchup_three_way_split(_workspace_home, db, lease, monkeypatch):
    """§18.4's three populations in one tick: a dead row (no claimant at
    all) is §18.3's GC job and never reaches this pass; a live claimant NOT
    in the server's currently-marked set was restored through a channel
    the feed's cursor can never replay — reconciled via §18.2's
    clear+suppress; a live claimant IN the marked set is genuinely still
    awaiting approval and is left alone."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/restored.md", external_id="doc-r", pending_delete=1)
    _seed_uploaded(db, "notion/still-pending.md", external_id="doc-p", pending_delete=1)
    _seed_uploaded(db, "notion/dead.md", external_id="doc-d", pending_delete=1)

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return {
            "paths": [
                {"source_path": "notion/keep.md", "claimant_count": 1},
                {"source_path": "notion/restored.md", "claimant_count": 1},
                {"source_path": "notion/still-pending.md", "claimant_count": 1},
            ],
            "cursor": None,
        }, None

    async def fake_pending_delete():
        return {"documents": [{"source_path": "notion/still-pending.md"}]}, None

    async def fake_mark(source_path, workspace_slug, reason=None):
        return {"source_path": source_path, "marked": [], "count": 0}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)
    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending_delete)
    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))

    assert result["blocked"] is False
    assert result["gc_removed"] == 1
    assert result["restore_reconciled"] == 1
    rows = {r["relpath"]: r for r in _journal_rows(db)}
    assert "notion/dead.md" not in rows
    assert rows["notion/restored.md"]["pending_delete"] == 0
    assert rows["notion/restored.md"]["restore_suppressed"] == 1
    assert rows["notion/still-pending.md"]["pending_delete"] == 1


def test_restore_reconcile_catchup_runs_once_then_stays_off(
    _workspace_home, db, lease, monkeypatch
):
    """Flag-gated, same shape as §17.4's `CATCHUP_FLAG`: a second tick must
    not re-fetch the server's marked set at all once the one-time pass has
    completed, idempotent across restarts (the gate is a `FLAGS_TABLE` row,
    not in-memory state)."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/restored.md", external_id="doc-r", pending_delete=1)

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return {
            "paths": [
                {"source_path": "notion/keep.md", "claimant_count": 1},
                {"source_path": "notion/restored.md", "claimant_count": 1},
            ],
            "cursor": None,
        }, None

    pending_delete_calls = []

    async def fake_pending_delete():
        pending_delete_calls.append(1)
        return {"documents": []}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)
    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending_delete)

    result1 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result1["restore_reconciled"] == 1
    assert fs_sync._restore_reconcile_done(db) is True
    assert len(pending_delete_calls) == 1

    result2 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result2["restore_reconciled"] == 0
    assert len(pending_delete_calls) == 1  # not called again — flag already set


def test_restore_reconcile_catchup_retries_after_a_failed_documents_fetch(
    _workspace_home, db, lease, monkeypatch
):
    """Crash-idempotent: the flag must be set ONLY when the pass completed
    without errors. A failed `GET /documents?pending_delete=1` leaves the
    row untouched and the flag unset, so the next tick retries clean."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/restored.md", external_id="doc-r", pending_delete=1)

    async def fake_sync_paths(workspace_slug, after=None, limit=500):
        return {
            "paths": [
                {"source_path": "notion/keep.md", "claimant_count": 1},
                {"source_path": "notion/restored.md", "claimant_count": 1},
            ],
            "cursor": None,
        }, None

    calls = {"n": 0}

    async def flaky_pending_delete():
        calls["n"] += 1
        if calls["n"] == 1:
            return None, "HTTP 500"
        return {"documents": []}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_sync_paths)
    monkeypatch.setattr(client, "get_documents_pending_delete", flaky_pending_delete)

    result1 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result1["restore_reconciled"] == 0
    assert fs_sync._restore_reconcile_done(db) is False
    row1 = next(r for r in _journal_rows(db) if r["relpath"] == "notion/restored.md")
    assert row1["pending_delete"] == 1  # untouched — the fetch failed

    result2 = _run(fs_sync.run_tick(db, lease, workspace_slug="aw"))
    assert result2["restore_reconciled"] == 1
    assert fs_sync._restore_reconcile_done(db) is True
