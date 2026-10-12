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
                   pending_delete: int = 0, scanned_at: float | None = None) -> None:
    """Seed one already-synced journal row directly — the no-wipe
    bootstrap's shape (§8's 2,046 rows): `status=uploaded`, a real
    `external_id`, and (after the §3.2 backfill) `last_synced_hash`."""
    bulk_ingest._ensure_table(db)
    now = scanned_at if scanned_at is not None else time.time()
    db.execute(
        fs_sync.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, external_id, scanned_at, updated_at, "
        "missing_scans, pending_delete, last_synced_hash) "
        "VALUES (:relpath, :sha256, :bucket, :status, :external_id, :now, :now, :missing, :pending, :last_synced)",
        {
            "relpath": relpath, "sha256": sha256, "bucket": "main", "status": bulk_ingest.STATUS_UPLOADED,
            "external_id": external_id, "now": now, "missing": missing_scans, "pending": pending_delete,
            "last_synced": last_synced_hash if last_synced_hash is not None else sha256,
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


def test_journal_gains_the_five_sync_columns_and_backfills_last_synced_hash(_workspace_home, db):
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


def test_adoption_excludes_authored_paths(_workspace_home, db, lease, monkeypatch):
    """§17.3 exclusion 2 — pre-card-C, a human-authored document can
    legitimately exist server-side with no file on disk; adopting it
    would mark that future downsync payload for deletion."""
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
    assert result["adopted"] == 1
    relpaths = {r["relpath"] for r in _journal_rows(db)}
    assert "authored/note.md" not in relpaths
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
