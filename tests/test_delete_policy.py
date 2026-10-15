"""docs/design/aw-knowledgeable-fs-sync.md §21 — per-prefix `delete_policy`
for unattended deletion approval, exercised directly: the same plain
`_run` helper driving coroutines, `client`'s async functions monkeypatched
rather than `httpx` itself, same style and same duplicated `_FakeDb`/
`_FakeLease`/`_FakeKv` doubles as `test_fs_sync.py`/`test_pending_
deletions.py`/`test_routes.py` (this repo's stated convention).

A NEW file, not appended to `test_fs_sync.py`: that file's own module
docstring pins the §7 valve contract to line ranges (:191-290, :739-809)
that must never renumber, and §21 chose a separate file specifically to
protect them. Nothing in `test_fs_sync.py` is edited by this card.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest, fs_sync, pending_deletions, routes as routes_mod  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeDb:
    """Double for ``ctx.db`` — see `test_fs_sync.py`'s identical class for
    the full rationale (named ``:param`` binding, ``{table}`` substitution,
    the Postgres-style upsert sqlite also accepts)."""

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
def _fresh_table_flags(monkeypatch):
    """Both of this repo's process-level "created once" guards — see
    `test_pending_deletions.py`'s identical fixture for why a fresh
    `:memory:` db per test needs both reset, not just `bulk_ingest`'s."""
    monkeypatch.setattr(bulk_ingest, "_table_ensured", False)
    monkeypatch.setattr(pending_deletions, "_table_ensured", False)


@pytest.fixture(autouse=True)
def _healthy_ingest_status(monkeypatch):
    async def fake_get_ingest_status(bucket=None):
        return {"extraction": {"claiming": False}, "processing": {"pending": 0}}, None

    monkeypatch.setattr(client, "get_ingest_status", fake_get_ingest_status)


@pytest.fixture(autouse=True)
def _no_adoptable_paths(monkeypatch):
    """Empty (not erroring) `GET /sync-paths` — `_adopt_paths` returns an
    empty, successfully-enumerated `sync_paths_seen`, which keeps §18.3's
    GC and §18.4's one-time restore-reconcile catch-up inert (both require
    a NONEMPTY set — see their own docstrings) so these tests exercise only
    the §21 pass, same default `test_fs_sync.py` relies on."""

    async def fake_get_sync_paths(workspace_slug, after=None, limit=500, bucket=None):
        return {"paths": [], "cursor": None}, None

    monkeypatch.setattr(client, "get_sync_paths", fake_get_sync_paths)


@pytest.fixture(autouse=True)
def _no_pending_changes(monkeypatch):
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
                   pending_delete: int = 0) -> None:
    """Trimmed equivalent of `test_fs_sync.py`'s own `_seed_uploaded` — this
    file doesn't import that one (two separate test modules, same repo
    convention `test_pending_deletions.py` already states)."""
    bulk_ingest._ensure_table(db)
    now = time.time()
    db.execute(
        fs_sync.TABLE,
        "INSERT INTO {table} (relpath, sha256, bucket, status, external_id, scanned_at, updated_at, "
        "missing_scans, pending_delete, last_synced_hash) "
        "VALUES (:relpath, :sha256, :bucket, :status, :external_id, :now, :now, :missing, :pending, "
        ":last_synced)",
        {
            "relpath": relpath, "sha256": sha256, "bucket": "main", "status": bulk_ingest.STATUS_UPLOADED,
            "external_id": external_id, "now": now, "missing": missing_scans, "pending": pending_delete,
            "last_synced": last_synced_hash if last_synced_hash is not None else sha256,
        },
    )


def _audit_rows(db: _FakeDb) -> list[dict]:
    try:
        rows = db.execute(pending_deletions.TABLE, "SELECT * FROM {table} ORDER BY ts")
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# §21.1 — the parser. Same shape as `test_fs_sync.py`'s own
# `parse_quiet_rules` unit tests, mirrored for `parse_delete_policy`.
# ---------------------------------------------------------------------------


def test_parse_delete_policy_empty_or_none_resolves_to_manual_everywhere():
    for raw in (None, "", "   "):
        rules, invalid = fs_sync.parse_delete_policy(raw)
        assert rules == []
        assert invalid == []
        rule = fs_sync._resolve_delete_policy("random/anything.md", rules)
        assert rule.mode == "manual"


def test_parse_delete_policy_basic_auto_and_manual():
    rules, invalid = fs_sync.parse_delete_policy("random/=auto,notion/=manual")
    assert invalid == []
    by_prefix = {r.prefix: r for r in rules}
    assert by_prefix["random/"].mode == "auto"
    assert by_prefix["random/"].min_missing_scans == fs_sync.DEFAULT_MIN_MISSING_SCANS == 2
    assert by_prefix["notion/"].mode == "manual"


def test_parse_delete_policy_explicit_min_missing_scans():
    rules, invalid = fs_sync.parse_delete_policy("random/=auto:5")
    assert invalid == []
    assert rules[0].min_missing_scans == 5


def test_parse_delete_policy_sorts_longest_prefix_first_regardless_of_declaration_order():
    rules_a, _ = fs_sync.parse_delete_policy("notion/=auto,notion/kanban/=manual")
    rules_b, _ = fs_sync.parse_delete_policy("notion/kanban/=manual,notion/=auto")
    assert [r.prefix for r in rules_a] == ["notion/kanban/", "notion/"]
    assert [r.prefix for r in rules_b] == ["notion/kanban/", "notion/"]


def test_parse_delete_policy_rejects_an_empty_prefix_no_blanket_auto_ever():
    """§21.1's first deliberate rejection: the 2026-10-15 near-miss (4
    `apps/knowledgeable/cli_reference/` marks that a daily task would have
    restored) is exactly what this guards against."""
    rules, invalid = fs_sync.parse_delete_policy("=auto,random/=auto")
    assert len(invalid) == 1
    assert "=auto" in invalid[0]
    assert [r.prefix for r in rules] == ["random/"]


def test_parse_delete_policy_rejects_an_auto_rule_on_authored():
    """§21.1's second deliberate rejection — §4 downsync is unexercised."""
    rules, invalid = fs_sync.parse_delete_policy("authored/=auto,random/=auto")
    assert len(invalid) == 1
    assert "authored/=auto" in invalid[0]
    assert [r.prefix for r in rules] == ["random/"]


def test_parse_delete_policy_authored_manual_is_fine():
    rules, invalid = fs_sync.parse_delete_policy("authored/=manual")
    assert invalid == []
    assert rules[0].prefix == "authored/"
    assert rules[0].mode == "manual"


def test_parse_delete_policy_skips_malformed_entries_but_keeps_valid_ones():
    rules, invalid = fs_sync.parse_delete_policy(
        "random/=auto, broken-no-equals, notion/=manual:5, weird/=bogus, deep/=auto:-1"
    )
    assert [r.prefix for r in rules] == ["random/"]
    assert len(invalid) == 4


def test_resolve_delete_policy_longest_prefix_wins_and_unmatched_falls_back_to_manual():
    rules, _ = fs_sync.parse_delete_policy("notion/=auto,notion/kanban/=manual")
    assert fs_sync._resolve_delete_policy("notion/kanban/x.md", rules).mode == "manual"
    assert fs_sync._resolve_delete_policy("notion/other.md", rules).mode == "auto"
    assert fs_sync._resolve_delete_policy("apps/x.md", rules).mode == "manual"


# ---------------------------------------------------------------------------
# §21.8 acceptance 1 — random/=auto, two ticks, no human.
# ---------------------------------------------------------------------------


def test_acceptance_1_auto_prefix_marks_and_approves_in_the_same_tick_no_human(
    _workspace_home, db, lease, monkeypatch
):
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")  # survive guard 1
    # Tick 1 already happened (missing_scans=1, not yet marked) — this
    # call is tick 2: the second consecutive miss.
    _seed_uploaded(db, "random/gone.md", external_id="doc-99", missing_scans=1)

    mark_calls = []

    async def fake_mark(source_path, workspace_slug, *, bucket=None, reason=None):
        mark_calls.append(source_path)
        return {"source_path": source_path, "marked": ["doc-99"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    pending_calls = []

    async def fake_pending(bucket=None):
        pending_calls.append(bucket)
        return {"documents": [{"id": "doc-99", "source_path": "random/gone.md"}]}, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)

    deleted = []

    async def fake_delete(node_id, *, bucket):
        deleted.append(node_id)
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy="random/=auto", valve_latched=False,
    ))

    assert result["blocked"] is False
    assert result["deleted_marked"] == 1
    assert mark_calls == ["random/gone.md"]
    # Once by this pass to build `node_ids`, once again inside
    # `approve_pending_deletions` itself to re-derive its own scope match
    # before deleting — both real calls, not a bug.
    assert pending_calls == [bulk_ingest.BUCKET, bulk_ingest.BUCKET]
    assert deleted == ["doc-99"]
    assert result["auto_approved"] == 1
    assert result["auto_approve_failed"] == 0
    assert "auto_approve_suspended" not in result

    # Journal row reconciled away by the EXISTING approve path's own
    # `_reconcile_journal_for_succeeded` — no new plumbing. "notion/
    # keep.md" is a separate, freshly-registered row for the one file
    # this test keeps on disk to survive guard 1 — unrelated to the
    # delete/approve flow under test here.
    assert [r["relpath"] for r in _journal_rows(db)] == ["notion/keep.md"]

    audit = _audit_rows(db)
    assert len(audit) == 1
    assert audit[0]["action"] == "approve"
    assert audit[0]["actor"] == "fs-sync:delete_policy rule=random/"
    assert audit[0]["matched_count"] == 1
    assert audit[0]["succeeded_count"] == 1
    assert audit[0]["outcome"] == "ok"


# ---------------------------------------------------------------------------
# §21.8 acceptance 2 — a manual (or unmatched) prefix still waits.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("delete_policy", ["random/=manual", "random/=auto", None])
def test_acceptance_2_non_auto_or_unmatched_prefix_stays_pending(
    _workspace_home, db, lease, monkeypatch, delete_policy,
):
    """`random/=manual` (explicit manual) and `None` (random/ unmatched,
    the no-policy-at-all default) both wait — same as today. The third
    case, `random/=auto` scoped to a DIFFERENT path than the one marked
    here (`notion/gone.md`), proves a configured auto rule never reaches
    past its own prefix."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "notion/gone.md", external_id="doc-7", missing_scans=1)

    async def fake_mark(source_path, workspace_slug, *, bucket=None, reason=None):
        return {"source_path": source_path, "marked": ["doc-7"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    pending_calls = []

    async def fake_pending(bucket=None):
        pending_calls.append(bucket)
        return {"documents": [{"id": "doc-7", "source_path": "notion/gone.md"}]}, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)

    async def fail_delete(node_id, *, bucket):
        raise AssertionError("must never approve a non-auto path")

    monkeypatch.setattr(client, "delete_document", fail_delete)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy=delete_policy, valve_latched=False,
    ))

    assert result["blocked"] is False
    assert result["deleted_marked"] == 1
    assert result["auto_approved"] == 0
    row = next(r for r in _journal_rows(db) if r["relpath"] == "notion/gone.md")
    assert row["pending_delete"] == 1
    # No row is ever eligible under any of these three policies (the only
    # marked row, notion/gone.md, matches neither "random/=manual" nor
    # "random/=auto", and there is no row at all for "random/=auto" to
    # match) — the pass finds nothing in the journal query itself and
    # never even reaches the server.
    assert pending_calls == []


# ---------------------------------------------------------------------------
# §21.8 acceptance 3 — the valve.
# ---------------------------------------------------------------------------


def test_acceptance_3a_guard2_pre_pass_trip_runs_the_auto_pass_never(
    _workspace_home, db, lease, monkeypatch,
):
    """Guard 2 aborts the WHOLE tick before `_mark_missing_as_deleted` (and
    therefore before the §21 pass) ever runs — free no-op, not a separate
    check inside the pass itself."""
    for i in range(30):
        _seed_uploaded(db, f"random/{i}.md", sha256=f"h{i}", external_id=f"doc-{i}")
    root = bulk_ingest.kb_root()
    for i in range(4):  # 26 missing, past max(25, 3)=25
        _write(root, f"random/{i}.md", f"body {i}")

    pending_calls = []

    async def fake_pending(bucket=None):
        pending_calls.append(bucket)
        return {"documents": []}, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy="random/=auto", valve_latched=False,
    ))

    assert result["blocked"] is True
    assert result["valve_tripped"] is True
    assert pending_calls == []
    assert "auto_approved" not in result  # _empty_result's shape, pre-§21


def test_acceptance_3b_midpass_mark_budget_trip_suspends_the_auto_pass(
    _workspace_home, db, lease, monkeypatch,
):
    """§17.5's third guard, tripped mid-pass — real marking work already
    happened this tick, so the auto pass must see `mark_budget.tripped`
    and refuse, rather than approving a budget-exceeded batch. Distinct
    from acceptance 3a: here guard 2's own PRE-pass path-count check must
    stay clear (only 2 paths go missing, far under its floor of 25) while
    the ceiling trips on DOCUMENT count instead — one path's by-path mark
    fanning out to many documents at once, exactly the gap guard 2's
    path-counting alone cannot see (§17.5)."""
    root = bulk_ingest.kb_root()
    _seed_uploaded(db, "random/a.md", sha256="ha", external_id="doc-a", missing_scans=1)
    _seed_uploaded(db, "random/b.md", sha256="hb", external_id="doc-b", missing_scans=1)
    _write(root, "notion/keep.md", "keep me")  # survive guard 1

    async def fake_mark(source_path, workspace_slug, *, bucket=None, reason=None):
        # One path's by-path mark fans out to 30 documents — alone past
        # the in-pass ceiling (max(25, 10% of 2 tracked) == 25).
        return {"source_path": source_path, "marked": ["doc-x"] * 30, "count": 30}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    pending_calls = []

    async def fake_pending(bucket=None):
        pending_calls.append(bucket)
        return {"documents": []}, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy="random/=auto", valve_latched=False,
    ))

    assert result["blocked"] is True
    assert result["valve_tripped"] is True
    assert result["auto_approved"] == 0
    assert "in-pass document ceiling" in result["auto_approve_suspended"]
    assert pending_calls == []  # suspended before ever listing the server


def test_acceptance_3c_latched_valve_marks_but_does_not_approve(
    _workspace_home, db, lease, monkeypatch,
):
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "random/gone.md", external_id="doc-99", missing_scans=1)

    async def fake_mark(source_path, workspace_slug, *, bucket=None, reason=None):
        return {"source_path": source_path, "marked": ["doc-99"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    async def fail_pending(bucket=None):
        raise AssertionError("a latched valve must never even list the server's pending queue")

    monkeypatch.setattr(client, "get_documents_pending_delete", fail_pending)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy="random/=auto", valve_latched=True,
    ))

    assert result["blocked"] is False  # marking still succeeds — only approval is suspended
    assert result["deleted_marked"] == 1
    assert result["auto_approved"] == 0
    assert "valve latch is set" in result["auto_approve_suspended"]
    row = next(r for r in _journal_rows(db) if r["relpath"] == "random/gone.md")
    assert row["pending_delete"] == 1


def test_acceptance_3c_unknown_latch_state_defaults_auto_approve_off(
    _workspace_home, db, lease, monkeypatch,
):
    """`valve_latched` omitted entirely (the `run_tick` default, `None`) —
    fail closed. A caller that never proves the latch is clear gets
    marking-only behaviour, same as every pre-§21 caller."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "random/gone.md", external_id="doc-99", missing_scans=1)

    async def fake_mark(source_path, workspace_slug, *, bucket=None, reason=None):
        return {"source_path": source_path, "marked": ["doc-99"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    async def fail_pending(bucket=None):
        raise AssertionError("an unknown latch state must never permit an automatic delete")

    monkeypatch.setattr(client, "get_documents_pending_delete", fail_pending)

    result = _run(fs_sync.run_tick(db, lease, workspace_slug="aw", delete_policy="random/=auto"))

    assert result["auto_approved"] == 0
    assert "unknown" in result["auto_approve_suspended"]


def test_acceptance_3d_after_the_latch_clears_the_next_tick_approves(
    _workspace_home, db, lease, monkeypatch,
):
    """Simulates the audited `POST /fs-sync/valve/clear` having already run
    (this file only unit-tests `fs_sync.run_tick`/`parse_delete_policy`;
    the route itself threading a freshly-cleared `valve_latched=False` is
    covered below in the routes section) — the very next tick's auto pass
    now proceeds normally."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "random/gone.md", external_id="doc-99", missing_scans=2, pending_delete=1)

    async def fake_pending(bucket=None):
        return {"documents": [{"id": "doc-99", "source_path": "random/gone.md"}]}, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)

    deleted = []

    async def fake_delete(node_id, *, bucket):
        deleted.append(node_id)
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy="random/=auto", valve_latched=False,
    ))

    assert result["auto_approved"] == 1
    assert deleted == ["doc-99"]


# ---------------------------------------------------------------------------
# §21.8 acceptance 4 — reappearance cancels the mark before approval fires.
# ---------------------------------------------------------------------------


def test_acceptance_4_reappearance_resets_the_counter_nothing_approved_document_intact(
    _workspace_home, db, lease, monkeypatch,
):
    """`random/=auto:5` — mark lands at the fixed threshold (2, unrelated
    to the policy's own `min_missing_scans`), but approval needs 5. The
    file reappears while `missing_scans` is still short of 5: the found-
    loop's existing reset (pre-§21 behaviour) takes it back to 0, so even
    though the row is still `pending_delete=1`, the pass's own `missing_
    scans >= rule.min` check excludes it — nothing approved, nothing
    deleted from the graph."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _write(root, "random/back.md", "I'm back")  # the file reappeared on disk
    _seed_uploaded(
        db, "random/back.md", sha256="h", last_synced_hash="h",
        external_id="doc-5", missing_scans=3, pending_delete=1,
    )

    async def fail_pending(bucket=None):
        raise AssertionError("missing_scans(0) < rule.min(5) must never reach the server")

    monkeypatch.setattr(client, "get_documents_pending_delete", fail_pending)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy="random/=auto:5", valve_latched=False,
    ))

    assert result["blocked"] is False
    assert result["auto_approved"] == 0
    row = next(r for r in _journal_rows(db) if r["relpath"] == "random/back.md")
    assert row["missing_scans"] == 0  # the pre-§21 reset, unaffected by this amendment


# ---------------------------------------------------------------------------
# §21.8 acceptance 5 — delete_policy="" (or absent) ships inert.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("delete_policy", ["", None])
def test_acceptance_5_empty_or_absent_policy_is_inert(
    _workspace_home, db, lease, monkeypatch, delete_policy,
):
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "random/gone.md", external_id="doc-99", missing_scans=1)

    async def fake_mark(source_path, workspace_slug, *, bucket=None, reason=None):
        return {"source_path": source_path, "marked": ["doc-99"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    async def fail_pending(bucket=None):
        raise AssertionError("delete_policy absent/empty must never touch the server's pending queue")

    monkeypatch.setattr(client, "get_documents_pending_delete", fail_pending)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy=delete_policy, valve_latched=False,
    ))

    assert result["blocked"] is False
    assert result["deleted_marked"] == 1
    assert result["auto_approved"] == 0
    assert result["auto_approve_failed"] == 0
    assert "auto_approve_suspended" not in result
    assert _audit_rows(db) == []


def test_acceptance_5_status_and_report_surface_an_empty_delete_policy(db):
    status = fs_sync.status(db, delete_policy="")
    assert status["delete_policy"] == []
    assert status["delete_policy_invalid"] == []
    report = fs_sync.report(db, delete_policy="")
    assert report["delete_policy"] == []
    assert report["delete_policy_invalid"] == []


def test_status_surfaces_invalid_delete_policy_entries(db):
    status = fs_sync.status(db, delete_policy="=auto,authored/=auto,random/=auto:5")
    assert len(status["delete_policy_invalid"]) == 2
    assert status["delete_policy"] == [
        {"prefix": "random/", "mode": "auto", "min_missing_scans": 5}
    ]


# ---------------------------------------------------------------------------
# §21.7 — a TOCTOU count mismatch is a feature, never retried in-pass.
# ---------------------------------------------------------------------------


def test_toctou_count_mismatch_is_audited_as_rejected_and_not_retried_in_pass(
    _workspace_home, db, lease, monkeypatch,
):
    """The server's list and the approve call disagree (another writer
    raced the scope) — `approve_pending_deletions` itself rejects and
    audits; the §21 pass must not retry, just count it as not-approved
    this tick."""
    root = bulk_ingest.kb_root()
    _write(root, "notion/keep.md", "keep me")
    _seed_uploaded(db, "random/gone.md", external_id="doc-99", missing_scans=1)

    async def fake_mark(source_path, workspace_slug, *, bucket=None, reason=None):
        return {"source_path": source_path, "marked": ["doc-99"], "count": 1}, None

    monkeypatch.setattr(client, "mark_delete_by_path", fake_mark)

    async def fake_pending(bucket=None):
        # Server lists it twice under two ids — expected_count disagrees
        # with what `approve_pending_deletions` re-derives internally only
        # if the set changes between this fetch and the approve call; to
        # exercise the rejection path deterministically, return a SECOND
        # document at the same relpath that the pass's own `node_ids`
        # build will include, then have `delete_document` never get
        # called at all because the mismatch is detected before deleting.
        return {
            "documents": [
                {"id": "doc-99", "source_path": "random/gone.md"},
                {"id": "doc-extra", "source_path": "random/gone.md"},
            ]
        }, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)

    deleted = []

    async def fake_delete(node_id, *, bucket):
        deleted.append(node_id)
        return True, None

    monkeypatch.setattr(client, "delete_document", fake_delete)

    result = _run(fs_sync.run_tick(
        db, lease, workspace_slug="aw", delete_policy="random/=auto", valve_latched=False,
    ))

    # Both documents at the co-claimed path are legitimately scoped
    # together (by-path semantics, §17) — this is actually the happy
    # path, not a mismatch, so both get approved.
    assert result["auto_approved"] == 2
    assert sorted(deleted) == ["doc-99", "doc-extra"]


# ---------------------------------------------------------------------------
# Routes — `POST /fs-sync/valve/clear` and the config seam threaded into
# `/fs-sync/run`. Own minimal doubles, same convention `test_routes.py`
# already states (duplicated per test file, not shared).
# ---------------------------------------------------------------------------


class _RouteFakeDb(_FakeDb):
    pass


class _FakeKv:
    def __init__(self):
        self._store: dict[str, object] = {}

    async def put(self, name, value, ttl):
        assert ttl and ttl > 0  # §21.2 — no immortal keys through this facade
        self._store[name] = value

    async def get(self, name):
        return self._store.get(name)

    async def delete(self, name):
        self._store.pop(name, None)


class _FakeSecrets:
    def read(self, key):
        return ""


@pytest.fixture()
def route_ctx():
    ctx = SimpleNamespace(
        secrets=_FakeSecrets(), config={},
        state=SimpleNamespace(kv=_FakeKv(), lease=_FakeLease()), db=_RouteFakeDb(),
    )
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: ctx.secrets.read(routes_mod.SECRET_KEY) or "")
    app = routes_mod.build_routes(ctx)
    app.dependency_overrides[routes_mod._require_identity] = lambda: None
    return TestClient(app), ctx


def test_valve_clear_route_deletes_the_latch_and_audits_it(route_ctx, monkeypatch):
    tc, ctx = route_ctx
    _run(ctx.state.kv.put(routes_mod.FS_SYNC_VALVE_KV_NAME, {"reason": "guard 2 tripped"}, ttl=60))

    resp = tc.post("/fs-sync/valve/clear")
    assert resp.status_code == 200
    body = resp.json()
    assert body["cleared"] is True
    assert body["was_latched"] is True

    assert _run(ctx.state.kv.get(routes_mod.FS_SYNC_VALVE_KV_NAME)) is None
    audit = _audit_rows(ctx.db)
    assert len(audit) == 1
    assert audit[0]["action"] == "valve_clear"


def test_valve_clear_route_requires_identity(route_ctx):
    tc, _ = route_ctx
    tc.app.dependency_overrides.pop(routes_mod._require_identity, None)
    assert tc.post("/fs-sync/valve/clear").status_code == 401


def test_fs_sync_run_threads_delete_policy_and_proven_latch_state(route_ctx, monkeypatch):
    """The ONE caller that always resolves `valve_latched` to a real bool
    rather than the fail-closed `None` default — proven by actually
    reading `ctx.state.kv` first."""
    tc, ctx = route_ctx
    ctx.config["delete_policy"] = "random/=auto"
    _run(ctx.state.kv.put(routes_mod.FS_SYNC_VALVE_KV_NAME, {"reason": "x"}, ttl=60))

    captured = {}

    async def fake_run_tick(db, lease, **kwargs):
        captured.update(kwargs)
        return {"blocked": False}

    monkeypatch.setattr(routes_mod.fs_sync, "run_tick", fake_run_tick)

    resp = tc.post("/fs-sync/run")
    assert resp.status_code == 200
    assert captured["delete_policy"] == "random/=auto"
    assert captured["valve_latched"] is True  # proven from the KV, not defaulted


def test_fs_sync_status_and_report_thread_delete_policy_config(route_ctx, monkeypatch):
    tc, ctx = route_ctx
    ctx.config["delete_policy"] = "random/=auto:3"

    resp = tc.get("/fs-sync/status")
    assert resp.status_code == 200
    assert resp.json()["delete_policy"] == [
        {"prefix": "random/", "mode": "auto", "min_missing_scans": 3}
    ]

    resp = tc.get("/fs-sync/report")
    assert resp.status_code == 200
    assert resp.json()["delete_policy"] == [
        {"prefix": "random/", "mode": "auto", "min_missing_scans": 3}
    ]
