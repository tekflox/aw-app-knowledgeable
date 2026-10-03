"""The §13 bulk-ingest driver's engine — scan/canonicalize, the bounded
tick, and the report, exercised directly (no FastAPI, no real network, no
pytest-asyncio — same style as ``test_ingest_key_push.py``: a plain ``_run``
helper driving coroutines, and ``client``'s own async functions monkeypatched
rather than ``httpx`` itself, since what's under test here is the engine's
own decisions, not the HTTP shaping (that's ``test_client.py``'s job).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


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


def _write(root: Path, relpath: str, content: str) -> None:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _ok_status(claiming: bool = False, pending: int = 0):
    async def fake(bucket=None):
        return {"extraction": {"claiming": claiming}, "processing": {"pending": pending}}, None

    return fake


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


def test_scan_finds_files_across_all_four_buckets(_workspace_home):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "crispal body")
    _write(root, "memory/b.md", "memory body")
    _write(root, "notion/c.md", "notion body")
    _write(root, "mapped_folders/repos/x/d.md", "mapped body")

    counts = bulk_ingest.scan()
    assert counts["scanned"] == 4
    assert counts["canonical"] == 4
    assert counts["alias"] == 0

    status = bulk_ingest.status()
    assert status["by_bucket"]["kb-crispal"] == {"pending": 1}
    assert status["by_bucket"]["kb-memory"] == {"pending": 1}
    assert status["by_bucket"]["kb-notion"] == {"pending": 1}
    assert status["by_bucket"]["kb-mapped-folders"] == {"pending": 1}


def test_scan_canonicalizes_duplicate_content_across_prefixes(_workspace_home):
    """§13.2's measured case: the same content under the monolith checkout,
    a real repo, and the app's installed mirror — exactly one upload."""
    root = bulk_ingest.kb_root()
    same = "duplicated across three prefixes"
    _write(root, "mapped_folders/repos/agentic-workspace/docs/knowledge_base/crispal/notes.md", same)
    _write(root, "mapped_folders/repos/aw-app-crispal/docs/knowledge_base/notes.md", same)
    _write(root, "mapped_folders/apps/crispal/docs/knowledge_base/notes.md", same)

    counts = bulk_ingest.scan()
    assert counts["scanned"] == 3
    assert counts["canonical"] == 1
    assert counts["alias"] == 2

    conn = bulk_ingest._connect()
    try:
        canonical = conn.execute(
            "SELECT relpath FROM files WHERE status = ?", (bulk_ingest.STATUS_PENDING,)
        ).fetchall()
        assert [r["relpath"] for r in canonical] == [
            "mapped_folders/repos/aw-app-crispal/docs/knowledge_base/notes.md"
        ]
        aliases = conn.execute(
            "SELECT relpath, canonical_relpath FROM files WHERE status = ? ORDER BY relpath",
            (bulk_ingest.STATUS_ALIAS,),
        ).fetchall()
        assert [r["canonical_relpath"] for r in aliases] == [
            "mapped_folders/repos/aw-app-crispal/docs/knowledge_base/notes.md",
            "mapped_folders/repos/aw-app-crispal/docs/knowledge_base/notes.md",
        ]
    finally:
        conn.close()


def test_scan_skips_oversized_files_with_a_reason(_workspace_home):
    root = bulk_ingest.kb_root()
    big = "x" * (bulk_ingest.MAX_FILE_BYTES + 1)
    _write(root, "memory/huge.md", big)

    counts = bulk_ingest.scan()
    assert counts["skipped"] == 1

    conn = bulk_ingest._connect()
    try:
        row = conn.execute("SELECT status, error FROM files WHERE relpath = ?", ("memory/huge.md",)).fetchone()
        assert row["status"] == bulk_ingest.STATUS_SKIPPED
        assert "10MB" in row["error"]
    finally:
        conn.close()


def test_rescan_does_not_duplicate_or_regress_an_uploaded_row(_workspace_home, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "stable content")
    bulk_ingest.scan()

    uploaded = {"calls": 0}

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        uploaded["calls"] += 1
        return {"id": "doc-1", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)
    result = _run(bulk_ingest.run_tick())
    assert result["uploaded"] == 1
    assert uploaded["calls"] == 1

    # Re-scan: the uploaded row must not flip back to pending.
    bulk_ingest.scan()
    status = bulk_ingest.status()
    assert status["by_bucket"]["kb-crispal"] == {"uploaded": 1}


# ---------------------------------------------------------------------------
# run_tick — the ignition guard, the per-tick ceiling, the backlog ceiling
# ---------------------------------------------------------------------------


def test_run_tick_with_nothing_pending_is_a_noop(_workspace_home, monkeypatch):
    calls = []
    monkeypatch.setattr(client, "get_ingest_status", lambda bucket=None: calls.append(bucket))
    result = _run(bulk_ingest.run_tick())
    assert result == {
        "blocked": False,
        "uploaded": 0,
        "deduplicated": 0,
        "failed": 0,
        "note": "nothing pending — run scan() first, or the pass is already complete",
    }
    assert calls == [], "must not even check ingest status with nothing to upload"


def test_run_tick_ignition_guard_blocks_when_extraction_is_claiming(_workspace_home, monkeypatch):
    """§13.5.2 — the non-negotiable: never upload while extraction is
    claiming work. MUTATION CHECK: dropping this check would let the next
    assertion's upload fake get called — it asserts zero calls."""
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content")
    bulk_ingest.scan()

    upload_calls = []

    async def fake_upload(*a, **k):
        upload_calls.append((a, k))
        return {"id": "doc-1", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status(claiming=True))
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick())

    assert result["blocked"] is True
    assert "claiming" in result["reason"]
    assert upload_calls == []


def test_run_tick_blocks_when_a_bucket_cannot_be_ensured(_workspace_home, monkeypatch):
    """§13.1 — the four buckets must exist before anything else touches
    them. Found live against production (2026-10-03): the buckets had never
    been created, and every subsequent call 404'd on 'bucket not found'
    until this guard existed."""
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content")
    bulk_ingest.scan()

    async def fake_create_bucket(name):
        return None, "HTTP 500: internal error"

    upload_calls = []

    async def fake_upload(*a, **k):
        upload_calls.append((a, k))
        return {"id": "doc-1", "deduplicated": False}, None

    monkeypatch.setattr(client, "create_bucket", fake_create_bucket)
    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick())

    assert result["blocked"] is True
    assert "could not ensure bucket" in result["reason"]
    assert upload_calls == []


def test_run_tick_blocks_when_status_check_itself_fails(_workspace_home, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content")
    bulk_ingest.scan()

    async def fake_status(bucket=None):
        return None, "could not reach aw-knowledgeable"

    monkeypatch.setattr(client, "get_ingest_status", fake_status)
    result = _run(bulk_ingest.run_tick())
    assert result["blocked"] is True


def test_run_tick_respects_the_per_bucket_backlog_ceiling(_workspace_home, monkeypatch):
    """§13.5.3 — a bucket already at/over the backlog ceiling is skipped for
    the tick rather than pushed further into backlog; buckets under the
    ceiling still proceed."""
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    _write(root, "memory/b.md", "content b")
    bulk_ingest.scan()

    async def fake_status(bucket=None):
        pending = bulk_ingest.MAX_PENDING_BACKLOG if bucket == "kb-crispal" else 0
        return {"extraction": {"claiming": False}, "processing": {"pending": pending}}, None

    uploaded = []

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        uploaded.append(bucket)
        return {"id": f"doc-{len(uploaded)}", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", fake_status)
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick())

    assert uploaded == ["kb-memory"]
    assert result["per_bucket"]["kb-crispal"]["skipped_reason"]
    assert result["per_bucket"]["kb-memory"]["uploaded"] == 1


def test_run_tick_respects_the_max_uploads_ceiling_across_buckets(_workspace_home, monkeypatch):
    root = bulk_ingest.kb_root()
    for i in range(3):
        _write(root, f"crispal/c{i}.md", f"content {i}")
    for i in range(3):
        _write(root, f"memory/m{i}.md", f"other content {i}")
    bulk_ingest.scan()

    uploaded = []

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        uploaded.append(source_path)
        return {"id": f"doc-{len(uploaded)}", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick(max_uploads=4))

    assert result["uploaded"] == 4
    assert len(uploaded) == 4
    # §13.3 order: kb-crispal's 3 pending go first, then 1 from kb-memory.
    assert uploaded[:3] == ["crispal/c0.md", "crispal/c1.md", "crispal/c2.md"]
    assert uploaded[3] == "memory/m0.md"


def test_run_tick_counts_a_server_dedup_hit_separately_from_a_fresh_upload(_workspace_home, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    _write(root, "crispal/b.md", "content b")
    bulk_ingest.scan()

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        dedup = source_path == "crispal/b.md"
        return {"id": "doc-shared" if dedup else "doc-a", "deduplicated": dedup}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick())

    assert result["uploaded"] == 1
    assert result["deduplicated"] == 1
    assert result["failed"] == 0


def test_run_tick_marks_an_upload_error_as_failed_and_keeps_going(_workspace_home, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    _write(root, "crispal/b.md", "content b")
    bulk_ingest.scan()

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        if source_path == "crispal/a.md":
            return None, "HTTP 500: internal error"
        return {"id": "doc-b", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)

    result = _run(bulk_ingest.run_tick())

    assert result["failed"] == 1
    assert result["uploaded"] == 1

    conn = bulk_ingest._connect()
    try:
        row = conn.execute("SELECT status, error FROM files WHERE relpath = ?", ("crispal/a.md",)).fetchone()
        assert row["status"] == bulk_ingest.STATUS_FAILED
        assert row["error"] == "HTTP 500: internal error"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# report — the §13.0 deliverable
# ---------------------------------------------------------------------------


def test_report_tallies_scanned_uploaded_deduplicated_and_pending(_workspace_home, monkeypatch):
    root = bulk_ingest.kb_root()
    _write(root, "crispal/a.md", "content a")
    _write(root, "crispal/b.md", "content b")
    _write(root, "mapped_folders/repos/x/c.md", "content a")  # alias of a.md's content
    bulk_ingest.scan()

    async def fake_upload(filename, raw, *, bucket, source_path=None, title=None):
        return {"id": "doc-a", "deduplicated": False}, None

    monkeypatch.setattr(client, "get_ingest_status", _ok_status())
    monkeypatch.setattr(client, "upload_bytes", fake_upload)
    _run(bulk_ingest.run_tick(max_uploads=1))

    report = bulk_ingest.report()
    crispal = report["buckets"]["kb-crispal"]
    assert crispal["scanned"] == 2
    assert crispal["uploaded_new"] == 1
    assert crispal["pending"] == 1
    mapped = report["buckets"]["kb-mapped-folders"]
    assert mapped["driver_deduplicated_alias"] == 1
    assert report["totals"]["scanned"] == 3
