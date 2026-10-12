"""docs/design/aw-knowledgeable-fs-sync.md §3/§4/§6/§7 — the fs-sync
upsync engine (card B of the fs-sync build). Card A's server seams
(``PUT /api/documents/by-path``, ``POST .../mark-delete``) are deployed
and live; this module is the connector half that makes `scan()`'s
create-only freeze (`bulk_ingest.py`'s own ``frozen`` set) stop mattering:
an edit now finds its existing path-keyed document and bumps ``rev``, and
a file missing for ≥2 consecutive scans gets marked pending-delete
(never hard-deleted — approval is card D's job).

**Reuses, does not rewrite, `bulk_ingest.py`** (its own module docstring's
promise): the KB-tree walk (`_discovered_subtrees`/`_iter_markdown_files`/
`is_skipped`/`MAX_FILE_BYTES`/`kb_root`), the shared journal table and its
`_ensure_table` (§3.2's columns live there, see that module), and
`client.py`'s already-configured base_url/secret. `bulk_ingest.scan()`/
`run_tick()` themselves are untouched — they still serve the existing
content-hash-identity, create-only flow on the same table; this engine is
a second, independent regime on the same journal rows (§1's "added NEXT
TO the existing MERGE, not replacing it").

**Card B scope only: upsync.** The pull/downsync half (§3.3 step 2,
``GET /api/documents/changes``, the echo-loop breaker, last-write-wins) is
card C and is not built here — there is no cursor state, no
``authored/``-only write-back in this module.

**The §7 mass-delete valve is the load-bearing piece of this file.**
`aw-workspace-cli agent sync` does exact-mirror deletions (AGENTS.md), so
a transiently empty or half-populated `knowledge_base/` is a real
scenario, not a hypothetical — see :func:`_check_mass_delete_valve`,
called before a single row's `missing_scans` is touched on any tick.
"""
from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path

from fastapi.concurrency import run_in_threadpool

from . import bulk_ingest
from .mcp import client

log = logging.getLogger("aw_apps.knowledgeable.fs_sync")

#: Same shared journal `bulk_ingest.py` owns — §3.2's extra columns live
#: on this table, added by that module's `_ensure_table`.
TABLE = bulk_ingest.TABLE

#: `ctx.state.lease` name for this engine's own tick — deliberately
#: distinct from `bulk_ingest.LEASE_NAME` ("bulk-ingest-tick"): the two
#: engines model different document-identity regimes (content-hash vs.
#: path-keyed) on the same table and are safe to run as two independent
#: ticks, never required to serialize against each other.
LEASE_NAME = "fs-sync-tick"

ORIGIN_SCAN = "scan"

#: §3.3 step 1 — never mark a path missing on the scan that first notices
#: it gone; only on the SECOND consecutive miss. A half-written
#: `agent sync` pass or a transiently unmounted tree must not read as
#: intent on a single scan.
MISSING_SCANS_THRESHOLD = 2

#: §3.3 step 4 — the debounce window before the upload pass acts on a
#: freshly observed hash change (new path or edit). notion-sync writes
#: 1,564 files in one burst; this collapses that into one tick's worth of
#: STABLE files rather than 1,564 half-written reads.
DEBOUNCE_SECONDS = 30

#: §7 valve guard 2 — the floor of `max(floor, 10% of uploaded rows)`.
MASS_DELETE_FLOOR = 25
MASS_DELETE_FRACTION = 0.10

MAX_UPLOADS_PER_TICK = bulk_ingest.MAX_UPLOADS_PER_TICK


def mass_delete_threshold(uploaded_rows: int) -> int:
    """§7 guard 2's declared formula — a free function so the valve test
    and an operator reading `report()` can both state the exact number
    without re-deriving it."""
    return max(MASS_DELETE_FLOOR, int(uploaded_rows * MASS_DELETE_FRACTION))


def _walk_sync_tree(root: Path) -> dict[str, str] | None:
    """Every live, non-skipped markdown file under `root`, as
    `relpath -> sha256`. Returns ``None`` when `root` itself is missing or
    not a directory — distinct from an empty dict (`root` exists but is
    legitimately empty) because §7 guard 1 treats "could not even walk
    it" and "walked it and saw nothing" the same way (abort), but a
    caller wanting to log *why* needs to tell them apart.

    No cross-path dedup/canonicalization here (unlike `bulk_ingest.scan`):
    under the path-keyed identity, two files with identical bytes at two
    different paths are just two independent synced documents — §8's
    accepted duplication, not this engine's problem to resolve."""
    if not root.is_dir():
        return None
    found: dict[str, str] = {}
    for subtree in bulk_ingest._discovered_subtrees(root):
        subtree_root = root / subtree
        if not subtree_root.is_dir():
            continue
        for path in bulk_ingest._iter_markdown_files(subtree_root):
            if not path.is_file():
                continue
            relpath = str(path.relative_to(root))
            if bulk_ingest.is_skipped(relpath):
                continue
            try:
                size = path.stat().st_size
            except OSError:
                continue
            if size > bulk_ingest.MAX_FILE_BYTES:
                continue
            try:
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError:
                continue
            found[relpath] = digest
    return found


def _uploaded_row_count(db) -> int:
    rows = db.execute(
        TABLE, "SELECT COUNT(*) FROM {table} WHERE status = :status",
        {"status": bulk_ingest.STATUS_UPLOADED},
    )
    return rows[0][0] if rows else 0


def _check_mass_delete_valve(db, found: dict[str, str] | None) -> dict | None:
    """§7 — the two stacked guards, checked BEFORE a single row's
    `missing_scans` advances or a single `mark-delete` call goes out.
    Returns ``None`` when clear, or a dict with a declared `reason` (and,
    for guard 2, the counts that tripped it) when the tick must abort
    the WHOLE pass and mark nothing.

    Guard 1 (zero-trust scan precondition): `found is None` (root missing
    or unreadable) or `found == {}` (walked it, saw zero files) — an
    unmounted volume or a half-recreated tree must mark nothing.

    Guard 2 (mass-delete threshold): more files would have their
    `missing_scans` advanced this tick than
    `max(25, 10% of uploaded rows)` — a transient near-total wipe (e.g. an
    `agent sync` pass caught mid-write) must not cascade into hundreds of
    delete marks."""
    if found is None:
        return {"reason": "kb_root() is missing or unreadable — aborting the tick, marking nothing"}
    if not found:
        return {"reason": "scan saw zero files — aborting the tick, marking nothing"}

    uploaded_rows = _uploaded_row_count(db)
    threshold = mass_delete_threshold(uploaded_rows)
    rows = db.execute(
        TABLE, "SELECT relpath FROM {table} WHERE status = :status",
        {"status": bulk_ingest.STATUS_UPLOADED},
    )
    tracked_paths = {r[0] for r in rows}
    missing_now = tracked_paths - set(found.keys())
    if len(missing_now) > threshold:
        return {
            "reason": (
                f"{len(missing_now)} files missing from the tree this tick exceeds the "
                f"mass-delete threshold max({MASS_DELETE_FLOOR}, 10% of {uploaded_rows}) = {threshold}"
            ),
            "missing_count": len(missing_now),
            "threshold": threshold,
        }
    return None


def _empty_result(blocked: bool, reason: str | None = None, **extra) -> dict:
    result = {
        "blocked": blocked,
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "deleted_marked": 0,
        "failed": 0,
    }
    if reason is not None:
        result["reason"] = reason
    result.update(extra)
    return result


async def run_tick(
    db, lease, *, max_uploads: int = MAX_UPLOADS_PER_TICK, workspace_slug: str = "",
) -> dict:
    """One bounded tick. §6: refuses to run at all when `workspace_slug`
    is empty — a quarter of the path-keyed document identity, so a
    defaulted slug would silently interleave two workspaces' documents
    under one identity, unlike core's own "legibility aid" fallback for
    this same env var.

    Refuses to overlap with another in-flight fs-sync tick via
    `ctx.state.lease` (`lease`, non-blocking, flock-backed — see
    `bulk_ingest.run_tick`'s own docstring for the same mechanism)."""
    if not workspace_slug:
        return _empty_result(
            True, "AW_WORKSPACE is unset — refusing to run with a defaulted workspace_slug (§6)",
        )
    if not lease.claim(LEASE_NAME):
        return _empty_result(
            True, "another fs-sync tick is already in progress — wait for it to finish",
        )
    try:
        return await _run_tick_locked(db, max_uploads, workspace_slug)
    finally:
        lease.release(LEASE_NAME)


async def _run_tick_locked(db, max_uploads: int, workspace_slug: str) -> dict:
    await run_in_threadpool(bulk_ingest._ensure_table, db)
    root = bulk_ingest.kb_root()
    found = await run_in_threadpool(_walk_sync_tree, root)

    valve = await run_in_threadpool(_check_mass_delete_valve, db, found)
    if valve is not None:
        log.warning("fs_sync: mass-delete valve tripped: %s", valve["reason"])
        return _empty_result(True, valve["reason"], valve_tripped=True, **{
            k: v for k, v in valve.items() if k != "reason"
        })

    # §13.5.2/§13.5.3's ignition guard and backlog ceiling, kept exactly as
    # `bulk_ingest.run_tick` already enforces them — one bucket, one
    # status call. The ignition guard blocks the whole tick (entity
    # extraction claiming is a reason to touch nothing, not just uploads);
    # the backlog ceiling only caps the upload budget — missing-scan
    # bookkeeping and delete-marking below are unrelated to upload/
    # extraction load and still run.
    status_payload, status_err = await client.get_ingest_status(bulk_ingest.BUCKET)
    if status_err:
        return _empty_result(True, f"could not reach GET /api/ingest/status: {status_err}")
    if (status_payload or {}).get("extraction", {}).get("claiming"):
        return _empty_result(
            True, "extraction.claiming=true — refusing to upload (§13.5.2 ignition guard)",
        )
    backlog = ((status_payload or {}).get("processing") or {}).get(bulk_ingest.STATUS_PENDING, 0)
    upload_budget = (
        0 if backlog >= bulk_ingest.MAX_PENDING_BACKLOG
        else max(0, min(max_uploads, MAX_UPLOADS_PER_TICK))
    )

    rows = await run_in_threadpool(
        db.execute, TABLE,
        "SELECT relpath, sha256, last_synced_hash, scanned_at, missing_scans, "
        "pending_delete, external_id FROM {table}",
    )
    journal = {
        r[0]: {
            "sha256": r[1], "last_synced_hash": r[2], "scanned_at": r[3],
            "missing_scans": r[4] or 0, "pending_delete": r[5] or 0, "external_id": r[6],
        }
        for r in rows
    }

    created = updated = unchanged = failed = 0
    budget = upload_budget

    for relpath in sorted(found):
        digest = found[relpath]
        row = journal.get(relpath)
        now = time.time()

        if row is None:
            # Brand-new path: register it and start the debounce window —
            # §3.3 step 4 applies to creates too, not only edits.
            await run_in_threadpool(
                db.execute, TABLE,
                "INSERT INTO {table} "
                "(relpath, sha256, bucket, status, scanned_at, updated_at, missing_scans, pending_delete) "
                "VALUES (:relpath, :sha256, :bucket, :status, :now, :now, 0, 0) "
                "ON CONFLICT (relpath) DO UPDATE SET sha256 = excluded.sha256, "
                "scanned_at = excluded.scanned_at, missing_scans = 0",
                {
                    "relpath": relpath, "sha256": digest, "bucket": bulk_ingest.BUCKET,
                    "status": bulk_ingest.STATUS_PENDING, "now": now,
                },
            )
            continue

        if row["missing_scans"]:
            # The path reappeared — the ≥2-consecutive-scans counter must
            # reset; a reappearing file is not a continuing absence.
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET missing_scans = 0 WHERE relpath = :relpath",
                {"relpath": relpath},
            )

        last_synced = row["last_synced_hash"]
        if digest == last_synced:
            unchanged += 1
            if row["sha256"] != digest:
                await run_in_threadpool(
                    db.execute, TABLE,
                    "UPDATE {table} SET sha256 = :sha256, scanned_at = :now WHERE relpath = :relpath",
                    {"sha256": digest, "now": now, "relpath": relpath},
                )
            continue

        if row["sha256"] != digest:
            # A change just appeared this tick — restart the quiet window
            # rather than upload immediately.
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET sha256 = :sha256, scanned_at = :now WHERE relpath = :relpath",
                {"sha256": digest, "now": now, "relpath": relpath},
            )
            continue

        if now - (row["scanned_at"] or 0) < DEBOUNCE_SECONDS:
            continue  # same hash observed, but not quiet for long enough yet

        if budget <= 0:
            continue

        full_path = root / relpath
        try:
            raw = full_path.read_bytes()
        except OSError as exc:
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET status = :status, error = :error, updated_at = :now "
                "WHERE relpath = :relpath",
                {"status": bulk_ingest.STATUS_FAILED, "error": str(exc), "now": now, "relpath": relpath},
            )
            failed += 1
            continue

        data, err = await client.put_document_by_path(
            Path(relpath).name, raw,
            source_path=relpath, workspace_slug=workspace_slug, base_hash=last_synced,
        )
        now = time.time()
        if err:
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET status = :status, error = :error, updated_at = :now "
                "WHERE relpath = :relpath",
                {"status": bulk_ingest.STATUS_FAILED, "error": err, "now": now, "relpath": relpath},
            )
            failed += 1
            continue

        outcome = (data or {}).get("outcome")
        new_hash = (data or {}).get("content_hash") or digest
        await run_in_threadpool(
            db.execute, TABLE,
            "UPDATE {table} SET status = :status, external_id = :external_id, "
            "last_synced_hash = :hash, origin = :origin, error = NULL, updated_at = :now "
            "WHERE relpath = :relpath",
            {
                "status": bulk_ingest.STATUS_UPLOADED,
                "external_id": (data or {}).get("id") or row["external_id"],
                "hash": new_hash, "origin": ORIGIN_SCAN, "now": now, "relpath": relpath,
            },
        )
        budget -= 1
        if outcome == "created":
            created += 1
        else:
            updated += 1

    deleted_marked = await _mark_missing_as_deleted(db, journal, found)

    result = {
        "blocked": False,
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "deleted_marked": deleted_marked,
        "failed": failed,
        "bucket": bulk_ingest.BUCKET,
    }
    if upload_budget == 0 and backlog >= bulk_ingest.MAX_PENDING_BACKLOG:
        result["skipped_reason"] = f"processing backlog {backlog} >= {bulk_ingest.MAX_PENDING_BACKLOG}"
    return result


async def _mark_missing_as_deleted(db, journal: dict, found: dict[str, str]) -> int:
    """§3.3 step 1 / §7 — advance `missing_scans` for every tracked path no
    longer on disk; at the SECOND consecutive miss, `mark-delete` it.
    Only rows with a real `external_id` (i.e. already synced via this
    engine) are tracked here — a row still waiting out its debounce
    window, or one `bulk_ingest`'s own alias/skip pass owns, was never
    this engine's document to begin with."""
    marked = 0
    for relpath, row in journal.items():
        if relpath in found or not row["external_id"]:
            continue
        new_missing = row["missing_scans"] + 1
        if new_missing >= MISSING_SCANS_THRESHOLD and not row["pending_delete"]:
            _, err = await client.mark_delete(
                row["external_id"], reason="missing from source tree for 2 consecutive scans",
            )
            if err:
                log.warning("fs_sync: mark-delete failed for %s: %s", relpath, err)
                await run_in_threadpool(
                    db.execute, TABLE,
                    "UPDATE {table} SET missing_scans = :missing WHERE relpath = :relpath",
                    {"missing": new_missing, "relpath": relpath},
                )
                continue
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET missing_scans = :missing, pending_delete = 1 WHERE relpath = :relpath",
                {"missing": new_missing, "relpath": relpath},
            )
            marked += 1
        else:
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET missing_scans = :missing WHERE relpath = :relpath",
                {"missing": new_missing, "relpath": relpath},
            )
    return marked


def status(db) -> dict:
    """A cheap journal-only snapshot for the CLI `status` subcommand —
    same no-network-call contract as `bulk_ingest.status`."""
    bulk_ingest._ensure_table(db)
    rows = db.execute(
        TABLE, "SELECT relpath, last_synced_hash, missing_scans, pending_delete FROM {table}",
    )
    synced = sum(1 for r in rows if r[1] is not None)
    missing_tracked = sum(1 for r in rows if (r[2] or 0) > 0)
    pending_delete = sum(1 for r in rows if r[3])
    return {
        "bucket": bulk_ingest.BUCKET,
        "journal_rows": len(rows),
        "synced": synced,
        "missing_tracked": missing_tracked,
        "pending_delete": pending_delete,
        "table": TABLE,
    }


def report(db) -> dict:
    """The §3/§7 deliverable report — per-subtree synced/missing/pending-
    delete counts plus totals, mirroring `bulk_ingest.report`'s shape."""
    bulk_ingest._ensure_table(db)
    subtrees: dict[str, dict] = {}
    for subtree in bulk_ingest._discovered_subtrees(bulk_ingest.kb_root()):
        rows = db.execute(
            TABLE,
            "SELECT "
            "COUNT(*) AS total, "
            "SUM(CASE WHEN last_synced_hash IS NOT NULL THEN 1 ELSE 0 END) AS synced, "
            "SUM(CASE WHEN missing_scans > 0 THEN 1 ELSE 0 END) AS missing_tracked, "
            "SUM(CASE WHEN pending_delete = 1 THEN 1 ELSE 0 END) AS pending_delete "
            "FROM {table} WHERE relpath LIKE :prefix",
            {"prefix": f"{subtree}/%"},
        )
        row = rows[0]
        subtrees[subtree] = {
            "total": row[0] or 0,
            "synced": row[1] or 0,
            "missing_tracked": row[2] or 0,
            "pending_delete": row[3] or 0,
        }
    totals: dict[str, int] = {}
    for b in subtrees.values():
        for key, value in b.items():
            totals[key] = totals.get(key, 0) + value
    return {"bucket": bulk_ingest.BUCKET, "subtrees": subtrees, "totals": totals, "table": TABLE}
