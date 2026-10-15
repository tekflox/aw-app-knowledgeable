"""Agent-facing pending-deletion tools — list / approve / decline (card
`Connector: agent-facing pending-deletion tools`, 2026-10-15).

**Why this exists.** docs/design/aw-knowledgeable-fs-sync.md §7's two-phase
delete only ever lets a sync tick MARK a document; a human approving or
declining in the Library UI (`frontend-react/src/components/
PendingDeletionGrid.tsx`) was, until this module, the ONLY path that could
clear a mark. With the I1 migration queuing hundreds of deletions at once,
and `aw-knowledgeable` not even a container on this host (no exec path to
its credentials), an agent had no way to help — this module is that path,
reached through the MCP tools `list_pending_deletions`/
`approve_pending_deletions`/`decline_pending_deletions` wired up in
`mcp/http_handler.py`.

**Approve is scoped and counted; decline may be blanket.** Frederico's own
words carrying the design constraint: a single over-broad approve call has
no safety net behind it — the §7 valve guards against a RUNAWAY SCAN, not
an approved delete, because approval is precisely what the valve defers to.
So `approve_pending_deletions` REQUIRES a scope (`path_prefix` and/or
`node_ids`) and a REQUIRED `expected_count` this module checks against what
it is actually about to delete, rejecting the whole call on any mismatch —
nothing is deleted on a rejection. `decline_pending_deletions` restores
rather than destroys, so it may omit all scoping and act on every pending
document in the bucket; its `expected_count` is an optional courtesy check,
never a requirement.

**Audit is the second non-negotiable.** On 2026-10-15 the queue went from 12
rows to zero with no record of who cleared it — a wasted investigation
treating a human clearing his own queue as a possible incident. Every call
into `approve`/`decline` below — including one this module rejects on a
count mismatch — is written to this app's own `TABLE` via `ctx.db` before
returning, so that gap cannot repeat through this path.

This module takes `db` as an explicit parameter and calls out through
`mcp/client.py` for the actual aw-knowledgeable HTTP calls, same shape as
`fs_sync.py`/`bulk_ingest.py` — NOT a third copy of `client.py`'s own
"thin HTTP client, no local state" doctrine (see that module's docstring):
this one is deliberately stateful, which is exactly why it lives beside
`fs_sync.py`/`bulk_ingest.py` rather than inside `client.py` itself.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid

from fastapi.concurrency import run_in_threadpool

from .mcp import client

log = logging.getLogger("aw_apps.knowledgeable.pending_deletions")

#: Enforced `app__<slug>__` prefix (ADR Decision 8) — this app's own table,
#: via `ctx.db` (db:own-tables). Deliberately separate from the bulk-ingest/
#: fs-sync journals: this is an append-only log of ACTIONS taken on the
#: queue, not state about a document.
TABLE = "app__knowledgeable__pending_delete_audit"

_table_ensured = False
_table_lock = threading.Lock()


def _ensure_table(db) -> None:
    """Idempotent, same guarded-once-per-process shape as `bulk_ingest.py`'s
    own `_ensure_table` — see that function's docstring for why the lock+flag
    pair is safe under concurrent first callers."""
    global _table_ensured
    if _table_ensured:
        return
    with _table_lock:
        if _table_ensured:
            return
        db.create(
            TABLE,
            "id TEXT PRIMARY KEY, ts DOUBLE PRECISION NOT NULL, action TEXT NOT NULL, "
            "actor TEXT NOT NULL, bucket TEXT NOT NULL, path_prefix TEXT, node_ids TEXT, "
            "expected_count INTEGER, matched_count INTEGER NOT NULL, "
            "succeeded_count INTEGER NOT NULL, failed_count INTEGER NOT NULL, "
            "outcome TEXT NOT NULL, detail TEXT",
        )
        db.execute(TABLE, "CREATE INDEX IF NOT EXISTS ix_pending_delete_audit_ts ON {table} (ts)")
        _table_ensured = True


def _record_audit(
    db,
    *,
    action: str,
    actor: str,
    bucket: str,
    path_prefix: str | None,
    node_ids: list[str] | None,
    expected_count: int | None,
    matched_count: int,
    succeeded_count: int,
    failed_count: int,
    outcome: str,
    detail: str | None = None,
) -> None:
    """Sync — a blocking `db.execute` round trip, same as every other
    `ctx.db` write in this app. Callers running on the event loop must wrap
    this in `run_in_threadpool`, same rule `bulk_ingest.py`'s module
    docstring states for its own writes."""
    _ensure_table(db)
    db.execute(
        TABLE,
        "INSERT INTO {table} (id, ts, action, actor, bucket, path_prefix, node_ids, "
        "expected_count, matched_count, succeeded_count, failed_count, outcome, detail) "
        "VALUES (:id, :ts, :action, :actor, :bucket, :path_prefix, :node_ids, "
        ":expected_count, :matched_count, :succeeded_count, :failed_count, :outcome, :detail)",
        {
            "id": uuid.uuid4().hex,
            "ts": time.time(),
            "action": action,
            "actor": actor,
            "bucket": bucket,
            "path_prefix": path_prefix,
            "node_ids": json.dumps(node_ids) if node_ids else None,
            "expected_count": expected_count,
            "matched_count": matched_count,
            "succeeded_count": succeeded_count,
            "failed_count": failed_count,
            "outcome": outcome,
            "detail": detail,
        },
    )
    log.info(
        "pending-delete %s by %s: bucket=%s path_prefix=%r node_ids=%s expected=%s "
        "matched=%d succeeded=%d failed=%d outcome=%s",
        action, actor, bucket, path_prefix, node_ids, expected_count,
        matched_count, succeeded_count, failed_count, outcome,
    )


# ---------------------------------------------------------------------------
# Scope + prefix-breakdown — the same "immediate parent directory" grouping
# `PendingDeletionGrid.tsx`'s own `dirOf`/`prefixBreakdown` use, so an agent
# sees the queue shaped the same way the human approval UI does.
# ---------------------------------------------------------------------------


def _dir_of(path: str) -> str:
    idx = path.rfind("/")
    return "(root)" if idx == -1 else path[: idx + 1]


def _prefix_breakdown(docs: list[dict]) -> list[dict]:
    counts: dict[str, int] = {}
    for d in docs:
        dir_ = _dir_of(d.get("source_path") or "")
        counts[dir_] = counts.get(dir_, 0) + 1
    return sorted(
        ({"dir": dir_, "count": count} for dir_, count in counts.items()),
        key=lambda row: row["count"],
        reverse=True,
    )


def _matches(doc: dict, path_prefix: str | None, node_ids: set[str] | None) -> bool:
    if path_prefix and not (doc.get("source_path") or "").startswith(path_prefix):
        return False
    if node_ids is not None and doc.get("id") not in node_ids:
        return False
    return True


def _as_text(payload) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# MCP tool bodies — dispatched from `mcp/http_handler.py`.
# ---------------------------------------------------------------------------


async def list_pending_deletions(args: dict) -> tuple[str, bool]:
    bucket = (args.get("bucket") or "").strip() or None
    data, err = await client.get_documents_pending_delete(bucket=bucket)
    if err:
        return f"list_pending_deletions failed: {err}", True
    docs = data.get("documents") or []
    payload = {
        "documents": docs,
        "count": len(docs),
        "prefix_breakdown": _prefix_breakdown(docs),
    }
    return _as_text(payload), False


async def approve_pending_deletions(args: dict, db, actor: str) -> tuple[str, bool]:
    """The non-reversible half — see module docstring for why this is the
    one path in this file that must never run without an exact count
    match, and why every outcome (including a rejection) is audited."""
    bucket = (args.get("bucket") or "").strip()
    if not bucket:
        return (
            "bucket is required — approve/decline act through require_bucket_write, "
            "which 400s as ambiguous without one"
        ), True
    path_prefix = (args.get("path_prefix") or "").strip() or None
    raw_node_ids = args.get("node_ids")
    node_ids = {str(n) for n in raw_node_ids} if raw_node_ids else None
    if path_prefix is None and node_ids is None:
        return (
            "approve_pending_deletions requires path_prefix and/or node_ids — "
            "there is no blanket 'approve everything pending'"
        ), True
    expected_count = args.get("expected_count")
    if expected_count is None:
        return "expected_count is required for approve_pending_deletions", True
    try:
        expected_count = int(expected_count)
    except (TypeError, ValueError):
        return "expected_count must be an integer", True

    data, err = await client.get_documents_pending_delete(bucket=bucket)
    if err:
        return f"approve_pending_deletions failed to list pending deletions: {err}", True
    docs = data.get("documents") or []
    matched = [d for d in docs if _matches(d, path_prefix, node_ids)]
    sorted_node_ids = sorted(node_ids) if node_ids else None

    if len(matched) != expected_count:
        await run_in_threadpool(
            _record_audit, db, action="approve", actor=actor, bucket=bucket,
            path_prefix=path_prefix, node_ids=sorted_node_ids, expected_count=expected_count,
            matched_count=len(matched), succeeded_count=0, failed_count=0,
            outcome="rejected_count_mismatch",
            detail=json.dumps({"matched_ids": [d["id"] for d in matched]}),
        )
        return (
            f"expected_count mismatch: scope matched {len(matched)} document(s), not "
            f"{expected_count} — nothing was deleted. Re-check the scope (bucket={bucket!r}, "
            f"path_prefix={path_prefix!r}, node_ids={sorted_node_ids})."
        ), True

    results = await asyncio.gather(
        *(client.delete_document(d["id"], bucket=bucket) for d in matched),
        return_exceptions=True,
    )
    succeeded: list[str] = []
    failed: list[dict] = []
    for doc, result in zip(matched, results):
        if isinstance(result, Exception):
            failed.append({"id": doc["id"], "source_path": doc.get("source_path"), "error": str(result)})
            continue
        ok, call_err = result
        if ok:
            succeeded.append(doc["id"])
        else:
            failed.append({"id": doc["id"], "source_path": doc.get("source_path"), "error": call_err})

    outcome = "ok" if not failed else ("partial" if succeeded else "failed")
    await run_in_threadpool(
        _record_audit, db, action="approve", actor=actor, bucket=bucket,
        path_prefix=path_prefix, node_ids=sorted_node_ids, expected_count=expected_count,
        matched_count=len(matched), succeeded_count=len(succeeded), failed_count=len(failed),
        outcome=outcome, detail=json.dumps({"succeeded": succeeded, "failed": failed}),
    )
    payload = {"matched_count": len(matched), "approved": succeeded, "failed": failed, "outcome": outcome}
    return _as_text(payload), bool(failed)


async def decline_pending_deletions(args: dict, db, actor: str) -> tuple[str, bool]:
    """The reversible half — may omit all scoping (declines every pending
    document in `bucket`) and treats `expected_count` as an optional
    courtesy check rather than a requirement. See module docstring for why
    that asymmetry with approve is deliberate."""
    bucket = (args.get("bucket") or "").strip()
    if not bucket:
        return (
            "bucket is required — approve/decline act through require_bucket_write, "
            "which 400s as ambiguous without one"
        ), True
    path_prefix = (args.get("path_prefix") or "").strip() or None
    raw_node_ids = args.get("node_ids")
    node_ids = {str(n) for n in raw_node_ids} if raw_node_ids else None
    expected_count = args.get("expected_count")
    if expected_count is not None:
        try:
            expected_count = int(expected_count)
        except (TypeError, ValueError):
            return "expected_count must be an integer", True

    data, err = await client.get_documents_pending_delete(bucket=bucket)
    if err:
        return f"decline_pending_deletions failed to list pending deletions: {err}", True
    docs = data.get("documents") or []
    matched = [d for d in docs if _matches(d, path_prefix, node_ids)]
    sorted_node_ids = sorted(node_ids) if node_ids else None

    if expected_count is not None and len(matched) != expected_count:
        await run_in_threadpool(
            _record_audit, db, action="decline", actor=actor, bucket=bucket,
            path_prefix=path_prefix, node_ids=sorted_node_ids, expected_count=expected_count,
            matched_count=len(matched), succeeded_count=0, failed_count=0,
            outcome="rejected_count_mismatch",
            detail=json.dumps({"matched_ids": [d["id"] for d in matched]}),
        )
        return (
            f"expected_count mismatch: scope matched {len(matched)} document(s), not "
            f"{expected_count} — nothing was restored."
        ), True

    results = await asyncio.gather(
        *(client.restore_document(d["id"], bucket=bucket) for d in matched),
        return_exceptions=True,
    )
    succeeded: list[str] = []
    failed: list[dict] = []
    for doc, result in zip(matched, results):
        if isinstance(result, Exception):
            failed.append({"id": doc["id"], "source_path": doc.get("source_path"), "error": str(result)})
            continue
        _, call_err = result
        if call_err:
            failed.append({"id": doc["id"], "source_path": doc.get("source_path"), "error": call_err})
        else:
            succeeded.append(doc["id"])

    outcome = "ok" if not failed else ("partial" if succeeded else "failed")
    await run_in_threadpool(
        _record_audit, db, action="decline", actor=actor, bucket=bucket,
        path_prefix=path_prefix, node_ids=sorted_node_ids, expected_count=expected_count,
        matched_count=len(matched), succeeded_count=len(succeeded), failed_count=len(failed),
        outcome=outcome, detail=json.dumps({"succeeded": succeeded, "failed": failed}),
    )
    payload = {"matched_count": len(matched), "declined": succeeded, "failed": failed, "outcome": outcome}
    return _as_text(payload), bool(failed)
