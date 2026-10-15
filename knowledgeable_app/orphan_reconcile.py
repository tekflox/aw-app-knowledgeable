"""Reconciler for the journal-orphaned duplicate documents the two-identity
gap left behind (card `3fa5bf3b-9510-812c-a439-d7fcc773f2bd`, 2026-10-15).

**What an orphan is here.** `bulk_ingest.py` used to upload through the
content-hash route (`create_document`, MERGE key `(tenant, bucket,
content_hash)`), which never stamps `workspace_slug`; `fs_sync.py` uploads
through the path-keyed route (MERGE key `(tenant, bucket, workspace_slug,
source_path)`), which therefore could never match that node and minted a
second document for the same file. The shared journal holds ONE
`external_id` per path and both uploaders overwrote it on success, so the
first writer's document lost its only reference. Delete detection reads
identity exclusively from that column, so such a document can never be
marked, never approved, never reaped — it keeps answering searches and
keeps consuming an embedding slot. That gap is closed at the source in
`bulk_ingest.py` (both uploaders now share one identity); this module is
for the 544 documents already stranded when it was.

**It marks, it never deletes.** Every candidate goes through `POST
/api/documents/{id}/mark-delete` — `graph.mark_document_pending_delete`,
the same two-phase gate a sync tick's own delete marking uses, NOT the
`force: true` service branch and never `DELETE /api/documents/{id}`. The
marks land in the existing approval queue, where `list_pending_deletions` /
`approve_pending_deletions` / `decline_pending_deletions` (and the
Library's own "Pending deletion" view) are the only things that can remove
a node. A wrong call here is therefore recoverable by declining.

**The §7 valve's intent, re-imposed by hand.** This reconciler runs outside
`fs_sync._run_tick_locked`, so `_check_mass_delete_valve` never sees it and
cannot bound it. Running 544 marks through in one pass with no human
checkpoint is precisely the shape that valve exists to prevent, so this
module carries its own two-level cap:

* a hard ceiling — `fs_sync.mass_delete_threshold(tracked rows)`, the
  valve's OWN formula (`max(25, 10% of tracked journal rows)`; 207 against
  the 2,079 tracked rows measured 2026-10-15), re-used rather than
  re-derived so the two can never drift apart. A caller cannot ask for
  more, whatever it passes.
* a default batch of :data:`DEFAULT_MARK_LIMIT` (100) — half the ceiling,
  so the routine case stays well inside the number of documents one
  fs-sync tick would have been allowed to mark, and the backlog drains in
  bounded passes with the approval queue (and a human) between them rather
  than in one irreversible-looking sweep.

**Dry run is the default.** `apply=False` reports exactly what it would
mark and touches nothing — the only safe way to re-measure, since the
counts drift continuously (the embedding queue drains, `notion/` churns).
Every applying call is written to `pending_deletions.TABLE`'s audit log,
same non-negotiable that module's docstring states.

**Sequential, not gathered.** Production aw-knowledgeable must stay
single-worker (memory `aw-knowledgeable-production-must-stay-single-worker`),
so a batch is marked one call at a time rather than through
`asyncio.gather` — this is a cleanup job with no latency requirement, and
100 concurrent writes against a single-worker service is a self-inflicted
outage.

**What this deliberately does NOT cover.** A graph document whose
`source_path` has no journal row at all, or whose journal row carries no
`external_id`, is not touched: it has no live claimant to prove it is a
stale duplicate rather than the only copy of something. Those are §17's
journal-blind documents, whose answer is the adoption pass, not a
reconciler. They are counted and reported, never marked.
"""
from __future__ import annotations

import json
import logging

from fastapi.concurrency import run_in_threadpool

from . import bulk_ingest, fs_sync, pending_deletions
from .mcp import client

log = logging.getLogger("aw_apps.knowledgeable.orphan_reconcile")

#: Default documents marked per applying run — see the module docstring for
#: why this is deliberately half the valve's own ceiling.
DEFAULT_MARK_LIMIT = 100

#: Written to each mark as `pending_delete_reason`, so the approval queue
#: says what put the row there and the card it came from is one grep away.
MARK_REASON = (
    "orphaned duplicate: no journal row references this document, and its source_path "
    "is claimed by a different, live document (card 3fa5bf3b)"
)

#: `pending_deletions`' audit `action` for this module's batches — distinct
#: from `approve`/`decline` so an operator reading the log can tell a mark
#: from a deletion.
AUDIT_ACTION = "mark_orphans"


def _journal_claims(db) -> tuple[dict[str, str], set[str]]:
    """``({relpath: external_id}, {every non-null external_id})`` — the two
    projections the classification below needs, read in one pass. Rows with
    a NULL `external_id` (never uploaded, or adopted and not yet converged)
    are absent from both, which is what makes them "no live claimant"."""
    bulk_ingest._ensure_table(db)
    rows = db.execute(
        bulk_ingest.TABLE,
        "SELECT relpath, external_id FROM {table} WHERE external_id IS NOT NULL",
    )
    claims = {row[0]: row[1] for row in rows}
    return claims, set(claims.values())


def _prefix_counts(docs: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for doc in docs:
        prefix = (doc.get("source_path") or "").split("/", 1)[0] or "(none)"
        counts[prefix] = counts.get(prefix, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True))


def classify(documents: list[dict], claims: dict[str, str], claimed_ids: set[str]) -> dict:
    """Split a bucket's documents into the four classes this reconciler
    distinguishes. A pure function of its inputs — no `db`, no HTTP — so the
    join rule itself is testable without either.

    * `eligible` — orphaned (no journal row names it) AND its `source_path`
      is claimed by a DIFFERENT, live document id. A stale duplicate.
    * `already_marked` — eligible-shaped but already `pending_delete`;
      re-marking is a no-op, so they are reported, not re-sent.
    * `no_live_claimant` — orphaned, but nothing else claims its path (no
      journal row, or a row with no `external_id`). §17's journal-blind
      class; out of scope, see the module docstring.
    * everything else is referenced by the journal and is left alone.
    """
    eligible: list[dict] = []
    already_marked: list[dict] = []
    no_live_claimant: list[dict] = []
    for doc in documents:
        doc_id = doc.get("id")
        if not doc_id or doc_id in claimed_ids:
            continue
        source_path = doc.get("source_path") or ""
        claimant = claims.get(source_path)
        if not claimant or claimant == doc_id:
            no_live_claimant.append(doc)
            continue
        if doc.get("sync_status") == "pending_delete":
            already_marked.append(doc)
            continue
        eligible.append(doc)
    # Sorted by path so repeated passes drain the backlog in a stable,
    # reviewable order rather than in whatever order the graph answered.
    eligible.sort(key=lambda d: ((d.get("source_path") or ""), d.get("id") or ""))
    return {
        "eligible": eligible,
        "already_marked": already_marked,
        "no_live_claimant": no_live_claimant,
    }


async def reconcile(
    db,
    *,
    bucket: str = bulk_ingest.BUCKET,
    limit: int = DEFAULT_MARK_LIMIT,
    apply: bool = False,
    actor: str = "unknown",
) -> dict:
    """One bounded reconciliation pass. `apply=False` (the default) is a
    dry run: it measures and reports, and marks nothing.

    Returns the full report rather than just counts — the per-prefix
    breakdown and the sampled paths are what makes a human able to say "yes,
    those are duplicates" before approving, and the `remaining` count is
    what tells the next pass whether there is more to do.
    """
    claims, claimed_ids = await run_in_threadpool(_journal_claims, db)
    data, err = await client.get_documents(bucket=bucket)
    if err:
        return {"ok": False, "error": f"could not list documents: {err}", "bucket": bucket}
    documents = (data or {}).get("documents") or []

    classes = classify(documents, claims, claimed_ids)
    eligible = classes["eligible"]

    tracked_rows = await run_in_threadpool(fs_sync._tracked_row_count, db)
    ceiling = fs_sync.mass_delete_threshold(tracked_rows)
    requested = max(0, int(limit))
    batch_size = min(requested, ceiling, len(eligible))
    batch = eligible[:batch_size]

    report = {
        "ok": True,
        "bucket": bucket,
        "applied": False,
        "graph_documents": len(documents),
        "journal_claims": len(claims),
        "orphans": len(eligible) + len(classes["already_marked"]) + len(classes["no_live_claimant"]),
        "eligible": len(eligible),
        "already_pending_delete": len(classes["already_marked"]),
        "no_live_claimant": len(classes["no_live_claimant"]),
        "eligible_by_prefix": _prefix_counts(eligible),
        # The cap, stated rather than implied — §7's own formula, applied to
        # a job the valve itself cannot see.
        "cap": {
            "requested": requested,
            "batch": batch_size,
            "valve_ceiling": ceiling,
            "tracked_rows": tracked_rows,
            "formula": f"max({fs_sync.MASS_DELETE_FLOOR}, 10% of {tracked_rows})",
        },
        "sample": [
            {"id": d.get("id"), "source_path": d.get("source_path"),
             "live_claimant": claims.get(d.get("source_path") or "")}
            for d in batch[:10]
        ],
    }

    if not apply:
        report["note"] = (
            f"dry run — nothing marked. Re-run with apply=true to mark {batch_size} "
            f"document(s); {len(eligible) - batch_size} would remain for the next pass."
        )
        report["remaining"] = len(eligible)
        return report

    marked: list[str] = []
    failed: list[dict] = []
    for doc in batch:
        _data, call_err = await client.mark_delete(
            doc["id"], bucket=bucket, reason=MARK_REASON
        )
        if call_err:
            failed.append({
                "id": doc["id"], "source_path": doc.get("source_path"), "error": call_err,
            })
            continue
        marked.append(doc["id"])

    outcome = "ok" if not failed else ("partial" if marked else "failed")
    await run_in_threadpool(
        pending_deletions._record_audit, db,
        action=AUDIT_ACTION, actor=actor, bucket=bucket, path_prefix=None,
        node_ids=marked, expected_count=batch_size, matched_count=len(eligible),
        succeeded_count=len(marked), failed_count=len(failed), outcome=outcome,
        detail=json.dumps({"failed": failed}),
    )
    report.update({
        "applied": True,
        "marked": len(marked),
        "marked_ids": marked,
        "failed": failed,
        "outcome": outcome,
        "remaining": len(eligible) - len(marked),
        "note": (
            f"{len(marked)} document(s) marked pending-delete and are now in the approval "
            "queue — nothing was deleted. Approve with approve_pending_deletions "
            "(node_ids + expected_count) or decline to undo."
        ),
    })
    log.info(
        "orphan_reconcile: bucket=%s eligible=%d marked=%d failed=%d remaining=%d (ceiling %d)",
        bucket, len(eligible), len(marked), len(failed), report["remaining"], ceiling,
    )
    return report
