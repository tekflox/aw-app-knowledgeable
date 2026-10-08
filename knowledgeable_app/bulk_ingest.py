"""The §13 batch-and-journal bulk-ingest driver (docs/design/
aw-knowledgeable-v2-retrieval.md §13, card `3ee5bf3b-9510-81ad-ab05-
e219bdd3a337`) — one engine, run in-process so it can reuse this app's
already-configured ``mcp/client.py`` (base_url + X-Internal-Secret), fed
through two doors that both just POST to this app's own
``/api/apps/knowledgeable/bulk-ingest/*`` routes (``routes.py``): the
``knowledgeable-ingest`` CLI command, and the ``contributes.tasks`` scheduled
task that runs the identical CLI command as a shell job.

**Four buckets, one per top-level KB subtree** (§13.1) — bucket is a
permission boundary (§7bis), and the four subtrees are four genuinely
different permission classes. Ingested in THIS order (§13.3): the curated
subtrees first (crispal, memory, notion — 1,572 files, validatable the same
day), `mapped_folders/` (the generated code-maps, 86% of the corpus and the
most duplicated) last.

**Dedup lives in two layers, with different jobs (§13.2).** The server's own
`content_hash` MERGE gate (`POST /api/documents`) is the idempotency safety
net — re-running this driver, or any other caller, never duplicates. THIS
module's job is the other layer: cross-prefix canonicalization. It is the
only thing with a full-corpus view, so it hashes every file first, groups by
hash, and uploads exactly one canonical copy per group — the rest are
recorded as journal aliases, never POSTed at all.

**No work without a ceiling (§13.5.3, the 228-container/37.2GB incident).**
`run_tick()` — the one entrypoint both doors call — enforces, in order: the
ignition guard (refuses to upload anything if entity extraction is
claiming), a hard per-tick upload ceiling, and a per-bucket upload-backlog
ceiling. It never touches `EXTRACTION_ENABLED`, never calls
`/api/ingest/key`, and contributes no config that could flip either — the
restriction the card requires be structural, not documental.

Journal: sqlite at ``<AW_WORKSPACE_HOME>/data/knowledgeable/
bulk_ingest.sqlite`` (§13.4) — durable across reinstalls per this workspace's
own storage convention (``src/apps/paths.py``). One row per KB-relative file
path; ``status`` is one of ``pending``/``uploaded``/``alias``/``skipped``/
``failed``, matching the design's own vocabulary so the report reads
directly off it.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sqlite3
import time
from pathlib import Path

from .mcp import client

log = logging.getLogger("aw_apps.knowledgeable.bulk_ingest")

# §13.3 — ingest order: curated subtrees first, mapped_folders/ last.
#
# kb-cli-reference/kb-skills (card quality:procedural-genre-absent-from-
# both-knowledge-indexes) are the procedural genre neither store had: one
# doc per `aw-workspace-cli` command/subcommand captured from its own
# --help (src/libs/cli_reference.py in aw-workspace core), and the
# materialized skills/*/SKILL.md tree indexed as content for the first
# time (previously only reachable via the separate search_skills surface).
# Small, curated, hand-structured sources — same tier as crispal/memory/
# notion, ingested before mapped_folders/ for the same reason those are.
BUCKET_ORDER: tuple[tuple[str, str], ...] = (
    ("kb-crispal", "crispal"),
    ("kb-memory", "memory"),
    ("kb-notion", "notion"),
    ("kb-cli-reference", "cli_reference"),
    ("kb-skills", "skills"),
    ("kb-mapped-folders", "mapped_folders"),
)
BUCKETS: tuple[str, ...] = tuple(bucket for bucket, _subtree in BUCKET_ORDER)

STATUS_PENDING = "pending"
STATUS_UPLOADED = "uploaded"
STATUS_ALIAS = "alias"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"

#: Mirrors aw-knowledgeable's own cap (`backend/app/api/documents.py`'s
#: `max_document_size_bytes`) — §13.9 risk 4: skipped with a journal reason,
#: never a driver crash.
MAX_FILE_BYTES = 10 * 1024 * 1024
#: §13.5.3 — the per-tick upload ceiling. `run_tick(max_uploads=...)` can ask
#: for less; it can never get more.
MAX_UPLOADS_PER_TICK = 200
#: §13.5.3 — the backpressure ceiling: a bucket whose own `processing_status:
#: pending` backlog is already at or past this is skipped for the tick,
#: never pushed further into backlog.
MAX_PENDING_BACKLOG = 500

#: Found live 2026-10-08: a slow tick (e.g. `max_uploads=200`, which can run
#: long enough for the CLIENT to give up with an `httpx.ReadTimeout`) keeps
#: running server-side — nothing cancels the coroutine just because the
#: caller disconnected — holding its sqlite write transaction open across
#: every upload's network round trip. A second tick starting while the first
#: is still mid-flight then hits `sqlite3.OperationalError: database is
#: locked` on its own first `UPDATE`, surfaced to the caller as a bare 500.
#: Serializing here turns that crash into the same declared `blocked` shape
#: every other non-negotiable in this function already uses.
_tick_lock = asyncio.Lock()


def _workspace_home() -> str:
    """Same resolution every other Tier-1 app in this estate uses (e.g.
    ``diff_app.storage.default_data_dir``) — deliberately NOT an import of
    aw-workspace core's own ``src.apps.paths``, which this decoupled app has
    no dependency on."""
    home = os.environ.get("AW_WORKSPACE_HOME")
    if home:
        return home
    root = os.path.realpath(os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace"))
    return os.path.join(root, ".aw-workspace")


def kb_root() -> Path:
    """``<AW_WORKSPACE_HOME>/knowledge_base`` — the same tree the `kb` app
    indexes, read-only from here (this driver never writes into it)."""
    return Path(_workspace_home()) / "knowledge_base"


def _data_dir() -> Path:
    d = Path(_workspace_home()) / "data" / "knowledgeable"
    d.mkdir(parents=True, exist_ok=True)
    return d


def journal_path() -> Path:
    return _data_dir() / "bulk_ingest.sqlite"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(journal_path()))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            relpath TEXT PRIMARY KEY,
            sha256 TEXT NOT NULL,
            bucket TEXT NOT NULL,
            status TEXT NOT NULL,
            external_id TEXT,
            canonical_relpath TEXT,
            server_deduplicated INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            scanned_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS files_status ON files(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS files_bucket ON files(bucket)")
    conn.execute("CREATE INDEX IF NOT EXISTS files_sha256 ON files(sha256)")
    conn.commit()
    return conn


def _priority(relpath: str) -> int:
    """Lower wins the canonical slot. §13.2's rule: curated subtrees beat
    `mapped_folders/`; inside `mapped_folders/`, a real `repos/<name>/...`
    checkout beats the monolith checkout AND the `apps/<slug>/` mirror.

    The monolith checkout is itself cloned as `repos/agentic-workspace/`
    (confirmed via the KB search this driver's own design was grounded on —
    `mapped_folders/repos/agentic-workspace/docs/knowledge_base/...` is a
    real, observed path), so without naming it explicitly it would tie with
    every genuine `repos/<name>/...` copy at the same priority tier instead
    of losing to it. `apps/<slug>/` is an installed COPY of `repos/aw-app-
    <slug>/` (memory `apps-slug-ui-is-an-installed-copy-not-the-source-
    repo`) — also always loses to the real repo.
    """
    if not relpath.startswith("mapped_folders/"):
        return 0
    rest = relpath[len("mapped_folders/"):]
    if rest.startswith("repos/agentic-workspace/"):
        return 3
    if rest.startswith("apps/"):
        return 2
    if rest.startswith("repos/"):
        return 1
    return 1


def scan() -> dict:
    """Walk the whole KB tree, hash every file, decide canonical vs alias
    GLOBALLY — this driver is the only thing with full-corpus sight (§13.2)
    — and upsert the journal.

    Safe to re-run: a row already `uploaded` or `skipped` is left alone (its
    hash/bucket is whatever is already live, or already a declared skip);
    only `pending`/`alias`/`failed` rows are recomputed on every call, so a
    changed file re-queues but a finished upload is never second-guessed by
    a rescan. §13.8 flags the one accepted limitation this implies: if the
    member set for a hash gains a higher-priority entry AFTER its canonical
    was already uploaded, the canonical choice stays frozen at the uploaded
    file — re-bucketing that is a future sync's job, not this driver's.
    """
    root = kb_root()
    by_hash: dict[str, list[tuple[str, str]]] = {}
    skipped: list[tuple[str, str, str]] = []  # (relpath, bucket, reason)

    for bucket, subtree in BUCKET_ORDER:
        subtree_root = root / subtree
        if not subtree_root.is_dir():
            continue
        for path in sorted(subtree_root.rglob("*.md")):
            if not path.is_file():
                continue
            relpath = str(path.relative_to(root))
            size = path.stat().st_size
            if size > MAX_FILE_BYTES:
                skipped.append((relpath, bucket, f"{size} bytes exceeds the 10MB cap"))
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            by_hash.setdefault(digest, []).append((relpath, bucket))

    conn = _connect()
    try:
        now = time.time()
        frozen = {
            row["relpath"]
            for row in conn.execute(
                "SELECT relpath FROM files WHERE status IN (?, ?)",
                (STATUS_UPLOADED, STATUS_SKIPPED),
            )
        }
        counts = {"scanned": 0, "canonical": 0, "alias": 0, "skipped": 0}
        for digest, entries in by_hash.items():
            entries.sort(key=lambda e: (_priority(e[0]), e[0]))
            canonical_relpath = entries[0][0]
            for relpath, bucket in entries:
                counts["scanned"] += 1
                if relpath in frozen:
                    continue
                is_canonical = relpath == canonical_relpath
                status = STATUS_PENDING if is_canonical else STATUS_ALIAS
                counts["canonical" if is_canonical else "alias"] += 1
                conn.execute(
                    """
                    INSERT INTO files
                        (relpath, sha256, bucket, status, canonical_relpath, scanned_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(relpath) DO UPDATE SET
                        sha256 = excluded.sha256,
                        bucket = excluded.bucket,
                        status = excluded.status,
                        canonical_relpath = excluded.canonical_relpath,
                        scanned_at = excluded.scanned_at,
                        updated_at = excluded.updated_at,
                        error = NULL
                    """,
                    (
                        relpath,
                        digest,
                        bucket,
                        status,
                        None if is_canonical else canonical_relpath,
                        now,
                        now,
                    ),
                )
        for relpath, bucket, reason in skipped:
            if relpath in frozen:
                continue
            counts["skipped"] += 1
            conn.execute(
                """
                INSERT INTO files (relpath, sha256, bucket, status, error, scanned_at, updated_at)
                VALUES (?, '', ?, ?, ?, ?, ?)
                ON CONFLICT(relpath) DO UPDATE SET
                    status = excluded.status, error = excluded.error, updated_at = excluded.updated_at
                """,
                (relpath, bucket, STATUS_SKIPPED, reason, now, now),
            )
        conn.commit()
        counts["total_in_journal"] = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        return counts
    finally:
        conn.close()


def _pending_counts_by_bucket(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT bucket, COUNT(*) AS n FROM files WHERE status = ? GROUP BY bucket",
        (STATUS_PENDING,),
    )
    return {row["bucket"]: row["n"] for row in rows}


async def _ensure_buckets() -> str | None:
    """§13.1 — ``POST /api/buckets`` before the first upload into any of the
    four. Idempotent across ticks (``client.create_bucket`` treats a 409 as
    steady state, not a failure) — cheap enough to just call every tick
    rather than tracking "did I already do this" as separate state that
    could drift from the server's own registry."""
    for bucket, _subtree in BUCKET_ORDER:
        _data, err = await client.create_bucket(bucket)
        if err:
            return f"could not ensure bucket {bucket!r} exists: {err}"
    return None


async def run_tick(max_uploads: int = MAX_UPLOADS_PER_TICK) -> dict:
    """One bounded tick — the only entrypoint either door (CLI `run`, the
    contributed scheduled task) ever calls. Enforces §13.5's three
    non-negotiables in order: the ignition guard, the per-tick ceiling, and
    the per-bucket backlog ceiling. Advances buckets in §13.3's order,
    stopping early once the ceiling is spent.

    Refuses to overlap with another in-flight tick (see ``_tick_lock``)
    rather than queuing behind it — a caller that already timed out once
    waiting on a slow tick should not be left waiting on a second one.
    """
    if _tick_lock.locked():
        return {
            "blocked": True,
            "reason": "another tick is already in progress — wait for it to finish",
            "uploaded": 0,
            "deduplicated": 0,
            "failed": 0,
        }
    async with _tick_lock:
        return await _run_tick_locked(max_uploads)


async def _run_tick_locked(max_uploads: int) -> dict:
    max_uploads = max(0, min(max_uploads, MAX_UPLOADS_PER_TICK))
    conn = _connect()
    try:
        pending_by_bucket = _pending_counts_by_bucket(conn)
        if not pending_by_bucket:
            return {
                "blocked": False,
                "uploaded": 0,
                "deduplicated": 0,
                "failed": 0,
                "note": "nothing pending — run scan() first, or the pass is already complete",
            }

        # §13.1 — the buckets must exist before anything else touches them
        # (GET /api/ingest/status?bucket=... 404s on an unregistered bucket
        # exactly like POST /api/documents would).
        bucket_err = await _ensure_buckets()
        if bucket_err:
            return {
                "blocked": True,
                "reason": bucket_err,
                "uploaded": 0,
                "deduplicated": 0,
                "failed": 0,
            }

        # §13.5.2 — the ignition guard, checked BEFORE any upload. `claiming`
        # is global state (ingest.py's own comment on the route) regardless
        # of which bucket answers it.
        probe_bucket = next(iter(pending_by_bucket))
        status_payload, err = await client.get_ingest_status(probe_bucket)
        if err:
            return {
                "blocked": True,
                "reason": f"could not reach GET /api/ingest/status: {err}",
                "uploaded": 0,
                "deduplicated": 0,
                "failed": 0,
            }
        if (status_payload or {}).get("extraction", {}).get("claiming"):
            return {
                "blocked": True,
                "reason": "extraction.claiming=true — refusing to upload (§13.5.2 ignition guard)",
                "uploaded": 0,
                "deduplicated": 0,
                "failed": 0,
            }

        budget = max_uploads
        uploaded = deduplicated = failed = 0
        per_bucket: dict[str, dict] = {}

        for bucket, _subtree in BUCKET_ORDER:
            if budget <= 0:
                break
            if bucket not in pending_by_bucket:
                continue

            # §13.5.3 — the backpressure ceiling, genuinely per bucket.
            bucket_status, err = await client.get_ingest_status(bucket)
            if err:
                per_bucket[bucket] = {"skipped_reason": f"status check failed: {err}"}
                continue
            backlog = ((bucket_status or {}).get("processing") or {}).get(STATUS_PENDING, 0)
            if backlog >= MAX_PENDING_BACKLOG:
                per_bucket[bucket] = {
                    "skipped_reason": f"processing backlog {backlog} >= {MAX_PENDING_BACKLOG}"
                }
                continue

            rows = conn.execute(
                "SELECT relpath FROM files WHERE bucket = ? AND status = ? ORDER BY relpath LIMIT ?",
                (bucket, STATUS_PENDING, budget),
            ).fetchall()

            b_uploaded = b_dedup = b_failed = 0
            for row in rows:
                relpath = row["relpath"]
                full_path = kb_root() / relpath
                now = time.time()
                try:
                    raw = full_path.read_bytes()
                except OSError as exc:
                    conn.execute(
                        "UPDATE files SET status = ?, error = ?, updated_at = ? WHERE relpath = ?",
                        (STATUS_FAILED, str(exc), now, relpath),
                    )
                    b_failed += 1
                    continue

                data, err = await client.upload_bytes(
                    Path(relpath).name, raw, bucket=bucket, source_path=relpath
                )
                now = time.time()
                if err:
                    conn.execute(
                        "UPDATE files SET status = ?, error = ?, updated_at = ? WHERE relpath = ?",
                        (STATUS_FAILED, err, now, relpath),
                    )
                    b_failed += 1
                    continue

                conn.execute(
                    """
                    UPDATE files
                    SET status = ?, external_id = ?, server_deduplicated = ?, error = NULL, updated_at = ?
                    WHERE relpath = ?
                    """,
                    (
                        STATUS_UPLOADED,
                        (data or {}).get("id"),
                        int(bool((data or {}).get("deduplicated"))),
                        now,
                        relpath,
                    ),
                )
                if (data or {}).get("deduplicated"):
                    b_dedup += 1
                else:
                    b_uploaded += 1
                budget -= 1
                if budget <= 0:
                    break

            conn.commit()
            uploaded += b_uploaded
            deduplicated += b_dedup
            failed += b_failed
            per_bucket[bucket] = {
                "uploaded": b_uploaded,
                "deduplicated": b_dedup,
                "failed": b_failed,
            }

        return {
            "blocked": False,
            "uploaded": uploaded,
            "deduplicated": deduplicated,
            "failed": failed,
            "per_bucket": per_bucket,
        }
    finally:
        conn.close()


def status() -> dict:
    """A cheap journal-only snapshot — no network call — for the CLI `status`
    subcommand and the task's own exit-code decision."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT bucket, status, COUNT(*) AS n FROM files GROUP BY bucket, status"
        ).fetchall()
        by_bucket: dict[str, dict[str, int]] = {}
        for row in rows:
            by_bucket.setdefault(row["bucket"], {})[row["status"]] = row["n"]
        return {"by_bucket": by_bucket, "journal_path": str(journal_path())}
    finally:
        conn.close()


def report() -> dict:
    """The §13.0/§13.9 risk 6 deliverable: scanned/uploaded/deduplicated
    (both layers)/failed/skipped, per bucket plus totals. `server_deduplicated`
    is §13.2's idempotency-gate hits (re-POSTs the server itself recognized);
    `driver_deduplicated_alias` is this driver's own cross-prefix
    canonicalization — a file never even POSTed because an earlier, higher-
    priority copy of the same content already was."""
    conn = _connect()
    try:
        buckets: dict[str, dict] = {}
        for bucket, _subtree in BUCKET_ORDER:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS scanned,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS uploaded_total,
                    SUM(CASE WHEN status = ? AND server_deduplicated = 1 THEN 1 ELSE 0 END)
                        AS server_deduplicated,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS driver_deduplicated_alias,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS skipped
                FROM files WHERE bucket = ?
                """,
                (
                    STATUS_UPLOADED,
                    STATUS_UPLOADED,
                    STATUS_ALIAS,
                    STATUS_PENDING,
                    STATUS_FAILED,
                    STATUS_SKIPPED,
                    bucket,
                ),
            ).fetchone()
            uploaded_total = row["uploaded_total"] or 0
            server_dedup = row["server_deduplicated"] or 0
            buckets[bucket] = {
                "scanned": row["scanned"] or 0,
                "uploaded_new": uploaded_total - server_dedup,
                "server_deduplicated": server_dedup,
                "driver_deduplicated_alias": row["driver_deduplicated_alias"] or 0,
                "pending": row["pending"] or 0,
                "failed": row["failed"] or 0,
                "skipped": row["skipped"] or 0,
            }
        totals: dict[str, int] = {}
        for b in buckets.values():
            for key, value in b.items():
                totals[key] = totals.get(key, 0) + value
        return {"buckets": buckets, "totals": totals, "journal_path": str(journal_path())}
    finally:
        conn.close()
