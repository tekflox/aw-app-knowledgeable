"""The §13 batch-and-journal bulk-ingest driver (docs/design/
aw-knowledgeable-v2-retrieval.md §13, card `3ee5bf3b-9510-81ad-ab05-
e219bdd3a337`) — one engine, run in-process so it can reuse this app's
already-configured ``mcp/client.py`` (base_url + X-Internal-Secret), fed
through two doors that both just POST to this app's own
``/api/apps/knowledgeable/bulk-ingest/*`` routes (``routes.py``): the
``knowledgeable-ingest`` CLI command, and the ``contributes.tasks`` scheduled
task that runs the identical CLI command as a shell job.

**ONE bucket, `main`; the subtrees are COLLECTIONS** (card `3f55bf3b-9510-
810c-99b4-cc473cd87815`, §8 + §15 of docs/design/aw-knowledgeable-fs-sync.md).

This module used to map each top-level KB subtree to its own `kb-*` bucket
(§13.1, "four genuinely different permission classes"). That was the wrong
axis: a bucket is a VISIBILITY boundary (who may see this — §7bis), and six
folders are not six audiences. They are one corpus with six source paths, and
the price of the mistake was that every cross-cutting question had to be asked
six times and merged by hand. The axis that answers "where did this come from"
shipped separately as a **collection** — a `source_path` prefix inside one
bucket, derived at read time from the `source_path` this driver already sends
with every upload (§15.1). So nothing here needs to send a folder label: the
upload carries the path, and the path IS the collection.

§13.3's ingest ORDER survives the collapse and still matters — the curated
subtrees go first because they are small and same-day validatable — which is
why `SUBTREE_ORDER` is still an ordered tuple, now of subtrees rather than of
(bucket, subtree) pairs.

**Two subtrees are not ingested at all** (`SKIP_PREFIXES`). `mapped_folders/
repos/` and `mapped_folders/aw-workspace/` are generated code maps that
codegraph already indexes and answers better — Frederico's call, 2026-10-10:
"I don't want to use the mapped code because we are using codegraph for those
documents, let's rely on it." They were 7,329 of 9,216 ingested documents,
~79% of the corpus. A skip here is structural, not a filter someone has to
remember: `scan()` never walks them, so they can never re-enter the journal
and no tick can re-upload what the §8 migration deleted.

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
ceiling (now one bucket, so one backlog). It never touches `EXTRACTION_ENABLED`, never calls
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

#: The one bucket everything lands in — see this module's docstring for why
#: six became one. The subtree a file came from is carried by `source_path`
#: on the upload, and the server derives the collection from it; this driver
#: therefore sends no folder label of its own.
BUCKET = "main"

# §13.3 — ingest order: curated subtrees first, mapped_folders/ last. Still an
# ORDERED tuple after the collapse: the order was never about the buckets, it
# was about validating the small curated corpora the same day.
#
# cli_reference/skills (card quality:procedural-genre-absent-from-both-
# knowledge-indexes) are the procedural genre neither store had: one doc per
# `aw-workspace-cli` command/subcommand captured from its own --help
# (src/libs/cli_reference.py in aw-workspace core), and the materialized
# skills/*/SKILL.md tree indexed as content for the first time (previously
# only reachable via the separate search_skills surface). Small, curated,
# hand-structured sources — same tier as crispal/memory/notion, ingested
# before mapped_folders/ for the same reason those are.
SUBTREE_ORDER: tuple[str, ...] = (
    "crispal",
    "memory",
    "notion",
    "cli_reference",
    "skills",
    "mapped_folders",
)

#: Subtrees never walked, never journalled, never uploaded. Generated code
#: maps that codegraph indexes and answers better (Frederico, 2026-10-10) —
#: see the module docstring. Checked against the KB-relative path, so the
#: whole of `mapped_folders/` is NOT skipped: `mapped_folders/docs/` is
#: hand-written design documentation and stays.
SKIP_PREFIXES: tuple[str, ...] = (
    "mapped_folders/repos/",
    "mapped_folders/aw-workspace/",
)


def is_skipped(relpath: str) -> bool:
    """True for a KB-relative path under one of :data:`SKIP_PREFIXES`."""
    return relpath.startswith(SKIP_PREFIXES)

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
    _migrate_journal_to_main(conn)
    conn.commit()
    return conn


def _migrate_journal_to_main(conn: sqlite3.Connection) -> dict[str, int]:
    """§8 step 4 of the fs-sync design — reconcile the journal with the
    collapse, idempotently, on every connect.

    Two statements, both no-ops after the first run:

    * **Drop the skipped subtrees.** A row saying `mapped_folders/repos/x.md`
      was `uploaded` into `kb-mapped-folders` describes a document that no
      longer exists, and `scan()`'s `frozen` set would keep believing it
      forever. Deleting the rows is what makes the skip rule a property of the
      journal too, not only of the walk.
    * **Rewrite `bucket` to `main`.** The alternative — leaving the old slug
      and letting the server's `content_hash` MERGE gate re-dedup — was
      rejected: the column is what `run_tick`'s own SELECT filters on, so a
      stale value means those rows are simply never picked up again.

    Deliberately NOT an ALTER: the schema is unchanged, the DATA is. Kept as
    plain idempotent DML on connect rather than a one-shot migration marker,
    which is this estate's house style for a store with no alembic — and the
    table is ~2k rows, so the two scans cost nothing measurable.

    The separate SQLite→Postgres card for this journal (`3f55bf3b-9510-8124-
    ae31-d67ea0cc96dd`) moves the store, not its contents; leaving the bucket
    column stale would have handed it a corpus it could not match.
    """
    skipped = 0
    for prefix in SKIP_PREFIXES:
        cur = conn.execute("DELETE FROM files WHERE relpath LIKE ?", (f"{prefix}%",))
        skipped += cur.rowcount if cur.rowcount > 0 else 0
    cur = conn.execute("UPDATE files SET bucket = ? WHERE bucket != ?", (BUCKET, BUCKET))
    rebucketed = cur.rowcount if cur.rowcount > 0 else 0
    if skipped or rebucketed:
        log.info(
            "journal reconciled with the §8 collapse: dropped %d row(s) under "
            "%s, re-bucketed %d row(s) onto %r",
            skipped,
            list(SKIP_PREFIXES),
            rebucketed,
            BUCKET,
        )
    return {"dropped": skipped, "rebucketed": rebucketed}


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

    for subtree in SUBTREE_ORDER:
        subtree_root = root / subtree
        if not subtree_root.is_dir():
            continue
        for path in sorted(subtree_root.rglob("*.md")):
            if not path.is_file():
                continue
            relpath = str(path.relative_to(root))
            # The code-map skip, enforced in the WALK — not as a status a
            # later pass could reinterpret. A skipped path never gets hashed,
            # never enters `by_hash`, and so can never win a canonical slot
            # from a copy that IS kept.
            if is_skipped(relpath):
                continue
            size = path.stat().st_size
            if size > MAX_FILE_BYTES:
                skipped.append((relpath, BUCKET, f"{size} bytes exceeds the 10MB cap"))
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            by_hash.setdefault(digest, []).append((relpath, BUCKET))

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


def _pending_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM files WHERE status = ?", (STATUS_PENDING,)
    ).fetchone()
    return row["n"] if row else 0


async def _ensure_bucket() -> str | None:
    """§13.1 — ``POST /api/buckets`` before the first upload. Idempotent
    across ticks (``client.create_bucket`` treats a 409 as steady state, not
    a failure) — cheap enough to just call every tick rather than tracking
    "did I already do this" as separate state that could drift from the
    server's own registry."""
    _data, err = await client.create_bucket(BUCKET)
    if err:
        return f"could not ensure bucket {BUCKET!r} exists: {err}"
    return None


async def run_tick(max_uploads: int = MAX_UPLOADS_PER_TICK) -> dict:
    """One bounded tick — the only entrypoint either door (CLI `run`, the
    contributed scheduled task) ever calls. Enforces §13.5's three
    non-negotiables in order: the ignition guard, the per-tick ceiling, and
    the backlog ceiling — now checked once, against the one bucket, which is
    what "per bucket" always meant. Advances SUBTREES in §13.3's order,
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
        if not _pending_count(conn):
            return {
                "blocked": False,
                "uploaded": 0,
                "deduplicated": 0,
                "failed": 0,
                "note": "nothing pending — run scan() first, or the pass is already complete",
            }

        # §13.1 — the bucket must exist before anything else touches it
        # (GET /api/ingest/status?bucket=... 404s on an unregistered bucket
        # exactly like POST /api/documents would).
        bucket_err = await _ensure_bucket()
        if bucket_err:
            return {
                "blocked": True,
                "reason": bucket_err,
                "uploaded": 0,
                "deduplicated": 0,
                "failed": 0,
            }

        # §13.5.2/§13.5.3 — the ignition guard and the backpressure ceiling,
        # both read off ONE status call now that there is one bucket.
        # `claiming` was always global state (ingest.py's own comment on the
        # route); the backlog was always per bucket, and the bucket is `main`.
        status_payload, err = await client.get_ingest_status(BUCKET)
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
        backlog = ((status_payload or {}).get("processing") or {}).get(STATUS_PENDING, 0)
        if backlog >= MAX_PENDING_BACKLOG:
            # NOT `blocked`, deliberately: `blocked` is what the CLI turns
            # into exit 1 and the scheduled task escalates to an agent, and a
            # full backlog is the ceiling working as designed, not an
            # incident. Pre-collapse this was a per-bucket `skipped_reason`
            # with exit 0 for the same reason; one bucket must not turn it
            # into a page.
            return {
                "blocked": False,
                "uploaded": 0,
                "deduplicated": 0,
                "failed": 0,
                "bucket": BUCKET,
                "skipped_reason": (
                    f"processing backlog {backlog} >= {MAX_PENDING_BACKLOG}"
                ),
            }

        budget = max_uploads
        uploaded = deduplicated = failed = 0
        per_subtree: dict[str, dict] = {}

        # §13.3's order, now over subtrees. The `relpath LIKE` prefix is what
        # replaces the old `bucket = ?` filter: a subtree is a path prefix,
        # which is the same thing a collection is on the server side.
        for subtree in SUBTREE_ORDER:
            if budget <= 0:
                break

            rows = conn.execute(
                "SELECT relpath FROM files WHERE relpath LIKE ? AND status = ? "
                "ORDER BY relpath LIMIT ?",
                (f"{subtree}/%", STATUS_PENDING, budget),
            ).fetchall()
            if not rows:
                continue

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

                # `source_path` is the load-bearing argument now: the server
                # derives the collection from it (§15.1), so the subtree this
                # file came from is carried by the path, not by the bucket.
                data, err = await client.upload_bytes(
                    Path(relpath).name, raw, bucket=BUCKET, source_path=relpath
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
            per_subtree[subtree] = {
                "uploaded": b_uploaded,
                "deduplicated": b_dedup,
                "failed": b_failed,
            }

        return {
            "blocked": False,
            "uploaded": uploaded,
            "deduplicated": deduplicated,
            "failed": failed,
            "bucket": BUCKET,
            "per_subtree": per_subtree,
        }
    finally:
        conn.close()


def status() -> dict:
    """A cheap journal-only snapshot — no network call — for the CLI `status`
    subcommand and the task's own exit-code decision.

    Keyed by SUBTREE, not by bucket: there is one bucket now, so a per-bucket
    breakdown would be a single row and the axis an operator actually wants to
    see — which part of the corpus is behind — would be gone. `bucket` is
    still reported, once, so a reader can tell where it all landed.
    """
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT relpath, status FROM files"
        ).fetchall()
        by_subtree: dict[str, dict[str, int]] = {}
        for row in rows:
            subtree = row["relpath"].split("/", 1)[0]
            counts = by_subtree.setdefault(subtree, {})
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        return {
            "bucket": BUCKET,
            "by_subtree": by_subtree,
            "journal_path": str(journal_path()),
        }
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
        subtrees: dict[str, dict] = {}
        for subtree in SUBTREE_ORDER:
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
                FROM files WHERE relpath LIKE ?
                """,
                (
                    STATUS_UPLOADED,
                    STATUS_UPLOADED,
                    STATUS_ALIAS,
                    STATUS_PENDING,
                    STATUS_FAILED,
                    STATUS_SKIPPED,
                    f"{subtree}/%",
                ),
            ).fetchone()
            uploaded_total = row["uploaded_total"] or 0
            server_dedup = row["server_deduplicated"] or 0
            subtrees[subtree] = {
                "scanned": row["scanned"] or 0,
                "uploaded_new": uploaded_total - server_dedup,
                "server_deduplicated": server_dedup,
                "driver_deduplicated_alias": row["driver_deduplicated_alias"] or 0,
                "pending": row["pending"] or 0,
                "failed": row["failed"] or 0,
                "skipped": row["skipped"] or 0,
            }
        totals: dict[str, int] = {}
        for b in subtrees.values():
            for key, value in b.items():
                totals[key] = totals.get(key, 0) + value
        return {
            "bucket": BUCKET,
            "subtrees": subtrees,
            "totals": totals,
            "journal_path": str(journal_path()),
        }
    finally:
        conn.close()
