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

**Card C adds the pull/downsync half** (§3.3 steps 2-3): a persisted
cursor over `GET /api/documents/changes`, the journal-before-file
echo-loop breaker, and §3.3 step 3's last-change-wins conflict rule for
`authored/` — the one subtree §4 lets this engine write back to disk.

**The §7 mass-delete valve is the load-bearing piece of this file.**
`aw-workspace-cli agent sync` does exact-mirror deletions (AGENTS.md), so
a transiently empty or half-populated `knowledge_base/` is a real
scenario, not a hypothetical — see :func:`_check_mass_delete_valve`,
called before a single row's `missing_scans` is touched on any tick.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from fastapi.concurrency import run_in_threadpool

from . import bulk_ingest, pending_deletions
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

#: §3.3 step 2 — the origin stamp for a journal row the PULL pass just
#: wrote (as opposed to `ORIGIN_SCAN`, written by the upload pass). Both
#: are real, reconciled rows; `origin` records which direction wrote them
#: last, same column §3.2 already reserves for this.
ORIGIN_DOWNSYNC = "downsync"

#: §17.2 — a journal row this engine minted from the server's own
#: `sync-paths` enumeration, not from a local file this connector ever
#: uploaded. `external_id` is NULL until the path converges (the file
#: turns up on disk and gets PUT by-path for the first time); until then
#: the row exists purely to extend the ≥2-consecutive-scans delete clock
#: and the §7 valve to a path the journal would otherwise never see.
ORIGIN_ADOPTED = "adopted"

#: §4 — the one subtree the pull pass ever writes back to disk, and (until
#: card C landed) the one adoption exclusion §17.3 carried as a deliberate
#: tripwire. §17.6 point 4: removed from adoption's exclusions now that the
#: pull pass exists to make the exclusion's reason moot — still used here by
#: the pull pass itself to decide which changes may touch the filesystem.
AUTHORED_PREFIX = "authored/"

#: §3.3 step 3 — the last-change-wins tie rule: within this many seconds of
#: each other, the filesystem (the corpus's primary home) wins rather than
#: whichever side happens to compare greater by a few milliseconds.
CONFLICT_SKEW_SECONDS = 5.0

#: §3.3 step 2 — page size for `GET /api/documents/changes`, matching the
#: server's own default.
CHANGES_PAGE_LIMIT = 200

#: §17.2 — page size for `GET /api/documents/sync-paths`, matching the
#: server's own default.
SYNC_PATHS_PAGE_LIMIT = 500

#: §17.4 — this app's own tiny flags table (`db:own-tables`'s `app__<slug>__`
#: prefix), one row per one-time migration. Separate from the shared
#: bulk-ingest journal so a flag row never shows up in `status()`/`report()`'s
#: per-path counts.
FLAGS_TABLE = "app__knowledgeable__fs_sync_flags"

#: §17.4 — the live-35 catch-up runs exactly once per upgrade, gated by this
#: row's presence, not by a per-tick check of `pending_delete` rows (which
#: would re-run forever since catch-up itself doesn't change `pending_delete`
#: on an already-marked row).
CATCHUP_FLAG = "mark_delete_by_path_catchup_v1"

#: §3.3 step 2 — this connector's own small key/value table, holding the
#: pull pass's persisted cursor over `GET /api/documents/changes` (and
#: nothing else; the §17.4 catch-up gate above has its own single-purpose
#: table, kept separate rather than overloaded with a differently-shaped
#: value). Surviving a process restart is the whole point: losing the
#: cursor would mean re-polling every change ever made, which is wasteful
#: but not unsafe (every change is idempotent to re-apply — rev <=
#: remote_rev is always a no-op).
STATE_TABLE = "app__knowledgeable__fs_sync_state"
DOWNSYNC_CURSOR_KEY = "downsync_cursor"

#: §3.3 step 1 — never mark a path missing on the scan that first notices
#: it gone; only on the SECOND consecutive miss. A half-written
#: `agent sync` pass or a transiently unmounted tree must not read as
#: intent on a single scan.
MISSING_SCANS_THRESHOLD = 2

#: §3.3 step 4 / §20 — the quiet window before the upload pass acts on a
#: freshly observed hash change (new path or edit). notion-sync writes
#: 1,564 files in one burst; this collapses that into one tick's worth of
#: STABLE files rather than 1,564 half-written reads. Also §20.5's default
#: for any prefix a configured rule doesn't match (and the value used when
#: `quiet_rules` itself is absent) — byte-identical to pre-§20 behaviour.
DEBOUNCE_SECONDS = 30

#: §20.5 — a rule's cap defaults to this multiple of its own quiet seconds
#: when the config string doesn't specify one explicitly (`prefix=quiet`,
#: no `:cap`). 4× keeps a hot `notion/` card syncing at most twice a day.
QUIET_CAP_MULTIPLIER = 4

#: §20.5 — the built-in rule applied to a path no configured prefix
#: matches, and to every path when `quiet_rules` itself is absent: the
#: pre-§20 debounce, uncapped (no starvation concept existed before this
#: amendment).
_DEFAULT_QUIET_RULE: tuple[float, float] = (float(DEBOUNCE_SECONDS), 0.0)


class _QuietRule(NamedTuple):
    prefix: str
    quiet_seconds: float
    cap_seconds: float  # 0 disables the starvation cap for this rule


def parse_quiet_rules(raw: str | None) -> tuple[list[_QuietRule], list[str]]:
    """§20.5 — parse the `sync_quiet_rules` config string: comma- or
    newline-separated ``prefix=quiet_seconds[:max_hold_seconds]`` entries.
    A pure function (no `db`/network) with its own unit tests, including
    malformed input — the optional-config-gated-paths lesson: a typo must
    not silently disable the feature, so a malformed entry is skipped and
    reported back to the caller rather than raising or being swallowed.

    Returns ``(rules, invalid)`` — `rules` sorted longest-prefix-first so
    `_resolve_quiet_rule`'s first match is always the most specific one
    (`notion/kanban/` before `notion/`, either declaration order); `invalid`
    is every raw entry that failed to parse, verbatim, for `status()`'s
    `quiet_rules_invalid`. ``raw`` falsy (unset/empty) returns ``([], [])``
    — `_resolve_quiet_rule` then falls back to `_DEFAULT_QUIET_RULE` for
    every path, which is exactly today's behaviour."""
    rules: list[_QuietRule] = []
    invalid: list[str] = []
    if not raw:
        return rules, invalid
    for entry in re.split(r"[,\n]", raw):
        entry = entry.strip()
        if not entry:
            continue
        prefix, sep, value = entry.partition("=")
        if not sep:
            invalid.append(entry)
            continue
        prefix = prefix.strip()
        quiet_part, _, cap_part = value.strip().partition(":")
        try:
            quiet_seconds = float(quiet_part)
            cap_seconds = (
                float(cap_part) if cap_part else quiet_seconds * QUIET_CAP_MULTIPLIER
            )
        except ValueError:
            invalid.append(entry)
            continue
        if quiet_seconds < 0 or cap_seconds < 0:
            invalid.append(entry)
            continue
        rules.append(_QuietRule(prefix=prefix, quiet_seconds=quiet_seconds, cap_seconds=cap_seconds))
    for entry in invalid:
        log.warning("fs_sync: invalid sync_quiet_rules entry %r — skipped", entry)
    rules.sort(key=lambda r: len(r.prefix), reverse=True)
    return rules, invalid


def _resolve_quiet_rule(relpath: str, rules: list[_QuietRule]) -> tuple[float, float]:
    """§20.5 — longest matching prefix wins (`rules` is pre-sorted by
    `parse_quiet_rules`, longest first). No match → `_DEFAULT_QUIET_RULE`,
    the pre-§20 debounce, uncapped."""
    for rule in rules:
        if relpath.startswith(rule.prefix):
            return rule.quiet_seconds, rule.cap_seconds
    return _DEFAULT_QUIET_RULE


#: §21.1 — the same tick a mark lands is the default approval latency: the
#: one measured false-positive class (cli_reference, restored by a *daily*
#: task) has a restore horizon of hours, far past any `min_missing_scans`
#: an operator would sanely dial in. Reuses `MISSING_SCANS_THRESHOLD`
#: itself rather than a second constant — approving before the mark exists
#: is meaningless, so this IS the floor, not a coincidence.
DEFAULT_MIN_MISSING_SCANS = MISSING_SCANS_THRESHOLD

#: §21.1 — no rule may ever resolve to a blanket auto-approve: an empty
#: prefix is parser-rejected (surfaced in `delete_policy_invalid`, same as
#: a malformed entry), never silently treated as "match everything".
_EMPTY_PREFIX_INVALID_SUFFIX = " (empty prefix — no blanket auto-approve, ever)"

#: §21.1 — `authored/` cannot carry an `auto` rule until §4's downsync is
#: exercised: "the file vanished" is weak evidence about a document a
#: human authored in the graph. Lift consciously, in an amendment.
_AUTHORED_AUTO_INVALID_SUFFIX = " (authored/ cannot carry an auto rule — §4 downsync is not yet exercised)"

#: §21.1 — the default policy for any path no configured rule matches, and
#: for every path when `delete_policy` itself is absent: today's behaviour,
#: unattended approval never happens.
_DEFAULT_DELETE_RULE_MODE = "manual"


class _DeleteRule(NamedTuple):
    prefix: str
    mode: str  # "auto" or "manual"
    min_missing_scans: int  # meaningless for "manual"


_DEFAULT_DELETE_RULE = _DeleteRule(prefix="", mode=_DEFAULT_DELETE_RULE_MODE, min_missing_scans=0)


def parse_delete_policy(raw: str | None) -> tuple[list[_DeleteRule], list[str]]:
    """§21.1 — parse the `delete_policy` config string: comma- or
    newline-separated ``prefix=auto[:min_missing_scans]`` or
    ``prefix=manual`` entries, same splitting/longest-prefix-sort/
    skip-and-surface shape as `parse_quiet_rules` above (a typo must not
    silently disable — or silently ENABLE — unattended deletion).

    Two rejections beyond a plain parse failure, both because this knob
    deletes documents rather than merely holding an upload: an empty
    prefix (no blanket auto-approve, ever — see the 2026-10-15
    cli_reference near-miss) and an `auto` rule anchored at `authored/`
    (§4's downsync is unexercised). Both are skipped and reported back in
    `invalid`, never ignored silently and never raised.

    ``raw`` falsy (unset/empty) returns ``([], [])`` — `_resolve_delete_
    policy` then falls back to `_DEFAULT_DELETE_RULE` (`manual`) for every
    path: ships inert, byte-identical to today."""
    rules: list[_DeleteRule] = []
    invalid: list[str] = []
    if not raw:
        return rules, invalid
    for entry in re.split(r"[,\n]", raw):
        entry = entry.strip()
        if not entry:
            continue
        prefix, sep, value = entry.partition("=")
        if not sep:
            invalid.append(entry)
            continue
        prefix = prefix.strip()
        value = value.strip()
        if not prefix:
            invalid.append(entry + _EMPTY_PREFIX_INVALID_SUFFIX)
            continue
        mode_part, _, min_part = value.partition(":")
        mode_part = mode_part.strip().lower()
        if mode_part == "manual":
            if min_part:
                invalid.append(entry)
                continue
            rules.append(_DeleteRule(prefix=prefix, mode="manual", min_missing_scans=0))
        elif mode_part == "auto":
            if prefix.startswith(AUTHORED_PREFIX):
                invalid.append(entry + _AUTHORED_AUTO_INVALID_SUFFIX)
                continue
            if min_part:
                try:
                    min_scans = int(min_part)
                except ValueError:
                    invalid.append(entry)
                    continue
                if min_scans < 0:
                    invalid.append(entry)
                    continue
            else:
                min_scans = DEFAULT_MIN_MISSING_SCANS
            rules.append(_DeleteRule(prefix=prefix, mode="auto", min_missing_scans=min_scans))
        else:
            invalid.append(entry)
            continue
    for entry in invalid:
        log.warning("fs_sync: invalid delete_policy entry %r — skipped", entry)
    rules.sort(key=lambda r: len(r.prefix), reverse=True)
    return rules, invalid


def _resolve_delete_policy(relpath: str, rules: list[_DeleteRule]) -> _DeleteRule:
    """§21.1 — longest matching prefix wins (`rules` pre-sorted by
    `parse_delete_policy`). No match → `_DEFAULT_DELETE_RULE` (`manual`) —
    unmatched paths always wait for a human, exactly like today."""
    for rule in rules:
        if relpath.startswith(rule.prefix):
            return rule
    return _DEFAULT_DELETE_RULE


#: §7 valve guard 2 — the floor of `max(floor, 10% of tracked rows)`.
MASS_DELETE_FLOOR = 25
MASS_DELETE_FRACTION = 0.10

MAX_UPLOADS_PER_TICK = bulk_ingest.MAX_UPLOADS_PER_TICK


def mass_delete_threshold(tracked_rows: int) -> int:
    """§7 guard 2's declared formula — a free function so the valve test
    and an operator reading `report()` can both state the exact number
    without re-deriving it. Also §17.5's in-pass document ceiling: the
    SAME threshold value, reused rather than re-derived, applied to a
    document count instead of a path count (see `_MarkBudget`)."""
    return max(MASS_DELETE_FLOOR, int(tracked_rows * MASS_DELETE_FRACTION))


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


#: §17.5 guard 2's widened WHERE — every row PARTICIPATING in
#: missing-tracking, not just `status = 'uploaded'`: a row this engine has
#: actually synced (`external_id IS NOT NULL`) OR a row the adoption pass
#: minted to watch a server-claimed path (`origin = 'adopted'`). Rows still
#: mid-debounce with neither were never this engine's document to begin
#: with (unchanged from before the amendment). Shared by `_tracked_row_count`
#: (the threshold's denominator) and the tracked-paths query below (the
#: threshold's numerator) so the two can never drift apart.
_TRACKED_ROWS_WHERE = "WHERE external_id IS NOT NULL OR origin = :adopted"


def _tracked_row_count(db) -> int:
    rows = db.execute(
        TABLE, f"SELECT COUNT(*) FROM {{table}} {_TRACKED_ROWS_WHERE}",
        {"adopted": ORIGIN_ADOPTED},
    )
    return rows[0][0] if rows else 0


#: §18.3 — guard 2's NUMERATOR narrows to rows that could actually produce a
#: NEW mark this tick: tracked (per `_TRACKED_ROWS_WHERE`) AND not already
#: `pending_delete` AND not `restore_suppressed`. The denominator
#: (`_tracked_row_count`, above) stays on `_TRACKED_ROWS_WHERE` alone —
#: narrowing it too was rejected (§18.5 point 5): it would shrink the
#: threshold exactly as the corpus accumulates the dead/suppressed rows the
#: operator just blessed, making the valve MORE trigger-happy, not less.
#:
#: Parenthesized rather than a bare `f"{_TRACKED_ROWS_WHERE} AND ..."`: SQL
#: binds AND tighter than OR, so an unparenthesized splice would silently
#: become `external_id IS NOT NULL OR (origin = :adopted AND pending_delete
#: = 0 AND restore_suppressed = 0)` — true for every synced row regardless
#: of `pending_delete`/`restore_suppressed`, which is the exact bug a
#: denominator-vs-numerator test below exists to catch.
_MARK_ELIGIBLE_ROWS_WHERE = (
    "WHERE (external_id IS NOT NULL OR origin = :adopted) "
    "AND pending_delete = 0 AND restore_suppressed = 0"
)


def _check_mass_delete_valve(db, found: dict[str, str] | None) -> tuple[dict | None, int]:
    """§7 — the two stacked guards, checked BEFORE a single row's
    `missing_scans` advances, a single `mark-delete` call goes out, or
    (§17.5) adoption/catch-up touch anything. Returns
    ``(violation, document_ceiling)``: `violation` is ``None`` when clear,
    or a dict with a declared `reason` (and, for guard 2, the counts that
    tripped it) when the tick must abort the WHOLE pass and mark nothing.
    `document_ceiling` is §17.5's in-pass ceiling threshold — computed here
    so the caller never re-derives a second, possibly-drifted number for
    `_MarkBudget`; it is meaningless (0) when guard 1 already aborted.

    Guard 1 (zero-trust scan precondition): `found is None` (root missing
    or unreadable) or `found == {}` (walked it, saw zero files) — an
    unmounted volume or a half-recreated tree must mark nothing, and
    nothing downstream (adoption, catch-up) may touch a row either.

    Guard 2 (mass-delete threshold): more paths would have their
    `missing_scans` advanced this tick than `max(25, 10% of tracked rows)`
    — a transient near-total wipe (e.g. an `agent sync` pass caught
    mid-write) must not cascade into hundreds of delete marks. §17.5
    widens both sides of this count to `_TRACKED_ROWS_WHERE` so a mass
    disappearance of adopted-only paths is not invisible to the valve.
    §18.3 then narrows the NUMERATOR ONLY to `_MARK_ELIGIBLE_ROWS_WHERE` —
    the denominator (`_tracked_row_count`, i.e. `_TRACKED_ROWS_WHERE`) is
    untouched, see that query's own docstring for why."""
    if found is None:
        return {"reason": "kb_root() is missing or unreadable — aborting the tick, marking nothing"}, 0
    if not found:
        return {"reason": "scan saw zero files — aborting the tick, marking nothing"}, 0

    tracked_rows = _tracked_row_count(db)
    threshold = mass_delete_threshold(tracked_rows)
    rows = db.execute(TABLE, f"SELECT relpath FROM {{table}} {_MARK_ELIGIBLE_ROWS_WHERE}", {"adopted": ORIGIN_ADOPTED})
    tracked_paths = {r[0] for r in rows}
    missing_now = tracked_paths - set(found.keys())
    if len(missing_now) > threshold:
        return {
            "reason": (
                f"{len(missing_now)} files missing from the tree this tick exceeds the "
                f"mass-delete threshold max({MASS_DELETE_FLOOR}, 10% of {tracked_rows}) = {threshold}"
            ),
            "missing_count": len(missing_now),
            "threshold": threshold,
        }, threshold
    return None, threshold


def _should_adopt(relpath: str) -> bool:
    """§17.3 — one of the three adoption exclusions, a correctness rule,
    not tidiness: `is_skipped()` paths (the code-map skip prefixes, and any
    dot-component — §16.1) may well have a live file the scan deliberately
    never looks at, so "missing from source tree" would be a lie.

    §17.6 point 4 — `authored/` is deliberately NOT excluded here any
    more. Pre-card-C that exclusion existed because adopting a
    human-authored, file-less document would have marked the pull pass's
    own future payload for deletion; now that the pull pass exists and
    writes the file before anything downstream can see the row as
    missing, the reason is moot, and leaving the exclusion in would make
    an `authored/` document whose file was genuinely deleted on disk
    invisible to delete detection again — a smaller re-instance of the
    exact bug §17 exists to close.

    The remaining exclusion (foreign-`workspace_slug` claims) needs no
    check here — `GET /sync-paths`' own slug-or-NULL filter already means
    this engine is never offered one."""
    return not bulk_ingest.is_skipped(relpath)


class _MarkBudget:
    """§17.5's third guard — a cross-call ceiling on DOCUMENTS actually
    marked this tick, not on paths or calls: by-path marking fans out (one
    path can mark several claimants at once), so the path-counting guard 2
    above cannot see it. Shared between the one-time catch-up and the
    live-miss pass below so both draw from the same tick's budget and the
    SAME `max(25, 10%)` number guard 2 already computed — never a second,
    independently-derived threshold."""

    def __init__(self, ceiling: int) -> None:
        self.ceiling = ceiling
        self.marked_documents = 0
        self.tripped = False

    def allows_more(self) -> bool:
        return not self.tripped

    def record(self, count: int) -> None:
        self.marked_documents += count
        if self.marked_documents > self.ceiling:
            self.tripped = True


def _ensure_flags_table(db) -> None:
    db.create(FLAGS_TABLE, "flag TEXT PRIMARY KEY, done_at DOUBLE PRECISION NOT NULL")


def _catchup_done(db) -> bool:
    return bool(db.execute(FLAGS_TABLE, "SELECT 1 FROM {table} WHERE flag = :flag", {"flag": CATCHUP_FLAG}))


def _mark_catchup_done(db) -> None:
    db.execute(
        FLAGS_TABLE,
        "INSERT INTO {table} (flag, done_at) VALUES (:flag, :now) ON CONFLICT (flag) DO NOTHING",
        {"flag": CATCHUP_FLAG, "now": time.time()},
    )


def _ensure_state_table(db) -> None:
    db.create(STATE_TABLE, "key TEXT PRIMARY KEY, value TEXT NOT NULL")


def _get_state(db, key: str) -> str | None:
    rows = db.execute(STATE_TABLE, "SELECT value FROM {table} WHERE key = :key", {"key": key})
    return rows[0][0] if rows else None


def _set_state(db, key: str, value: str) -> None:
    db.execute(
        STATE_TABLE,
        "INSERT INTO {table} (key, value) VALUES (:key, :value) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        {"key": key, "value": value},
    )


#: Neo4j's `toString(datetime())` can carry up to nanosecond-precision
#: fractional seconds — more digits than `datetime.fromisoformat` accepts
#: (max 6, microseconds). Matches only when there ARE excess digits, so a
#: normal 0-6-digit fraction (e.g. Python's own `isoformat()` output, used
#: by this module's tests) passes through unchanged.
_EXCESS_FRACTION_RE = re.compile(r"\.(\d{6})\d+")


def _parse_iso_timestamp(value: str) -> float:
    """§3.3 step 3's conflict rule compares this against a local file's
    mtime, so both sides need to land on the same epoch-seconds scale.
    Handles the trailing ``Z`` `toString(datetime())` emits (Python's
    `fromisoformat` only accepts an explicit offset) and truncates any
    fractional-second digits past microsecond precision — §3.3's ±5s skew
    epsilon dwarfs any rounding error that truncation could introduce."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    text = _EXCESS_FRACTION_RE.sub(r".\1", text)
    return datetime.fromisoformat(text).timestamp()


async def _apply_restore(db, row: dict, relpath: str) -> None:
    """§18.2 — the restore semantics: clear AND suppress. A bare reset
    (`pending_delete = 0, missing_scans = 0` alone, card C's original
    behaviour) re-enters a still-missing file into delete detection and
    `_mark_missing_as_deleted` re-marks it two ticks later — the declined
    file's absence from disk never changed, only the human's decision did.
    `restore_suppressed = 1` holds that decision until the scan loop sees
    the file present again (`_run_tick_locked`'s found-loop, below).

    Shared by the live feed's restore detection (`_run_pull_pass`) and the
    one-time `restore_reconcile_catchup_v1` (§18.4) — same journal
    mutation, two different triggers for noticing it."""
    await run_in_threadpool(
        db.execute, TABLE,
        "UPDATE {table} SET pending_delete = 0, missing_scans = 0, restore_suppressed = 1 "
        "WHERE relpath = :relpath",
        {"relpath": relpath},
    )
    row["pending_delete"] = 0
    row["missing_scans"] = 0
    row["restore_suppressed"] = 1


async def _run_pull_pass(
    db, journal: dict, found: dict[str, str], workspace_slug: str, root: Path,
) -> dict:
    """§3.3 steps 2-3 — the downsync. Pages `GET /api/documents/changes`
    from this connector's own persisted cursor (`STATE_TABLE`); for every
    change with a `source_path`:

    - §17.6 point 5 / §18.2 — a stale journal mark is cleared the moment the
      feed shows the document is no longer `pending_delete` (a human
      declined it), regardless of subtree, via `_apply_restore` — which also
      SUPPRESSES re-marking until the scan sees the file present again,
      so a later genuine miss (the file is, separately, still gone) is not
      instantly re-marked and does not manufacture a decline/re-mark loop.
    - outside `authored/`: only `remote_rev` advances — never written to
      disk (§4's mirror-image rule; drift stays visible in `report()`
      without making the server a writer to the tree).
    - under `authored/` with `rev` past the journal's `remote_rev`: §3.3
      step 3's conflict rule runs (local hash vs. the journal's
      `last_synced_hash` base) — no local edit, or the remote side wins a
      real conflict, writes the JOURNAL ROW FIRST and only then the file
      (the echo-loop breaker: the next scan sees hash == `last_synced_hash`
      and no-ops); the filesystem winning a conflict touches neither,
      leaving `last_synced_hash` stale on purpose so the next scan's own
      upload PUTs with that stale `base_hash` and the server records the
      remote's content as `conflict_overwrote` — the existing upload path
      is what records the loser, not new code here.

    `found` is mutated in place for a path the pull pass just wrote, so
    the scan/upload loop and `_mark_missing_as_deleted` later in this same
    tick see it as present with a matching hash rather than reacting to a
    `found` snapshot taken before the pull pass ran.
    """
    await run_in_threadpool(_ensure_state_table, db)
    cursor = await run_in_threadpool(_get_state, db, DOWNSYNC_CURSOR_KEY)
    pulled = authored_written = remote_rev_only = restored = conflicts_local_won = 0

    while True:
        data, err = await client.get_document_changes(since=cursor, limit=CHANGES_PAGE_LIMIT)
        if err:
            log.warning("fs_sync: pull pass GET /changes failed: %s", err)
            break
        changes = (data or {}).get("changes") or []
        new_cursor = (data or {}).get("cursor")
        if not changes:
            break

        for change in changes:
            relpath = change.get("source_path")
            if not relpath:
                continue  # pathless ad-hoc document — outside sync's contract (§4)
            pulled += 1
            row = journal.get(relpath)

            if row and row["pending_delete"] and change.get("sync_status") != "pending_delete":
                await _apply_restore(db, row, relpath)
                restored += 1

            rev = change.get("rev")
            if rev is None:
                continue
            remote_rev_known = (row or {}).get("remote_rev") or 0
            if rev <= remote_rev_known:
                continue

            if not relpath.startswith(AUTHORED_PREFIX):
                now = time.time()
                await run_in_threadpool(
                    db.execute, TABLE,
                    "INSERT INTO {table} (relpath, sha256, bucket, status, scanned_at, updated_at, "
                    "missing_scans, pending_delete, remote_rev) "
                    "VALUES (:relpath, '', :bucket, :status, :now, :now, 0, 0, :rev) "
                    "ON CONFLICT (relpath) DO UPDATE SET remote_rev = excluded.remote_rev",
                    {
                        "relpath": relpath, "bucket": bulk_ingest.BUCKET,
                        "status": bulk_ingest.STATUS_PENDING, "now": now, "rev": rev,
                    },
                )
                if row is not None:
                    row["remote_rev"] = rev
                else:
                    journal[relpath] = {
                        "sha256": "", "last_synced_hash": None, "scanned_at": now,
                        "missing_scans": 0, "pending_delete": 0, "external_id": None,
                        "origin": None, "remote_rev": rev, "restore_suppressed": 0,
                        "pending_hash": None, "pending_since": None, "dirty_since": None,
                    }
                remote_rev_only += 1
                continue

            # authored/ — §3.3 step 3's conflict rule.
            base_hash = (row or {}).get("last_synced_hash")
            full_path = root / relpath
            local_hash = None
            if full_path.is_file():
                try:
                    local_hash = hashlib.sha256(full_path.read_bytes()).hexdigest()
                except OSError:
                    local_hash = None
            local_changed = local_hash is not None and local_hash != base_hash

            filesystem_wins = False
            if local_changed:
                try:
                    local_mtime = full_path.stat().st_mtime
                except OSError:
                    local_mtime = 0.0
                remote_mtime = _parse_iso_timestamp(change["updated_at"])
                # Decision 4 + §3.3 step 3's tie rule: a tie within the
                # skew epsilon goes to the filesystem, the corpus's
                # primary home — hence >= rather than a strict >.
                filesystem_wins = local_mtime >= remote_mtime - CONFLICT_SKEW_SECONDS

            if filesystem_wins:
                await run_in_threadpool(
                    db.execute, TABLE,
                    "UPDATE {table} SET remote_rev = :rev WHERE relpath = :relpath",
                    {"rev": rev, "relpath": relpath},
                )
                if row is not None:
                    row["remote_rev"] = rev
                conflicts_local_won += 1
                continue

            content, content_err = await client.get_document_content(change["external_id"])
            if content_err or content is None:
                log.warning(
                    "fs_sync: pull pass could not fetch content for %s: %s", relpath, content_err,
                )
                continue
            new_hash = hashlib.sha256(content).hexdigest()
            now = time.time()
            if row is None:
                await run_in_threadpool(
                    db.execute, TABLE,
                    "INSERT INTO {table} (relpath, sha256, bucket, status, origin, external_id, "
                    "last_synced_hash, remote_rev, scanned_at, updated_at, missing_scans, pending_delete) "
                    "VALUES (:relpath, :sha256, :bucket, :status, :origin, :external_id, :hash, :rev, "
                    ":now, :now, 0, 0) "
                    "ON CONFLICT (relpath) DO UPDATE SET sha256 = excluded.sha256, "
                    "origin = excluded.origin, external_id = excluded.external_id, "
                    "last_synced_hash = excluded.last_synced_hash, remote_rev = excluded.remote_rev, "
                    "scanned_at = excluded.scanned_at, updated_at = excluded.updated_at",
                    {
                        "relpath": relpath, "sha256": new_hash, "bucket": bulk_ingest.BUCKET,
                        "status": bulk_ingest.STATUS_UPLOADED, "origin": ORIGIN_DOWNSYNC,
                        "external_id": change.get("external_id"), "hash": new_hash, "rev": rev, "now": now,
                    },
                )
            else:
                await run_in_threadpool(
                    db.execute, TABLE,
                    "UPDATE {table} SET sha256 = :sha256, last_synced_hash = :hash, remote_rev = :rev, "
                    "origin = :origin, status = :status, external_id = :external_id, "
                    "scanned_at = :now, updated_at = :now WHERE relpath = :relpath",
                    {
                        "sha256": new_hash, "hash": new_hash, "rev": rev, "origin": ORIGIN_DOWNSYNC,
                        "status": bulk_ingest.STATUS_UPLOADED, "external_id": change.get("external_id"),
                        "now": now, "relpath": relpath,
                    },
                )
            # Journal row committed above, THEN the file — the echo-loop
            # breaker: a scan that saw the bytes before `last_synced_hash`
            # reflected them would PUT them straight back up and loop
            # forever.
            full_path.parent.mkdir(parents=True, exist_ok=True)
            await run_in_threadpool(full_path.write_bytes, content)

            if row is not None:
                row["sha256"] = new_hash
                row["last_synced_hash"] = new_hash
                row["remote_rev"] = rev
                row["origin"] = ORIGIN_DOWNSYNC
                row["external_id"] = change.get("external_id") or row["external_id"]
            else:
                journal[relpath] = {
                    "sha256": new_hash, "last_synced_hash": new_hash, "scanned_at": now,
                    "missing_scans": 0, "pending_delete": 0,
                    "external_id": change.get("external_id"), "origin": ORIGIN_DOWNSYNC,
                    "remote_rev": rev, "restore_suppressed": 0,
                    "pending_hash": None, "pending_since": None, "dirty_since": None,
                }
            # The scan/upload loop and `_mark_missing_as_deleted` below
            # read `found`, computed before this pass ran — without this,
            # a freshly pulled file would look "new" (and the loop would
            # restart its debounce window pointlessly) or "missing" (and
            # advance `missing_scans` for a file that is, right now, on
            # disk) for the rest of this tick.
            found[relpath] = new_hash
            authored_written += 1

        if not new_cursor or new_cursor == cursor:
            break
        cursor = new_cursor
        await run_in_threadpool(_set_state, db, DOWNSYNC_CURSOR_KEY, cursor)

    return {
        "pulled": pulled,
        "authored_written": authored_written,
        "remote_rev_only": remote_rev_only,
        "restored": restored,
        "conflicts_local_won": conflicts_local_won,
    }


async def _adopt_paths(
    db, known_relpaths: set[str], workspace_slug: str,
) -> tuple[int, set[str] | None]:
    """§17.2 point 2 — insert a journal row (`origin='adopted'`,
    `external_id` NULL) for every server-claimed path `GET /sync-paths`
    returns that `known_relpaths` (the journal as of the start of this
    tick) doesn't already have, except the §17.3 exclusions. Adoption
    NEVER marks — it only starts observation; the existing ≥2-consecutive-
    scans clock and the §7 valve then cover the new row exactly like any
    other. Mutates `known_relpaths` in place so a second page of the same
    tick, or a path repeated across pages, is never double-inserted.

    Returns ``(adopted_count, paths_seen)``. ``paths_seen`` is EVERY
    `source_path` the enumeration returned across all pages — including
    the §17.3-excluded ones, since §18.3's GC and §18.4's catch-up both
    need the server's full claim set, not just what this pass chose to
    adopt. It is ``None`` when any page errored: the zero-trust guard §18.3
    needs to tell a complete, successful listing from a partial one, and a
    partial `seen` set would make GC wrongly treat an unreached page's rows
    as having no claimant at all. A full pass that genuinely saw zero paths
    returns an empty (not ``None``) set — still unusable for GC's own
    nonzero-set guard, but for a different, non-error reason."""
    adopted = 0
    seen: set[str] = set()
    after: str | None = None
    while True:
        data, err = await client.get_sync_paths(
            workspace_slug, after=after, limit=SYNC_PATHS_PAGE_LIMIT, bucket=bulk_ingest.BUCKET,
        )
        if err:
            log.warning("fs_sync: adoption GET /sync-paths failed: %s", err)
            return adopted, None
        paths = (data or {}).get("paths") or []
        if not paths:
            return adopted, seen
        now = time.time()
        for entry in paths:
            relpath = (entry or {}).get("source_path")
            if not relpath:
                continue
            seen.add(relpath)
            if relpath in known_relpaths or not _should_adopt(relpath):
                continue
            await run_in_threadpool(
                db.execute, TABLE,
                "INSERT INTO {table} "
                "(relpath, sha256, bucket, status, origin, scanned_at, updated_at, "
                "missing_scans, pending_delete) "
                "VALUES (:relpath, '', :bucket, :status, :origin, :now, :now, 0, 0) "
                "ON CONFLICT (relpath) DO NOTHING",
                {
                    "relpath": relpath, "bucket": bulk_ingest.BUCKET,
                    "status": bulk_ingest.STATUS_PENDING, "origin": ORIGIN_ADOPTED, "now": now,
                },
            )
            known_relpaths.add(relpath)
            adopted += 1
        cursor = (data or {}).get("cursor")
        if not cursor or cursor == after:
            return adopted, seen
        after = cursor


async def _gc_dead_pending_delete_rows(db, journal: dict, sync_paths_seen: set[str] | None) -> int:
    """§18.3 — an approval is structurally invisible to the feed even after
    H1 (the node is deleted, no tombstone — §11-8's rejection): the
    `pending_delete=1` journal row left behind needs a different signal.
    `sync_paths_seen` (from this tick's `_adopt_paths`, which already pages
    the full enumeration for free) deliberately includes already-marked
    claimants (G1's own gate), so a `pending_delete=1` row absent from it
    means the last claimant at that path is genuinely gone — approved and
    deleted, not merely declined (a decline keeps the node, so its path
    stays claimed).

    Zero-trust, same basis as §7 guard 1: ``sync_paths_seen`` is ``None``
    (an enumeration page errored) or empty (nothing claimed anything, which
    on a real corpus means the listing itself is suspect, not that
    everything was approved) — GC touches NOTHING. Only `pending_delete=1`
    rows are even considered; a live row whose claimant vanished is the
    §17 journal-blind family, not deletion evidence, and must survive to
    be adopted/marked normally instead."""
    if not sync_paths_seen:
        return 0
    removed = 0
    for relpath, row in list(journal.items()):
        if not row["pending_delete"]:
            continue
        if relpath in sync_paths_seen:
            continue
        await run_in_threadpool(db.execute, TABLE, "DELETE FROM {table} WHERE relpath = :relpath", {"relpath": relpath})
        del journal[relpath]
        removed += 1
    return removed


#: §18.4 — the one-time catch-up for the already-stuck rows the feed can
#: never retroactively surface (the cursor is already past their stale
#: `updated_at`). Same shape/table as `CATCHUP_FLAG` above, a distinct flag
#: because it is a distinct one-time migration.
RESTORE_RECONCILE_FLAG = "restore_reconcile_catchup_v1"


def _restore_reconcile_done(db) -> bool:
    return bool(db.execute(FLAGS_TABLE, "SELECT 1 FROM {table} WHERE flag = :flag", {"flag": RESTORE_RECONCILE_FLAG}))


def _mark_restore_reconcile_done(db) -> None:
    db.execute(
        FLAGS_TABLE,
        "INSERT INTO {table} (flag, done_at) VALUES (:flag, :now) ON CONFLICT (flag) DO NOTHING",
        {"flag": RESTORE_RECONCILE_FLAG, "now": time.time()},
    )


async def _run_restore_reconcile_catchup(
    db, journal: dict, sync_paths_seen: set[str] | None,
) -> int:
    """§18.4 — the 18 already-stuck rows §18.1/§18.2 cannot retroactively
    fix: a human restored (declined) them before H1 shipped, so their
    stale `updated_at` sits behind the pull pass's cursor and the feed will
    never replay them. One-time, flag-gated: fetch the server's currently-
    marked set (`GET /api/documents?pending_delete=1`), then for every
    journal row still `pending_delete=1` (post-GC, so a dead claimant is
    already gone from `journal` by the time this runs):

    - a live claimant (`relpath in sync_paths_seen`) NOT in the marked set
      → restored through a channel this tick's feed poll could never see
      → apply §18.2's clear+suppress via `_apply_restore`.
    - a live claimant IN the marked set → genuinely still awaiting
      approval → untouched.
    - no live claimant at all → §18.3's GC already collected it before
      this ran; nothing left to do here (the branch is unreachable in
      practice, not specifically handled).

    Same zero-trust basis as GC: `sync_paths_seen` must be the full,
    successful enumeration this tick already paged, or this pass does
    nothing and leaves the flag unset for a clean retry next tick. The
    flag is set ONLY when both the paths enumeration and the documents
    fetch succeeded — "done" means every `pending_delete=1` row this run
    saw was actually resolved, not "we got through some of them."
    """
    await run_in_threadpool(_ensure_flags_table, db)
    if await run_in_threadpool(_restore_reconcile_done, db):
        return 0
    if not sync_paths_seen:
        return 0
    data, err = await client.get_documents_pending_delete()
    if err:
        log.warning("fs_sync: restore-reconcile catch-up GET /documents?pending_delete=1 failed: %s", err)
        return 0
    marked_paths = {
        path for doc in (data or {}).get("documents") or []
        if (path := (doc or {}).get("source_path"))
    }
    reconciled = 0
    for relpath, row in journal.items():
        if not row["pending_delete"]:
            continue
        if relpath not in sync_paths_seen or relpath in marked_paths:
            continue
        await _apply_restore(db, row, relpath)
        reconciled += 1
    await run_in_threadpool(_mark_restore_reconcile_done, db)
    return reconciled


async def _run_catchup(db, journal: dict, workspace_slug: str, budget: "_MarkBudget") -> tuple[int, int]:
    """§17.4 — the one-time heal for the live 35 (and any sibling like
    them): for every journal row already `pending_delete=1`, re-issue the
    by-path mark once. Idempotent server-side (`SET` on an already-marked
    document is a no-op — G1's own gate), and idempotent ACROSS RESTARTS
    here via `CATCHUP_FLAG` in `FLAGS_TABLE` rather than any in-memory
    state. Runs inside the same valve-cleared tick as everything else and
    draws from the SAME `budget` the live-miss pass below shares.

    A row skipped this tick — the budget ran out, or a single call
    errored — leaves `CATCHUP_FLAG` UNSET: marking is idempotent
    server-side, so retrying the whole pass next tick is free (an
    already-swept row's re-mark returns `count: 0`), and "done" must mean
    every `pending_delete=1` row at the start of this run was actually
    attempted, not "we got through some of them."

    Returns ``(swept, failed)`` — ``failed`` is the count of calls that came
    back with an error (rather than silently vanishing into the `deferred`
    log line), summed into the tick result's `delete_marks_failed`."""
    await run_in_threadpool(_ensure_flags_table, db)
    if await run_in_threadpool(_catchup_done, db):
        return 0, 0
    swept = 0
    failed = 0
    deferred = False
    for relpath, row in journal.items():
        if not row["pending_delete"]:
            continue
        if not budget.allows_more():
            deferred = True
            break
        data, err = await client.mark_delete_by_path(
            relpath, workspace_slug, bucket=bulk_ingest.BUCKET,
            reason="fs-sync §17.4 one-time catch-up",
        )
        if err:
            log.warning("fs_sync: catch-up mark-delete-by-path failed for %s: %s", relpath, err)
            deferred = True
            failed += 1
            continue
        budget.record((data or {}).get("count") or 0)
        swept += 1
    if not deferred:
        await run_in_threadpool(_mark_catchup_done, db)
    return swept, failed


def _empty_result(blocked: bool, reason: str | None = None, **extra) -> dict:
    result = {
        "blocked": blocked,
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "deleted_marked": 0,
        "delete_marks_failed": 0,
        "failed": 0,
    }
    if reason is not None:
        result["reason"] = reason
    result.update(extra)
    return result


async def run_tick(
    db, lease, *, max_uploads: int = MAX_UPLOADS_PER_TICK, workspace_slug: str = "",
    quiet_rules: str | None = None, delete_policy: str | None = None,
    valve_latched: bool | None = None,
) -> dict:
    """One bounded tick. §6: refuses to run at all when `workspace_slug`
    is empty — a quarter of the path-keyed document identity, so a
    defaulted slug would silently interleave two workspaces' documents
    under one identity, unlike core's own "legibility aid" fallback for
    this same env var.

    Refuses to overlap with another in-flight fs-sync tick via
    `ctx.state.lease` (`lease`, non-blocking, flock-backed — see
    `bulk_ingest.run_tick`'s own docstring for the same mechanism).

    `quiet_rules` (§20.5) is the raw, unparsed `sync_quiet_rules` config
    string — threaded straight through from `routes.py`'s `ctx.config`,
    parsed once per tick here rather than per row. ``None`` (the config
    key absent) behaves exactly as before this amendment: every path uses
    the built-in 30s debounce, uncapped.

    `delete_policy` (§21.1) is the same shape of raw config string for the
    `_auto_approve_pending` pass, below. `valve_latched` (§21.2) defaults
    to ``None`` = unknown = auto-approve DISABLED — a caller that doesn't
    prove the §7 valve latch is clear gets marking-only behaviour. Fail
    closed: an unknown latch state must never permit an automatic delete."""
    if not workspace_slug:
        return _empty_result(
            True, "AW_WORKSPACE is unset — refusing to run with a defaulted workspace_slug (§6)",
        )
    if not lease.claim(LEASE_NAME):
        return _empty_result(
            True, "another fs-sync tick is already in progress — wait for it to finish",
        )
    try:
        return await _run_tick_locked(
            db, max_uploads, workspace_slug, quiet_rules,
            delete_policy=delete_policy, valve_latched=valve_latched,
        )
    finally:
        lease.release(LEASE_NAME)


async def _run_tick_locked(
    db, max_uploads: int, workspace_slug: str, quiet_rules: str | None = None, *,
    delete_policy: str | None = None, valve_latched: bool | None = None,
) -> dict:
    rules, _invalid = parse_quiet_rules(quiet_rules)
    delete_rules, delete_policy_invalid = parse_delete_policy(delete_policy)
    await run_in_threadpool(bulk_ingest._ensure_table, db)
    root = bulk_ingest.kb_root()
    found = await run_in_threadpool(_walk_sync_tree, root)

    valve, doc_ceiling = await run_in_threadpool(_check_mass_delete_valve, db, found)
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
    # bookkeeping, adoption, catch-up and delete-marking below are
    # unrelated to upload/extraction load and still run.
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

    # §17.2 point 2 — adoption, strictly after the valve above and the
    # ignition guard (a tick that touches nothing on extraction-claiming
    # must touch nothing here either). Needs the journal's relpath set
    # BEFORE the full load below, so it can tell "a path the journal
    # lacks" from "a path already here" and never double-insert across a
    # paged GET /sync-paths response.
    known_rows = await run_in_threadpool(db.execute, TABLE, "SELECT relpath FROM {table}")
    known_relpaths = {r[0] for r in known_rows}
    adopted_count, sync_paths_seen = await _adopt_paths(db, known_relpaths, workspace_slug)

    rows = await run_in_threadpool(
        db.execute, TABLE,
        "SELECT relpath, sha256, last_synced_hash, scanned_at, missing_scans, "
        "pending_delete, external_id, origin, remote_rev, restore_suppressed, "
        "pending_hash, pending_since, dirty_since FROM {table}",
    )
    journal = {
        r[0]: {
            "sha256": r[1], "last_synced_hash": r[2], "scanned_at": r[3],
            "missing_scans": r[4] or 0, "pending_delete": r[5] or 0, "external_id": r[6],
            "origin": r[7], "remote_rev": r[8] or 0, "restore_suppressed": r[9] or 0,
            "pending_hash": r[10], "pending_since": r[11], "dirty_since": r[12],
        }
        for r in rows
    }

    # §18.3 — GC, right after the journal load (it deletes rows OUT of
    # `journal`) and before anything else touches `pending_delete=1` rows
    # this tick, so §18.4's catch-up below never has to re-derive "no
    # claimant at all" — by the time it runs, such a row is simply gone.
    gc_removed = await _gc_dead_pending_delete_rows(db, journal, sync_paths_seen)

    # §3.3 steps 2-3 — the pull pass, after the journal load above (it
    # needs each row's `last_synced_hash`/`remote_rev` to decide per
    # change) and before the catch-up/upload/missing passes below (it
    # mutates `found` in place for anything it just wrote, so the rest of
    # this tick sees accurate state rather than a pre-pull snapshot).
    pull_result = await _run_pull_pass(db, journal, found, workspace_slug, root)

    # §18.4 — the one-time restore-reconcile catch-up for the rows the live
    # feed can never retroactively surface. Independent of the §17.4
    # catch-up/marking budget below: this one never marks, only clears and
    # suppresses, so it carries no mass-delete risk of its own.
    restore_reconciled = await _run_restore_reconcile_catchup(db, journal, sync_paths_seen)

    # §17.4/§17.5 — the one-time catch-up and the live-miss marking pass
    # below share one `_MarkBudget`, drawing on the SAME threshold guard 2
    # already computed: one document-marking ceiling per tick, not two.
    mark_budget = _MarkBudget(doc_ceiling)
    catchup_marked, catchup_failed = await _run_catchup(db, journal, workspace_slug, mark_budget)

    created = updated = unchanged = failed = 0
    pending_restored = 0  # §22 — undecided marks cleared by file reappearance
    upload_remaining = upload_budget

    for relpath in sorted(found):
        digest = found[relpath]
        row = journal.get(relpath)
        now = time.time()

        if row is None:
            # Brand-new path: register it and start the quiet window right
            # now, same moment §3.3 step 4 always started it (not one tick
            # later) — §20.1: creates are held exactly like edits.
            await run_in_threadpool(
                db.execute, TABLE,
                "INSERT INTO {table} "
                "(relpath, sha256, bucket, status, scanned_at, updated_at, missing_scans, pending_delete, "
                "pending_hash, pending_since, dirty_since) "
                "VALUES (:relpath, :sha256, :bucket, :status, :now, :now, 0, 0, :sha256, :now, :now) "
                "ON CONFLICT (relpath) DO UPDATE SET sha256 = excluded.sha256, "
                "scanned_at = excluded.scanned_at, missing_scans = 0",
                {
                    "relpath": relpath, "sha256": digest, "bucket": bulk_ingest.BUCKET,
                    "status": bulk_ingest.STATUS_PENDING, "now": now,
                },
            )
            continue

        if row["restore_suppressed"]:
            # §18.2 — the file is present at this path again: re-arm
            # detection unconditionally (not just when `missing_scans` was
            # already nonzero), the first time the scan sees it. From here
            # on a later disappearance is marked normally after the usual
            # ≥2 scans — the suppression's job was one cycle, not forever.
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET restore_suppressed = 0 WHERE relpath = :relpath",
                {"relpath": relpath},
            )
            row["restore_suppressed"] = 0

        if row["pending_delete"]:
            # §22 — the mark is still UNDECIDED (no human declined it, or
            # the branch above would have cleared `restore_suppressed`
            # instead) and the file is back on disk: clear it, on every
            # §17.1 claimant at this path, not just the one `external_id`
            # names. This is the gap that let 37 docs sit queued on
            # 2026-10-21, 24 of them with their file present — nothing
            # cleared an undecided mark, so §1.2's "file came back"
            # restore trigger was never actually wired up.
            #
            # Deliberately NOT `_apply_restore`, and deliberately NO
            # `restore_suppressed`: suppression encodes "a human declined;
            # keep the doc even though its file is gone" (§18.2), so it
            # buys the path one free disappearance cycle. Here the trigger
            # is hard disk evidence the file is PRESENT, so detection must
            # stay fully armed — a later genuine disappearance is marked
            # again through the ordinary ≥2-scans clock.
            #
            # Placement is load-bearing: this runs BEFORE the `digest ==
            # last_synced` short-circuit below, because unchanged-content
            # reappearance is exactly the incident class above and never
            # reaches the upload path.
            #
            # The response's `count` is deliberately discarded — see the
            # success branch below for why 0 is still success.
            _, err = await client.restore_by_path(
                relpath, workspace_slug, bucket=bulk_ingest.BUCKET,
            )
            if err:
                # Leave the journal row exactly as it was and retry next
                # tick — the file is still present then, so this
                # self-heals without a second mechanism. Never fall
                # through: uploading into a still-marked document would
                # leave the server's `pending_delete` standing over fresh
                # content.
                log.warning("fs_sync: restore-by-path failed for %s: %s", relpath, err)
                continue
            # `count == 0` is success too: the server was already restored
            # through another channel (a human declined, §18.4's catch-up
            # got there first), and the goal state — no mark anywhere —
            # holds either way.
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET pending_delete = 0, missing_scans = 0 WHERE relpath = :relpath",
                {"relpath": relpath},
            )
            row["pending_delete"] = 0
            row["missing_scans"] = 0
            pending_restored += 1

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
            # §20.2 rule 1 — unchanged, or an edit that reverted: either way
            # cancels its own hold. `sha256`/`scanned_at` are updated purely
            # for reporting (never again trusted for timing — see this
            # module's docstring); `pending_hash`/`pending_since`/
            # `dirty_since` are the clock those two used to be.
            unchanged += 1
            if row["sha256"] != digest or row["pending_hash"] is not None:
                await run_in_threadpool(
                    db.execute, TABLE,
                    "UPDATE {table} SET sha256 = :sha256, scanned_at = :now, "
                    "pending_hash = NULL, pending_since = NULL, dirty_since = NULL "
                    "WHERE relpath = :relpath",
                    {"sha256": digest, "now": now, "relpath": relpath},
                )
                row["pending_hash"] = None
                row["pending_since"] = None
                row["dirty_since"] = None
            continue

        if digest != row["pending_hash"]:
            # §20.2 rule 2 — a new hash just appeared (or changed again)
            # this tick: restart the quiet window. `dirty_since` is the one
            # exception — it is set ONLY the first time a row diverges from
            # `last_synced_hash` and never restarts; it is the starvation
            # cap's anchor (§20.2's "dirty_since does NOT restart").
            dirty_since = row["dirty_since"] if row["dirty_since"] is not None else now
            await run_in_threadpool(
                db.execute, TABLE,
                "UPDATE {table} SET sha256 = :sha256, scanned_at = :now, pending_hash = :digest, "
                "pending_since = :now, dirty_since = :dirty_since WHERE relpath = :relpath",
                {"sha256": digest, "digest": digest, "now": now, "dirty_since": dirty_since, "relpath": relpath},
            )
            continue

        # §20.2 rule 3 — stable pending hash: upload iff the quiet period
        # has elapsed, or (§20.2's starvation cap) the row has been
        # continuously dirty past its cap. §20.5: the rule a configured
        # prefix doesn't match, or no `quiet_rules` at all, resolves to the
        # pre-§20 debounce, uncapped — byte-identical to today.
        quiet_seconds, cap_seconds = _resolve_quiet_rule(relpath, rules)
        pending_since = row["pending_since"] if row["pending_since"] is not None else now
        dirty_since = row["dirty_since"] if row["dirty_since"] is not None else pending_since
        quiet_elapsed = now - pending_since >= quiet_seconds
        cap_fired = cap_seconds > 0 and now - dirty_since >= cap_seconds
        if not (quiet_elapsed or cap_fired):
            continue  # stable pending hash, but not quiet (or capped) long enough yet

        if upload_remaining <= 0:
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
            Path(relpath).name, raw, bucket=bulk_ingest.BUCKET,
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
            "last_synced_hash = :hash, origin = :origin, error = NULL, updated_at = :now, "
            "pending_hash = NULL, pending_since = NULL, dirty_since = NULL "
            "WHERE relpath = :relpath",
            {
                "status": bulk_ingest.STATUS_UPLOADED,
                "external_id": (data or {}).get("id") or row["external_id"],
                "hash": new_hash, "origin": ORIGIN_SCAN, "now": now, "relpath": relpath,
            },
        )
        upload_remaining -= 1
        if outcome == "created":
            created += 1
        else:
            updated += 1

    deleted_marked, delete_marks_failed_live = await _mark_missing_as_deleted(
        db, journal, found, workspace_slug, mark_budget
    )

    # §21.2 — the in-tick auto-approve pass, immediately after the marking
    # pass above so a mark and its approval land in the SAME tick (§21.3's
    # "mark-at-2 + same-tick approval" acceptance contract). Entirely
    # inert when `delete_policy` has no `auto` rule at all. Otherwise
    # no-ops (and says why) on any of the three stop signals: §17.5's
    # mid-pass `_MarkBudget` trip, a latched §7 valve, or an UNKNOWN latch
    # state (fail closed — see `run_tick`'s own docstring). Guard 2's
    # pre-pass trip is handled for free: it already returned `_empty_
    # result` above, long before this line ever runs. Never clears the
    # latch itself — a trip always waits for an explicit, audited,
    # out-of-tick decision via `POST /fs-sync/valve/clear`.
    auto_approved = 0
    auto_approve_failed = 0
    auto_approve_suspended = None
    if any(r.mode == "auto" for r in delete_rules):
        if mark_budget.tripped:
            auto_approve_suspended = (
                "in-pass document ceiling tripped this tick — deferring to the next tick"
            )
        elif valve_latched is None:
            auto_approve_suspended = (
                "valve latch state unknown — auto-approve defaults to OFF (fail closed)"
            )
        elif valve_latched:
            auto_approve_suspended = (
                "mass-delete valve latch is set — clear it via POST /fs-sync/valve/clear"
            )
        else:
            auto_approved, auto_approve_failed = await _auto_approve_pending(db, delete_rules)

    result = {
        "blocked": False,
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "deleted_marked": deleted_marked,
        "delete_marks_failed": delete_marks_failed_live + catchup_failed,
        "failed": failed,
        "bucket": bulk_ingest.BUCKET,
        "adopted": adopted_count,
        "gc_removed": gc_removed,
        "catchup_marked": catchup_marked,
        "restore_reconciled": restore_reconciled,
        "pending_restored": pending_restored,
        "pulled": pull_result["pulled"],
        "authored_written": pull_result["authored_written"],
        "remote_rev_only": pull_result["remote_rev_only"],
        "restored": pull_result["restored"],
        "conflicts_local_won": pull_result["conflicts_local_won"],
        "auto_approved": auto_approved,
        "auto_approve_failed": auto_approve_failed,
    }
    if auto_approve_suspended:
        result["auto_approve_suspended"] = auto_approve_suspended
    if upload_budget == 0 and backlog >= bulk_ingest.MAX_PENDING_BACKLOG:
        result["skipped_reason"] = f"processing backlog {backlog} >= {bulk_ingest.MAX_PENDING_BACKLOG}"
    if mark_budget.tripped:
        # §17.5's third guard, tripped mid-pass (NOT a pre-pass abort like
        # guard 1/2 above): real work already happened this tick, so this
        # augments the result rather than replacing it with `_empty_result`.
        # Marking is idempotent/two-phase (G1's own gate), so every
        # deferred mark is simply retried next tick at no cost.
        result["blocked"] = True
        result["valve_tripped"] = True
        result["reason"] = (
            f"in-pass document ceiling exceeded: {mark_budget.marked_documents} documents marked "
            f"this tick vs threshold {mark_budget.ceiling} — deferring the remainder to the next tick"
        )
        result["marked_documents"] = mark_budget.marked_documents
        result["document_ceiling"] = mark_budget.ceiling
    return result


async def _mark_missing_as_deleted(
    db, journal: dict, found: dict[str, str], workspace_slug: str, budget: "_MarkBudget",
) -> tuple[int, int]:
    """§3.3 step 1 / §7, by-path since §17.2: advance `missing_scans` for
    every tracked path no longer on disk; at the SECOND consecutive miss,
    mark the WHOLE path via `mark_delete_by_path` — never `external_id`
    again, so a journal row that only ever named one of several documents
    sharing a path (§17.1) can no longer leave the others permanently
    unreachable. Tracked here: a row this engine has actually synced
    (`external_id IS NOT NULL`) OR one the adoption pass is watching
    (`origin == ORIGIN_ADOPTED`) — the same widened set §7's valve now
    uses. A row still waiting out its debounce window, with neither, was
    never this engine's document to begin with.

    §18.2 — `restore_suppressed` rows are excluded from marking alongside
    the existing `pending_delete` exclusion (both still advance
    `missing_scans`, harmlessly, via the `else` branch below; only the
    MARK itself is withheld). Clearing the flag is the found-loop's job
    (`_run_tick_locked`, the moment the file reappears), not this one's.

    Returns ``(marked, failed)`` — ``failed`` counts a `mark_delete_by_path`
    call that came back with an error. That used to vanish into a
    `log.warning` with no trace in the tick's own JSON result, which is
    exactly how `deleted_marked: 0` read as "nothing to do" while rows sat
    at 7/10/14/15 missing scans for hours — see the sibling card that
    diagnosed this. `failed` is summed into `delete_marks_failed` by the
    caller so a broken marking path can never again read as a quiet
    success."""
    marked = 0
    failed = 0
    for relpath, row in journal.items():
        if relpath in found:
            continue
        if not row["external_id"] and row["origin"] != ORIGIN_ADOPTED:
            continue
        new_missing = row["missing_scans"] + 1
        if (
            new_missing >= MISSING_SCANS_THRESHOLD and not row["pending_delete"]
            and not row["restore_suppressed"] and budget.allows_more()
        ):
            data, err = await client.mark_delete_by_path(
                relpath, workspace_slug, bucket=bulk_ingest.BUCKET,
                reason="missing from source tree for 2 consecutive scans",
            )
            if err:
                log.warning("fs_sync: mark-delete-by-path failed for %s: %s", relpath, err)
                failed += 1
                await run_in_threadpool(
                    db.execute, TABLE,
                    "UPDATE {table} SET missing_scans = :missing WHERE relpath = :relpath",
                    {"missing": new_missing, "relpath": relpath},
                )
                continue
            budget.record((data or {}).get("count") or 0)
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
    return marked, failed


async def _auto_approve_pending(db, rules: list[_DeleteRule]) -> tuple[int, int]:
    """§21.2 steps 2-3 — the auto-approve pass's own body, called only
    when the caller (`_run_tick_locked`) has already confirmed an `auto`
    rule exists and none of the three stop signals apply.

    Queries the journal FRESH via `db` rather than reusing the in-memory
    `journal` dict `_mark_missing_as_deleted` was given above — that dict's
    rows are never mutated in place (see that function's own body), so a
    path this very tick just marked would otherwise be invisible here,
    breaking the "mark-at-2 + same-tick approval" acceptance contract.

    Groups eligible relpaths by the SPECIFIC rule that matched them (not
    one call for the whole tick) so each approve call's audit actor names
    the rule that actually authorized it. Fetches the server's pending
    list once, also fresh (after this tick's marks), and reuses the
    EXISTING `pending_deletions.approve_pending_deletions` — one approve
    path, one audit shape, one journal-reconcile, for human, agent and
    policy alike. A TOCTOU count mismatch (§21.7) is handled entirely
    inside that call (rejected, audited, counted as neither approved nor
    failed here) and is never retried in-pass — the next tick re-derives
    eligibility from scratch and tries again."""
    eligible = await run_in_threadpool(
        db.execute, TABLE, "SELECT relpath, missing_scans FROM {table} WHERE pending_delete = 1",
    )
    by_prefix: dict[str, set[str]] = {}
    for relpath, missing_scans in eligible:
        rule = _resolve_delete_policy(relpath, rules)
        if rule.mode != "auto":
            continue
        if (missing_scans or 0) < rule.min_missing_scans:
            continue
        by_prefix.setdefault(rule.prefix, set()).add(relpath)
    if not by_prefix:
        return 0, 0

    data, err = await client.get_documents_pending_delete(bucket=bulk_ingest.BUCKET)
    if err:
        log.warning("fs_sync: delete_policy auto-approve could not list pending deletions: %s", err)
        return 0, 0
    docs = (data or {}).get("documents") or []

    approved = 0
    failed = 0
    for prefix, relpaths in by_prefix.items():
        node_ids = [d["id"] for d in docs if d.get("source_path") in relpaths]
        if not node_ids:
            continue
        text, is_error = await pending_deletions.approve_pending_deletions(
            {"bucket": bulk_ingest.BUCKET, "node_ids": node_ids, "expected_count": len(node_ids)},
            db, f"fs-sync:delete_policy rule={prefix}",
        )
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            payload = {}
        succeeded = payload.get("approved") or []
        failed_docs = payload.get("failed") or []
        approved += len(succeeded)
        failed += len(failed_docs)
        if is_error and not succeeded:
            log.warning(
                "fs_sync: delete_policy auto-approve rule=%s did not complete: %s", prefix, text,
            )
    return approved, failed


def status(db, quiet_rules: str | None = None, delete_policy: str | None = None) -> dict:
    """A cheap journal-only snapshot for the CLI `status` subcommand —
    same no-network-call contract as `bulk_ingest.status`.

    §20.7 — `quiet_rules` (the raw `sync_quiet_rules` config string,
    threaded through from `routes.py`) adds `holding` (rows currently
    mid-hold — `pending_hash` set, not yet eligible to upload), the
    parsed, effective `quiet_rules`, and `quiet_rules_invalid` (any entry
    that failed to parse — a typo must be visible here, not silently
    disable the feature). Without these, "held for 3h" and "silently
    broken" are indistinguishable from the operator's seat.

    §21.2 adds the same shape for `delete_policy`: the parsed, effective
    `delete_policy` rules and `delete_policy_invalid` (any entry rejected
    — malformed, an empty prefix, or an `auto` rule on `authored/`)."""
    bulk_ingest._ensure_table(db)
    rows = db.execute(
        TABLE, "SELECT relpath, last_synced_hash, missing_scans, pending_delete, pending_hash FROM {table}",
    )
    synced = sum(1 for r in rows if r[1] is not None)
    missing_tracked = sum(1 for r in rows if (r[2] or 0) > 0)
    pending_delete = sum(1 for r in rows if r[3])
    holding = sum(1 for r in rows if r[4] is not None)
    rules, invalid = parse_quiet_rules(quiet_rules)
    delete_rules, delete_invalid = parse_delete_policy(delete_policy)
    return {
        "bucket": bulk_ingest.BUCKET,
        "journal_rows": len(rows),
        "synced": synced,
        "missing_tracked": missing_tracked,
        "pending_delete": pending_delete,
        "holding": holding,
        "quiet_rules": [
            {"prefix": r.prefix, "quiet_seconds": r.quiet_seconds, "cap_seconds": r.cap_seconds}
            for r in rules
        ],
        "quiet_rules_invalid": invalid,
        "delete_policy": [
            {"prefix": r.prefix, "mode": r.mode, "min_missing_scans": r.min_missing_scans}
            for r in delete_rules
        ],
        "delete_policy_invalid": delete_invalid,
        "table": TABLE,
    }


def report(db, quiet_rules: str | None = None, delete_policy: str | None = None) -> dict:
    """The §3/§7 deliverable report — per-subtree synced/missing/pending-
    delete counts plus totals, mirroring `bulk_ingest.report`'s shape.
    §20.7 adds per-subtree `holding` (same `pending_hash IS NOT NULL`
    definition as `status()`). §21.2 adds the same top-level
    `delete_policy`/`delete_policy_invalid` pair `status()` carries."""
    bulk_ingest._ensure_table(db)
    subtrees: dict[str, dict] = {}
    for subtree in bulk_ingest._discovered_subtrees(bulk_ingest.kb_root()):
        rows = db.execute(
            TABLE,
            "SELECT "
            "COUNT(*) AS total, "
            "SUM(CASE WHEN last_synced_hash IS NOT NULL THEN 1 ELSE 0 END) AS synced, "
            "SUM(CASE WHEN missing_scans > 0 THEN 1 ELSE 0 END) AS missing_tracked, "
            "SUM(CASE WHEN pending_delete = 1 THEN 1 ELSE 0 END) AS pending_delete, "
            "SUM(CASE WHEN pending_hash IS NOT NULL THEN 1 ELSE 0 END) AS holding "
            "FROM {table} WHERE relpath LIKE :prefix",
            {"prefix": f"{subtree}/%"},
        )
        row = rows[0]
        subtrees[subtree] = {
            "total": row[0] or 0,
            "synced": row[1] or 0,
            "missing_tracked": row[2] or 0,
            "pending_delete": row[3] or 0,
            "holding": row[4] or 0,
        }
    totals: dict[str, int] = {}
    for b in subtrees.values():
        for key, value in b.items():
            totals[key] = totals.get(key, 0) + value
    delete_rules, delete_invalid = parse_delete_policy(delete_policy)
    return {
        "bucket": bulk_ingest.BUCKET,
        "subtrees": subtrees,
        "totals": totals,
        "delete_policy": [
            {"prefix": r.prefix, "mode": r.mode, "min_missing_scans": r.min_missing_scans}
            for r in delete_rules
        ],
        "delete_policy_invalid": delete_invalid,
        "table": TABLE,
    }
