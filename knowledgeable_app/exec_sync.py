"""The execution-history sync job (docs/design/execution-history-in-
production-neo4j.md §4/§8).

One bounded tick: read AP-MT Postgres, **scrub** (``exec_scrub``), post
byte-bounded batches to aw-knowledgeable's service-authed ingest route, stamp
the freshness sentinel. Runs every 10 minutes as a contributed scheduled
task, behind ``exec_sync_enabled`` which ships **false**.

Why the work lives on this side, matching Frederico's 2026-10-12 direction
that the connector does the heavy work: this process holds the Postgres
credential and the service secret it already had, and **never holds the Neo4j
password** (§4). That is also what retires the v2 throwaway loader's
direct-HTTP-to-Neo4j pattern from anything production-facing — see
``scripts/load_exec_retrieval_neo4j.py``'s refusal guard in aw-workspace.

Four facts this file stands on, each verified rather than assumed:

* **``runs.agent_slug`` does not exist.** The column carrying the agent
  identity is ``runs.source_slug`` (§12 risk 2). The graph property keeps the
  name ``agent_slug`` because that is what the measurement's Cypher and the
  tool prompts already use.
* **``run_events.payload`` contains literal ``\\u0000``, and the column is
  ``json``, not ``jsonb``.** So *no* server-side extraction is safe — not
  ``payload::text``, and not ``payload->>'key'`` over a single key either,
  because a text-backed ``json`` has to parse the whole document to reach any
  part of it and the NUL fails the conversion ("unsupported Unicode escape
  sequence"). The payload is therefore only ever selected raw: psycopg parses
  it client-side (where the escape is legal) and the NULs are stripped before
  anything leaves here. The ``->>'name'`` exception this file used to claim
  was wrong and crashed the backfill mid-flight — see :func:`rollup_called`.
* **``runs`` has NO ``updated_at``.** §8 told the Coder to check and tighten
  the re-sweep to it if present. Checked against
  ``agents-platform-multitenant/backend/app/models.py`` (``class Run``, lines
  474-720, 2026-10-18): there is no such column — ``started_at`` and
  ``ended_at`` are the only timestamps. So the re-sweep stays on the
  design's fallback shape: every non-terminal run started inside
  ``RESWEEP_HOURS``. Do not "optimise" that to a watermark-only pass; a run
  mutates after insert (status flips, output lands at the end) and a
  watermark on ``started_at`` alone would freeze it mid-flight forever.
* **Batches are bounded by BYTES, not row count** (§12 risk 4). Run text is
  wildly variable; production is single-worker and the tunnel edge cuts
  requests at 30 s, so an over-large batch does not fail loudly — it starves
  live search and then gets cut mid-request.

Every statement behind the ingest route is a MERGE, so a tick killed
mid-flight (the nightly backup stops the Neo4j container; the host stalls) is
simply re-run by the next tick. The watermark is advanced **once, at the
end** — never per batch, or a pass that died halfway would skip every run it
had not reached.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from . import event_kinds, exec_scrub, vault_coverage
from .mcp import client

log = logging.getLogger("aw_apps.knowledgeable.exec_sync")

#: `ctx.state.lease` name — one tick at a time, same non-blocking flock
#: mechanism `fs_sync`/`bulk_ingest` already use. Production is
#: single-worker, but a manual CLI run overlapping the scheduled tick is the
#: realistic collision and MERGE idempotency is not a reason to let two
#: passes fight over the watermark.
LEASE_NAME = "exec_sync_tick"

#: §8 — re-sweep every non-terminal run started inside this window, on top of
#: the watermark pass. Runs mutate after insert; 48 h is the design's figure.
RESWEEP_HOURS = 48

#: AP-MT run statuses that will never change again. Everything else is
#: re-swept. Taken from `models.py`'s own comment on `Run.status`
#: ("pending|queued|running|success|error|cancelled").
TERMINAL_STATUSES = ("success", "error", "cancelled")

#: Serialised payload bytes per ingest request (§12 risk 4). 2 MB, half the
#: throwaway loader's 4 MB: that one wrote straight to a dedicated instance
#: with nothing else on it, whereas this shares a single-worker service with
#: live search traffic and crosses a tunnel that cuts at 30 s.
BATCH_BYTES = 2_000_000

#: Hard ceiling on runs per tick, so a first-ever pass over ~28k runs is many
#: bounded ticks rather than one unbounded one. The backfill is the same
#: route run repeatedly, off-hours — not a special path.
MAX_RUNS_PER_TICK = 2_000

#: Fetched this many runs at a time from the server-side cursor.
_CURSOR_ITERSIZE = 200


def _empty_result(blocked: bool, reason: str | None = None, **extra) -> dict:
    return {
        "blocked": blocked,
        "reason": reason,
        "runs": 0,
        "tool_calls": 0,
        "events": 0,
        "batches": 0,
        "watermark": None,
        **extra,
    }


def _clean(value):
    """Strip NUL from anything heading for the graph (see module docstring)."""
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


def _as_text(value):
    """A JSON column -> one string, compactly re-serialised. psycopg has
    already parsed it client-side, so a `\\u0000` inside is a Python string
    here and `_clean` can reach it."""
    if value is None:
        return None
    if isinstance(value, str):
        return _clean(value)
    return _clean(json.dumps(value, ensure_ascii=False))


def _iso(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return value.isoformat()


# ---------------------------------------------------------------------------
# SQL — one place, so what is loaded and what the reconcile counts cannot
# drift apart. Scoped to ONE source tenant per pass: §2's two namespaces
# (AP-MT's `tenant_id` and aw-knowledgeable's graph `tenant`) are different,
# and the write-side tenant is whatever this connector's service secret
# resolves to server-side. The source tenant is config, looked up at runtime
# — never the AP-MT string assumed to equal the graph tenant (§12 risk 7).
# ---------------------------------------------------------------------------

#: The selection predicate, in words: anything that STARTED or ENDED after
#: the watermark, plus anything still non-terminal inside the re-sweep
#: window. `ended_at` is in there because a run that started before the
#: watermark and finished after it is exactly the row whose output just
#: landed.
_RUN_SCOPE = """
    r.tenant_id = %(tenant)s
    AND (
        %(watermark)s IS NULL
        OR r.started_at > %(watermark)s
        OR r.ended_at > %(watermark)s
        OR (r.status NOT IN %(terminal)s AND r.started_at > %(resweep_since)s)
    )
"""

SQL_RUNS = f"""
    SELECT r.id, r.source_slug, r.status, r.model_slug, r.flow_slug,
           r.flow_outcome, r.hop_count, r.cost_usd, r.tokens_in, r.tokens_out,
           r.started_at, r.ended_at,
           r.parent_run_id, r.qa_run_id, r.session_id, r.notion_task_id,
           r.input, r.output
    FROM runs r
    WHERE {_RUN_SCOPE}
    ORDER BY r.started_at
    LIMIT %(max_runs)s
"""

#: Tool calls for a batch of run ids. Fetched per batch rather than in one
#: sweep so a batch arrives at the ingest route with its own calls — the
#: route refuses a tool call whose run is not in the same request, because
#: the template MATCHes the run and would otherwise silently write nothing.
SQL_TOOL_CALLS = """
    SELECT e.run_id, e.payload, e.ts,
           row_number() OVER (PARTITION BY e.run_id ORDER BY e.ts, e.id) AS seq
    FROM run_events e
    WHERE e.kind = 'tool_call' AND e.run_id = ANY(%(run_ids)s)
    ORDER BY e.run_id, e.ts, e.id
"""

#: §13 slice 1 — the Event timeline, for the same batch of run ids, in
#: `SQL_TOOL_CALLS`' shape and for the same hard-won reasons (§13.12 point 5):
#:
#: * **`payload` is selected RAW.** `run_events.payload` is `json`, NOT
#:   `jsonb` — a text-backed type — so EVERY server-side extraction has to
#:   parse the whole document to reach any part of it, and a literal
#:   `\\u0000` anywhere inside fails the conversion. That includes `->>` over
#:   a single key: the `rollup_called` crash (2026-10-18, ~21.4k runs in,
#:   `UntranslatableCharacter`) is the precedent, and the first nine ticks
#:   had passed only because no payload in those windows happened to carry a
#:   NUL. psycopg parses it client-side, where the escape is legal, and
#:   `_clean` strips the NULs before anything leaves this process.
#: * **`kind = ANY(%(kinds)s)` with the shipped kinds bound as a parameter.**
#:   `kind` is a plain varchar column, so filtering on it server-side is
#:   safe — only `payload` is radioactive. The kind list comes from
#:   `event_kinds.shipped_kinds()`, never from a literal here, so the SQL
#:   cannot select a kind the registry has not verdicted.
#: * **`e.id` is selected and used verbatim** as `ExecEvent.id`: the table
#:   has its own varchar PK, so unlike `ExecToolCall` there is no synthetic
#:   `run_id + ':' + seq` to mint, and no `row_number()` window either —
#:   which also means event identity does not shift if a row is ever
#:   backfilled out of ts order.
SQL_EVENTS = """
    SELECT e.id, e.run_id, e.kind, e.ts, e.node_id, e.payload
    FROM run_events e
    WHERE e.kind = ANY(%(kinds)s) AND e.run_id = ANY(%(run_ids)s)
    ORDER BY e.run_id, e.ts, e.id
"""

#: §13.6's open-set alarm: every DISTINCT kind this tenant has ever emitted.
#: `kind` only — no `payload` in the projection, so the NUL trap above cannot
#: bite this one at all.
#:
#: Tenant-wide rather than restricted to the runs of this tick, deliberately:
#: a new kind must be visible the tick after it first appears ANYWHERE, not
#: only if it happens to land on a run the watermark selected, and a tick
#: that found no runs still has to answer the question. Measured against
#: production 2026-10-19: 253 ms over 1.34M rows (a sequential scan — there
#: is no index on `kind`), once per 10-minute tick. That is the whole reason
#: this is its own query instead of being derived from the fetched rows:
#: deriving it would make the alarm silent exactly when the sync is quiet.
SQL_EVENT_KINDS = """
    SELECT DISTINCT e.kind
    FROM run_events e
    WHERE e.tenant_id = %(tenant)s
"""

#: §5's `CALLED {n}` rollup, kept alongside `MADE_CALL` exactly as the
#: measurement's graph had it — computed CLIENT-SIDE, with no SQL of its own.
#:
#: There used to be a second aggregate query here doing
#: ``GROUP BY e.run_id, e.payload->>'name'``. It crashed the backfill on
#: 2026-10-18 after ~21.4k runs had landed, with
#: ``psycopg2.errors.UntranslatableCharacter: unsupported Unicode escape
#: sequence``, and the first nine 2000-run ticks had passed only because no
#: payload in those windows happened to carry a NUL.
#:
#: The reason is the one this module's docstring already states, applied one
#: step further than it was: ``run_events.payload`` is ``json``, NOT
#: ``jsonb`` — a text-backed type — so **every** server-side extraction,
#: ``->>`` over a single key very much included, has to parse the whole
#: document first, and a literal ``\\u0000`` anywhere in it fails the
#: conversion. Verified straight against the production table, outside this
#: code path entirely::
#:
#:     SELECT count(*) FROM run_events e
#:      WHERE e.kind='tool_call' AND e.payload->>'name' IS NOT NULL;
#:     DETAIL:  \\u0000 cannot be converted to text.
#:
#: So there is no narrower server-side form to retreat to, and the rollup is
#: derived from the rows :data:`SQL_TOOL_CALLS` has already fetched and
#: psycopg has already parsed client-side. That is exact rather than
#: approximate: both used the identical predicate
#: (``kind = 'tool_call' AND run_id = ANY(...)``) with no LIMIT, so they
#: always described the same row set — and it is now one query per batch
#: instead of two.


def rollup_called(toolcall_rows: list[dict]) -> list[dict]:
    """``[{run_id, name, n}]`` — the per-run tool-name counts, from the
    already-parsed tool-call rows.

    Rows with no name are dropped, exactly as the old ``GROUP BY`` did: a
    ``CALLED`` edge needs a tool to point at.
    """
    counts: dict[tuple[str, str], int] = {}
    for row in toolcall_rows:
        name = row.get("name")
        if not name:
            continue
        key = (row["run_id"], name)
        counts[key] = counts.get(key, 0) + 1
    return [
        {"run_id": run_id, "name": name, "n": n}
        for (run_id, name), n in counts.items()
    ]

#: The weekly count-reconcile's SQL side (§8). Mirrors `exec_counts`'
#: Cypher arms label for label, so a mismatch names which layer drifted
#: rather than only that something did.
SQL_RECONCILE = {
    "ExecRun": "SELECT count(*) FROM runs r WHERE r.tenant_id = %(tenant)s",
    "ExecSession": (
        "SELECT count(DISTINCT r.session_id) FROM runs r "
        "WHERE r.tenant_id = %(tenant)s AND r.session_id IS NOT NULL"
    ),
    "ExecCard": (
        "SELECT count(DISTINCT r.notion_task_id) FROM runs r "
        "WHERE r.tenant_id = %(tenant)s AND r.notion_task_id IS NOT NULL"
    ),
    "ExecToolCall": (
        "SELECT count(*) FROM run_events e JOIN runs r ON r.id = e.run_id "
        "WHERE e.kind = 'tool_call' AND r.tenant_id = %(tenant)s"
    ),
    # §13 slice 1. `kind = ANY(...)` bound from the registry, exactly like
    # `SQL_EVENTS` — if the two used different kind lists the reconcile would
    # report drift every time a slice shipped, which is how a reconcile stops
    # being read. Parameterised at call time (`reconcile` below) rather than
    # interpolated here, so the predicate is a real bind and not a literal
    # this dict has to keep in sync with the registry by hand.
    "ExecEvent": (
        "SELECT count(*) FROM run_events e JOIN runs r ON r.id = e.run_id "
        "WHERE e.kind = ANY(%(kinds)s) AND r.tenant_id = %(tenant)s"
    ),
    # Every ExecEvent hangs off exactly one run, and the ingest route refuses
    # an event whose run is absent — so EMITTED must equal ExecEvent. A
    # mismatch here means events landed with no edge, which would make them
    # invisible to the timeline while still counting as present.
    "EMITTED": (
        "SELECT count(*) FROM run_events e JOIN runs r ON r.id = e.run_id "
        "WHERE e.kind = ANY(%(kinds)s) AND r.tenant_id = %(tenant)s"
    ),
    "IN_SESSION": (
        "SELECT count(*) FROM runs r "
        "WHERE r.tenant_id = %(tenant)s AND r.session_id IS NOT NULL"
    ),
    "ON_CARD": (
        "SELECT count(*) FROM runs r "
        "WHERE r.tenant_id = %(tenant)s AND r.notion_task_id IS NOT NULL"
    ),
    # CHILD_OF/QA_OF are joins, not column-not-null counts: an endpoint
    # outside the loaded set creates no edge (the ingest template OPTIONAL
    # MATCHes it), so counting the raw column would report drift that is
    # correct behaviour.
    "CHILD_OF": (
        "SELECT count(*) FROM runs r JOIN runs p ON p.id = r.parent_run_id "
        "AND p.tenant_id = %(tenant)s WHERE r.tenant_id = %(tenant)s "
        "AND r.parent_run_id IS NOT NULL"
    ),
    "QA_OF": (
        "SELECT count(*) FROM runs r JOIN runs q ON q.id = r.qa_run_id "
        "AND q.tenant_id = %(tenant)s WHERE r.tenant_id = %(tenant)s "
        "AND r.qa_run_id IS NOT NULL"
    ),
}


def connect(dsn: str):
    """A psycopg2 connection to AP-MT Postgres.

    Imported lazily so this module is importable (and its tests runnable)
    without the driver — the app's `pip_requires` only guarantees httpx, and
    a hard import here would take the whole plugin down on a host that has
    no psycopg2 rather than degrading this one feature.
    """
    import psycopg2  # noqa: PLC0415

    return psycopg2.connect(dsn, connect_timeout=15)


def build_run_row(record, scrubber: exec_scrub.Scrubber) -> dict:
    """One Postgres row -> one ingest row, scrubbed.

    `agent_slug` <- `source_slug` happens HERE and nowhere else, so there is
    one place the §12 risk 2 fact is encoded.
    """
    (
        run_id, source_slug, status, model_slug, flow_slug, flow_outcome,
        hop_count, cost_usd, tokens_in, tokens_out, started_at, ended_at,
        parent_run_id, qa_run_id, session_id, notion_task_id, run_input, run_output,
    ) = record
    return {
        "id": run_id,
        "agent_slug": source_slug,
        "status": status,
        "model_slug": model_slug,
        "flow_slug": flow_slug,
        "flow_outcome": flow_outcome,
        "hop_count": hop_count,
        # Postgres `float`/`numeric` -> float; the exact value stays in
        # Postgres, which is the system of record for anything billing-shaped.
        "cost_usd": None if cost_usd is None else float(cost_usd),
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "started_at": _iso(started_at),
        "ended_at": _iso(ended_at),
        "parent_run_id": parent_run_id,
        "qa_run_id": qa_run_id,
        "session_id": session_id,
        "card_page_id": notion_task_id,
        "input_text": scrubber.scrub(_as_text(run_input)),
        "output_text": scrubber.scrub(_as_text(run_output)),
    }


def build_toolcall_row(record, scrubber: exec_scrub.Scrubber) -> dict:
    """One `run_events` tool_call row -> one ingest row, scrubbed — and with
    a secret-handling tool's arguments DROPPED, not scrubbed (§6 point 3)."""
    run_id, payload, ts, seq = record
    payload = payload if isinstance(payload, dict) else {}
    name = _clean(payload.get("name"))
    return {
        "run_id": run_id,
        "seq": int(seq),
        "name": name,
        "args_text": scrubber.scrub_tool_args(
            name=name, args_text=_as_text(payload.get("input"))
        ),
        "ts": _iso(ts),
    }


def build_event_row(record, scrubber: exec_scrub.Scrubber) -> tuple[dict, int]:
    """One `run_events` row -> one ingest event row, per its registry
    verdict (§13 slice 1). Returns ``(row, fields_dropped)``.

    What this function is, in one line: the §13.6 registry applied. Nothing
    about a kind is decided here — the field allowlist, the text field and
    the cap all come from `event_kinds`, so a reader asking "what of
    `system.init` reaches the graph" reads one table rather than this
    control flow.

    Three orderings matter, and each one is a recorded scar:

    * **Scrub, THEN cap** (§13.12 point 6). `cap_text` is called on the
      output of `scrubber.scrub`, never the reverse: a cap applied first can
      cut a secret in half, which hides it from the exact-match pass AND
      from every pattern while leaving the surviving half greppable in the
      graph forever.
    * **`_clean` every string, always** (§13.12 point 5). The NUL is legal
      in the client-side-parsed payload and illegal as a Neo4j property, and
      it arrives when it arrives — the first N ticks passing proves nothing.
      So it is applied to the text AND to every allowlisted string
      property, not just to the one field that looked risky.
    * **An uncoercible field is DROPPED, not raised.** `event_kinds.coerce`
      returns `None` and the count comes back to the caller for
      `event_fields_dropped` on the sync status. Raising would 400 the whole
      batch server-side and park the watermark behind it on every
      subsequent tick — a silent stall traded for a visible gap in one
      property, which is the wrong way round.

    The `kind` is trusted to be shipped: `SQL_EVENTS` binds
    `event_kinds.shipped_kinds()`, so an unshipped kind cannot be in the
    result set. aw-knowledgeable refuses one anyway (`SHIPPED_EVENT_KINDS`),
    which is what makes that trust checkable rather than assumed.
    """
    event_id, run_id, kind, ts, node_id, payload = record
    payload = payload if isinstance(payload, dict) else {}
    spec = event_kinds.EVENT_KIND_REGISTRY.get(kind)
    row: dict = {
        "id": event_id,
        "run_id": run_id,
        "kind": kind,
        "ts": _iso(ts),
        "node_id": _clean(node_id),
    }
    if spec is None or not spec.shipped or spec.verdict == "excluded":
        # Unreachable through `SQL_EVENTS`, which binds the registry's own
        # shipped set — this is the defensive branch for a hand-built or
        # replayed row, and it projects NOTHING derived from the payload
        # rather than guessing a shape.
        #
        # `not spec.shipped` is in the condition, not just `spec is None`,
        # and that is the point: a reviewed-but-unshipped row has a real
        # verdict and a real `text_field`, so honouring it here would make
        # the SQL's `kind = ANY(...)` the ONLY thing standing between
        # `node_end`'s final output text and the graph — one predicate, one
        # slice early, for exactly the kind §13.12 point 3 names as the trap.
        # The server refuses the kind too (`SHIPPED_EVENT_KINDS`); this is
        # the third line, and it is the cheap one.
        return row, 0

    dropped = 0
    if spec.text_field:
        text, text_len, truncated = event_kinds.cap_text(
            scrubber.scrub(_clean(payload.get(spec.text_field)))
        )
        row["text"] = text
        row["text_len"] = text_len
        row["text_truncated"] = truncated
    for prop, payload_key in spec.fields.items():
        raw = payload.get(payload_key)
        if raw is None:
            continue
        value = event_kinds.coerce(prop, raw)
        if value is None:
            dropped += 1
            log.warning(
                "exec sync: event %s (kind=%s) field %r could not be coerced to %s "
                "— dropped, not projected",
                event_id,
                kind,
                prop,
                event_kinds.EVENT_FIELD_TYPES.get(prop),
            )
            continue
        row[prop] = _clean(value)
    for prop, payload_key in spec.count_fields.items():
        raw = payload.get(payload_key)
        if isinstance(raw, (list, tuple, dict, str)):
            row[prop] = len(raw)
    return row, dropped


def _batch_bytes(rows) -> int:
    return len(json.dumps(rows, ensure_ascii=False, default=str))


def plan_batches(run_rows: list[dict], *, batch_bytes: int = BATCH_BYTES) -> list[list[dict]]:
    """Split run rows into byte-bounded batches (§12 risk 4).

    A single row larger than `batch_bytes` gets its own batch rather than
    being dropped or split: run text really is that variable, and a run whose
    output is 5 MB is exactly the one worth having in the graph. Splitting a
    row is not an option (it is one node), so the bound is advisory for that
    one row and exact for every other.
    """
    batches: list[list[dict]] = []
    current: list[dict] = []
    size = 0
    for row in run_rows:
        row_size = _batch_bytes([row])
        if current and size + row_size > batch_bytes:
            batches.append(current)
            current, size = [], 0
        current.append(row)
        size += row_size
    if current:
        batches.append(current)
    return batches


def plan_posts(
    run_rows: list[dict],
    toolcall_rows: list[dict],
    event_rows: list[dict],
    called_rows: list[dict],
    *,
    batch_bytes: int = BATCH_BYTES,
) -> list[tuple[list[dict], list[dict], list[dict], list[dict]]]:
    """Split one fetched batch into byte-bounded POSTs, each carrying its own
    runs WITH their tool calls, events and CALLED rollup.

    **Why this exists now and did not before** (§12 risk 4, extended by §13).
    `plan_batches` bounds the RUN rows of a batch, and until this slice the
    tool calls fetched for those runs rode along unmeasured. Events make that
    under-count material: a workflow run carries a
    node_start/node_end/finalizing triple per node, so the event rows of a
    2 MB run batch can exceed the run rows themselves — and an over-large
    POST does not fail loudly here, it starves live search on a single-worker
    service and then gets cut by the tunnel edge at 30 s.

    So the bound is applied to what is actually POSTED: runs plus their own
    calls, events and rollup. The grouping keeps run order (the watermark's
    `landed` prefix depends on it) and gives a single over-large run its own
    POST rather than dropping it — a run whose text is 5 MB is exactly the
    one worth having in the graph, and a node cannot be split.

    The ingest route refuses a tool call or event whose run is not in the
    same request, which is why the split is by RUN and never by row: every
    POST is self-consistent by construction.
    """
    by_run_calls: dict[str, list[dict]] = {}
    for row in toolcall_rows:
        by_run_calls.setdefault(row["run_id"], []).append(row)
    by_run_events: dict[str, list[dict]] = {}
    for row in event_rows:
        by_run_events.setdefault(row["run_id"], []).append(row)
    by_run_called: dict[str, list[dict]] = {}
    for row in called_rows:
        by_run_called.setdefault(row["run_id"], []).append(row)

    posts: list[tuple[list[dict], list[dict], list[dict], list[dict]]] = []
    runs: list[dict] = []
    calls: list[dict] = []
    events: list[dict] = []
    called: list[dict] = []
    size = 0
    for run in run_rows:
        run_id = run["id"]
        own_calls = by_run_calls.get(run_id, [])
        own_events = by_run_events.get(run_id, [])
        own_called = by_run_called.get(run_id, [])
        own_size = _batch_bytes([run]) + _batch_bytes(own_calls) + _batch_bytes(own_events)
        if runs and size + own_size > batch_bytes:
            posts.append((runs, calls, events, called))
            runs, calls, events, called, size = [], [], [], [], 0
        runs.append(run)
        calls.extend(own_calls)
        events.extend(own_events)
        called.extend(own_called)
        size += own_size
    if runs:
        posts.append((runs, calls, events, called))
    return posts


def _watermark_of(run_rows: list[dict], *, complete: bool) -> str | None:
    """The new watermark after a pass that landed ``run_rows``.

    The watermark's contract is a **frontier**: nothing at or below it may be
    unprocessed. What makes that non-obvious is that the selection is
    ``ORDER BY started_at LIMIT max_runs`` while the predicate also matches on
    ``ended_at`` — so the two columns mean different things here.

    * ``complete`` — the pass consumed *everything* the predicate matched (the
      batch was not truncated by ``max_runs`` and no ingest batch failed).
      Nothing was left behind, so the max across BOTH columns is sound, and
      using it is what stops a long-running run from being re-read on every
      tick forever. This is the steady-state 10-minute tick.
    * not ``complete`` — rows above the last landed ``started_at`` were left
      unfetched, so the watermark must not pass that point. Max over
      ``started_at`` ONLY.

    **Why this distinction is load-bearing** (found in production 2026-10-18,
    card 3fd5bf3b-9510-81bf-9ea5-ca5e2f3a1282). This function used to take the
    max across both columns unconditionally. During the backfill every pass is
    truncated at ``max_runs``, and agent runs in this estate routinely end
    *days* after they start — a watchdog kill, a host stall (run
    ``245b745a82004a34a65407f4d4df8d15``: started 2026-09-02 12:40, ended
    2026-09-06 07:12, status ``error``). One such run inside a batch whose true
    frontier was 2026-09-02 17:24 pushed the watermark to 2026-09-06 07:12, and
    every run that both started and ended inside that span dropped below the
    watermark unread. They are not recoverable by the 48 h re-sweep either: it
    only looks at non-terminal runs, and these are terminal.

    Measured cost of getting this wrong: the watermark ran ahead of the loaded
    data by a monotonically growing margin — 2 runs, then 1,716, then 5,806,
    then 5,966 — i.e. 5,966 of 27,853 runs silently absent from a graph whose
    sentinel read ``fresh`` and whose every tick reported success.

    Re-reading a run is free (every write behind the ingest route is a MERGE);
    skipping one is permanent. So when the two options conflict, this errs
    toward re-reading.
    """
    keys = ("started_at", "ended_at") if complete else ("started_at",)
    stamps = [
        value for row in run_rows for key in keys if (value := row.get(key))
    ]
    return max(stamps) if stamps else None


async def scrub_status(scrubber: exec_scrub.Scrubber | None = None) -> dict:
    """The §6 point 1 / §12 risk 8 monitoring dict — ``exact_values``,
    ``exact_match_unavailable``, ``pattern_families``, ``vault_coverage_unknown``
    and ``vault_names_uncovered``. Shared by the sync tick (``_run_tick_locked``)
    and ``GET /exec-sync/status`` so the two never compute this differently.

    Pass the tick's own already-built ``scrubber`` to avoid re-reading the
    workspace secret store a second time; omit it for a status-only check
    that needs no actual redaction pass."""
    scrubber = scrubber or exec_scrub.Scrubber.from_workspace()
    return {**scrubber.status, **(await vault_coverage.coverage_status())}


def event_kinds_unreviewed(graph_status: dict | None) -> list[str]:
    """The §13.6 alarm, read back off the freshness sentinel.

    The tick computes it (a 253 ms scan is too expensive for a doctor poll)
    and posts it inside the sentinel's `counts`, so this is where the status
    and doctor surfaces read it from — exactly as they read `reported_counts`
    for every other number the tick observed.

    An absent key is an empty list, NOT a degradation: it is the normal shape
    of a sentinel stamped by a connector older than this release, and a
    doctor that went red for that would be red on every deployment before the
    one that fixes it. The companion signal for "the check could not run at
    all" already exists and is not this one — an unreachable sentinel is
    `degraded` by itself in `_exec_sync_status_payload`.
    """
    reported = (graph_status or {}).get("reported_counts") or {}
    value = reported.get("event_kinds_unreviewed")
    if not isinstance(value, list):
        return []
    return [str(k) for k in value]


def scrub_degraded(status: dict) -> bool:
    """Whether a `scrub_status()` dict represents a degraded coverage
    guarantee — a vault secret with no argued pattern coverage, the coverage
    check itself unreachable, or the exact-match pass unavailable. Any one of
    these means §6 point 1's replacement guarantee is not currently holding."""
    return bool(
        status.get("exact_match_unavailable")
        or status.get("vault_coverage_unknown")
        or status.get("vault_names_uncovered")
    )


async def run_tick(
    lease,
    *,
    dsn: str,
    source_tenant: str,
    enabled: bool,
    max_runs: int = MAX_RUNS_PER_TICK,
    batch_bytes: int = BATCH_BYTES,
    now: datetime | None = None,
) -> dict:
    """One bounded sync tick.

    Refuses rather than guesses, in three places, because each one is a
    silent-wrong-answer if defaulted:

    * `enabled` false -> no-op (the flag ships false; §9 step 2);
    * no `dsn` -> blocked, not "sync nothing successfully";
    * no `source_tenant` -> blocked. Defaulting it to `tenant-bootstrap`
      would reproduce exactly the class of bug `resolve_tenant_id`'s minting
      branch exists to prevent (§12 risk 7), and it would do so invisibly,
      since a wrong tenant reads zero rows and reports a clean tick.
    """
    if not enabled:
        return _empty_result(True, "exec_sync_enabled is false — nothing to do")
    if not dsn:
        return _empty_result(True, "apmt_database_url is not set — refusing to guess a DSN")
    if not source_tenant:
        return _empty_result(
            True,
            "exec_sync_source_tenant is not set — refusing to default the AP-MT "
            "tenant to read (§12 risk 7: a wrong tenant reads zero rows and looks "
            "like a clean tick)",
        )
    if not client.configured():
        return _empty_result(True, "no service secret saved — cannot reach the ingest route")
    if not lease.claim(LEASE_NAME):
        return _empty_result(True, "another exec-sync tick is already in progress")
    try:
        return await _run_tick_locked(
            dsn=dsn,
            source_tenant=source_tenant,
            max_runs=max_runs,
            batch_bytes=batch_bytes,
            now=now or datetime.now(timezone.utc),
        )
    finally:
        lease.release(LEASE_NAME)


async def _run_tick_locked(
    *, dsn: str, source_tenant: str, max_runs: int, batch_bytes: int, now: datetime
) -> dict:
    status, err = await client.get_exec_status()
    if err:
        return _empty_result(True, f"cannot read the sync sentinel: {err}")
    watermark = (status or {}).get("watermark")

    scrubber = exec_scrub.Scrubber.from_workspace()
    # §6 point 1 correction / §12 risk 8 — checked every tick, alongside
    # `exact_match_unavailable`: a vault secret with no argued pattern
    # coverage must degrade the status visibly, not pass unnoticed.
    scrub_status_ = await scrub_status(scrubber)
    params = {
        "tenant": source_tenant,
        "watermark": watermark,
        "terminal": TERMINAL_STATUSES,
        "resweep_since": (now - timedelta(hours=RESWEEP_HOURS)).replace(tzinfo=None),
        "max_runs": max_runs,
    }

    conn = connect(dsn)
    try:
        # §13.6's open-set alarm, computed BEFORE the "no runs due" early
        # return: a kind that appeared upstream must degrade the doctor check
        # on the next tick whether or not that tick had any runs to project.
        # Computing it from the fetched rows instead would make the alarm go
        # quiet exactly when the sync is quiet.
        unreviewed = event_kinds.unreviewed_kinds(_fetch_event_kinds(conn, source_tenant))
        if unreviewed:
            log.warning(
                "exec sync: %d run_events kind(s) have no EVENT_KIND_REGISTRY row "
                "and are NOT projected: %s",
                len(unreviewed),
                unreviewed,
            )

        run_rows = _fetch_runs(conn, params, scrubber)
        if not run_rows:
            # A tick that found nothing still stamps the sentinel: freshness
            # means "the sync ran", not "the sync wrote". Without this, a
            # quiet night reads as six missed passes and trips the staleness
            # alarm.
            await client.put_exec_sync_state(
                source_tenant=source_tenant,
                watermark=watermark,
                counts={"event_kinds_unreviewed": unreviewed},
                last_error=None,
            )
            return _empty_result(
                False,
                "no runs due",
                watermark=watermark,
                scrub=scrub_status_,
                event_kinds_unreviewed=unreviewed,
            )

        totals = {
            "runs": 0,
            "tool_calls": 0,
            "events": 0,
            "called": 0,
            "batches": 0,
            "event_fields_dropped": 0,
        }
        last_error = None
        aborted = False
        for batch in plan_batches(run_rows, batch_bytes=batch_bytes):
            run_ids = [r["id"] for r in batch]
            toolcalls = _fetch_tool_calls(conn, run_ids, scrubber)
            events, dropped = _fetch_events(conn, run_ids, scrubber)
            totals["event_fields_dropped"] += dropped
            called = rollup_called(toolcalls)
            # One fetched batch can be several POSTs now: the byte bound is
            # applied to runs + calls + events together (`plan_posts`), not
            # to the run rows alone.
            for post_runs, post_calls, post_events, post_called in plan_posts(
                batch, toolcalls, events, called, batch_bytes=batch_bytes
            ):
                ok, err = await client.post_exec_ingest(
                    runs=post_runs,
                    tool_calls=post_calls,
                    called=post_called,
                    events=post_events,
                )
                totals["batches"] += 1
                if not ok:
                    # Stop at the first failed batch rather than pressing on:
                    # the watermark must not advance past a gap, and a partial
                    # pass that keeps going produces a graph missing an
                    # arbitrary middle slice that no count-reconcile can
                    # localise.
                    last_error = f"ingest batch {totals['batches']} failed: {err}"
                    log.warning("exec sync: %s", last_error)
                    aborted = True
                    break
                totals["runs"] += len(post_runs)
                totals["tool_calls"] += len(post_calls)
                totals["events"] += len(post_events)
                totals["called"] += len(post_called)
            if aborted:
                break
    finally:
        conn.close()

    # Only advance the watermark over the prefix that actually landed — and
    # only treat the pass as `complete` (see `_watermark_of`) when every row
    # the predicate matched was both fetched AND landed. Either a `max_runs`
    # truncation or a failed batch leaves rows above the frontier unread, and
    # a watermark that passes them loses them permanently.
    landed = run_rows[: totals["runs"]]
    complete = len(landed) == len(run_rows) < max_runs
    new_watermark = _watermark_of(landed, complete=complete) or watermark
    counts = {k: v for k, v in totals.items() if k != "batches"}
    # Rides the sentinel's `counts` rather than being recomputed by the
    # status route: the sweep is a 253 ms sequential scan over 1.3M rows, and
    # the doctor check polls far more often than every 10 minutes. The
    # sentinel therefore carries this tick's observation, which is what
    # "freshness" already means for every other number on it.
    counts["event_kinds_unreviewed"] = unreviewed
    await client.put_exec_sync_state(
        source_tenant=source_tenant,
        watermark=new_watermark,
        counts=counts,
        last_error=last_error,
    )
    return {
        "blocked": False,
        "reason": last_error,
        "watermark": new_watermark,
        "scrub": scrub_status_,
        "event_kinds_unreviewed": unreviewed,
        **totals,
    }


def _fetch_runs(conn, params: dict, scrubber: exec_scrub.Scrubber) -> list[dict]:
    """Streamed through a named (server-side) cursor so a 28k-run first pass
    does not materialise in this process's memory — the same reason the
    throwaway loader used one."""
    cur = conn.cursor("exec_sync_runs")
    cur.itersize = _CURSOR_ITERSIZE
    try:
        cur.execute(SQL_RUNS, params)
        return [build_run_row(record, scrubber) for record in cur]
    finally:
        cur.close()


def _fetch_tool_calls(conn, run_ids: list[str], scrubber: exec_scrub.Scrubber) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(SQL_TOOL_CALLS, {"run_ids": run_ids})
        return [build_toolcall_row(record, scrubber) for record in cur]
    finally:
        cur.close()


def _fetch_events(
    conn, run_ids: list[str], scrubber: exec_scrub.Scrubber
) -> tuple[list[dict], int]:
    """§13 slice 1's events for one batch of runs. Returns
    ``(rows, fields_dropped)`` — the drop count is reported on the sync
    status rather than swallowed, and `kinds` is bound from the registry so
    this query can never select an unverdicted kind."""
    cur = conn.cursor()
    try:
        cur.execute(
            SQL_EVENTS,
            {"kinds": list(event_kinds.shipped_kinds()), "run_ids": run_ids},
        )
        rows: list[dict] = []
        dropped = 0
        for record in cur:
            row, n = build_event_row(record, scrubber)
            rows.append(row)
            dropped += n
        return rows, dropped
    finally:
        cur.close()


def _fetch_event_kinds(conn, source_tenant: str) -> list[str]:
    """Every DISTINCT `run_events.kind` this tenant has emitted (§13.6).

    Selects the `kind` column and nothing else: `kind` is a plain varchar,
    so it is safe server-side, and keeping `payload` out of the projection
    means the NUL trap that governs every other query in this file cannot
    reach this one at all.
    """
    cur = conn.cursor()
    try:
        cur.execute(SQL_EVENT_KINDS, {"tenant": source_tenant})
        return [row[0] for row in cur if row[0]]
    finally:
        cur.close()


async def reconcile(*, dsn: str, source_tenant: str) -> dict:
    """§8's weekly count-reconcile: SQL counts vs the graph's own
    `exec_counts`, per label/type, mismatch -> degraded.

    Reports every row rather than just a verdict. A reconcile that only says
    "drift" sends the next person to re-derive which layer drifted, and the
    layers fail for different reasons: a missing `ExecToolCall` is an ingest
    batch that failed, a missing `CHILD_OF` is usually an endpoint outside
    the loaded window, which is correct behaviour.
    """
    if not dsn or not source_tenant:
        return {"ok": False, "error": "apmt_database_url and exec_sync_source_tenant are required"}
    status, err = await client.get_exec_status()
    if err:
        return {"ok": False, "error": f"cannot read graph counts: {err}"}
    graph_counts = (status or {}).get("graph_counts") or {}

    conn = connect(dsn)
    rows = []
    try:
        for label, sql in SQL_RECONCILE.items():
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    {
                        "tenant": source_tenant,
                        "kinds": list(event_kinds.shipped_kinds()),
                    },
                )
                sql_n = cur.fetchone()[0]
            finally:
                cur.close()
            graph_n = graph_counts.get(label, 0)
            rows.append(
                {"entity": label, "sql": sql_n, "graph": graph_n, "match": sql_n == graph_n}
            )
    finally:
        conn.close()

    degraded = [r for r in rows if not r["match"]]
    return {
        "ok": not degraded,
        "degraded": bool(degraded),
        "rows": rows,
        "source_tenant": source_tenant,
    }
