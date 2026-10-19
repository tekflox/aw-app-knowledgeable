"""The Event-timeline kind registry (docs/design/execution-history-in-
production-neo4j.md §13.1 / §13.6).

**What this module is.** §13.1's per-kind verdict table, as code: one row per
`run_events.kind` that anyone has looked at, carrying the verdict, the exact
payload fields allowed across the seam, and the reason. `exec_sync.py` reads
it to decide what to project and how; nothing else in this connector decides
that.

**Why it exists rather than a `WHERE kind IN (...)` in the SQL.** The kind
set is OPEN. Kinds are minted at AP-MT's `core/executor.py::emit()`, at
`bus.publish` sites across `api/runs.py`/`api/telegram.py`/`api/
playground.py`, and forwarded verbatim from runner CLIs (`system.init`,
`cli.*`, `thinking`) — `auto_compact.*` appeared in 2026 with no schema
change anywhere. So the question is not "which kinds exist" but "what
happens to a kind nobody has reviewed", and this module answers it the same
way `vault_coverage.py` answers it for a new vault secret:

* **A kind in no row below is NOT projected**, and the tick reports it as
  ``event_kinds_unreviewed`` on the sync status, which degrades the existing
  `contributes.doctor` check. New kind ⇒ visible degradation ⇒ a card ⇒ an
  Architect verdict ⇒ one row here. That is the `vault_names_uncovered`
  mechanism, already proven in this estate to fire and to clear.
* **DISTINCT kinds, never row counts** (§13.12 point 8): a burst of ten
  thousand events of one new kind is ONE decision owed, and reading as
  10,000 would make the signal unusable.
* **Degrade visibly, never fatally.** An unreviewed kind does not stop the
  tick; everything registered still projects. The alarm is the output, not
  the refusal.

**Three states, not two** — the distinction `vault_coverage.py` had to learn
the hard way, carried over deliberately:

* ``verdict`` says what the kind's payload IS (``structured`` / ``text`` /
  ``excluded``) — a judgement about the data, made once, from measured
  payload shapes.
* ``shipped`` says whether THIS release projects it as an `ExecEvent` — a
  judgement about slices.

Keeping them separate is what makes a row like `tool_result` readable:
verdict ``text``, shipped ``False``, reason "slice 3". It is reviewed, the
verdict is recorded, and it is still not crossing yet. Folding the two into
one flag would make "nobody looked" and "looked, decided, not yet"
indistinguishable — and an unreviewed kind must never be able to hide behind
a row that exists.

**No `payload_json` catch-all, anywhere** (§13.3 / §13.10). Only the fields
named in a row cross the seam. The scenario this is for is concrete and was
named by the card: `system.init`'s payload shape is owned by the upstream
CLI, which is exactly where an env snapshot would show up one day — and an
added field is invisible to the graph by construction here, not by a filter
someone has to remember to write.

**Fixtures in the tests next door are INVENTED** (§13.12 point 9). A real
credential in a fixture is the leak this whole mechanism exists to prevent.
"""
from __future__ import annotations

from dataclasses import dataclass, field

#: §13.4's cap on an event's text, in characters, enforced at build time —
#: **after scrubbing, never before** (§13.12 point 6: a cap applied first can
#: bisect a secret and hide it from both the exact and the pattern pass while
#: leaving the surviving half greppable). `cap_text` below is the only place
#: it is applied, so that ordering is a property of one function rather than
#: a rule every caller has to re-honour.
#:
#: Measured p95s are far under it (`thinking` 5.1 KB, `node_end` 2.8 KB); it
#: exists for the unbounded tail — `llm_token`'s 63 KB maximum is what one
#: event can be. aw-knowledgeable declares the same number server-side
#: (`core/exec_history.EVENT_TEXT_MAX`) and re-applies it as a size bound on
#: a caller that forgot; two copies on purpose, same as `SECRET_TOOL_NAMES`.
EVENT_TEXT_MAX = 16_384

#: Every `ExecEvent` property any row below can fill, and the type it is
#: coerced to before it crosses the seam.
#:
#: Why the types live here rather than being left to the server: a payload
#: field whose type surprises us (a `threshold` that arrives as a dict from
#: some future emitter) would otherwise reach aw-knowledgeable's
#: `normalize_event`, which rightly refuses what it cannot coerce — and a
#: refusal 400s the whole BATCH, which stalls the watermark behind it for
#: every subsequent tick. A connector that coerces and drops the one odd
#: field keeps the contract the design actually asks for: degrade visibly,
#: never fatally. `event_fields_dropped` on the sync status is where the drop
#: becomes visible.
#:
#: Mirrors `core/exec_history.py`'s four `_EVENT_*_FIELDS` tuples. A property
#: here that the service does not know is simply ignored there, which is the
#: safe direction for the two to drift in.
EVENT_FIELD_TYPES: dict[str, type] = {
    "agent": str,
    "label": str,
    "provider": str,
    "model": str,
    "model_slug": str,
    "status": str,
    "subtype": str,
    "is_error": bool,
    "cwd": str,
    "permission_mode": str,
    "session_id": str,
    "tools_count": int,
    "tokens_in": int,
    "tokens_out": int,
    "cost_usd": float,
    "threshold": int,
    "completed_runs": int,
    "current_tokens": int,
    "post_compact_tokens": int,
    "seconds_since_compact": int,
    "cooldown_s": int,
    "phase": str,
    "duration_s": float,
    "timeout_s": int,
}


@dataclass(frozen=True)
class EventKind:
    """One row of §13.1's verdict table.

    `fields` maps an `ExecEvent` property -> the payload key it is read
    from. A mapping rather than a bare list because the two names are not
    always equal and pretending they are is how a field silently stops
    being projected: `error`'s text comes from `payload["error"]`, `log`'s
    from `payload["msg"]`.

    `count_fields` maps a property -> a payload key whose LENGTH is the
    value. Exactly one user today, and it is the reason the mechanism
    exists at all: `system.init`'s `tools` is a list of ~250 tool NAMES, and
    §13.1's allowlist for that kind is `tools_count`, not `tools`. Storing
    the names would be ~5 KB of low-value text per run and would put every
    tool name a session was given into a fulltext-adjacent property; storing
    the count answers "how big was this session's tool surface" for a
    fraction of it.
    """

    verdict: str  # "structured" | "text" | "excluded"
    reason: str
    shipped: bool = False
    fields: dict[str, str] = field(default_factory=dict)
    count_fields: dict[str, str] = field(default_factory=dict)
    text_field: str | None = None


#: §13.1's table. Every kind measured in production on 2026-10-19 has a row,
#: INCLUDING the ones this slice does not ship — an unreviewed kind must not
#: be able to look like a reviewed one, and a reviewed-but-unshipped kind
#: must not read as an oversight.
#:
#: Field lists are not guesses: each one was read off the live
#: `run_events.payload` key sets, tenant-wide, on 2026-10-19 (the exact
#: shapes the tests next door pin as fixtures). Where a kind has more than
#: one observed shape, the rarer keys are named in the reason and
#: deliberately NOT projected — that is the allowlist working, not a gap.
EVENT_KIND_REGISTRY: dict[str, EventKind] = {
    # ---- slice 1: the 13 structured/low-risk kinds -----------------------
    "node_start": EventKind(
        verdict="structured",
        shipped=True,
        fields={
            "label": "label",
            "agent": "agent",
            "provider": "provider",
            "model": "model",
            "model_slug": "model_slug",
        },
        reason=(
            "Workflow/agent node entry. Measured payload: {label, agent, provider, "
            "model, model_slug, run_id, parent_run_id} — 233 B, no free text. "
            "`run_id`/`parent_run_id` are deliberately NOT projected: the event "
            "already carries `run_id`, and lineage is the ExecRun layer's "
            "CHILD_OF edge, so copying them onto 29k events would be two more "
            "places for the same fact to disagree. A rarer shape "
            "({agent, budget, hop, label}, 6 rows tenant-wide) drops its "
            "budget/hop keys here by the same allowlist rule."
        ),
    ),
    "finalizing": EventKind(
        verdict="structured",
        shipped=True,
        fields={"tokens_in": "tokens_in", "tokens_out": "tokens_out", "cost_usd": "cost_usd"},
        reason=(
            "End-of-node accounting. Measured payload: {tokens_in, tokens_out, "
            "cost_usd} — 66 B, numbers only, one observed shape."
        ),
    ),
    "cli.first_byte": EventKind(
        verdict="structured",
        shipped=True,
        reason=(
            "The timestamp IS the value — first-byte latency. Measured payload: "
            "{} (3 B) on every one of 25k rows, so there is nothing to allowlist "
            "and an empty field map is the correct projection, not an omission."
        ),
    ),
    "system.init": EventKind(
        verdict="structured",
        shipped=True,
        fields={
            "model": "model",
            "cwd": "cwd",
            "permission_mode": "permission_mode",
            "session_id": "session_id",
        },
        count_fields={"tools_count": "tools"},
        reason=(
            "The runner CLI's own init frame. Measured payload: {session_id, "
            "model, cwd, tools[names], permission_mode} — 587 B avg, 787 B max, "
            "uniformly bounded, tool NAMES only, NO env snapshot today. But the "
            "shape is owned by the upstream CLI, which is precisely where an env "
            "snapshot would appear later, so the field allowlist is the whole "
            "point of this row: only these five cross, `tools` as a COUNT, and "
            "an added field is invisible by construction. A `cli` key appears in "
            "603 rows and is not projected."
        ),
    ),
    "done": EventKind(
        verdict="structured",
        shipped=True,
        fields={"status": "status"},
        reason=(
            "Run/workflow completion. Measured payload: {} on 4,079 rows, "
            "{status} on 95, and {children}/{budget}/{limit_reached} on ~28 — "
            "only `status` crosses; the rarer keys are per-run rollups already "
            "on the ExecRun node or in the CHILD_OF layer."
        ),
    ),
    "cli.error": EventKind(
        verdict="structured",
        shipped=True,
        fields={"subtype": "subtype", "is_error": "is_error"},
        reason=(
            "Runner-CLI error classification. Measured payload: {subtype, "
            "is_error} — 45 B avg / 112 B max, a fixed enum plus a boolean. NOT "
            "classified as text even though 2 rows tenant-wide carry a "
            "`message` key: that key is not projected, so this kind cannot "
            "smuggle free text. If `message` ever becomes common it is a "
            "re-review and a verdict flip to `text` with its own scrub test — "
            "not a field quietly added to this list."
        ),
    ),
    "error": EventKind(
        verdict="text",
        shipped=True,
        text_field="error",
        reason=(
            "A run's error string, free text, SCRUBBED (§13.1). Error messages "
            "are where connection strings and DSNs surface — a failed connect "
            "prints the URL it tried — so the `url-userinfo` and "
            "`env-assignment` pattern families are the ones that matter here, "
            "and `tests/test_event_kinds.py` plants an invented credential in "
            "each measured shape. Measured payload: {error: msg}, 107 B avg / "
            "1.1 KB max."
        ),
    ),
    "auto_compact.blind": EventKind(
        verdict="structured",
        shipped=True,
        fields={
            "session_id": "session_id",
            "threshold": "threshold",
            "completed_runs": "completed_runs",
        },
        reason=(
            "Auto-compaction fired without a token reading. Measured payload: "
            "{session_id, threshold, completed_runs} — ids and numbers."
        ),
    ),
    "auto_compact.ineffective": EventKind(
        verdict="structured",
        shipped=True,
        fields={
            "session_id": "session_id",
            "threshold": "threshold",
            "current_tokens": "current_tokens",
            "post_compact_tokens": "post_compact_tokens",
            "seconds_since_compact": "seconds_since_compact",
            "cooldown_s": "cooldown_s",
        },
        reason=(
            "Auto-compaction ran and did not help. Measured payload: "
            "{session_id, threshold, current_tokens, post_compact_tokens, "
            "seconds_since_compact, cooldown_s} — token/cooldown numbers only."
        ),
    ),
    "connector.timing": EventKind(
        verdict="structured",
        shipped=True,
        fields={"phase": "phase", "duration_s": "duration_s"},
        reason=(
            "Runner-connector phase timing. Measured payload has two shapes, "
            "both {phase, duration_s} plus epoch floats "
            "(ready_epoch/first_line_epoch/connector_start_epoch). Only phase "
            "and duration cross: the epochs are absolute restatements of the "
            "event's own `ts` and the duration, so projecting them would add "
            "three properties that say nothing new."
        ),
    ),
    "container.started": EventKind(
        verdict="structured",
        shipped=True,
        reason=(
            "Agent container came up; the timestamp is the value. Measured "
            "payload: {} on all 31 rows."
        ),
    ),
    "log": EventKind(
        verdict="text",
        shipped=True,
        text_field="msg",
        reason=(
            "A workflow's own log line, free text, SCRUBBED. Only 4 rows "
            "tenant-wide today, which is exactly why it is worth getting right "
            "now: a log line is written by whatever code felt like logging, so "
            "its content is unbounded by construction, and 4 rows is no "
            "evidence about the 4,000th. Measured payload: {msg}."
        ),
    ),
    "cli.timeout": EventKind(
        verdict="structured",
        shipped=True,
        fields={"timeout_s": "timeout_s"},
        reason="Runner CLI hit its own timeout. Measured payload: {timeout_s}.",
    ),
    # ---- slice 2: the text kinds -----------------------------------------
    "thinking": EventKind(
        verdict="text",
        shipped=True,
        text_field="text",
        reason=(
            "SLICE 2 — SHIPPED. Reasoning text, scrubbed. Reviewed and NOT "
            "credential-free: a model can restate a secret it just read, and the "
            "earlier claim to the contrary was wrong and is corrected on the "
            "card. §13.3 decided against a per-run drop for runs that touched a "
            "secret tool, with grounds and a reopen condition — the by-definition "
            "channels (a secret tool's args and its result) are dropped "
            "wholesale, so what remains is a restatement risk that `output_text` "
            "has carried since v1. Sparse: 8,349 events over ~27% of runs."
        ),
    ),
    "node_end": EventKind(
        verdict="text",
        shipped=True,
        text_field="text",
        fields={"tokens_in": "tokens_in", "tokens_out": "tokens_out", "cost_usd": "cost_usd"},
        reason=(
            "SLICE 2 — SHIPPED, and the trap §13.12 point 3 names: the NAME "
            "lies. This is not lifecycle metadata — the measured payload is "
            "{text, tokens_in, tokens_out, cost_usd, run_id} and `text` is the "
            "node's final OUTPUT. Classified by measurement, not by name. "
            "Shipping it in slice 1 would have projected unscrubbed-class "
            "output text through the structured path, which is the single most "
            "expensive mistake available in this registry."
        ),
    ),
    # ---- reviewed, with a verdict, NOT shipped by this slice --------------
    "tool_call": EventKind(
        verdict="structured",
        shipped=False,
        reason=(
            "ALREADY PROJECTED, on its own label: `ExecToolCall` (name, "
            "args_text, ts), with secret-tool arguments dropped by the "
            "terminal-segment rule. `shipped=False` here means "
            "\"not as an ExecEvent\" — duplicating 290k calls as events would "
            "double-store them and split the tool chain across two labels."
        ),
    ),
    "tool_result": EventKind(
        verdict="text",
        shipped=False,
        text_field="content",
        reason=(
            "SLICE 3, and NOT as an ExecEvent: it lands on the existing "
            "`ExecToolCall` as `result_text`, paired through "
            "`tool_result.payload.tool_use_id` <-> `tool_call.payload.id` "
            "(§13.12 point 2 — the two keys have different names). The one "
            "genuinely dangerous kind: results are the channel through which "
            "secrets ENTER a run (a read_secret return, a `cat .env`, a "
            "`docker inspect`), and the exact-match pass structurally cannot "
            "cover a third-party credential that was never in the file store. "
            "Secret-tool results are dropped wholesale under the same "
            "terminal-segment rule."
        ),
    ),
    # ---- reviewed and permanently excluded --------------------------------
    "llm_token": EventKind(
        verdict="excluded",
        shipped=False,
        reason=(
            "NEVER. This is the one kind AP-MT's own run-detail page excludes "
            "(`events.filter(e => e.kind !== \"llm_token\")`), and that filter is "
            "this feature's spec rather than something it re-litigates. 569k rows "
            "of streamed `{delta}` chunks that reassemble into assistant text "
            "already projected, scrubbed, as `ExecRun.output_text` — pure cost "
            "with no per-row retrieval value. Reassembled per-node assistant "
            "prose, if ever wanted, is a new ask against `node_end`, not this "
            "stream."
        ),
    ),
}


def shipped_kinds() -> tuple[str, ...]:
    """The kinds this release projects as `ExecEvent`, sorted.

    What `exec_sync`'s SQL binds to `kind = ANY(%(kinds)s)`. Derived from the
    registry rather than kept as a second list, so "which kinds does the
    query select" cannot drift from "which kinds have a verdict that says
    project".
    """
    return tuple(
        sorted(k for k, row in EVENT_KIND_REGISTRY.items() if row.shipped and row.verdict != "excluded")
    )


def unreviewed_kinds(kinds) -> list[str]:
    """Kinds seen in `run_events` that have no registry row at all — the
    visible-degradation signal (§13.6).

    A kind leaves this list by getting a row, whatever the verdict: an
    `excluded` row is a decision, and a decision is not a gap. Only a kind
    nobody has looked at belongs here, so a doctor that goes red over this
    means something genuinely NEW appeared upstream.

    DISTINCT kinds, de-duplicated and sorted (§13.12 point 8) — never row
    counts. Pure and synchronous, like `vault_coverage.uncovered_names`, so
    the logic is testable with no database.
    """
    return sorted({k for k in kinds if k and k not in EVENT_KIND_REGISTRY})


def cap_text(text):
    """``(text, true_length, truncated)`` under §13.4's cap.

    Call this with ALREADY-SCRUBBED text, always. The order is the rule
    (§13.12 point 6): scrubbing truncated text can leave the first half of a
    secret in the graph, matched by nothing and greppable by anyone, whereas
    truncating scrubbed text can only ever cut a redaction marker in half.
    The true length is returned rather than the stored one so a cut stays
    visible instead of reading as naturally short text.
    """
    if not isinstance(text, str):
        return None, None, None
    length = len(text)
    if length > EVENT_TEXT_MAX:
        return text[:EVENT_TEXT_MAX], length, True
    return text, length, False


def coerce(prop: str, value):
    """One payload value -> the declared type for `prop`, or ``None`` if it
    cannot be coerced.

    Returns `None` rather than raising, and the caller counts the drop
    (`event_fields_dropped` on the sync status). The reason is the
    availability one spelled out on `EVENT_FIELD_TYPES`: a field this
    connector refuses to coerce would otherwise be refused SERVER-side,
    which 400s the whole batch and parks the watermark behind it on every
    subsequent tick. One missing property on one event is a visible
    degradation; a stalled sync is a silent one.

    `bool` is checked before `int` deliberately — `bool` is a subclass of
    `int` in Python, so `int(True)` would quietly turn a flag into `1`.
    """
    declared = EVENT_FIELD_TYPES.get(prop)
    if declared is None or value is None:
        return None
    try:
        if declared is bool:
            return bool(value)
        if declared is str:
            if isinstance(value, (dict, list)):
                return None
            return str(value)
        if isinstance(value, bool):
            # A boolean where a number is declared is a shape change, not a 1.
            return None
        if declared is int:
            return int(value)
        if declared is float:
            return float(value)
    except (TypeError, ValueError):
        return None
    return None
