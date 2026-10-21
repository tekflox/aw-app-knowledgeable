"""The human-editable half of the per-tool indexing policy — overrides,
acknowledgements, and the identities the sync has actually observed
(docs/design/exec-index-per-tool-policy.md §6, §7, §8; slice 3).

**Where this lives, and why not where it would have been easier.** Everything
here rides ``ctx.state.kv`` — the state facade, gated by ``state:own``,
Redis-backed and cross-worker safe — under its own dedicated key namespace.
§8's seam table says exactly that, and §6 and §10 both say what it is NOT:
the app-config blob. This app's own ``POST /settings`` route has clobbered a
sibling config key before (the ``service_secret`` incident, recorded in this
workspace's memory), and the store that can change what gets indexed must not
ride a JSON blob a form rewrites wholesale. The routes in :mod:`routes` are
the only door; the settings form cannot reach these keys at all.

**The floor is not expressible here** (§2). :func:`put_override` refuses the
four secret-store tool names before anything is written, so the invariant
holds at the EDGE as well as in the engine. ``exec_policy``'s floor check
runs first regardless and a lying row would lose there too — that test exists
and passes — but a store that can hold a row claiming ``write_secret`` is
indexable is a store whose contents cannot be read at face value by a human,
and §7's whole point is that a human can read this.

**What TTL means on a key that must not expire.** ``ctx.state.kv`` has no
immortal keys by design (``state_facade._KvNamespace.put`` requires
``ttl > 0``), and a human's decision to restrict a tool is not transient. So
these use the same ~100-year stand-in, and for the same stated reason, as
``routes.FS_SYNC_VALVE_KV_TTL``: a silent expiry of a gate is this estate's
named failure mode. Every write re-stamps the TTL on the whole blob, so the
clock only ever runs from the last edit.

**Redis down is reported, never flattened into "nothing stored".** This is
§12 risk 11's lesson applied to a second read path: under a blacklist an
empty override map means *a human restricted nothing*, so "I could not read
the overrides" must not be spelled the same way. Every loader here returns
``(value, error)`` exactly as ``exec_policy._declared_entries`` does. Unlike
a manifest read, this one cannot fail CLOSED — an unreadable store does not
say which tools a human had restricted, so there is nothing to deny — which
makes being loud the only available defence: the error rides the sentinel as
``policy_overrides_unreadable``, degrades the doctor, and shows as a banner on
the §7 page. The floor is unaffected either way, being hard-coded.

**One blob per concern, not one key per row.** ``state_facade`` validates
every kv name against ``^[a-z0-9][a-z0-9_.-]{0,127}$``, and real tool names
do not all fit it (``API-retrieve-a-page`` has capitals; a federated identity
has more shapes than that regex allows). Encoding an identity into a key name
would mean inventing an escaping scheme whose bugs would silently lose or
merge a human's decisions. Three fixed key names holding JSON lists have no
such failure mode, and the data is tiny by construction — overrides are
exceptions, not a registry of 662 tools (§10's dissolved objection).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from . import exec_policy, exec_scrub

log = logging.getLogger("aw_apps.knowledgeable.exec_policy_store")

#: ``ctx.state.kv`` names. The ``exec_policy.`` prefix is the "dedicated
#: namespace" §8 asks for — greppable, and far enough from this app's other
#: kv users (``git_last_state``, ``fs_sync_valve_tripped``) that no future
#: wholesale clear of one can take the others with it.
OVERRIDES_KV_NAME = "exec_policy.overrides"
ACK_KV_NAME = "exec_policy.acknowledged"
OBSERVED_KV_NAME = "exec_policy.observed"

#: See the module docstring: ``ctx.state.kv`` has no immortal keys, and a
#: human's policy decision is not transient. Same value and same reasoning as
#: ``routes.FS_SYNC_VALVE_KV_TTL``.
KV_TTL = 60 * 60 * 24 * 365 * 100  # ~100 years — "never", as closely as the facade allows

#: How many observed identities the store keeps (§7's "kept by the policy
#: layer in the state facade, capped"). Generous against the ~660 tools this
#: gateway actually serves, bounded so a pathological burst of distinct bare
#: names cannot grow one Redis value without limit. Oldest-dropped is NOT the
#: eviction rule — see :func:`merge_observed`.
OBSERVED_CAP = 1000

#: What a loader reports when the stored value was present but not the shape
#: this module writes. Not an exception, so it needs a name of its own — and
#: it is the same failure in the same direction as an unreachable Redis, so it
#: must not be the one shape that still reads as "a human restricted nothing".
MALFORMED = "MalformedValue"

#: What a loader reports when the state facade's own circuit breaker is open,
#: i.e. Redis is unreachable and ``kv.get`` degraded to a miss rather than
#: raising (``state_facade``'s documented degrade-open semantics).
UNREACHABLE = "StateFacadeDegraded"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _facade_degraded() -> bool:
    """Whether ``ctx.state.kv`` is currently degraded in THIS worker.

    ``kv.get`` swallows a Redis failure and returns ``None`` — the facade
    degrades open on purpose — which makes "absent" and "unreachable"
    indistinguishable from the return value alone. ``state_facade.
    breaker_status()`` is the documented snapshot of that fact (its own
    docstring: "Snapshot for ``doctor``"), and the breaker is tripped by the
    very call that just failed, so reading it immediately after a miss is
    accurate rather than a guess about some earlier window.

    Same privileged-but-read-only core import, with the same fallback, as
    ``exec_policy._apps_root``: this connector is Tier-1, so core's process IS
    the one that owns the answer. Out of process, or in tests, there is no
    breaker to read and the honest answer is "no evidence of a failure".
    """
    try:
        from src.apps import state_facade  # noqa: PLC0415

        return bool(state_facade.breaker_status().get("degraded"))
    except Exception:  # pragma: no cover - import shape, not logic
        return False


async def _load_rows(kv, name: str) -> tuple[list[dict], str | None]:
    """``(rows, error)`` for one of this module's three blobs.

    The ``(value, error)`` shape is not decoration: see the module docstring.
    A missing key with a healthy Redis is ``([], None)`` — the genuine empty
    state, which is where this feature starts life and where most workspaces
    will stay.
    """
    try:
        raw = await kv.get(name)
    except Exception as exc:  # noqa: BLE001 - facade degrades open; a raise is still possible out of process
        log.warning("exec policy store: %s unreadable — %s", name, type(exc).__name__)
        return [], type(exc).__name__
    if raw is None:
        return ([], UNREACHABLE) if _facade_degraded() else ([], None)
    if not isinstance(raw, dict) or not isinstance(raw.get("rows"), list):
        log.warning("exec policy store: %s holds an unexpected shape — treating as unreadable", name)
        return [], MALFORMED
    return [r for r in raw["rows"] if isinstance(r, dict)], None


async def _save_rows(kv, name: str, rows: list[dict]) -> None:
    await kv.put(name, {"version": 1, "rows": rows, "at": _now()}, ttl=KV_TTL)


# --- overrides ---------------------------------------------------------------

class FloorRefusal(ValueError):
    """An override tried to name one of the four secret-store tools."""


class BadIdentity(ValueError):
    """An override named something that is not an ``(app, bare tool)`` pair."""


def _check_identity(app: Any, tool: Any) -> tuple[str, str]:
    """Validate an override's identity, refusing the floor FIRST.

    Three refusals, in this order, and the order is the point:

    1. **The floor** (§2) — before any shape check, so no amount of creative
       spelling gets a floor name as far as the store. Checked through
       ``exec_scrub.tool_name_segment``, not string equality, because a
       prefixed ``…aw__aw_secrets__write_secret`` IS ``write_secret`` and the
       138-call defect was precisely a name that did not match (§12 risk 6 —
       one segmentation function, never two).
    2. **Bare segment only** (§4) — the same rule the marketplace validator
       enforces on a manifest entry. A prefixed spelling in a manifest can
       never be enshrined; it must not be enshrinable here either, or the
       store and the manifest would disagree about what a tool is called.
    3. **An app slug is required.** An override has to say which app's tool it
       governs: §3 step 6's bare-name path takes the INTERSECTION of every
       matching declaration, so an app-less override row would silently
       restrict same-named tools across every app — §11's named surprise,
       granted deliberately to an unattributable observed call, not to a
       human filling in a form.
    """
    tool_str = tool if isinstance(tool, str) else ""
    segment = exec_scrub.tool_name_segment(tool_str)
    if segment in exec_scrub.SECRET_TOOL_NAMES:
        raise FloorRefusal(
            f"{segment!r} is one of the four secret-store tools: their arguments and "
            "results are never indexable, by a hard-coded floor that no manifest "
            "entry and no override can lift (design §2). Nothing was stored."
        )
    if not isinstance(tool, str) or not tool.strip():
        raise BadIdentity("tool is required")
    if "__" in tool or "." in tool:
        raise BadIdentity(
            f"tool {tool!r} must be a bare terminal segment — no '__' and no '.' "
            "(design §4, the same rule the manifest validator enforces)"
        )
    if not isinstance(app, str) or not app.strip():
        raise BadIdentity(
            "app is required — an override names one app's tool, never every "
            "same-named tool in the estate (design §3 step 6, §11)"
        )
    return app.strip(), tool.strip()


async def load_overrides(kv) -> tuple[dict[tuple[str, str], dict], str | None]:
    """``({(app, tool): row}, error)`` — every override a human has set.

    Later rows win a duplicated identity, which cannot happen through
    :func:`put_override` (it replaces in place) and is only reachable if
    something else wrote the blob. Last-wins rather than an intersection here
    on purpose: this is not ambiguity to be tightened, it is one corrupted
    value, and the page shows the row it actually resolved to.
    """
    rows, error = await _load_rows(kv, OVERRIDES_KV_NAME)
    out: dict[tuple[str, str], dict] = {}
    for row in rows:
        app, tool = row.get("app"), row.get("tool")
        if not isinstance(app, str) or not isinstance(tool, str) or not app or not tool:
            continue
        out[(app, tool)] = {
            "app": app,
            "tool": tool,
            # `is True`, never truthiness: a stored `"false"` must deny, the
            # same coercion direction `_declared_entries` applies to a
            # manifest. This is a security default and every coercion in it
            # tightens.
            "call": row.get("call") is True,
            "output": row.get("output") is True,
            "note": str(row.get("note") or ""),
            "actor": str(row.get("actor") or "unknown"),
            "at": str(row.get("at") or ""),
        }
    return out, error


def axes(overrides: dict[tuple[str, str], dict]) -> dict[tuple[str, str], tuple[bool, bool]]:
    """Just the booleans, in the shape ``PolicyTable`` takes. The who/when/note
    is provenance for §7's UI and has no business reaching a decision."""
    return {key: (row["call"], row["output"]) for key, row in overrides.items()}


async def put_override(
    kv, *, app: str, tool: str, call: bool, output: bool, note: str, actor: str
) -> dict:
    """Write (or replace) one override. Raises :class:`FloorRefusal` /
    :class:`BadIdentity` before touching the store — see :func:`_check_identity`.

    Read-modify-write on one blob. Not guarded by a lease: these are
    hand-edited exceptions, so two concurrent writes means two humans clicking
    different rows in the same instant, and the loser's row would simply need
    clicking again. A lease here would buy a 409 nobody can act on better than
    the retry they would make anyway.
    """
    app, tool = _check_identity(app, tool)
    rows, error = await _load_rows(kv, OVERRIDES_KV_NAME)
    if error:
        # Refuse rather than write: a read-modify-write on a blob that could
        # not be read would DELETE every other override in it. Losing a
        # human's opt-out silently is the failure this whole card exists
        # because of.
        raise RuntimeError(
            f"the override store could not be read ({error}) — refusing to write, "
            "because replacing a blob that was not read would discard every other "
            "override in it"
        )
    row = {
        "app": app,
        "tool": tool,
        "call": bool(call),
        "output": bool(output),
        "note": str(note or ""),
        "actor": actor,
        "at": _now(),
    }
    kept = [
        r for r in rows
        if not (r.get("app") == app and r.get("tool") == tool)
    ]
    kept.append(row)
    await _save_rows(kv, OVERRIDES_KV_NAME, kept)
    log.info(
        "exec policy: override set by %s — %s/%s call=%s output=%s",
        actor, app, tool, row["call"], row["output"],
    )
    return row


async def delete_override(kv, *, app: str, tool: str, actor: str) -> bool:
    """Drop one override, returning whether there was one. The row falls back
    to whatever the manifest says, or to the blacklist default — which can be
    a WIDENING, so the §7 page says so on the confirm row rather than treating
    a delete as housekeeping."""
    rows, error = await _load_rows(kv, OVERRIDES_KV_NAME)
    if error:
        raise RuntimeError(
            f"the override store could not be read ({error}) — refusing to write"
        )
    kept = [r for r in rows if not (r.get("app") == app and r.get("tool") == tool)]
    if len(kept) == len(rows):
        return False
    await _save_rows(kv, OVERRIDES_KV_NAME, kept)
    log.info("exec policy: override cleared by %s — %s/%s", actor, app, tool)
    return True


# --- acknowledgements --------------------------------------------------------

async def load_acknowledged(kv) -> tuple[dict[str, dict], str | None]:
    """``({upstream: row}, error)`` — the upstreams a human has accepted as
    attributable to nothing, on purpose (§9's escalation to slice 3).

    **This is not an exemption and the code must never let it become one.**
    The returned reasons reach ``PolicyTable(acknowledged=...)``, which uses
    them for ``unresolved_split`` CLASSIFICATION only; no path from here
    touches ``allow_call``/``allow_output``. An acknowledged upstream's names
    still fail closed — arguments and result dropped, name and timestamp kept.

    What it buys is the one thing an always-red check cannot give: a doctor
    line that goes green when everything unresolvable has been accounted for,
    and therefore goes red again when something NEW appears. Two of this
    gateway's 40 live upstream segments (``fredericowu``,
    ``fredericowu_browser`` — linked-workspace BYOD federations with no
    ``apps/<slug>/mcp.json``) will never resolve, and before this they pinned
    the check red forever. A check that is always red detects nothing, which
    is the same defect as a date-hardcoded test that goes red every day until
    nobody reads it.
    """
    rows, error = await _load_rows(kv, ACK_KV_NAME)
    out: dict[str, dict] = {}
    for row in rows:
        upstream = row.get("upstream")
        if not isinstance(upstream, str) or not upstream:
            continue
        out[upstream] = {
            "upstream": upstream,
            "reason": str(row.get("reason") or ""),
            "actor": str(row.get("actor") or "unknown"),
            "at": str(row.get("at") or ""),
        }
    return out, error


def ack_reasons(acknowledged: dict[str, dict]) -> dict[str, str]:
    """``{upstream: reason}`` in the shape ``PolicyTable`` takes."""
    return {k: row["reason"] for k, row in acknowledged.items()}


def _ack_key(upstream: str) -> str:
    """Normalise a typed upstream to the segment the GATEWAY spells.

    Through ``exec_policy.sanitise_server_key`` — the one named place that
    copy of the gateway's transform lives (§12 risk 1) — rather than a second
    ``replace('-', '_')`` here. The acknowledgement is matched against
    ``Decision.upstream``, which is always the sanitised form, so an operator
    who reasonably types the dashed name of a linked workspace
    (``fredericowu-browser``) would otherwise store a row that can never
    match anything: the alarm stays red, the page shows the acknowledgement,
    and the two disagree with nothing saying why. A silently ineffective
    control is worse than a missing one.
    """
    return exec_policy.sanitise_server_key(upstream.strip())


async def put_acknowledgement(kv, *, upstream: str, reason: str, actor: str) -> dict:
    """Accept one unresolvable upstream. A reason is REQUIRED, for the same
    reason §4 requires one on a manifest opt-out: an acknowledgement nobody
    can review is the rot this design exists to avoid, and this one silences
    an alarm."""
    if not isinstance(upstream, str) or not upstream.strip():
        raise BadIdentity("upstream is required")
    if "__" in upstream or "." in upstream:
        raise BadIdentity(
            f"upstream {upstream!r} must be a single sanitised segment as the gateway "
            "spells it (e.g. 'fredericowu_browser'), not a whole tool name"
        )
    if not isinstance(reason, str) or not reason.strip():
        raise BadIdentity(
            "reason is required — an acknowledgement silences an alarm, so why it is "
            "safe has to be readable by the next person (design §4's rule, applied here)"
        )
    rows, error = await _load_rows(kv, ACK_KV_NAME)
    if error:
        raise RuntimeError(
            f"the acknowledgement store could not be read ({error}) — refusing to write"
        )
    upstream = _ack_key(upstream)
    row = {"upstream": upstream, "reason": reason.strip(), "actor": actor, "at": _now()}
    kept = [r for r in rows if r.get("upstream") != upstream]
    kept.append(row)
    await _save_rows(kv, ACK_KV_NAME, kept)
    log.info("exec policy: upstream %s acknowledged by %s — %s", upstream, actor, row["reason"])
    return row


async def delete_acknowledgement(kv, *, upstream: str, actor: str) -> bool:
    """Un-acknowledge an upstream: its names go back into the actionable
    bucket on the next tick, and the doctor goes red again if they are still
    being observed. Reversible on purpose — an acknowledgement is a judgement,
    and a judgement someone cannot withdraw is not reviewable."""
    rows, error = await _load_rows(kv, ACK_KV_NAME)
    if error:
        raise RuntimeError(
            f"the acknowledgement store could not be read ({error}) — refusing to write"
        )
    # Same normalisation as the writer, or a dashed spelling could never
    # withdraw what it created.
    upstream = _ack_key(upstream)
    kept = [r for r in rows if r.get("upstream") != upstream]
    if len(kept) == len(rows):
        return False
    await _save_rows(kv, ACK_KV_NAME, kept)
    log.info("exec policy: acknowledgement of %s withdrawn by %s", upstream, actor)
    return True


# --- observed identities (§7 row population, source (c)) --------------------

async def load_observed(kv) -> tuple[list[tuple[str | None, str]], str | None, str]:
    """``(identities, error, at)`` — the ``(app, tool)`` pairs the sync has
    actually resolved, and when they were last recorded.

    ``at`` is returned because an empty list has two very different meanings
    and the page must not present them alike: nothing observed YET (the sync
    has never run — ``exec_sync_enabled`` ships false), versus observed and
    genuinely nothing there. The page shows the timestamp, or says the sync
    has not recorded anything yet.
    """
    try:
        raw = await kv.get(OBSERVED_KV_NAME)
    except Exception as exc:  # noqa: BLE001
        return [], type(exc).__name__, ""
    if raw is None:
        return ([], UNREACHABLE, "") if _facade_degraded() else ([], None, "")
    if not isinstance(raw, dict) or not isinstance(raw.get("rows"), list):
        return [], MALFORMED, ""
    out: list[tuple[str | None, str]] = []
    for row in raw["rows"]:
        if not isinstance(row, dict):
            continue
        tool = row.get("tool")
        if not isinstance(tool, str) or not tool:
            continue
        app = row.get("app")
        out.append((app if isinstance(app, str) and app else None, tool))
    return out, None, str(raw.get("at") or "")


def merge_observed(
    stored: list[tuple[str | None, str]],
    fresh: list[tuple[str | None, str]],
) -> list[tuple[str | None, str]]:
    """Union, sorted, capped at :data:`OBSERVED_CAP`.

    A union rather than a replacement: one tick sees one bounded slice of runs
    (``MAX_RUNS_PER_TICK``), so replacing would make the page's row list
    flicker with whatever the last ten minutes happened to contain, and §7
    wants the list of tools that run HERE, not the ones that ran just now.

    The cap drops in sort order, not by age, which is a deliberate trade: age
    is not stored (one timestamp for the blob, not per row — a timestamp per
    identity would be a usage log this feature has no business keeping), and a
    stable, explainable truncation beats a wrong claim about recency. At the
    cap the page says how many were dropped rather than quietly showing a
    prefix — the measured surface is ~660 tools against a cap of 1000, so
    reaching it means something changed that someone should look at.
    """
    merged = sorted(
        set(stored) | set(fresh),
        key=lambda pair: (pair[0] or "", pair[1]),
    )
    return merged[:OBSERVED_CAP]


async def record_observed(kv, fresh: list[tuple[str | None, str]]) -> int:
    """Merge this tick's resolved identities into the store. Returns how many
    are now kept.

    Best-effort by construction: this is the page's row list, not a policy
    input, so a failed write costs a slightly shorter list on a settings page
    and nothing else. It is NOT allowed to affect the tick's own result, which
    is why the caller treats a raise here as a log line.
    """
    if not fresh:
        return 0
    stored, error, _at = await load_observed(kv)
    if error:
        # Same rule as the override writer: never replace a blob that could
        # not be read. Here the cost of refusing is only a stale page.
        log.debug("exec policy: observed identities not recorded — store unreadable (%s)", error)
        return len(stored)
    merged = merge_observed(stored, fresh)
    await kv.put(
        OBSERVED_KV_NAME,
        {
            "version": 1,
            "at": _now(),
            "rows": [{"app": app, "tool": tool} for app, tool in merged],
        },
        ttl=KV_TTL,
    )
    return len(merged)
