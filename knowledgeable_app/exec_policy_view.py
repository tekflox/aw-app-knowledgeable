"""The effective-policy TABLE as §7's maintenance page renders it — rows,
states, sources and provenance (docs/design/exec-index-per-tool-policy.md §7;
slice 3).

**Why this is its own module and not three more methods on ``PolicyTable``.**
One of the two things §7 needs is ``contributes.mcp.provides``, and §12 risk 3
is unambiguous about it: *never feed ``provides`` into enforcement.* It is a
hand-maintained marketplace list that predates the ``indexing`` field, it goes
stale, and it is exactly the registry this design exists to avoid trusting.
Writing "display only" in a docstring inside ``exec_policy`` would be a
comment; reading it from a module that the sync path does not import is a
structure. ``exec_sync`` imports ``exec_policy``; nothing on the tick's path
imports this file, so there is no call graph along which a ``provides`` entry
could reach a ``Decision``. That is the boundary made visible in the code
rather than only in the doc, which is what the card asked for.

The same split is why every decision here comes from ``PolicyTable.effective``
rather than being recomputed: this module decides which ROWS exist, never what
a row's answer is.

**The five states are the deliverable, not the decoration** (§7, card point
f). Under a blacklist ``never_declared`` and ``declared_indexable`` behave
identically — both index both axes — so if the page renders them the same,
nobody can ever tell which tools have actually been reviewed. They are
different rows here, with different labels, and that difference is the single
most load-bearing thing on the page. ``unresolved`` gets its own section
carrying RAW observed spellings, because a fourth gateway prefix is supposed
to be diagnosed from this page instead of from the next incident — the whole
original defect was a name that did not match.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from . import exec_policy, exec_policy_store, exec_scrub

log = logging.getLogger("aw_apps.knowledgeable.exec_policy_view")

#: Which layer a row's current value came from — §7's "source" column, and
#: §11's warning about it: "three sources per row make the UI's source column
#: load-bearing; if it ever lies, humans will edit the wrong layer and
#: conclude the system ignores them". So it is derived from the very
#: ``Decision.state`` the engine produced, never recomputed alongside it.
_SOURCE_BY_STATE = {
    exec_policy.STATE_FLOOR: "floor",
    exec_policy.STATE_OVERRIDE: "override",
    exec_policy.STATE_DECLARED_RESTRICTED: "manifest",
    exec_policy.STATE_DECLARED_INDEXABLE: "manifest",
    exec_policy.STATE_NEVER_DECLARED: "blacklist-default",
    exec_policy.STATE_UNRESOLVED: "fail-closed",
}

#: Human labels for the states, kept here rather than in the page's JS so the
#: API response is self-describing (and so a CLI reading `GET /exec-policy`
#: gets the same words a human sees).
_LABEL_BY_STATE = {
    exec_policy.STATE_FLOOR: "floor — never indexable",
    exec_policy.STATE_OVERRIDE: "override — set by a human",
    exec_policy.STATE_DECLARED_RESTRICTED: "declared — restricted",
    exec_policy.STATE_DECLARED_INDEXABLE: "declared — indexable",
    exec_policy.STATE_NEVER_DECLARED: "never declared",
    exec_policy.STATE_UNRESOLVED: "unresolved — denied",
}


def display_only_provides(root: str | None = None) -> dict[str, list[str]]:
    """``{app_slug: [tool, ...]}`` from each installed app's
    ``contributes.mcp.provides`` — **display only, never enforcement**
    (§12 risk 3).

    Read here, in the view module, precisely so that there is no path from
    this data to a policy decision; see this module's own docstring. What it
    buys the page is the one thing sources (a) and (c) cannot: a tool an app
    says it ships, which nobody has declared an ``indexing`` entry for and
    which the sync has not happened to observe yet, still appears as a row a
    human can review. A stale entry here costs a phantom row on a settings
    page, which is the correct price for that.

    Failure is per-app and silent by design: this is cosmetic enumeration, and
    an unreadable manifest already has a loud, enforcement-side consequence
    (``policy_manifest_unreadable``, both axes denied for that app). Reporting
    it twice, from a cosmetic reader, would add noise and no information.
    """
    root = root or exec_policy._apps_root()
    out: dict[str, list[str]] = {}
    try:
        slugs = sorted(os.listdir(root))
    except OSError:
        return out
    for slug in slugs:
        path = os.path.join(root, slug, "aw-app.json")
        try:
            with open(path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
        except (OSError, ValueError):
            continue
        if not isinstance(loaded, dict):
            continue
        contributes = loaded.get("contributes")
        mcp = contributes.get("mcp") if isinstance(contributes, dict) else None
        provides = mcp.get("provides") if isinstance(mcp, dict) else None
        if not isinstance(provides, list):
            continue
        tools = sorted({t for t in provides if isinstance(t, str) and t})
        if tools:
            out[slug] = tools
    return out


def build(
    table: exec_policy.PolicyTable,
    *,
    overrides: dict[tuple[str, str], dict],
    acknowledged: dict[str, dict],
    observed: list[tuple[str | None, str]],
    observed_at: str = "",
    provides: dict[str, list[str]] | None = None,
    unresolved: list[str] | None = None,
    unresolved_known: list[str] | None = None,
    store_errors: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    """The whole payload ``GET /exec-policy`` returns and the page renders.

    ``unresolved`` / ``unresolved_known`` come from the freshness sentinel the
    last tick stamped (``exec_sync.policy_unresolved``), not from this table:
    a page load resolves no rows, so the table this function is handed has an
    empty observation of its own. The sentinel is the only thing that saw the
    names, which is the same reason the status route reads them from there.
    """
    provides = provides or {}
    rows: dict[tuple[str, str], dict] = {}

    def _row(app: str | None, tool: str, source_tag: str) -> None:
        """Add ``(app, tool)``, or just record another reason it is listed."""
        # A floor tool is keyed on the TOOL ALONE, never on `(app, tool)`.
        # The floor is an invariant on four names, not a per-app setting, and
        # the sources below reach them by different routes: the floor block
        # adds them unattributed, `provides`/`indexing` add them as
        # `secrets`', and an observed bare name adds them again. Keyed per app
        # that produced EIGHT locked rows for four tools — the same invariant
        # listed twice, reading as two different tools, which is precisely the
        # kind of thing §7 says the page must not do. Caught on the first real
        # screenshot rather than by a fixture, because no fixture had the real
        # tree's `provides` on `apps/secrets`.
        is_floor = tool in exec_scrub.SECRET_TOOL_NAMES
        key = ("", tool) if is_floor else (app or "", tool)
        existing = rows.get(key)
        if existing is not None:
            if source_tag not in existing["listed_because"]:
                existing["listed_because"].append(source_tag)
            # Attribute a floor row to the app that actually serves it, once
            # some source says so — "write_secret, from secrets" is more use
            # to a reader than "write_secret, unattributed", and the first
            # source to mention it is the unattributed floor block.
            if is_floor and app and existing["app"] is None:
                existing["app"] = app
            return
        decision = table.effective(app, tool)
        override = overrides.get((app or "", tool)) if app else None
        declared = table.declared_axes(app, tool) if app else None
        rows[key] = {
            "app": app,
            "tool": tool,
            "call": decision.allow_call,
            "output": decision.allow_output,
            "state": decision.state,
            "state_label": _LABEL_BY_STATE.get(decision.state, decision.state),
            "source": _SOURCE_BY_STATE.get(decision.state, "unknown"),
            # Rendered locked, no toggle (§7). The floor is an invariant, and
            # a UI that offers a switch for it is a UI that teaches an
            # operator the invariant is negotiable.
            "locked": decision.state == exec_policy.STATE_FLOOR,
            # Present whenever a human has one on this row — who/when/note, so
            # the page can show "a human set this, and here is why".
            "override": override,
            # What the MANIFEST says, independently of what currently wins.
            # Shown next to an override so an operator can see exactly what
            # they are overriding, which is the difference between an
            # override and a mystery.
            "declared": (
                None if declared is None
                else {"call": declared[0], "output": declared[1]}
            ),
            # The pointer to the file to fix, when this app's manifest could
            # not be read at all (every routed tool of such an app fails
            # closed — the §12 risk 11 fix).
            "manifest_error": table.manifest_errors.get(app or ""),
            "listed_because": [source_tag],
        }

    # (floor) Always present, always first, whether or not anything has
    # observed them: the four names are the invariant, and a page that only
    # showed them once they had been CALLED would be a page that goes quiet
    # exactly when the estate is behaving.
    for tool in sorted(exec_scrub.SECRET_TOOL_NAMES):
        _row(None, tool, "floor")
    # (a) manifest `indexing` entries — the declarations themselves.
    for app, tool in table.declared_identities():
        _row(app, tool, "manifest")
    # Overrides, including one on a tool no other source mentions: a human
    # who pre-emptively restricted a tool must be able to see their own row.
    for app, tool in table.override_identities():
        _row(app, tool, "override")
    # (b) `contributes.mcp.provides` — DISPLAY ONLY, see module docstring.
    for app, tools in sorted(provides.items()):
        for tool in tools:
            _row(app, tool, "provides")
    # (c) what the sync has actually resolved.
    for app, tool in observed:
        _row(app, tool, "observed")

    ordered = sorted(
        rows.values(),
        # Floor first, then by app (unattributed last), then by tool. The
        # floor leading is not alphabetical vanity: it is the one group whose
        # rows answer "can this be changed at all", and the answer is no.
        key=lambda r: (
            0 if r["locked"] else 1,
            r["app"] or "￿",
            r["tool"],
        ),
    )

    known_names = set(unresolved_known or [])
    known_upstreams = table.known_upstreams()
    unresolved_rows = []
    for name in unresolved or []:
        upstream, _routed = exec_policy.parse_identity(name)
        ack = acknowledged.get(upstream or "")
        unresolved_rows.append({
            # The RAW spelling, verbatim. §7's reason for this section: the
            # original defect was a name that did not match, and a fourth
            # gateway prefix should be diagnosed from here rather than from
            # the next incident. Truncating or normalising it would delete
            # the only evidence the page carries.
            "name": name,
            "upstream": upstream,
            "tool": exec_scrub.tool_name_segment(name),
            # Already accounted for — by this code (`UNRESOLVABLE_UPSTREAMS`)
            # or by a human. Either way its arguments and result are still
            # dropped; `known` is about the COUNTER, never the denial.
            "known": name in known_names,
            "acknowledged": ack,
            "reason": (ack or {}).get("reason") or known_upstreams.get(upstream or "") or "",
            # Nothing a human can acknowledge without an upstream segment to
            # key it on — a bare unnamed row, or a third-party `mcp__` server
            # with no `aw__` marker, has none.
            "acknowledgeable": bool(upstream),
        })

    counts = {
        "rows": len(ordered),
        "floor": sum(1 for r in ordered if r["state"] == exec_policy.STATE_FLOOR),
        "override": sum(1 for r in ordered if r["state"] == exec_policy.STATE_OVERRIDE),
        "declared_restricted": sum(
            1 for r in ordered if r["state"] == exec_policy.STATE_DECLARED_RESTRICTED
        ),
        "declared_indexable": sum(
            1 for r in ordered if r["state"] == exec_policy.STATE_DECLARED_INDEXABLE
        ),
        "never_declared": sum(
            1 for r in ordered if r["state"] == exec_policy.STATE_NEVER_DECLARED
        ),
        "unresolved_rows": sum(
            1 for r in ordered if r["state"] == exec_policy.STATE_UNRESOLVED
        ),
        "unresolved_names": len(unresolved_rows),
        "unresolved_actionable": sum(1 for r in unresolved_rows if not r["known"]),
    }
    return {
        "rows": ordered,
        "unresolved": unresolved_rows,
        "acknowledged": sorted(acknowledged.values(), key=lambda r: r["upstream"]),
        # The built-in half of the acknowledge registry, so the page can show
        # that these two are accounted for by this code rather than leaving an
        # operator wondering why they are not red.
        "known_upstreams": exec_policy.UNRESOLVABLE_UPSTREAMS,
        "counts": counts,
        "table": table.status,
        "observed_at": observed_at,
        "observed_count": len(observed),
        "observed_capped": len(observed) >= exec_policy_store.OBSERVED_CAP,
        # Every way this page can be showing less than the truth, named. A
        # settings page that silently degrades is how the estate's own named
        # failure mode gets one more instance.
        "store_errors": {k: v for k, v in (store_errors or {}).items() if v},
        "states": _LABEL_BY_STATE,
    }
