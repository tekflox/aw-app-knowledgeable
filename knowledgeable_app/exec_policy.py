"""Per-tool exec-indexing policy — identity resolution and the per-axis
decision (docs/design/exec-index-per-tool-policy.md).

**What this module decides.** For one observed tool name, whether its CALL
arguments and its OUTPUT may be indexed into the execution graph. The model
is a blacklist: a tool nobody declared is indexable. The declaration lives in
the manifest of the app that contributes the tool
(``contributes.mcp.indexing``, validated core-side by
``src/apps/manifest.py::_validate_mcp_indexing`` — slice 1), so it arrives
and leaves with the app instead of rotting in a central list (§10's
dissolved §13.10 objection).

**What it is a replacement for, and why that history is load-bearing.** The
pair of hard-coded names this layer sits on top of never fired in
production: `SECRET_TOOL_NAMES` held bare names and the drop was an exact
match, while every real MCP-routed call arrives gateway-prefixed. 138
secret-tool calls landed with their arguments intact; 29 of 34
``write_secret`` calls carry no redaction marker at all. Slice 0 fixed the
recognition (``exec_scrub.tool_name_segment``); this module is the policy
layer on top of it, and it is written to the same standard, which means:

* **Fail closed, everywhere.** A name that parses as MCP-routed but cannot
  be attributed to an installed app DENIES both axes — arguments dropped,
  name and timestamp kept — and is counted and surfaced on the doctor.
  "No opt-out found, therefore index" is the defect this card exists to
  delete; it must not survive as a default anywhere in here.
* **The floor is checked FIRST**, before any registry, manifest or override
  lookup (§2, §3 step 2). The four secret-store tools are never indexable
  on either axis, whatever a manifest or an override row says. An invariant
  must not be expressible as data — and the override store is exactly the
  kind of data this app has already clobbered once (the ``service_secret``
  incident).
* **One segmentation function, never two** (§12 risk 6). The terminal
  segment comes from ``exec_scrub.tool_name_segment`` and the floor set from
  ``exec_scrub.SECRET_TOOL_NAMES``; this module imports both rather than
  restating them, because a second copy is how one of the two paths
  re-learns ``endswith`` — and ``spread_secret`` ends with ``read_secret``
  and is not it.

**The join key is NOT the app slug** (§3, §10). The ``<upstream>`` segment of
an observed name is the app's ``mcp.json`` *server key*, sanitised:
``apps/secrets/mcp.json`` declares ``aw-secrets`` and the observed name is
``…aw__aw_secrets__write_secret``; ``apps/kb/mcp.json`` declares ``kb`` and
the observed name is ``…aw__kb__search_knowledge_base``. One app may declare
several keys (``apps/notion`` ships both ``notion`` and ``aw-kanban``). A
slug join resolves nothing, which under fail-closed would deny everything —
the loud version of wrong, but still wrong.

**The sanitiser is copied, not re-derived** (§12 risk 1):
``apps/mcp-gateway/back/gateway/upstream.py::public_name`` is
``f"{server.replace('-', '_')}__{tool}"``. See
:func:`sanitise_server_key`, which exists so there is one named place to
re-check against that function.

**Built once per tick, like the scrubber.** ``from_workspace()`` reads ~36
small JSON files; every decision after that is a dict lookup, memoised per
distinct name (§9e — no per-row I/O on a 300k-row backfill). There is no
module-level cache on purpose: core runs multi-worker and a worker-local
policy table is the WORKERS=N silent-divergence bug this estate has already
catalogued (§12 risk 5).

**Both axes are enforced since slice 4.** The CALL axis lands in
``build_toolcall_row`` (slice 2); the OUTPUT axis lands in
``attach_results`` (slice 4), against the ``result_text`` §13 slice 3
projects. The output axis is the more dangerous of the two and it is NOT
weaker: the four secret-store tools' results are already dropped wholesale
by the floor, so what the output axis actually governs is the middle ground
the floor cannot see — a tool that is not a secret-store tool but whose
OUTPUT carries a credential anyway (``remote_host_read_file`` on a
``.env``, a bash tool with a token in stdout, a config route echoing a
key). Those are otherwise governed by the pattern scrubber alone, which is
the weakest tier in this system and is explicitly the *fallback* for
vault-only credentials rather than a policy. ``output: false`` is how such a
tool says "not my output", and an unresolvable name denies it just as hard
as it denies the call axis (§3.5) — "unattributable, therefore index the
result" would make the output axis weaker than the call axis for no reason.

**``contributes.mcp.provides`` is never read here** (§12 risk 3). It is a
hand-maintained marketplace list, display-only, and exactly the stale
registry this design exists to avoid trusting.

**``mcp.json`` is read for its server KEYS and nothing else.** Those files
hold live API keys and tokens in their values; nothing in this module
inspects, logs or returns a value out of them.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from . import exec_scrub

log = logging.getLogger("aw_apps.knowledgeable.exec_policy")

#: The six states of §7, as they are reported on a decision. Behaviour is
#: carried by ``allow_call``/``allow_output``; the state is what makes a row
#: readable to a human — above all the distinction between "declared
#: indexable" and "never declared", which are behaviourally identical under
#: the blacklist and must never be visually identical (§7, card point f).
STATE_FLOOR = "floor"
STATE_OVERRIDE = "override"
STATE_DECLARED_RESTRICTED = "declared_restricted"
STATE_DECLARED_INDEXABLE = "declared_indexable"
STATE_NEVER_DECLARED = "never_declared"
STATE_UNRESOLVED = "unresolved"

#: What lands in ``args_text`` when policy denied the call axis.
#:
#: **Deliberately not the floor's treatment** (§12 risk 4). The floor drops
#: to a plain ``None``, which is also what §13.2's purge leaves behind on a
#: remediated row; if a policy drop looked the same, the verification queries
#: could no longer tell a remediated credential exposure from routine policy.
#: Two markers rather than one for the same reason one level down: a name
#: that failed closed because nothing could attribute it is a *diagnosis*
#: (gateway spelling #4, an uninstalled app), not a declared opt-out, and the
#: graph is where someone will notice it.
POLICY_DROP_MARKER = "<DROPPED:exec-policy>"
UNRESOLVED_DROP_MARKER = "<DROPPED:exec-policy-unresolved>"

#: How many distinct unresolved names the tick reports on the sentinel.
#: DISTINCT names, never row counts: ten thousand calls of one unattributable
#: name is ONE decision owed, the same reasoning as
#: ``event_kinds_unreviewed`` and ``vault_names_uncovered``. The cap is on
#: the *reported* list only — the tick counts them all.
UNRESOLVED_NAMES_CAP = 50

#: Stands in for an empty/absent tool name in the unresolved report, so a
#: row the sync could not attribute AT ALL is visible rather than rendering
#: as a blank line nobody can act on.
EMPTY_NAME_LABEL = "<empty>"

#: Upstream segments that are unresolvable BY CONSTRUCTION, each with the
#: reason — ``vault_coverage.VAULT_REVIEWED_NOT_CREDENTIAL``'s shape, and for
#: the same reason that dict exists rather than an empty tuple in the first
#: one: "never looked at" and "looked at, nothing to do" must stay
#: distinguishable at a glance, or the next person re-triages what someone
#: already cleared.
#:
#: **Why this registry is not an exemption.** Nothing here lifts a denial.
#: Every name routed through one of these upstreams still fails closed —
#: arguments and result dropped, name and timestamp kept — exactly as §3.5
#: says. What it changes is only the *counting*: a standing, permanent
#: condition must not be added to a number whose whole job is to say
#: "something new happened".
#:
#: **The defect this exists to prevent, measured on slice 2.** The doctor
#: goes red the first time a tick observes one of these and stays red
#: forever, because there is no manifest anyone could write that would ever
#: attribute them. An always-red check detects nothing — the same defect
#: class as ``vault_names_uncovered`` over ~20 pre-existing names, and as a
#: test that fails every day until nobody reads it. So
#: ``policy_denied_results`` excludes the unresolved path entirely (see
#: :attr:`PolicyTable.counters`) and the unresolved side itself reports an
#: *actionable* count that these do not enter.
#:
#: **Slice 3 built the human half of this, and the two are the same
#: mechanism.** This dict is the registry of upstreams THIS CODE knows can
#: never be attributed; ``acknowledged`` (``PolicyTable(acknowledged=...)``,
#: filled from the connector's state facade by ``exec_policy_store``) is the
#: registry of upstreams A HUMAN has looked at and accepted from the §7
#: maintenance page. :meth:`PolicyTable.unresolved_split` reads the UNION,
#: because the distinction between "the coder knew" and "an operator decided"
#: matters for provenance and not at all for counting. Acknowledging is NOT
#: permitting: an acknowledged upstream's names still fail closed, still drop
#: their arguments and result, and still appear on the page — they simply stop
#: being added to a number whose whole job is to say "something NEW happened".
#:
#: Slice 3 also made the doctor read the actionable count rather than the full
#: ``policy_unresolved`` list (``routes._exec_sync_status_payload``), which is
#: what finally stops it going permanently red on the two names below. That
#: was deliberately left to this slice rather than done from inside slice 2's
#: counter property: narrowing a shipped surface silently is how an alarm
#: quietly stops alarming.
#:
#: Entries are upstream SEGMENTS as the gateway spells them (already
#: sanitised — dash→underscore), not app slugs: there is no app.
UNRESOLVABLE_UPSTREAMS: dict[str, str] = {
    # Linked-workspace BYOD federations (`/link`): registered into the
    # gateway's OWN config, not into any `apps/<slug>/mcp.json`, so the
    # manifest-join this module is built on has nothing to join against.
    # Measured 2026-10-19 (slice 2): 38 of the gateway's 40 live upstream
    # segments resolve to the right app; these are the 2 that never will.
    # Their observed names nest the workspace name rather than a second
    # `aw__` marker — `…aw__fredericowu__fredericowu__google_workspace__…`,
    # `…aw__fredericowu_browser__fredericowu__playwright__…` — which is why
    # they are not the federated case `kali` is (§12 risk 7).
    "fredericowu": "linked-workspace federation — no apps/<slug>/mcp.json exists to attribute it",
    "fredericowu_browser": (
        "linked-workspace federation — no apps/<slug>/mcp.json exists to attribute it"
    ),
}

_DEFAULT_APPS_ROOT = "/opt/aw-workspace/apps"


def sanitise_server_key(key: str) -> str:
    """An ``mcp.json`` server key -> the segment the gateway puts in a tool
    name.

    **Copied from the gateway, not derived** (§12 risk 1):
    ``gateway/upstream.py::public_name`` is
    ``f"{server.replace('-', '_')}__{tool}"``, and the aggregated display
    server is ``<workspace_name>__<upstream>`` with the same replacement
    applied to the whole string (``gateway/config_gateway.py``). Since the
    workspace name carries no dash in this estate, sanitising the upstream
    key alone is the same transform — and this is the one function to
    re-check if the gateway's prefixing ever changes.

    A near-match here resolves nothing, and fail-closed turns that into "no
    arguments indexed anywhere": loud in ``policy_unresolved``, invisible in
    tests built from hand-written fixtures.
    """
    return key.replace("-", "_")


def split_envelope(name: str) -> tuple[bool, str]:
    """Strip one leading client envelope. Returns ``(had_envelope, rest)``.

    The three measured spellings (§3, §13.2) differ only in this envelope:

    * ``mcp__aw-gateway__aw__aw_secrets__write_secret``
    * ``aw-gateway.aw__aw_secrets__write_secret``
    * ``mcp__workspace-gateway__aw__aw_secrets__write_secret``

    **The envelope segment is NOT allowlisted.** Matching on the literal
    ``aw-gateway`` / ``workspace-gateway`` names is precisely how spelling #3
    was missed last time, and a fourth rename would be missed the same way.
    Anything between ``mcp__`` and the next ``__``, or anything before the
    first ``.``, is an envelope — what matters is the ``aw__`` marker that
    comes after it.
    """
    if name.startswith("mcp__"):
        _, sep, tail = name[len("mcp__"):].partition("__")
        return True, tail if sep else ""
    head, sep, tail = name.partition(".")
    if sep:
        return True, tail
    return False, name


def parse_identity(name: str) -> tuple[str | None, bool]:
    """``(upstream_segment, routed)`` for an observed tool name — §3 step 3.

    ``routed`` means "this name claims to come through an MCP server", which
    is the thing that must be attributable. Three outcomes:

    * ``(segment, True)`` — routed and the upstream segment is readable.
      ``aw__`` is the marker, with or without a client envelope: the
      gateway's own aggregated name has no envelope, and treating it as a
      bare harness name would send an attributable call down §3 step 6's
      intersection path instead.
    * ``(None, True)`` — routed but unattributable: an envelope with no
      ``aw__`` marker (a third-party ``mcp__…`` server), or an ``aw__`` with
      nothing after it. Fails closed in :meth:`PolicyTable.decide`.
    * ``(None, False)`` — no envelope and no marker: a bare harness name
      (``Bash``, ``run_agent_async``) or a direct-connected MCP tool
      arriving unprefixed.

    Federated nesting resolves to the OUTER upstream, which is the installed
    app (§12 risk 7): ``…aw__kali__aw__playwright__browser_click`` is
    ``kali``'s, not ``playwright``'s — the inner ``aw__`` is the trap, and
    only the outer segment names something this workspace installed.
    """
    had_envelope, rest = split_envelope(name)
    if rest.startswith("aw__"):
        head, sep, _ = rest[len("aw__"):].partition("__")
        if sep and head:
            return head, True
        return None, True
    return None, bool(had_envelope)


@dataclass(frozen=True)
class Decision:
    """One tool name's resolved policy. ``state`` and ``app`` carry the
    provenance §7's UI needs; the two booleans carry the behaviour."""

    allow_call: bool
    allow_output: bool
    state: str
    app: str | None
    tool: str
    upstream: str | None = None

    @property
    def call_drop_marker(self) -> str | None:
        """What replaces denied arguments. ``None`` for the floor, so the
        floor's treatment stays byte-identical to what it has always been
        (and to what §13.2's purge leaves behind) even if this module —
        rather than the scrubber — is the thing that drops them."""
        if self.state == STATE_FLOOR:
            return None
        if self.state == STATE_UNRESOLVED:
            return UNRESOLVED_DROP_MARKER
        return POLICY_DROP_MARKER


def apply_call_axis(decision: Decision, args_text: Any) -> Any:
    """Enforce the call axis on one row's already-scrubbed arguments.

    Deliberately does not invent arguments that were never there: a call
    with no ``input`` stays ``None`` rather than gaining a marker that would
    claim something was dropped. The counter is what carries "policy denied
    this row" in that case.
    """
    if decision.allow_call:
        return args_text
    if args_text is None:
        return None
    return decision.call_drop_marker


def apply_output_axis(decision: Decision, result_text: Any) -> Any:
    """Enforce the output axis on one row's already-scrubbed result — slice 4.

    **``None``, not a marker, and that asymmetry with the call axis is
    deliberate.** The call axis stamps ``<DROPPED:exec-policy>`` because
    §12 risk 4 needs §13.2's purge queries to tell a *remediated credential
    exposure* (``args_text`` nulled by the purge) from *routine policy* on
    the same property. The result axis has no such history to disambiguate:
    §13.2's purge never touched ``result_text``, and the floor's own result
    drop is already a plain ``None`` + ``result_dropped``. So this reuses
    that existing signal rather than inventing a parallel one — the caller
    records ``result_dropped`` on the row exactly as it already does for the
    floor, and what distinguishes an ingest-time drop from a retro-purge is
    ``result_dropped_at``, which only the purge ever writes.

    Same "do not claim a drop that did not happen" rule as the call axis: a
    call whose result was absent or empty stays ``None`` without being
    reported as dropped. The counter carries the denial in that case.
    """
    if decision.allow_output:
        return result_text
    return None


def _apps_root() -> str:
    """Where core installed the apps.

    Asks core itself (``src.apps.paths.apps_root``) rather than re-deriving
    the path, the same privileged-but-read-only import ``exec_scrub`` already
    makes for ``SecretStore`` — this connector is Tier-1, so that IS the
    process that owns the answer. The fallback exists for the out-of-process
    and test shapes, where it is also the documented default.
    """
    try:
        from src.apps import paths  # noqa: PLC0415

        root = paths.apps_root()
        if root:
            return root
    except Exception:  # pragma: no cover - import shape, not logic
        pass
    base = os.environ.get("AW_WORKSPACE_CONTAINER_DIR")
    return os.path.join(base, "apps") if base else _DEFAULT_APPS_ROOT


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        loaded = json.load(fh)
    return loaded if isinstance(loaded, dict) else {}


def _server_keys(app_dir: str) -> list[str]:
    """The ``mcpServers`` KEYS of one app's ``mcp.json``, sanitised.

    Keys only. Those files hold live tokens in their values (an
    ``X-Api-Key`` header, a Notion integration token); nothing here reads,
    logs or returns one.
    """
    try:
        servers = _read_json(os.path.join(app_dir, "mcp.json")).get("mcpServers")
    except (OSError, ValueError):
        return []
    if not isinstance(servers, dict):
        return []
    return [sanitise_server_key(k) for k in servers if isinstance(k, str) and k]


#: What :func:`_declared_entries` reports when the manifest parsed as valid
#: JSON but is not an object (a top-level list, string or number). Not an
#: exception, so it needs a label of its own — and it is the same failure in
#: the same direction as a parse error, so it must not be the one shape that
#: still silently reads as "declared nothing".
MANIFEST_NOT_AN_OBJECT = "NotAnObject"


def _declared_entries(app_dir: str) -> tuple[dict[str, tuple[bool, bool]], str | None]:
    """``contributes.mcp.indexing`` of one app -> ``({tool: (call, output)},
    read_error)``.

    Reads the installed manifest copy, which §5 names as the
    install-time-generated artifact — there is nothing else to generate.

    **"Could not read" is returned, never flattened into "declared
    nothing".** This is the whole reason the function has a second return
    value, and it is a fix for a real fail-open found by QA on slice 2 (run
    ``ee833426cc074f1ea716d7001497160e``) and reclassified as blocking:
    under a BLACKLIST, an empty declaration map does not mean "unknown", it
    means *this app opted out of nothing*, i.e. every tool of that app is
    indexable on BOTH axes. So an unreadable or malformed ``aw-app.json``
    used to convert every declared opt-out in that app into permission to
    index — silently, in the one direction this card exists because of (138
    secret-tool calls landed with their arguments intact because a policy
    failed open). The floor still covers four tools; ``indexing`` is the only
    thing between the other ~658 and the index, and a declared
    ``output: false`` that evaporates on an ``IOError`` is exactly the
    middle-tier protection slice 4 was dispatched to build.

    It also fixes an inconsistency that was the sharper half of the finding:
    :meth:`PolicyTable.from_workspace` catching ``OSError`` on the apps ROOT
    fails closed (everything unresolved, therefore denied), and
    :func:`_server_keys` losing an ``mcp.json`` also fails closed (the app's
    upstreams vanish from the map, so its routed names are unattributable).
    Only this path failed open. Either direction can be argued; two read
    paths over the same tree disagreeing cannot.

    The error is the exception's type NAME (``FileNotFoundError``,
    ``PermissionError``, ``JSONDecodeError``, …) rather than a single
    boolean: a missing manifest, an unreadable one and a corrupt one are
    different operational problems and the distinction costs one
    ``type().__name__``. It is never the exception's *message*, which can
    carry a path.

    Every coercion in the loop below tightens rather than widens, because
    this is a *security* default and the shapes are already rejected upstream
    by core's validator (slice 1) and the marketplace's:

    * an axis is permitted only when it is literally ``True``, so a
      malformed ``"false"`` (or a missing key) denies rather than coercing to
      permitted;
    * a ``tool`` value is read through the shared terminal-segment rule, so a
      prefixed spelling that slipped past validation still matches the thing
      it names instead of matching nothing;
    * duplicate entries for one tool are intersected rather than
      last-wins — ambiguity tightens (§3), and the validator rejects
      duplicates anyway.

    A manifest that reads fine and simply has no ``indexing`` key returns
    ``({}, None)`` — the genuine "never declared" state ~36 installed apps
    are in, which is valid forever (§12 risk 9) and must stay
    distinguishable from the two above.
    """
    try:
        with open(os.path.join(app_dir, "aw-app.json"), "r", encoding="utf-8") as fh:
            loaded = json.load(fh)
    except (OSError, ValueError) as exc:
        # `OSError` and `ValueError` deliberately share this branch, which
        # `type(exc).__name__` then tells apart — the caller needs to know
        # WHICH file is broken and roughly how, not to branch on it.
        return {}, type(exc).__name__
    # NOT via `_read_json`: its non-dict coercion to `{}` is precisely the
    # flattening this function exists to stop, one shape further in.
    if not isinstance(loaded, dict):
        return {}, MANIFEST_NOT_AN_OBJECT
    contributes = loaded.get("contributes")
    mcp = contributes.get("mcp") if isinstance(contributes, dict) else None
    entries = mcp.get("indexing") if isinstance(mcp, dict) else None
    if not isinstance(entries, list):
        return {}, None
    out: dict[str, tuple[bool, bool]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        raw = entry.get("tool")
        tool = exec_scrub.tool_name_segment(raw if isinstance(raw, str) else "")
        if not tool:
            continue
        call = entry.get("call") is True
        output = entry.get("output") is True
        if tool in out:
            call = call and out[tool][0]
            output = output and out[tool][1]
        out[tool] = (call, output)
    return out, None


class PolicyTable:
    """The per-tick effective policy table, plus this tick's counters.

    Not thread-shared and not cached across ticks (§12 risk 5): one tick
    builds one of these, asks it about every row, and reports what it
    tallied.
    """

    __slots__ = (
        "_upstreams",
        "_declared",
        "_overrides",
        "_by_tool",
        "_apps",
        "_scan_error",
        "_manifest_errors",
        "_acknowledged",
        "_overrides_error",
        "_cache",
        "_unresolved",
        "_denied_calls",
        "_denied_results",
        "_unresolved_results",
    )

    def __init__(
        self,
        *,
        upstreams: Mapping[str, str] | None = None,
        declared: Mapping[tuple[str, str], tuple[bool, bool]] | None = None,
        overrides: Mapping[tuple[str, str], tuple[bool, bool]] | None = None,
        apps: Iterable[str] = (),
        scan_error: str | None = None,
        manifest_errors: Mapping[str, str] | None = None,
        acknowledged: Mapping[str, str] | None = None,
        overrides_error: str | None = None,
    ) -> None:
        self._upstreams = dict(upstreams or {})
        self._declared = dict(declared or {})
        #: ``{slug: error_name}`` for apps whose ``aw-app.json`` could not be
        #: read or parsed. A routed name attributed to one of these fails
        #: CLOSED rather than falling through to the blacklist default,
        #: because "I could not read this app's declarations" is not "this
        #: app declared nothing" — see :func:`_declared_entries`.
        #:
        #: Only ever populated for apps that contribute upstreams. An app
        #: with no readable ``mcp.json`` serves no tools through the gateway,
        #: so its manifest can neither grant nor deny anything, and listing
        #: it would put permanent dead entries in the actionable bucket —
        #: the same always-on-counter defect `UNRESOLVABLE_UPSTREAMS` exists
        #: to keep out of it. Measured: this workspace's `apps/` holds 7
        #: `.<app>.fetch-*` download-staging directories with no manifest
        #: and no `mcp.json`.
        self._manifest_errors = dict(manifest_errors or {})
        #: Slice 3 fills this from the connector's state facade, keyed
        #: ``(slug, tool)`` (§6) — never from the app-config blob the
        #: settings form rewrites wholesale. Slice 2 carries the parameter so
        #: precedence is implemented and testable (including the test that
        #: matters: an override row that LIES about a floor tool).
        self._overrides = dict(overrides or {})
        #: ``{upstream: reason}`` a human accepted from the §7 page as
        #: attributable to nothing, on purpose — merged with
        #: :data:`UNRESOLVABLE_UPSTREAMS` for CLASSIFICATION only. Nothing
        #: here reaches :meth:`_resolve`, so an acknowledged upstream's names
        #: fail closed exactly as hard as before it was acknowledged. That
        #: separation is the whole design of the feature: the operator is
        #: silencing a counter, not opening a door, and the only way to keep
        #: those two things from ever being confused is for the permit path
        #: not to exist.
        self._acknowledged = dict(acknowledged or {})
        #: Why the override store could not be read, if it could not be. Under
        #: a blacklist an empty override map is a legitimate everyday state
        #: (nobody has overridden anything), so "could not read" must NOT be
        #: spelled the same way — the same lesson, in the same direction, as
        #: :func:`_declared_entries`'s second return value. It cannot fail
        #: closed the way a manifest read can (an unreadable store does not
        #: say WHICH tools a human had restricted, so there is nothing to
        #: deny), so it is loud instead: reported on the sentinel as
        #: ``policy_overrides_unreadable`` and degrading the doctor. The floor
        #: is untouched by it either way, being hard-coded (§2).
        self._overrides_error = overrides_error
        self._apps = sorted(apps)
        self._scan_error = scan_error
        self._cache: dict[str, Decision] = {}
        self._unresolved: dict[str, None] = {}
        self._denied_calls = 0
        self._denied_results = 0
        self._unresolved_results = 0
        # §3 step 6's bare-name intersection set: every declaration AND
        # override that names this bare tool, whichever app it came from.
        self._by_tool: dict[str, list[tuple[bool, bool]]] = {}
        for source in (self._declared, self._overrides):
            for (_app, tool), axes in source.items():
                self._by_tool.setdefault(tool, []).append(axes)

    @classmethod
    def from_workspace(
        cls,
        root: str | None = None,
        overrides: Mapping[tuple[str, str], tuple[bool, bool]] | None = None,
        *,
        acknowledged: Mapping[str, str] | None = None,
        overrides_error: str | None = None,
    ) -> "PolicyTable":
        """Build the table from the installed tree — §5's materialisation.

        A scan that fails is recorded, not swallowed: with no upstream map
        every routed name becomes unresolved, which denies every argument
        and fills ``policy_unresolved`` rather than quietly reverting to
        "index". That is the correct direction and it is deliberately the
        loud one.

        **The same rule now holds one level down, per app.** A single app's
        unreadable or malformed ``aw-app.json`` is recorded in
        ``manifest_errors`` instead of being flattened into "this app
        declared nothing", which under a blacklist would have granted every
        tool of that app permission on both axes
        (:func:`_declared_entries`). Recorded only for apps that actually
        contribute upstreams — an app serving no tools cannot have its
        policy widened, and a permanent dead entry in the actionable bucket
        is the defect :data:`UNRESOLVABLE_UPSTREAMS` exists to keep out of
        it.
        """
        root = root or _apps_root()
        upstreams: dict[str, str] = {}
        declared: dict[tuple[str, str], tuple[bool, bool]] = {}
        manifest_errors: dict[str, str] = {}
        apps: list[str] = []
        scan_error = None
        try:
            # Sorted, and later apps win a duplicated server key — the same
            # precedence `gateway/config.py::scan_app_mcp_servers` applies
            # when two apps declare one name, so the connector attributes a
            # collision to the same app the gateway actually routed it to.
            entries = sorted(os.listdir(root))
        except OSError as exc:
            log.warning("exec policy: cannot read %s — %s", root, type(exc).__name__)
            # Overrides and acknowledgements still ride along: they come from
            # the state facade, not from this tree, and the §7 page has to be
            # able to show what a human said even when the tree it would be
            # said ABOUT is unreadable. They change nothing about enforcement
            # in this state — every routed name is unresolved and denied.
            return cls(
                overrides=overrides,
                acknowledged=acknowledged,
                overrides_error=overrides_error,
                scan_error=f"apps root unreadable: {type(exc).__name__}",
            )
        for slug in entries:
            app_dir = os.path.join(root, slug)
            if not os.path.isdir(app_dir):
                continue
            apps.append(slug)
            keys = _server_keys(app_dir)
            for key in keys:
                upstreams[key] = slug
            entries, read_error = _declared_entries(app_dir)
            for tool, axes in entries.items():
                declared[(slug, tool)] = axes
            if read_error and keys:
                manifest_errors[slug] = read_error
                log.warning(
                    "exec policy: %s/aw-app.json unreadable (%s) — every routed "
                    "tool of that app now FAILS CLOSED rather than defaulting to "
                    "indexable; fix the manifest",
                    slug,
                    read_error,
                )
        return cls(
            upstreams=upstreams,
            declared=declared,
            overrides=overrides,
            apps=apps,
            scan_error=scan_error,
            manifest_errors=manifest_errors,
            acknowledged=acknowledged,
            overrides_error=overrides_error,
        )

    def _cached(self, name: str | None) -> Decision:
        """Resolve once per distinct name (§9e), and record an unresolved
        name the first time it is seen.

        A 300k-row backfill over a few hundred distinct names does a few
        hundred resolutions and 300k dict hits. The *counters*, by contrast,
        tally per ROW — which is what an operator reading
        ``policy_denied_calls`` or ``policy_denied_results`` means by it —
        so they live in the two public ``decide*`` methods rather than here.
        """
        key = name or ""
        decision = self._cache.get(key)
        if decision is None:
            decision = self._resolve(key)
            self._cache[key] = decision
            if decision.state == STATE_UNRESOLVED:
                self._unresolved[key or EMPTY_NAME_LABEL] = None
        return decision

    def decide(self, name: str | None) -> Decision:
        """The resolved decision for one observed name, tallying the CALL
        axis — one call per ``ExecToolCall`` row built (slice 2)."""
        decision = self._cached(name)
        if not decision.allow_call and decision.state != STATE_FLOOR:
            self._denied_calls += 1
        return decision

    def decide_output(self, name: str | None) -> Decision:
        """The same decision, tallying the OUTPUT axis — one call per
        ``tool_result`` row paired onto a call (slice 4).

        A separate entry point rather than a flag on :meth:`decide` because
        the two axes are counted over DIFFERENT row populations: every
        tool_call gets a call-axis decision, and only the ~99 % of them that
        actually paired with a result get an output-axis one. One method
        tallying both would attribute a result denial to every call that
        never returned.

        **The unresolved denials are tallied apart from the policy ones**,
        and that split is the whole reason this is not one counter. See
        :attr:`counters` and :data:`UNRESOLVABLE_UPSTREAMS`.
        """
        decision = self._cached(name)
        if not decision.allow_output and decision.state != STATE_FLOOR:
            if decision.state == STATE_UNRESOLVED:
                self._unresolved_results += 1
            else:
                self._denied_results += 1
        return decision

    def unresolved_split(self) -> tuple[list[str], list[str]]:
        """``(actionable, known)`` — this tick's distinct unresolved names,
        split on :data:`UNRESOLVABLE_UPSTREAMS`.

        ``known`` is the standing condition: a name routed through an
        upstream that is unresolvable by construction (:data:`UNRESOLVABLE_
        UPSTREAMS`) **or one a human acknowledged** from the §7 page — the
        union, because "the coder knew" and "an operator decided" differ in
        provenance and not at all in what they mean for a counter. Neither
        lifts the denial; see :data:`UNRESOLVABLE_UPSTREAMS`. ``actionable``
        is everything else — an uninstalled
        app, a fourth gateway spelling, a third-party ``mcp__`` server, a
        row with no name at all. That second list is the one where a NEW
        entry means something changed and somebody should look.

        **A FAILED SCAN puts everything in ``actionable``, including the
        names this registry normally covers.** When the apps root could not
        be read at all, a federation's name is unresolved because the TREE
        is unreadable, not because it is a federation — and the registry's
        whole claim ("no manifest could ever attribute this") is a statement
        about a tree that was successfully read. Letting those names keep
        filing themselves under "known and unactionable" in that state would
        both understate the actionable count by however many federations the
        tick happened to see, and do it in exactly the direction this
        registry exists to avoid: a `FileNotFoundError` on the apps root is
        the most actionable thing in this subsystem, and no part of it may
        be classified as expected. ``policy_scan_error`` is reported
        alongside, but a reader who only watches the counters must not be
        told the quiet version.

        A per-app MANIFEST read failure is actionable for the same reason and
        is pinned separately: that denial is attributed to a real installed
        app, so its upstream would never match the registry anyway — but a
        corrupt manifest is a file somebody has to go and fix, and it must
        not be able to drift into "known and unactionable" if an app ever
        declares a server key that collides with a federation's name.
        """
        actionable: list[str] = []
        known: list[str] = []
        degraded = self._scan_error is not None
        for name in sorted(self._unresolved):
            decision = self._cache.get(name if name != EMPTY_NAME_LABEL else "")
            upstream = decision.upstream if decision else None
            broken_manifest = bool(decision and decision.app in self._manifest_errors)
            is_known = (
                not degraded
                and not broken_manifest
                and upstream in self.known_upstreams()
            )
            (known if is_known else actionable).append(name)
        return actionable, known

    def known_upstreams(self) -> dict[str, str]:
        """``{upstream: reason}`` for every upstream whose unresolvability is
        accounted for — the built-in registry plus what a human acknowledged.

        The built-in entries win a collision: a hard-coded reason is a
        statement this code can defend, and an acknowledgement that happened
        to name the same upstream adds nothing to it.
        """
        return {**self._acknowledged, **UNRESOLVABLE_UPSTREAMS}

    # --- read-only views, for the §7 maintenance page -------------------
    #
    # None of these feed a decision; they exist so the page can enumerate
    # rows (§7's "row population") without a second scan of the tree or a
    # second copy of the precedence rules.

    def apps(self) -> list[str]:
        return list(self._apps)

    @property
    def manifest_errors(self) -> dict[str, str]:
        return dict(self._manifest_errors)

    @property
    def overrides_error(self) -> str | None:
        return self._overrides_error

    def declared_identities(self) -> list[tuple[str, str]]:
        """``(app, tool)`` for every manifest ``indexing`` entry — §7 row
        population source (a)."""
        return sorted(self._declared)

    def declared_axes(self, app: str, tool: str) -> tuple[bool, bool] | None:
        """What the MANIFEST says about one identity, independently of what
        currently wins. The page shows this beside an override so an operator
        can see exactly what they are overriding — §11 again: the source
        column is load-bearing, and "you are overriding something" is only
        reviewable if the thing is visible."""
        return self._declared.get((app, tool))

    def override_identities(self) -> list[tuple[str, str]]:
        """``(app, tool)`` for every override row this table was built with."""
        return sorted(self._overrides)

    def observed_identities(self) -> list[tuple[str | None, str]]:
        """``(app, tool)`` for every identity this table actually RESOLVED
        while the tick ran — §7 row population source (c), and the only one
        that is honest about what runs in this workspace rather than what a
        manifest claims.

        The filter is "has an identity to key a row on", NOT "resolved
        successfully", and the difference is a row an operator genuinely needs:
        a tool denied because its own app's ``aw-app.json`` could not be read
        (``_resolve`` step 4b) is ``unresolved`` AND attributed, so it has a
        real ``(app, tool)`` and belongs in the table with its
        ``manifest_error`` pointing at the file to fix. What is excluded is the
        case with no identity at all — an unattributable spelling or an empty
        name, which is exactly why §7 gives those their own section showing
        their RAW spellings instead.

        The floor's own observations are included: a ``write_secret`` that
        actually ran is worth saying so, and the page renders it locked either
        way.
        """
        return sorted(
            {
                (d.app, d.tool)
                for d in self._cache.values()
                if d.tool and (d.app or d.state != STATE_UNRESOLVED)
            },
            key=lambda pair: (pair[0] or "", pair[1]),
        )

    def _resolve(self, name: str) -> Decision:
        """§3's resolution order, in order. The ordering IS the contract."""
        tool = exec_scrub.tool_name_segment(name)
        # 2. FLOOR, always first — before any registry, manifest or override
        #    lookup. `exec_scrub`'s set and segmentation rule, not a second
        #    copy (§12 risk 6).
        if tool in exec_scrub.SECRET_TOOL_NAMES:
            return Decision(False, False, STATE_FLOOR, None, tool)
        # 3. Envelope parse.
        upstream, routed = parse_identity(name)
        if routed:
            slug = self._upstreams.get(upstream) if upstream else None
            # 5. Routed and attributable to nothing -> FAIL CLOSED.
            if not slug:
                return Decision(False, False, STATE_UNRESOLVED, None, tool, upstream)
            # 4. Routed and attributed.
            return self._attributed(slug, tool, upstream)
        if not tool:
            # A row with no name at all is not a harness tool, it is a row
            # nothing can attribute. Fails closed with the rest of §3.5.
            return Decision(False, False, STATE_UNRESOLVED, None, tool)
        # 6. Bare name.
        return self._bare(tool)

    def _attributed(self, slug: str, tool: str, upstream: str | None) -> Decision:
        """§3 step 4 / §6's precedence for a tool that IS attributed to an
        installed app: override, then declaration, then the blacklist default
        (floor > override > manifest > default; the floor is checked by both
        callers before they get here).

        One copy, two callers — :meth:`_resolve` (an observed name, during a
        tick) and :meth:`effective` (an ``(app, tool)`` identity, for the §7
        page). §12 risk 6's rule applied one level up from segmentation: a
        second copy of the precedence order is how the page starts telling an
        operator something the tick does not do, and §11 names that exact
        failure — "if the source column ever lies, humans will edit the wrong
        layer and conclude the system ignores them".
        """
        axes = self._overrides.get((slug, tool))
        if axes is not None:
            return Decision(axes[0], axes[1], STATE_OVERRIDE, slug, tool, upstream)
        # The app is installed and attributable, but its manifest could not be
        # read — so its declarations are UNKNOWN, not absent, and falling
        # through to the blacklist default would turn a declared
        # `output: false` into permission to index. Checked AFTER the override
        # lookup on purpose: §6 puts a human override above the manifest, and
        # a corrupt manifest does not invalidate what a human said about one
        # tool.
        if slug in self._manifest_errors:
            return Decision(False, False, STATE_UNRESOLVED, slug, tool, upstream)
        axes = self._declared.get((slug, tool))
        if axes is not None:
            return Decision(axes[0], axes[1], self._declared_state(axes), slug, tool, upstream)
        return Decision(True, True, STATE_NEVER_DECLARED, slug, tool, upstream)

    def _bare(self, tool: str) -> Decision:
        """§3 step 6: the intersection of every declaration AND override that
        names this bare tool, whichever app owns it. Any restriction restricts
        — ambiguity tightens, never widens. Known consequence (§11): one app's
        opt-out can restrict a same-named tool from another app when a call
        arrives unattributed.
        """
        matches = self._by_tool.get(tool)
        if not matches:
            return Decision(True, True, STATE_NEVER_DECLARED, None, tool)
        axes = (all(m[0] for m in matches), all(m[1] for m in matches))
        return Decision(axes[0], axes[1], self._declared_state(axes), None, tool)

    def effective(self, app: str | None, tool: str) -> Decision:
        """The decision for an ``(app, tool)`` IDENTITY rather than an observed
        name — what §7's maintenance page renders one row from.

        Deliberately not routed through :meth:`decide`: that one parses a
        gateway spelling and tallies a tick's counters, and a page refresh is
        neither a row nor an observation. It is the same precedence though,
        because it is literally the same two methods (:meth:`_attributed`,
        :meth:`_bare`) the tick uses — see :meth:`_attributed`'s docstring for
        why that matters more here than anywhere else in this module.

        ``app=None`` means an unattributed/bare identity (a harness tool like
        ``Bash``), which takes the intersection path exactly as a bare observed
        name does. The floor is checked FIRST, as everywhere (§2): a floor row
        renders locked whatever any override or declaration claims about it.
        """
        tool = exec_scrub.tool_name_segment(tool)
        if tool in exec_scrub.SECRET_TOOL_NAMES:
            return Decision(False, False, STATE_FLOOR, None, tool)
        if not tool:
            return Decision(False, False, STATE_UNRESOLVED, None, "")
        if app:
            return self._attributed(app, tool, None)
        return self._bare(tool)

    @staticmethod
    def _declared_state(axes: tuple[bool, bool]) -> str:
        """"Declared and indexable" is its own state, never folded into the
        default (§7): behaviourally identical to "never declared", and the
        only way anyone can tell which tools have actually been considered."""
        return STATE_DECLARED_INDEXABLE if axes[0] and axes[1] else STATE_DECLARED_RESTRICTED

    @property
    def status(self) -> dict[str, Any]:
        """What the table itself is — sizes and a scan error, never content."""
        return {
            "apps_scanned": len(self._apps),
            "upstreams": len(self._upstreams),
            "declarations": len(self._declared),
            "overrides": len(self._overrides),
            "acknowledged": len(self._acknowledged),
            "scan_error": self._scan_error,
            "overrides_error": self._overrides_error,
            "manifest_errors": len(self._manifest_errors),
        }

    @property
    def counters(self) -> dict[str, Any]:
        """This tick's observation, for the sentinel's ``counts`` — the
        ``event_kinds_unreviewed`` mechanism, for tool identities.

        ``policy_unresolved`` is a list of DISTINCT raw spellings so the
        doctor line names what to go and look at; ``policy_denied_calls``
        counts rows whose arguments policy dropped. The floor's own drops are
        NOT in that number: it exists to say what this layer changed, and the
        floor's drops are the scrubber's and predate it.

        **Slice 4's four additions, and why the output axis is counted in
        three numbers rather than one.** ``policy_denied_results`` is the
        signal the slice adds: rows whose RESULT a declaration or an
        override denied. It deliberately excludes both the floor (whose
        result drops are `scrub_tool_result`'s and predate this layer) and
        the fail-closed unresolved path — which is counted separately as
        ``policy_unresolved_results``. That exclusion is not tidiness: two
        of the gateway's live upstreams are unresolvable by construction
        (:data:`UNRESOLVABLE_UPSTREAMS`), so folding their denials into
        ``policy_denied_results`` would give it a permanent non-zero floor
        and the first REAL ``output: false`` opt-out to fire would be
        indistinguishable from background. A counter that is always on
        measures nothing, the same way an always-red doctor detects nothing.

        ``policy_unresolved_actionable`` is then the number slice 3's
        acknowledge path should read in place of ``len(policy_unresolved)``,
        and ``policy_unresolved_known`` names the standing ones so the split
        is auditable rather than asserted. ``policy_unresolved`` itself is
        left exactly as slice 2 shipped it, known names included: the doctor
        check reads that key, and quietly narrowing it here would change a
        shipped surface from inside a counter property.

        ``policy_manifest_unreadable`` is ``{slug: error_name}`` for apps
        whose ``aw-app.json`` could not be read, and it is the actionable
        detail the bare counts cannot carry: without it an operator sees
        tools denied and an actionable count going up with no pointer to the
        file to fix. Error NAME only (``FileNotFoundError``,
        ``JSONDecodeError``, ``NotAnObject``) — never the exception message,
        which can carry a path.
        """
        unresolved = sorted(self._unresolved)
        actionable, known = self.unresolved_split()
        return {
            "policy_unresolved": unresolved[:UNRESOLVED_NAMES_CAP],
            "policy_unresolved_total": len(unresolved),
            "policy_unresolved_actionable": len(actionable),
            "policy_unresolved_known": known[:UNRESOLVED_NAMES_CAP],
            "policy_denied_calls": self._denied_calls,
            "policy_denied_results": self._denied_results,
            "policy_unresolved_results": self._unresolved_results,
            "policy_scan_error": self._scan_error,
            "policy_manifest_unreadable": dict(sorted(self._manifest_errors.items())),
            # Slice 3. A human's override is the ONLY layer above an app's
            # self-assertion (§6), so a tick that could not read the override
            # store ran with a weaker policy than the operator configured —
            # and under a blacklist "no overrides" is a legitimate everyday
            # state, so this cannot be inferred from the count being zero.
            # It is reported rather than fail-closed because an unreadable
            # store does not say WHICH tools a human restricted; there is
            # nothing to deny, only something to say out loud.
            "policy_overrides_unreadable": self._overrides_error,
        }
