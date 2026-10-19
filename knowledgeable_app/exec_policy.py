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
#: **What this registry is NOT, and what slice 3 still owes.** It does not
#: touch ``policy_unresolved`` or the doctor check that reads it — those are
#: slice 2's shipped surface and the acknowledge path is slice 3's to build
#: (§9: "give the UI a way to acknowledge a row as attributable to nothing,
#: on purpose", or have core expose the gateway's non-app upstreams so they
#: can be attributed properly). Until then the doctor still degrades
#: permanently on these two names. This registry is the data slice 3's UI
#: needs and the guard that keeps slice 4's own counters readable; it is
#: deliberately not a silent fix to somebody else's surface.
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


def _declared_entries(app_dir: str) -> dict[str, tuple[bool, bool]]:
    """``contributes.mcp.indexing`` of one app, as ``{tool: (call, output)}``.

    Reads the installed manifest copy, which §5 names as the
    install-time-generated artifact — there is nothing else to generate.

    Every coercion here tightens rather than widens, because this is a
    *security* default and the shapes are already rejected upstream by core's
    validator (slice 1) and the marketplace's:

    * an axis is permitted only when it is literally ``True``, so a
      malformed ``"false"`` (or a missing key) denies rather than coercing to
      permitted;
    * a ``tool`` value is read through the shared terminal-segment rule, so a
      prefixed spelling that slipped past validation still matches the thing
      it names instead of matching nothing;
    * duplicate entries for one tool are intersected rather than
      last-wins — ambiguity tightens (§3), and the validator rejects
      duplicates anyway.
    """
    try:
        contributes = _read_json(os.path.join(app_dir, "aw-app.json")).get("contributes")
    except (OSError, ValueError):
        return {}
    mcp = contributes.get("mcp") if isinstance(contributes, dict) else None
    entries = mcp.get("indexing") if isinstance(mcp, dict) else None
    if not isinstance(entries, list):
        return {}
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
    return out


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
    ) -> None:
        self._upstreams = dict(upstreams or {})
        self._declared = dict(declared or {})
        #: Slice 3 fills this from the connector's state facade, keyed
        #: ``(slug, tool)`` (§6) — never from the app-config blob the
        #: settings form rewrites wholesale. Slice 2 carries the parameter so
        #: precedence is implemented and testable (including the test that
        #: matters: an override row that LIES about a floor tool).
        self._overrides = dict(overrides or {})
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
    ) -> "PolicyTable":
        """Build the table from the installed tree — §5's materialisation.

        A scan that fails is recorded, not swallowed: with no upstream map
        every routed name becomes unresolved, which denies every argument
        and fills ``policy_unresolved`` rather than quietly reverting to
        "index". That is the correct direction and it is deliberately the
        loud one.
        """
        root = root or _apps_root()
        upstreams: dict[str, str] = {}
        declared: dict[tuple[str, str], tuple[bool, bool]] = {}
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
            return cls(scan_error=f"apps root unreadable: {type(exc).__name__}")
        for slug in entries:
            app_dir = os.path.join(root, slug)
            if not os.path.isdir(app_dir):
                continue
            apps.append(slug)
            for key in _server_keys(app_dir):
                upstreams[key] = slug
            for tool, axes in _declared_entries(app_dir).items():
                declared[(slug, tool)] = axes
        return cls(
            upstreams=upstreams,
            declared=declared,
            overrides=overrides,
            apps=apps,
            scan_error=scan_error,
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
        upstream that is unresolvable by construction, which no manifest can
        ever attribute. ``actionable`` is everything else — an uninstalled
        app, a fourth gateway spelling, a third-party ``mcp__`` server, a
        row with no name at all. That second list is the one where a NEW
        entry means something changed and somebody should look.
        """
        actionable: list[str] = []
        known: list[str] = []
        for name in sorted(self._unresolved):
            decision = self._cache.get(name if name != EMPTY_NAME_LABEL else "")
            upstream = decision.upstream if decision else None
            (known if upstream in UNRESOLVABLE_UPSTREAMS else actionable).append(name)
        return actionable, known

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
            # 4. Routed and attributed: override, then declaration, then the
            #    blacklist default (§6: floor > override > manifest >
            #    default).
            axes = self._overrides.get((slug, tool))
            if axes is not None:
                return Decision(axes[0], axes[1], STATE_OVERRIDE, slug, tool, upstream)
            axes = self._declared.get((slug, tool))
            if axes is not None:
                return Decision(axes[0], axes[1], self._declared_state(axes), slug, tool, upstream)
            return Decision(True, True, STATE_NEVER_DECLARED, slug, tool, upstream)
        # 6. Bare name: the intersection of every declaration and override
        #    that names this tool, whichever app owns it. Any restriction
        #    restricts — ambiguity tightens, never widens. Known consequence
        #    (§11): one app's opt-out can restrict a same-named tool from
        #    another app when a call arrives unattributed.
        if not tool:
            # A row with no name at all is not a harness tool, it is a row
            # nothing can attribute. Fails closed with the rest of §3.5.
            return Decision(False, False, STATE_UNRESOLVED, None, tool)
        matches = self._by_tool.get(tool)
        if not matches:
            return Decision(True, True, STATE_NEVER_DECLARED, None, tool)
        axes = (all(m[0] for m in matches), all(m[1] for m in matches))
        return Decision(axes[0], axes[1], self._declared_state(axes), None, tool)

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
            "scan_error": self._scan_error,
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
        }
