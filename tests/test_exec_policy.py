"""The per-tool exec-indexing policy engine
(docs/design/exec-index-per-tool-policy.md, slice 2).

**The standard this file is held to.** The defect this module replaces was a
deny rule that FAILED OPEN on a name-spelling detail: `SECRET_TOOL_NAMES`
held bare names, every real call arrives gateway-prefixed, and 138
secret-tool calls landed in the production graph with their arguments
intact — 29 of 34 `write_secret` calls with no redaction marker at all. So
the tests that matter here are not "does an opt-out work". They are:

* every spelling that was actually measured in production resolves, and
  resolves to the right app;
* every path that cannot attribute a name ends in DENY, never in "no opt-out
  found, therefore index";
* the floor wins over a data layer that LIES — a manifest entry and an
  override row that both claim a secret tool is indexable;
* the identity rule is segment EQUALITY, so `spread_secret` is not
  `read_secret`.

**The spellings and server keys below are measured, not invented** (§3,
§13.2): `apps/secrets/mcp.json` really does declare the server key
`aw-secrets` (hence `aw__aw_secrets__write_secret`, NOT the slug `secrets`),
`apps/kb/mcp.json` really does declare the bare `kb`, `apps/notion/mcp.json`
really does declare two keys, and `apps/kali-linux/mcp.json` really does
declare `kali` behind which a whole second gateway's `aw__playwright__*`
tools are federated. A fixture that got those wrong would pass against
itself and resolve nothing in production, which §12 risk 1 names as the one
failure this suite has to be able to see.

No app manifest ships `contributes.mcp.indexing` yet, so the declarations
here are invented — but the TREE SHAPE they are read out of is the real one.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import exec_policy, exec_scrub, exec_sync  # noqa: E402

# ---------------------------------------------------------------------------
# Fixture helpers — a fake `apps/` tree in the real shape.
# ---------------------------------------------------------------------------


def _install(root: Path, slug: str, *, servers=(), indexing=None, manifest=True) -> None:
    """Write one installed app: `apps/<slug>/mcp.json` + `aw-app.json`."""
    app_dir = root / slug
    app_dir.mkdir(parents=True, exist_ok=True)
    if servers:
        (app_dir / "mcp.json").write_text(
            json.dumps({"mcpServers": {k: {"type": "http"} for k in servers}}),
            encoding="utf-8",
        )
    if manifest:
        contributes: dict = {}
        if indexing is not None:
            contributes["mcp"] = {"indexing": indexing}
        (app_dir / "aw-app.json").write_text(
            json.dumps({"id": slug, "name": slug, "contributes": contributes}),
            encoding="utf-8",
        )


def _estate(tmp_path: Path) -> Path:
    """The slice of this workspace's real installed tree the tests need, with
    invented declarations on top of real server keys."""
    root = tmp_path / "apps"
    # Real: server key `aw-secrets`, slug `secrets` — the key != slug join.
    _install(root, "secrets", servers=["aw-secrets"])
    # Real: a short key equal to the slug.
    _install(
        root,
        "kb",
        servers=["kb"],
        indexing=[
            {"tool": "search_knowledge_base", "call": True, "output": True},
            {
                "tool": "update_knowledge_base",
                "call": False,
                "output": True,
                "reason": "arguments carry whole documents",
            },
        ],
    )
    # Real: two server keys on one app.
    _install(root, "notion", servers=["notion", "aw-kanban"])
    # Real: a federating upstream — `kali` fronts another gateway whose own
    # tools are already `aw__playwright__*`.
    _install(
        root,
        "kali-linux",
        servers=["kali"],
        indexing=[
            {
                "tool": "browser_type",
                "call": False,
                "output": True,
                "reason": "typed text is often a credential",
            }
        ],
    )
    return root


def _table(tmp_path: Path, overrides=None) -> exec_policy.PolicyTable:
    return exec_policy.PolicyTable.from_workspace(
        str(_estate(tmp_path)), overrides=overrides
    )


# ---------------------------------------------------------------------------
# §3 — the three measured spellings, and the envelope is not allowlisted.
# ---------------------------------------------------------------------------

#: The three spellings §13.2 measured in production, for one real tool of a
#: real installed app (`kb`, server key `kb`). `aw__kb__search_knowledge_base`
#: is the gateway's own aggregated name, carried by neither envelope.
THREE_SPELLINGS = (
    "mcp__aw-gateway__aw__kb__search_knowledge_base",
    "aw-gateway.aw__kb__search_knowledge_base",
    "mcp__workspace-gateway__aw__kb__search_knowledge_base",
)


def test_all_three_measured_spellings_resolve_to_the_same_app_and_tool(tmp_path):
    table = _table(tmp_path)
    for name in THREE_SPELLINGS:
        decision = table.decide(name)
        assert decision.app == "kb", name
        assert decision.tool == "search_knowledge_base", name
        assert decision.upstream == "kb", name
        # Declared with both axes true: the visibly distinct third state.
        assert decision.state == exec_policy.STATE_DECLARED_INDEXABLE, name
        assert (decision.allow_call, decision.allow_output) == (True, True), name


def test_an_unknown_fourth_gateway_spelling_still_resolves(tmp_path):
    """The envelope segment is NOT allowlisted (§3 step 3).

    Allowlisting `aw-gateway`/`workspace-gateway` is exactly how spelling #3
    was missed last time; a rename to anything else must keep resolving,
    because the thing that carries the identity is the `aw__` marker after
    the envelope, not the envelope.
    """
    table = _table(tmp_path)
    decision = table.decide("mcp__some-future-gateway-name__aw__kb__search_knowledge_base")
    assert (decision.app, decision.state) == ("kb", exec_policy.STATE_DECLARED_INDEXABLE)


def test_a_restricted_declaration_denies_the_call_axis(tmp_path):
    table = _table(tmp_path)
    decision = table.decide("mcp__aw-gateway__aw__kb__update_knowledge_base")
    assert decision.state == exec_policy.STATE_DECLARED_RESTRICTED
    assert decision.allow_call is False
    # Resolved and carried, consumed by nobody in this slice (slice 4).
    assert decision.allow_output is True


def test_the_key_is_the_sanitised_server_key_not_the_app_slug(tmp_path):
    """`apps/secrets/mcp.json` declares `aw-secrets`, so the observed segment
    is `aw_secrets` and the slug `secrets` appears nowhere in a tool name
    (§3, §10's rejected slug join)."""
    table = _table(tmp_path)
    assert exec_policy.sanitise_server_key("aw-secrets") == "aw_secrets"
    decision = table.decide("mcp__aw-gateway__aw__aw_secrets__list_secrets")
    assert (decision.app, decision.upstream) == ("secrets", "aw_secrets")
    assert decision.state == exec_policy.STATE_NEVER_DECLARED
    # A slug join would have produced this instead — resolving nothing, and
    # under fail-closed denying everything.
    assert table.decide("mcp__aw-gateway__aw__secrets__list_secrets").state == (
        exec_policy.STATE_UNRESOLVED
    )


def test_one_app_can_own_several_server_keys(tmp_path):
    table = _table(tmp_path)
    assert table.decide("mcp__aw-gateway__aw__notion__API-post-search").app == "notion"
    assert table.decide("mcp__aw-gateway__aw__aw_kanban__add_kanban_comment").app == "notion"


def test_federated_double_prefix_attributes_the_outer_installed_app(tmp_path):
    """§12 risk 7 — `kali` is what this workspace installed; `playwright` is a
    server inside its container and the inner `aw__` is the trap."""
    table = _table(tmp_path)
    decision = table.decide("mcp__aw-gateway__aw__kali__aw__playwright__browser_click")
    assert (decision.app, decision.upstream) == ("kali-linux", "kali")
    assert decision.tool == "browser_click"
    assert decision.state == exec_policy.STATE_NEVER_DECLARED

    # And the declaration on the federated tool applies through both prefixes.
    typed = table.decide("mcp__aw-gateway__aw__kali__aw__playwright__browser_type")
    assert typed.app == "kali-linux"
    assert typed.allow_call is False
    assert typed.state == exec_policy.STATE_DECLARED_RESTRICTED


# ---------------------------------------------------------------------------
# §3.5 — fail closed. Every one of these used to mean "index it".
# ---------------------------------------------------------------------------


def test_an_uninstalled_app_denies_both_axes_and_is_counted(tmp_path):
    """Uninstalling flips an app's tools from "declared" to "unresolved ->
    deny" automatically, with a visible counter — never back to "index"
    (§3's anti-§13.2 guarantees, §5's uninstall consequence)."""
    table = _table(tmp_path)
    name = "mcp__aw-gateway__aw__aw_plaud__plaud_get_transcript"
    decision = table.decide(name)
    assert decision.state == exec_policy.STATE_UNRESOLVED
    assert (decision.allow_call, decision.allow_output) == (False, False)
    assert decision.app is None
    assert table.counters["policy_unresolved"] == [name]
    assert table.counters["policy_denied_calls"] == 1


def test_a_third_party_mcp_server_with_no_aw_marker_denies(tmp_path):
    """An `mcp__…` name that never went through this workspace's gateway has
    no app to attribute it to, so it fails closed (§3 step 5)."""
    table = _table(tmp_path)
    decision = table.decide("mcp__some-vendor-server__fetch_everything")
    assert decision.state == exec_policy.STATE_UNRESOLVED
    assert decision.allow_call is False
    assert decision.tool == "fetch_everything"


def test_a_row_with_no_tool_name_at_all_denies(tmp_path):
    table = _table(tmp_path)
    for name in (None, ""):
        decision = table.decide(name)
        assert decision.state == exec_policy.STATE_UNRESOLVED
        assert decision.allow_call is False
    assert table.counters["policy_unresolved"] == [exec_policy.EMPTY_NAME_LABEL]


def test_an_unreadable_apps_tree_denies_every_routed_name(tmp_path):
    """The scan is the whole source of truth; losing it must deny and say so,
    not revert to the blacklist default."""
    table = exec_policy.PolicyTable.from_workspace(str(tmp_path / "does-not-exist"))
    assert table.status["scan_error"]
    decision = table.decide("mcp__aw-gateway__aw__kb__search_knowledge_base")
    assert decision.state == exec_policy.STATE_UNRESOLVED
    assert decision.allow_call is False
    assert table.counters["policy_scan_error"]


def test_distinct_unresolved_names_are_what_is_reported(tmp_path):
    """DISTINCT spellings, never row counts — ten thousand calls of one
    unattributable name is ONE decision owed (§8, the
    `event_kinds_unreviewed`/`vault_names_uncovered` mechanism)."""
    table = _table(tmp_path)
    for _ in range(50):
        table.decide("mcp__aw-gateway__aw__gone__a_tool")
        table.decide("mcp__aw-gateway__aw__also_gone__a_tool")
    assert table.counters["policy_unresolved"] == [
        "mcp__aw-gateway__aw__also_gone__a_tool",
        "mcp__aw-gateway__aw__gone__a_tool",
    ]
    assert table.counters["policy_unresolved_total"] == 2
    assert table.counters["policy_denied_calls"] == 100


def test_the_reported_unresolved_list_is_capped_but_the_total_is_not(tmp_path):
    table = _table(tmp_path)
    for i in range(exec_policy.UNRESOLVED_NAMES_CAP + 7):
        table.decide(f"mcp__aw-gateway__aw__gone_{i:03d}__a_tool")
    counters = table.counters
    assert len(counters["policy_unresolved"]) == exec_policy.UNRESOLVED_NAMES_CAP
    assert counters["policy_unresolved_total"] == exec_policy.UNRESOLVED_NAMES_CAP + 7


# ---------------------------------------------------------------------------
# §2 / §6 — the floor, and the data layers that lie about it.
# ---------------------------------------------------------------------------


def test_the_floor_beats_a_manifest_that_declares_a_secret_tool_indexable(tmp_path):
    """Core's validator rejects this manifest at install AND load (slice 1),
    and the marketplace validator rejects it at release — so this entry can
    only exist if a data path we do not control put it there. The floor must
    still win (§2: an invariant must not be expressible as data)."""
    root = tmp_path / "apps"
    _install(
        root,
        "secrets",
        servers=["aw-secrets"],
        indexing=[{"tool": "write_secret", "call": True, "output": True}],
    )
    table = exec_policy.PolicyTable.from_workspace(str(root))
    decision = table.decide("mcp__aw-gateway__aw__aw_secrets__write_secret")
    assert decision.state == exec_policy.STATE_FLOOR
    assert (decision.allow_call, decision.allow_output) == (False, False)


def test_the_floor_beats_an_override_row_that_lies(tmp_path):
    """The test §12 risk 2 says is the one that matters: the floor wins over a
    LYING override store — the clobberable layer, keyed by a human, that this
    app has already managed to corrupt once (the `service_secret` incident).

    Checked through every spelling, because a floor that holds for the bare
    name and not the prefixed one is exactly the 138-row defect.
    """
    overrides = {
        (slug, tool): (True, True)
        for slug in ("secrets", "kb", "kali-linux")
        for tool in sorted(exec_scrub.SECRET_TOOL_NAMES)
    }
    table = _table(tmp_path, overrides=overrides)
    for tool in sorted(exec_scrub.SECRET_TOOL_NAMES):
        for name in (
            tool,
            f"mcp__aw-gateway__aw__aw_secrets__{tool}",
            f"aw-gateway.aw__aw_secrets__{tool}",
            f"mcp__workspace-gateway__aw__aw_secrets__{tool}",
            f"mcp__aw-gateway__aw__kali__aw__playwright__{tool}",
            f"mcp__a-vendor__{tool}",
        ):
            decision = table.decide(name)
            assert decision.state == exec_policy.STATE_FLOOR, name
            assert decision.allow_call is False, name
            assert decision.allow_output is False, name


def test_the_floor_is_checked_before_any_lookup(tmp_path):
    """Order, not just outcome (§2's enforcement-order contract, §3 step 2).

    A table with NOTHING in it — no upstream map, no declarations, no
    overrides — still denies a floor tool, which is only true if the floor
    check runs before anything consults them. The same assertion catches the
    inverse mistake: a floor implemented as a *default* applied when a lookup
    finds nothing would pass the outcome tests above and fail here against a
    table that does contain a lookup result.
    """
    empty = exec_policy.PolicyTable()
    assert empty.decide("write_secret").state == exec_policy.STATE_FLOOR
    assert empty.decide("mcp__aw-gateway__aw__aw_secrets__read_secret").allow_call is False

    # ... and the floor's own drop is NOT counted as a policy denial: the
    # counter exists to say what this new layer changed.
    assert empty.counters["policy_denied_calls"] == 0
    assert empty.counters["policy_unresolved"] == []


def test_an_override_beats_a_manifest_declaration(tmp_path):
    """§6's precedence: a human override beats an app's self-assertion,
    because the weakest evidence in the system is an app describing
    itself."""
    table = _table(tmp_path, overrides={("kb", "search_knowledge_base"): (False, False)})
    decision = table.decide("mcp__aw-gateway__aw__kb__search_knowledge_base")
    assert decision.state == exec_policy.STATE_OVERRIDE
    assert (decision.allow_call, decision.allow_output) == (False, False)

    # And upward, too — an override can re-permit what a manifest restricted.
    up = _table(tmp_path, overrides={("kb", "update_knowledge_base"): (True, True)})
    assert up.decide("mcp__aw-gateway__aw__kb__update_knowledge_base").allow_call is True


# ---------------------------------------------------------------------------
# §3.6 — bare names, and the intersection rule.
# ---------------------------------------------------------------------------


def test_a_bare_harness_name_nobody_declared_is_indexable(tmp_path):
    table = _table(tmp_path)
    for name in ("Bash", "run_agent_async", "Read"):
        decision = table.decide(name)
        assert decision.state == exec_policy.STATE_NEVER_DECLARED, name
        assert decision.allow_call is True, name
    assert table.counters["policy_denied_calls"] == 0


def test_a_bare_name_takes_the_intersection_across_every_app(tmp_path):
    """Any restriction restricts (§3 step 6). Two apps declare
    `browser_click`; one opts its call axis out; an unattributed bare
    `browser_click` is restricted. §11 records that this will one day
    surprise the other app's owner — it is the chosen direction, because
    ambiguity must tighten and never widen."""
    root = tmp_path / "apps"
    _install(
        root,
        "kali-linux",
        servers=["kali"],
        indexing=[{"tool": "browser_click", "call": True, "output": True}],
    )
    _install(
        root,
        "mcp-tools",
        servers=["playwright"],
        indexing=[
            {
                "tool": "browser_click",
                "call": False,
                "output": True,
                "reason": "selector text leaks page content",
            }
        ],
    )
    table = exec_policy.PolicyTable.from_workspace(str(root))
    bare = table.decide("browser_click")
    assert bare.allow_call is False
    assert bare.allow_output is True
    assert bare.state == exec_policy.STATE_DECLARED_RESTRICTED
    assert bare.app is None

    # Attributed, the permissive app's own declaration still stands: the
    # intersection is the price of AMBIGUITY, not a workspace-wide ban.
    attributed = table.decide("mcp__aw-gateway__aw__kali__aw__playwright__browser_click")
    assert (attributed.app, attributed.allow_call) == ("kali-linux", True)


def test_a_bare_name_intersects_overrides_too(tmp_path):
    table = _table(tmp_path, overrides={("kb", "list_documents"): (False, False)})
    assert table.decide("list_documents").allow_call is False


# ---------------------------------------------------------------------------
# Slice 0's rule, underneath: segment EQUALITY, never `endswith`.
# ---------------------------------------------------------------------------


def test_spread_secret_is_not_read_secret(tmp_path):
    """The counter-example that kills `endswith`, pinned as a string
    (§12 risk 6, and `exec_scrub.is_secret_tool`'s own docstring):
    `"spread_secret".endswith("read_secret")` is True, and `spread_secret`
    is not a secret-store tool. An over-correction that drops its arguments
    is a different wrong answer, not a safer one."""
    assert "spread_secret".endswith("read_secret")
    table = _table(tmp_path)
    for name in (
        "spread_secret",
        "mcp__aw-gateway__aw__kb__spread_secret",
        "misread_secret",
        "mcp__aw-gateway__aw__kb__unwrite_secret",
    ):
        decision = table.decide(name)
        assert decision.state != exec_policy.STATE_FLOOR, name
        assert decision.allow_call is True, name


def test_the_segmentation_rule_is_not_forked(tmp_path):
    """§12 risk 6 — one segmentation function. If this module ever grows its
    own, this assertion is what notices."""
    assert exec_policy.exec_scrub.tool_name_segment is exec_scrub.tool_name_segment
    table = exec_policy.PolicyTable()
    assert table.decide("a.b__c.d__write_secret").state == exec_policy.STATE_FLOOR

    # And the floor SET is not forked either. There are two declarations of
    # these four names in this estate on purpose — this connector's (it
    # scrubs) and aw-knowledgeable's `core/exec_history.SECRET_TOOL_NAMES`
    # (it refuses). A third, here, would be a third place for them to drift,
    # so this module imports rather than restates.
    assert exec_policy.exec_scrub.SECRET_TOOL_NAMES is exec_scrub.SECRET_TOOL_NAMES
    assert not any(
        isinstance(getattr(exec_policy, attr, None), frozenset)
        for attr in dir(exec_policy)
        if "FLOOR" in attr or "SECRET" in attr
    )


# ---------------------------------------------------------------------------
# §9e — resolve once per distinct name per tick, not once per row.
# ---------------------------------------------------------------------------


def test_resolution_happens_once_per_distinct_name(tmp_path, monkeypatch):
    table = _table(tmp_path)
    resolved: list[str] = []
    original = exec_policy.PolicyTable._resolve

    def _spy(self, name):
        resolved.append(name)
        return original(self, name)

    monkeypatch.setattr(exec_policy.PolicyTable, "_resolve", _spy)
    names = list(THREE_SPELLINGS) + ["Bash"]
    for _ in range(300):
        for name in names:
            table.decide(name)
    assert sorted(resolved) == sorted(names)
    # The counters still describe ROWS, which is what an operator reading
    # `policy_denied_calls` means by it — memoisation must not deduplicate
    # those.
    assert table.counters["policy_denied_calls"] == 0


def test_no_per_row_filesystem_read(tmp_path):
    """The table is built from the tree ONCE; a 300k-row backfill must not
    touch the disk again (§5, card point e). Proven by deleting the tree
    after the build and asking it 1,000 more questions."""
    root = _estate(tmp_path)
    table = exec_policy.PolicyTable.from_workspace(str(root))
    assert table.status["apps_scanned"] == 4
    for app_dir in sorted(root.iterdir()):
        for leaf in sorted(app_dir.iterdir()):
            leaf.unlink()
        app_dir.rmdir()
    root.rmdir()
    for _ in range(1000):
        assert table.decide(THREE_SPELLINGS[0]).app == "kb"
        assert table.decide("mcp__aw-gateway__aw__kb__update_knowledge_base").allow_call is False


# ---------------------------------------------------------------------------
# The call axis, as `build_toolcall_row` applies it.
# ---------------------------------------------------------------------------

_TS = "2026-10-19T12:00:00+00:00"


def _row(name, args, table):
    record = ("run-1", {"name": name, "input": args}, _TS, 7)
    return exec_sync.build_toolcall_row(record, exec_scrub.Scrubber([]), table)


def test_an_indexable_call_keeps_its_arguments(tmp_path):
    row = _row("Bash", {"command": "ls"}, _table(tmp_path))
    assert row["args_text"] == '{"command": "ls"}'
    assert row["name"] == "Bash"


def test_a_policy_denied_call_keeps_name_and_timestamp_and_loses_arguments(tmp_path):
    table = _table(tmp_path)
    row = _row(
        "mcp__aw-gateway__aw__kb__update_knowledge_base",
        {"path": "notes.md", "content": "..."},
        table,
    )
    assert row["args_text"] == exec_policy.POLICY_DROP_MARKER
    assert row["name"] == "mcp__aw-gateway__aw__kb__update_knowledge_base"
    assert row["ts"] == _TS
    assert row["seq"] == 7
    assert table.counters["policy_denied_calls"] == 1


def test_an_unresolved_call_is_marked_distinctly_from_a_declared_denial(tmp_path):
    """§12 risk 4 — and one level further down: "nothing could attribute
    this" is a diagnosis (a fourth gateway prefix, an uninstalled app), not a
    declared opt-out, and the graph is where someone will notice it."""
    table = _table(tmp_path)
    row = _row("mcp__aw-gateway__aw__gone__some_tool", {"x": 1}, table)
    assert row["args_text"] == exec_policy.UNRESOLVED_DROP_MARKER
    assert exec_policy.UNRESOLVED_DROP_MARKER != exec_policy.POLICY_DROP_MARKER


def test_a_floor_tool_still_drops_to_none_not_to_a_marker(tmp_path):
    """The floor's treatment stays byte-identical to what it has been since
    slice 0 — a plain `None` (§12 risk 4): §13.2's purge and its verification
    queries read a null `args_text` as "floor-handled or remediated", and a
    marker there would make a remediated credential exposure
    indistinguishable from routine policy."""
    table = _table(tmp_path)
    for name in THREE_SPELLINGS[:1] + (
        "mcp__aw-gateway__aw__aw_secrets__write_secret",
        "write_secret",
    ):
        row = _row(name, {"name": "a-secret", "value": "invented-not-real"}, table)
        if exec_scrub.is_secret_tool(name):
            assert row["args_text"] is None, name
            assert row["name"] == name, name


def test_a_denied_call_with_no_arguments_gains_no_marker(tmp_path):
    """Nothing was dropped, so nothing claims to have been. The counter is
    what carries this row."""
    table = _table(tmp_path)
    row = _row("mcp__aw-gateway__aw__gone__some_tool", None, table)
    assert row["args_text"] is None
    assert table.counters["policy_denied_calls"] == 1


def test_the_scrubber_still_runs_on_an_indexable_call(tmp_path):
    """Policy is a gate in FRONT of the scrubber, never a replacement for it:
    an indexable tool's arguments must still be redacted."""
    secret = "invented-store-secret-long-enough-to-match"
    table = _table(tmp_path)
    record = ("run-1", {"name": "Bash", "input": f"export TOKEN={secret}"}, _TS, 1)
    row = exec_sync.build_toolcall_row(record, exec_scrub.Scrubber([secret]), table)
    assert secret not in row["args_text"]


# ---------------------------------------------------------------------------
# §8 — the doctor surface.
# ---------------------------------------------------------------------------


def test_policy_unresolved_is_read_off_the_sentinel():
    assert exec_sync.policy_unresolved(None) == []
    assert exec_sync.policy_unresolved({}) == []
    # The normal shape of a sentinel stamped by an older connector: absent,
    # which is not a degradation.
    assert exec_sync.policy_unresolved({"reported_counts": {}}) == []
    assert exec_sync.policy_unresolved(
        {"reported_counts": {"policy_unresolved": ["mcp__x__aw__gone__t"]}}
    ) == ["mcp__x__aw__gone__t"]
    # Shape garbage degrades to "nothing to report", exactly as
    # `event_kinds_unreviewed` does.
    assert exec_sync.policy_unresolved({"reported_counts": {"policy_unresolved": 3}}) == []
