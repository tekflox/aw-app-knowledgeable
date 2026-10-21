"""The §7 maintenance table as the page renders it
(docs/design/exec-index-per-tool-policy.md §7; slice 3).

**What this file is actually protecting.** §7's five states are the
deliverable of this slice, not its styling. Under the blacklist
`never_declared` and `declared_indexable` have IDENTICAL behaviour — both
index both axes — so the only thing that makes "which tools has anyone
actually reviewed?" an answerable question is that these two render
differently. A regression that collapsed them would break nothing a
behavioural test can see, and would quietly delete the point of the slice
(card point f). Hence the tests below assert on `state` per row, not on
`call`/`output`.

The other two things pinned here:

* **`contributes.mcp.provides` is display-only** (§12 risk 3). It may ADD a
  row; it may never change one's answer. The structural half of that
  guarantee is that nothing on the tick's path imports `exec_policy_view`;
  this is the behavioural half.
* **unresolved rows carry RAW spellings** (§7). The original defect was a name
  that did not match, and a fourth gateway prefix is supposed to be diagnosed
  from this page rather than from the next incident, so the name must arrive
  verbatim — not normalised, not segmented, not truncated.

Fixture trees only. `tests/conftest.py`'s autouse guard points
`exec_policy._apps_root()` at a path that does not exist, so a test here
that forgot to declare a tree fails the same way in CI as it does locally
(§12 risk 10).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import (  # noqa: E402
    exec_policy,
    exec_policy_view,
    exec_scrub,
)


def _install(root: Path, slug: str, *, servers=(), indexing=None, provides=None) -> None:
    app_dir = root / slug
    app_dir.mkdir(parents=True, exist_ok=True)
    if servers:
        (app_dir / "mcp.json").write_text(
            json.dumps({"mcpServers": {k: {"type": "http"} for k in servers}}),
            encoding="utf-8",
        )
    mcp: dict = {}
    if indexing is not None:
        mcp["indexing"] = indexing
    if provides is not None:
        mcp["provides"] = provides
    (app_dir / "aw-app.json").write_text(
        json.dumps({"id": slug, "name": slug, "contributes": {"mcp": mcp} if mcp else {}}),
        encoding="utf-8",
    )


@pytest.fixture()
def estate(tmp_path):
    """Real tree SHAPE, invented declarations — same convention as
    `test_exec_policy.py`'s `_estate`, and the same reason: a fixture that got
    the shape wrong would pass against itself and resolve nothing in
    production."""
    root = tmp_path / "apps"
    _install(root, "secrets", servers=["aw-secrets"])
    _install(
        root, "kb", servers=["kb"],
        indexing=[
            # The explicit third state: considered, and indexable.
            {"tool": "search_knowledge_base", "call": True, "output": True},
            # Restricted.
            {"tool": "update_knowledge_base", "call": False, "output": True,
             "reason": "arguments carry whole documents"},
        ],
        # Display-only, and deliberately naming a tool with NO declaration so
        # the row exists in the `never declared` state.
        provides=["search_knowledge_base", "list_documents"],
    )
    _install(root, "notion", servers=["notion", "aw-kanban"])
    return root


def _build(root, **kwargs):
    table = exec_policy.PolicyTable.from_workspace(
        str(root),
        overrides=kwargs.pop("override_axes", None),
        acknowledged=kwargs.pop("acknowledged_reasons", None),
        overrides_error=kwargs.pop("overrides_error", None),
    )
    # Resolve any names the caller wants "observed", exactly as a tick would,
    # so `observed_identities()` is populated by the real path rather than
    # hand-written.
    for name in kwargs.pop("resolve", ()):
        table.decide(name)
    payload = exec_policy_view.build(
        table,
        overrides=kwargs.pop("overrides", {}),
        acknowledged=kwargs.pop("acknowledged", {}),
        observed=kwargs.pop("observed", table.observed_identities()),
        observed_at=kwargs.pop("observed_at", "2026-10-21T00:00:00+00:00"),
        provides=kwargs.pop(
            "provides", exec_policy_view.display_only_provides(str(root))
        ),
        unresolved=kwargs.pop("unresolved", []),
        unresolved_known=kwargs.pop("unresolved_known", []),
        store_errors=kwargs.pop("store_errors", None),
    )
    assert not kwargs, f"unused kwargs {list(kwargs)}"
    return payload


def _floor_row(payload, tool):
    """A floor row is keyed on the TOOL alone — the floor is an invariant on
    four names, not a per-app setting, so which app it gets attributed to is
    informational and depends on which source mentioned it first."""
    matches = [r for r in payload["rows"] if r["tool"] == tool]
    assert len(matches) == 1, f"{tool} rendered {len(matches)} rows, must be exactly 1"
    return matches[0]


def _row(payload, app, tool):
    for row in payload["rows"]:
        if row["app"] == app and row["tool"] == tool:
            return row
    raise AssertionError(f"no row for {app!r}/{tool!r} in {[(r['app'], r['tool']) for r in payload['rows']]}")


# ---------------------------------------------------------------------------
# §7 / card point f — the five states, visibly distinct.
# ---------------------------------------------------------------------------

def test_the_five_states_are_all_present_and_distinct(estate):
    payload = _build(
        estate,
        override_axes={("notion", "add_kanban_comment"): (False, False)},
        overrides={("notion", "add_kanban_comment"): {
            "app": "notion", "tool": "add_kanban_comment", "call": False,
            "output": False, "note": "comments quote card bodies",
            "actor": "identity:fred", "at": "2026-10-21T00:00:00+00:00",
        }},
    )
    states = {
        _floor_row(payload, "write_secret")["state"],
        _row(payload, "notion", "add_kanban_comment")["state"],
        _row(payload, "kb", "update_knowledge_base")["state"],
        _row(payload, "kb", "search_knowledge_base")["state"],
        _row(payload, "kb", "list_documents")["state"],
    }
    assert states == {
        exec_policy.STATE_FLOOR,
        exec_policy.STATE_OVERRIDE,
        exec_policy.STATE_DECLARED_RESTRICTED,
        exec_policy.STATE_DECLARED_INDEXABLE,
        exec_policy.STATE_NEVER_DECLARED,
    }, "five states, five distinct values — collapsing any two deletes the point of §7"


def test_never_declared_and_declared_indexable_differ_despite_identical_behaviour(estate):
    """The one assertion this slice exists for. Both rows index both axes;
    only the STATE tells a human that one was reviewed and the other has never
    been looked at."""
    payload = _build(estate)
    reviewed = _row(payload, "kb", "search_knowledge_base")
    never = _row(payload, "kb", "list_documents")
    assert (reviewed["call"], reviewed["output"]) == (True, True)
    assert (never["call"], never["output"]) == (True, True)
    assert reviewed["state"] != never["state"]
    assert reviewed["state_label"] != never["state_label"]
    assert reviewed["source"] == "manifest"
    assert never["source"] == "blacklist-default"


@pytest.mark.parametrize("tool", sorted(exec_scrub.SECRET_TOOL_NAMES))
def test_floor_rows_are_present_and_locked(estate, tool):
    """Rendered locked, no toggle (§7). Present whether or not anything has
    observed them: a page that only showed the floor once a secret tool had
    been CALLED would go quiet exactly when the estate is behaving."""
    payload = _build(estate)
    row = _floor_row(payload, tool)
    assert row["locked"] is True
    assert row["state"] == exec_policy.STATE_FLOOR
    assert (row["call"], row["output"]) == (False, False)


def test_a_floor_row_stays_locked_even_with_a_lying_override(estate):
    """The data layer lies and loses — the same test slice 4 pins in the
    engine, re-pinned at the layer a human actually reads. The route refuses
    such a row before it can be stored, so this can only arise from a
    hand-written blob; it still must not render as togglable."""
    payload = _build(
        estate,
        override_axes={("secrets", "write_secret"): (True, True)},
    )
    row = _floor_row(payload, "write_secret")
    assert row["state"] == exec_policy.STATE_FLOOR
    assert row["locked"] is True
    assert (row["call"], row["output"]) == (False, False)


def test_floor_rows_sort_first(estate):
    payload = _build(estate)
    assert all(r["locked"] for r in payload["rows"][: len(exec_scrub.SECRET_TOOL_NAMES)])


# ---------------------------------------------------------------------------
# §7 row population — the union of (a), (b) display-only, (c).
# ---------------------------------------------------------------------------

def test_rows_are_the_union_of_all_three_sources(estate):
    payload = _build(estate, resolve=["mcp__aw-gateway__aw__aw_kanban__add_kanban_comment"])
    assert "manifest" in _row(payload, "kb", "update_knowledge_base")["listed_because"]
    assert "provides" in _row(payload, "kb", "list_documents")["listed_because"]
    assert "observed" in _row(payload, "notion", "add_kanban_comment")["listed_because"]


def test_a_row_records_every_reason_it_is_listed(estate):
    """`search_knowledge_base` is both declared and in `provides`. One row, two
    reasons — a duplicate row per source would read as two different tools."""
    payload = _build(estate)
    row = _row(payload, "kb", "search_knowledge_base")
    assert sorted(row["listed_because"]) == ["manifest", "provides"]


def test_provides_adds_a_row_but_never_changes_an_answer(estate):
    """§12 risk 3, behaviourally. The same table built without `provides` has
    fewer rows and identical verdicts on the rows it does have."""
    with_p = _build(estate)
    without_p = _build(estate, provides={})
    assert len(with_p["rows"]) > len(without_p["rows"])
    for row in without_p["rows"]:
        twin = _row(with_p, row["app"], row["tool"])
        assert (twin["call"], twin["output"], twin["state"]) == (
            row["call"], row["output"], row["state"],
        )


def test_a_stale_provides_entry_for_an_uninstalled_app_is_just_a_row(estate):
    """A hand-maintained list naming a tool nothing else knows about costs a
    phantom row on a settings page, which is the correct price for display-only
    data — not a policy effect."""
    payload = _build(estate, provides={"kb": ["a_tool_that_no_longer_exists"]})
    row = _row(payload, "kb", "a_tool_that_no_longer_exists")
    assert row["state"] == exec_policy.STATE_NEVER_DECLARED
    assert row["listed_because"] == ["provides"]


def test_an_override_on_an_otherwise_unknown_tool_still_gets_a_row(estate):
    """A human who pre-emptively restricted a tool must be able to see their
    own row, even when no manifest declares it and the sync has not observed
    it."""
    payload = _build(
        estate,
        override_axes={("kb", "a_future_tool"): (False, False)},
        overrides={("kb", "a_future_tool"): {
            "app": "kb", "tool": "a_future_tool", "call": False, "output": False,
            "note": "pre-emptive", "actor": "identity:fred", "at": "2026-10-21",
        }},
    )
    row = _row(payload, "kb", "a_future_tool")
    assert row["state"] == exec_policy.STATE_OVERRIDE
    assert row["override"]["note"] == "pre-emptive"


def test_display_only_provides_tolerates_a_missing_tree(tmp_path):
    """Cosmetic enumeration: failure is silent here because an unreadable
    manifest already has a loud, enforcement-side consequence
    (`policy_manifest_unreadable`, both axes denied)."""
    assert exec_policy_view.display_only_provides(str(tmp_path / "nope")) == {}


# ---------------------------------------------------------------------------
# §7's provenance — the source column is load-bearing (§11).
# ---------------------------------------------------------------------------

def test_an_override_row_shows_who_when_and_what_it_overrode(estate):
    payload = _build(
        estate,
        override_axes={("kb", "update_knowledge_base"): (True, True)},
        overrides={("kb", "update_knowledge_base"): {
            "app": "kb", "tool": "update_knowledge_base", "call": True,
            "output": True, "note": "reviewed, the args are fine",
            "actor": "identity:fred", "at": "2026-10-21T09:00:00+00:00",
        }},
    )
    row = _row(payload, "kb", "update_knowledge_base")
    assert row["state"] == exec_policy.STATE_OVERRIDE
    assert row["source"] == "override"
    assert row["override"]["actor"] == "identity:fred"
    # What it overrode, shown beside it — "you are overriding something" is
    # only reviewable if the something is visible.
    assert row["declared"] == {"call": False, "output": True}
    assert (row["call"], row["output"]) == (True, True)


def test_an_unreadable_manifest_names_the_file_to_fix(tmp_path):
    root = tmp_path / "apps"
    _install(root, "kb", servers=["kb"])
    (root / "kb" / "aw-app.json").write_text("{ this is not json", encoding="utf-8")
    payload = _build(root, resolve=["mcp__aw-gateway__aw__kb__search_graph"])
    row = _row(payload, "kb", "search_graph")
    assert row["state"] == exec_policy.STATE_UNRESOLVED
    assert (row["call"], row["output"]) == (False, False)
    assert row["manifest_error"] == "JSONDecodeError"


def test_every_row_source_matches_its_state(estate):
    payload = _build(estate)
    for row in payload["rows"]:
        assert row["source"] == exec_policy_view._SOURCE_BY_STATE[row["state"]]
        assert row["state_label"] == payload["states"][row["state"]]


# ---------------------------------------------------------------------------
# §7's unresolved section — raw spellings, and known != permitted.
# ---------------------------------------------------------------------------

UNRESOLVED_NAME = (
    "mcp__aw-gateway__aw__fredericowu_browser__fredericowu__playwright__browser_click"
)


def test_unresolved_rows_carry_the_raw_spelling_verbatim(estate):
    payload = _build(estate, unresolved=[UNRESOLVED_NAME])
    entry = payload["unresolved"][0]
    assert entry["name"] == UNRESOLVED_NAME, "verbatim — it is the only evidence here"
    assert entry["upstream"] == "fredericowu_browser"
    assert entry["tool"] == "browser_click"


def test_a_fourth_gateway_spelling_is_diagnosable_from_this_page(estate):
    """§7's stated reason for the section. A rename of the gateway envelope
    degrades to deny + counter (§11), and this row is the mitigation: the raw
    name shows the new envelope, and the upstream segment shows the join key
    that failed."""
    name = "mcp__brand-new-gateway-name__aw__kb__search_graph"
    payload = _build(estate, unresolved=[name])
    entry = payload["unresolved"][0]
    assert entry["name"] == name
    assert entry["upstream"] == "kb"
    assert entry["known"] is False, "a new spelling is actionable, never background"


def test_known_unresolved_rows_are_marked_without_being_permitted(estate):
    payload = _build(
        estate,
        unresolved=[UNRESOLVED_NAME],
        unresolved_known=[UNRESOLVED_NAME],
        acknowledged={"fredericowu_browser": {
            "upstream": "fredericowu_browser", "reason": "BYOD federation",
            "actor": "identity:fred", "at": "2026-10-21",
        }},
        acknowledged_reasons={"fredericowu_browser": "BYOD federation"},
    )
    entry = payload["unresolved"][0]
    assert entry["known"] is True
    assert entry["reason"] == "BYOD federation"
    assert entry["acknowledged"]["actor"] == "identity:fred"
    # The counting changed; the denial did not, and this payload has no field
    # that could say otherwise — there is no `call`/`output` on an unresolved
    # entry, because the answer is always "neither".
    assert "call" not in entry and "output" not in entry
    assert payload["counts"]["unresolved_actionable"] == 0


def test_an_unresolved_name_with_no_upstream_is_not_acknowledgeable(estate):
    """A third-party `mcp__…` server with no `aw__` marker has no segment to
    key an acknowledgement on, so the page says so rather than offering a
    button that cannot work."""
    payload = _build(estate, unresolved=["mcp__some-third-party__unknown_tool"])
    assert payload["unresolved"][0]["acknowledgeable"] is False


def test_the_builtin_known_upstreams_are_shown(estate):
    """So an operator can see that these two are accounted for by this code,
    rather than wondering why they are not red."""
    payload = _build(estate)
    assert set(payload["known_upstreams"]) == {"fredericowu", "fredericowu_browser"}


# ---------------------------------------------------------------------------
# "This page is showing less than the truth", named rather than silent.
# ---------------------------------------------------------------------------

def test_store_errors_are_surfaced_and_empty_ones_dropped(estate):
    payload = _build(
        estate,
        overrides_error="StateFacadeDegraded",
        store_errors={"overrides": "StateFacadeDegraded", "observed": None, "sentinel": None},
    )
    assert payload["store_errors"] == {"overrides": "StateFacadeDegraded"}
    assert payload["table"]["overrides_error"] == "StateFacadeDegraded"


def test_a_healthy_page_reports_no_store_errors(estate):
    assert _build(estate)["store_errors"] == {}


def test_counts_add_up(estate):
    payload = _build(estate)
    c = payload["counts"]
    assert c["rows"] == len(payload["rows"])
    assert (
        c["floor"] + c["override"] + c["declared_restricted"]
        + c["declared_indexable"] + c["never_declared"] + c["unresolved_rows"]
    ) == c["rows"]


def test_a_floor_tool_is_exactly_one_row_however_many_sources_name_it(estate):
    """Found on the first real screenshot, not by a fixture.

    The real tree has `apps/secrets` listing the four secret tools in
    `contributes.mcp.provides`, so they arrive at `_row` three different ways:
    unattributed from the floor block, as `secrets`' from `provides`, and as a
    bare observed name. Keyed per app that rendered EIGHT locked rows for four
    tools — the same invariant listed twice, reading as two different tools.
    The floor is an invariant on four NAMES, not a per-app setting.
    """
    root = estate
    (root / "secrets" / "aw-app.json").write_text(
        json.dumps({"id": "secrets", "contributes": {"mcp": {
            "provides": sorted(exec_scrub.SECRET_TOOL_NAMES),
            "indexing": [{"tool": "write_secret", "call": False, "output": False,
                          "reason": "arguments carry the secret value"}],
        }}}),
        encoding="utf-8",
    )
    payload = _build(
        root,
        resolve=["mcp__aw-gateway__aw__aw_secrets__write_secret", "write_secret"],
    )
    for tool in sorted(exec_scrub.SECRET_TOOL_NAMES):
        matches = [r for r in payload["rows"] if r["tool"] == tool]
        assert len(matches) == 1, f"{tool} rendered {len(matches)} rows, must be exactly 1"
        assert matches[0]["locked"] is True
        assert matches[0]["state"] == exec_policy.STATE_FLOOR
    assert payload["counts"]["floor"] == len(exec_scrub.SECRET_TOOL_NAMES)
    # ...and it is attributed to the app that actually serves it, which is more
    # use to a reader than "unattributed".
    assert _row(payload, "secrets", "write_secret")["app"] == "secrets"
