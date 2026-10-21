"""The override / acknowledgement / observed store
(docs/design/exec-index-per-tool-policy.md §6, §7; slice 3).

**The standard this file is held to**, inherited from `test_exec_policy.py`:
this store is the only layer that sits ABOVE an app's own self-assertion (§6),
so the tests that matter are not "can a human save a row". They are:

* the floor is refused HERE, at the edge, before anything is written — for
  every spelling of a floor name, not just the bare one (§2, §12 risk 6);
* "could not read the store" is never flattened into "a human restricted
  nothing", because under a blacklist those two mean opposite things (§12
  risk 11, one read path further out);
* a write never replaces a blob that could not be read, because that would
  silently delete every other human's opt-out;
* an acknowledgement changes COUNTING and nothing else — there is no path
  from this module to `allow_call`/`allow_output`.

The kv double below is a plain dict with no TTL semantics, the same shape
`test_routes.py`'s `_FakeKv` already uses: TTL behaviour belongs to core's
`state_facade`, not to this app.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import exec_policy, exec_policy_store, exec_scrub  # noqa: E402


class _Kv:
    """Double for `ctx.state.kv`. `fail` makes every read miss the way the
    real facade's degrade-open does; `raise_on` makes it raise, which is the
    out-of-process shape."""

    def __init__(self, *, fail: bool = False, raise_on: bool = False) -> None:
        self.store: dict[str, object] = {}
        self.fail = fail
        self.raise_on = raise_on
        self.puts = 0

    async def get(self, name):
        if self.raise_on:
            raise OSError("redis unreachable")
        if self.fail:
            return None
        return self.store.get(name)

    async def put(self, name, value, ttl):
        assert ttl > 0, "ctx.state.kv has no immortal keys — a 0 ttl would raise in prod"
        self.puts += 1
        self.store[name] = value


@pytest.fixture()
def kv():
    return _Kv()


# ---------------------------------------------------------------------------
# §2 — the floor is refused at the EDGE, not only in the engine.
# ---------------------------------------------------------------------------

#: Every spelling of one floor name that this estate has actually measured
#: (§3/§13.2), plus the bare one. The engine resolves all of these to
#: `write_secret`; so must the store's refusal, or the "invariant not
#: expressible as data" guarantee holds in one place and not the other.
FLOOR_SPELLINGS = [
    "write_secret",
    "mcp__aw-gateway__aw__aw_secrets__write_secret",
    "aw-gateway.aw__aw_secrets__write_secret",
    "mcp__workspace-gateway__aw__aw_secrets__write_secret",
]


@pytest.mark.parametrize("spelling", FLOOR_SPELLINGS)
@pytest.mark.asyncio
async def test_put_override_refuses_every_floor_spelling(kv, spelling):
    with pytest.raises(exec_policy_store.FloorRefusal):
        await exec_policy_store.put_override(
            kv, app="secrets", tool=spelling, call=True, output=True,
            note="let me index it", actor="test",
        )
    assert kv.store == {}, "nothing may reach the store when the floor refused"


@pytest.mark.parametrize("tool", sorted(exec_scrub.SECRET_TOOL_NAMES))
@pytest.mark.asyncio
async def test_put_override_refuses_all_four_floor_names(kv, tool):
    """All four, not just `write_secret` — the floor is a set, and a test that
    only pinned one name is how the other three would be free to drift."""
    with pytest.raises(exec_policy_store.FloorRefusal):
        await exec_policy_store.put_override(
            kv, app="secrets", tool=tool, call=False, output=False,
            note="even a redundant one", actor="test",
        )


@pytest.mark.asyncio
async def test_floor_refusal_beats_shape_validation(kv):
    """A floor name that is ALSO malformed must be refused as a floor name.

    The order in `_check_identity` is deliberate: if the shape check ran
    first, a prefixed floor spelling would come back as "must be a bare
    segment", a caller would helpfully bare it, and the second attempt would
    be a floor row. The refusal has to be the FIRST thing that happens.
    """
    with pytest.raises(exec_policy_store.FloorRefusal):
        await exec_policy_store.put_override(
            kv, app="secrets", tool="mcp__aw-gateway__aw__aw_secrets__read_secret",
            call=True, output=True, note="", actor="test",
        )


@pytest.mark.asyncio
async def test_spread_secret_is_not_a_floor_name(kv):
    """Segment EQUALITY, never `endswith` — `spread_secret` ends with
    `read_secret` and is an ordinary tool. The whole module exists because a
    name-matching detail failed open; this is the mirror case, where it must
    not fail closed on something it does not own."""
    row = await exec_policy_store.put_override(
        kv, app="some-app", tool="spread_secret", call=True, output=True,
        note="", actor="test",
    )
    assert row["tool"] == "spread_secret"


# ---------------------------------------------------------------------------
# §4's rules, applied to a human's row rather than a manifest's.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tool", ["aw__kb__search_graph", "kb.search_graph"])
@pytest.mark.asyncio
async def test_put_override_requires_a_bare_segment(kv, tool):
    with pytest.raises(exec_policy_store.BadIdentity):
        await exec_policy_store.put_override(
            kv, app="kb", tool=tool, call=False, output=False, note="x", actor="test",
        )


@pytest.mark.parametrize("app", [None, "", "   ", 7])
@pytest.mark.asyncio
async def test_put_override_requires_an_app(kv, app):
    """§3 step 6 / §11: an app-less row would restrict every same-named tool
    in the estate through the bare-name intersection. That widening is granted
    deliberately to an unattributable OBSERVED call, never to a form."""
    with pytest.raises(exec_policy_store.BadIdentity):
        await exec_policy_store.put_override(
            kv, app=app, tool="search_graph", call=False, output=False,
            note="x", actor="test",
        )


# ---------------------------------------------------------------------------
# Round-trip, and the provenance §7 renders.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_override_round_trips_with_provenance(kv):
    written = await exec_policy_store.put_override(
        kv, app="kb", tool="search_graph", call=False, output=True,
        note="queries carry customer names", actor="identity:fred",
    )
    loaded, error = await exec_policy_store.load_overrides(kv)
    assert error is None
    assert loaded[("kb", "search_graph")] == {
        "app": "kb",
        "tool": "search_graph",
        "call": False,
        "output": True,
        "note": "queries carry customer names",
        "actor": "identity:fred",
        "at": written["at"],
    }
    assert exec_policy_store.axes(loaded) == {("kb", "search_graph"): (False, True)}


@pytest.mark.asyncio
async def test_put_override_replaces_in_place(kv):
    await exec_policy_store.put_override(
        kv, app="kb", tool="search_graph", call=False, output=False, note="a", actor="x",
    )
    await exec_policy_store.put_override(
        kv, app="kb", tool="search_graph", call=True, output=False, note="b", actor="y",
    )
    loaded, _ = await exec_policy_store.load_overrides(kv)
    assert len(loaded) == 1
    assert loaded[("kb", "search_graph")]["call"] is True
    assert loaded[("kb", "search_graph")]["note"] == "b"


@pytest.mark.asyncio
async def test_other_overrides_survive_a_write_and_a_delete(kv):
    await exec_policy_store.put_override(
        kv, app="kb", tool="search_graph", call=False, output=False, note="a", actor="x",
    )
    await exec_policy_store.put_override(
        kv, app="notion", tool="add_kanban_comment", call=False, output=True,
        note="b", actor="x",
    )
    await exec_policy_store.delete_override(kv, app="kb", tool="search_graph", actor="x")
    loaded, _ = await exec_policy_store.load_overrides(kv)
    assert list(loaded) == [("notion", "add_kanban_comment")]


@pytest.mark.asyncio
async def test_delete_reports_whether_there_was_one(kv):
    assert await exec_policy_store.delete_override(
        kv, app="kb", tool="nope", actor="x") is False
    await exec_policy_store.put_override(
        kv, app="kb", tool="search_graph", call=True, output=True, note="", actor="x")
    assert await exec_policy_store.delete_override(
        kv, app="kb", tool="search_graph", actor="x") is True


# ---------------------------------------------------------------------------
# §12 risk 11, one read path further out: "could not read" is not "empty".
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_store_with_a_healthy_facade_is_not_an_error(kv, monkeypatch):
    """The state every workspace starts in, and most will stay in. It must not
    look like a degradation, or the doctor is red from day one — the very
    defect this slice was dispatched to remove."""
    monkeypatch.setattr(exec_policy_store, "_facade_degraded", lambda: False)
    loaded, error = await exec_policy_store.load_overrides(kv)
    assert (loaded, error) == ({}, None)


@pytest.mark.asyncio
async def test_a_degraded_facade_is_reported_not_flattened(monkeypatch):
    """A miss while Redis is unreachable must NOT read as "a human restricted
    nothing". Under a blacklist those are opposite claims: the second one says
    every tool is indexable on both axes."""
    monkeypatch.setattr(exec_policy_store, "_facade_degraded", lambda: True)
    loaded, error = await exec_policy_store.load_overrides(_Kv(fail=True))
    assert loaded == {}
    assert error == exec_policy_store.UNREACHABLE


@pytest.mark.asyncio
async def test_a_raising_kv_is_reported(monkeypatch):
    loaded, error = await exec_policy_store.load_overrides(_Kv(raise_on=True))
    assert (loaded, error) == ({}, "OSError")


@pytest.mark.asyncio
async def test_a_malformed_blob_is_reported_not_flattened(kv):
    """Same direction as a parse error in `_declared_entries`: a value that is
    not the shape this module writes is a failure, not an empty policy."""
    kv.store[exec_policy_store.OVERRIDES_KV_NAME] = ["not", "a", "dict"]
    loaded, error = await exec_policy_store.load_overrides(kv)
    assert (loaded, error) == ({}, exec_policy_store.MALFORMED)


@pytest.mark.asyncio
async def test_a_stored_axis_that_is_not_literally_true_denies(kv):
    """`is True`, never truthiness — the same coercion direction
    `_declared_entries` applies to a manifest. Every coercion in a security
    default tightens."""
    kv.store[exec_policy_store.OVERRIDES_KV_NAME] = {
        "version": 1,
        "rows": [{"app": "kb", "tool": "search_graph", "call": "true", "output": 1}],
    }
    loaded, error = await exec_policy_store.load_overrides(kv)
    assert error is None
    assert loaded[("kb", "search_graph")]["call"] is False
    assert loaded[("kb", "search_graph")]["output"] is False


@pytest.mark.asyncio
async def test_a_write_refuses_rather_than_discarding_an_unread_blob(monkeypatch):
    """The sharp one. A read-modify-write on a blob that could not be read
    would REPLACE it — deleting every other override in it. Losing a human's
    opt-out silently is the exact failure this whole card exists because of,
    so the writer refuses and says why."""
    monkeypatch.setattr(exec_policy_store, "_facade_degraded", lambda: True)
    kv = _Kv(fail=True)
    with pytest.raises(RuntimeError, match="could not be read"):
        await exec_policy_store.put_override(
            kv, app="kb", tool="search_graph", call=False, output=False,
            note="x", actor="test",
        )
    assert kv.puts == 0
    with pytest.raises(RuntimeError, match="could not be read"):
        await exec_policy_store.delete_override(kv, app="kb", tool="search_graph", actor="t")
    assert kv.puts == 0


# ---------------------------------------------------------------------------
# Acknowledgements — §9's escalation. Acknowledging is NOT permitting.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_acknowledgement_round_trips(kv):
    row = await exec_policy_store.put_acknowledgement(
        kv, upstream="fredericowu_browser",
        reason="BYOD linked-workspace federation — no apps/<slug>/mcp.json exists",
        actor="identity:fred",
    )
    loaded, error = await exec_policy_store.load_acknowledged(kv)
    assert error is None
    assert loaded["fredericowu_browser"]["reason"] == row["reason"]
    assert exec_policy_store.ack_reasons(loaded) == {
        "fredericowu_browser": row["reason"]
    }


@pytest.mark.asyncio
async def test_acknowledgement_requires_a_reason(kv):
    """§4's rule applied here: an opt-out nobody can review is rot, and this
    one silences an alarm."""
    with pytest.raises(exec_policy_store.BadIdentity, match="reason is required"):
        await exec_policy_store.put_acknowledgement(
            kv, upstream="whatever", reason="   ", actor="test")


@pytest.mark.asyncio
async def test_acknowledgement_refuses_a_whole_tool_name(kv):
    """An upstream is one sanitised segment. Accepting a whole tool name would
    silence one spelling and leave the upstream's other names red, which reads
    as the acknowledgement not working."""
    with pytest.raises(exec_policy_store.BadIdentity):
        await exec_policy_store.put_acknowledgement(
            kv, upstream="mcp__aw-gateway__aw__fredericowu__x", reason="r", actor="t")


@pytest.mark.asyncio
async def test_acknowledgement_is_withdrawable(kv):
    await exec_policy_store.put_acknowledgement(
        kv, upstream="ghost", reason="gone", actor="t")
    assert await exec_policy_store.delete_acknowledgement(
        kv, upstream="ghost", actor="t") is True
    loaded, _ = await exec_policy_store.load_acknowledged(kv)
    assert loaded == {}
    assert await exec_policy_store.delete_acknowledgement(
        kv, upstream="ghost", actor="t") is False


@pytest.mark.asyncio
async def test_an_acknowledged_upstream_is_still_denied(kv, tmp_path):
    """**The test that matters for this feature.** Acknowledging must change
    the COUNTING and nothing else. If it ever lifts a denial, the operator
    pressing "stop telling me about this" has silently opened the fail-closed
    path that §3.5 exists to keep shut.
    """
    await exec_policy_store.put_acknowledgement(
        kv, upstream="fredericowu", reason="BYOD federation", actor="t")
    acknowledged, _ = await exec_policy_store.load_acknowledged(kv)
    table = exec_policy.PolicyTable(
        upstreams={"kb": "kb"},
        acknowledged=exec_policy_store.ack_reasons(acknowledged),
    )
    name = "mcp__aw-gateway__aw__fredericowu__fredericowu__google_workspace__list_calendars"
    decision = table.decide(name)
    assert decision.state == exec_policy.STATE_UNRESOLVED
    assert (decision.allow_call, decision.allow_output) == (False, False)
    assert table.decide_output(name).allow_output is False
    # ... and the only thing that DID change:
    actionable, known = table.unresolved_split()
    assert actionable == []
    assert known == [name]


# ---------------------------------------------------------------------------
# Observed identities — §7 row population source (c).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_observed_identities_union_across_ticks(kv):
    """A union, not a replacement: one tick sees one bounded slice of runs, so
    replacing would make the page's row list flicker with whatever the last
    ten minutes happened to contain."""
    await exec_policy_store.record_observed(kv, [("kb", "search_graph"), (None, "Bash")])
    await exec_policy_store.record_observed(kv, [("notion", "add_kanban_comment")])
    rows, error, at = await exec_policy_store.load_observed(kv)
    assert error is None
    assert at
    assert set(rows) == {
        ("kb", "search_graph"), (None, "Bash"), ("notion", "add_kanban_comment"),
    }


@pytest.mark.asyncio
async def test_observed_is_capped(kv, monkeypatch):
    monkeypatch.setattr(exec_policy_store, "OBSERVED_CAP", 3)
    merged = exec_policy_store.merge_observed(
        [("a", "t1"), ("a", "t2")], [("a", "t3"), ("a", "t4")]
    )
    assert len(merged) == 3


@pytest.mark.asyncio
async def test_observed_not_recorded_when_the_store_is_unreadable(monkeypatch):
    """Same never-replace-an-unread-blob rule. Here the cost of refusing is
    only a stale settings page, which is why it logs rather than raising."""
    monkeypatch.setattr(exec_policy_store, "_facade_degraded", lambda: True)
    kv = _Kv(fail=True)
    await exec_policy_store.record_observed(kv, [("kb", "search_graph")])
    assert kv.puts == 0


@pytest.mark.asyncio
async def test_observed_timestamp_distinguishes_never_run_from_nothing_there(kv):
    """An empty list has two meanings — the sync has never run
    (`exec_sync_enabled` ships false), or it ran and saw nothing. The page
    must not present those alike, so `at` is returned beside the rows."""
    rows, error, at = await exec_policy_store.load_observed(kv)
    assert (rows, error, at) == ([], None, "")
    await exec_policy_store.record_observed(kv, [("kb", "search_graph")])
    _rows, _error, at = await exec_policy_store.load_observed(kv)
    assert at


@pytest.mark.asyncio
async def test_a_dashed_upstream_is_normalised_to_what_the_gateway_spells(kv):
    """An operator reasonably types the dashed name of a linked workspace.
    The acknowledgement is matched against `Decision.upstream`, which is
    always the SANITISED form, so storing the dashed string would create a row
    that can never match: the alarm stays red, the page shows the
    acknowledgement, and the two disagree with nothing saying why.

    Normalised through `exec_policy.sanitise_server_key` — the one named place
    that copy of the gateway's transform lives (§12 risk 1), never a second
    `replace('-', '_')`.
    """
    await exec_policy_store.put_acknowledgement(
        kv, upstream="fredericowu-browser", reason="BYOD federation", actor="t")
    loaded, _ = await exec_policy_store.load_acknowledged(kv)
    assert list(loaded) == ["fredericowu_browser"]

    table = exec_policy.PolicyTable(
        acknowledged=exec_policy_store.ack_reasons(loaded))
    name = (
        "mcp__aw-gateway__aw__fredericowu_browser__fredericowu__playwright__browser_click"
    )
    table.decide(name)
    assert table.unresolved_split() == ([], [name]), (
        "a dashed acknowledgement must still silence the real observed name"
    )
    # ...and the dashed spelling withdraws what it created.
    assert await exec_policy_store.delete_acknowledgement(
        kv, upstream="fredericowu-browser", actor="t") is True
