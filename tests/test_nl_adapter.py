"""nl_adapter.py — the NL→Tool-Calling Adapter behind ``POST /source-query``
(docs/design/knowledgeable-nl-tool-adapter.md §3, card N1).

The matrix here is §9.7's, which exists because this estate's recorded pattern
is that **optional-config paths ship untested**: the adapter is reached only by
an entry that opts in, so every degraded mode is a path no ordinary run
exercises. Each one is pinned: no key in the vault, ap-mt down, rounds
exhausted, budget exhausted, an unlisted tool or argument fed back as a
correction and then succeeding, two unparsable replies, an oversized final
answer truncated, and the NL-true entry staying byte-identical.

Deliberately no real network and no real clock: ``_chat`` is scripted per
test (so the loop is driven, not the model), ``forwarding.forward`` is faked
where a tool result is needed, and ``time.monotonic`` is scripted where the
wall-clock bound is the thing under test — the same monkeypatch-the-seam
pattern as ``test_forwarding.py``'s fake ``httpx.AsyncClient``.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import forwarding, nl_adapter, routes as routes_mod  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    """``test_forwarding.py``'s helper, but closing the loop.

    That file leaks one event loop per test and gets away with it; this file
    also drives ``TestClient``, which runs its own loop, and the two together
    turn the leak into an intermittent ``PytestUnraisableExceptionWarning``
    ("Invalid file descriptor: -1") when the garbage collector finalizes a
    leaked loop after a selector it shares has already gone. Closing here is
    the local fix — ``test_forwarding.py`` must stay untouched (§9.6).
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


SIGNOZ_ENTRY = {
    "id": "signoz",
    "transport": "app-mcp-json",
    "app_slug": "signoz-connector",
    "server": "signoz-query",
    "accepts_natural_language": False,
    "problem_domain": "observability for this workspace's own services",
    "query_guidance": "searchText is literal CONTAINS, never a restated question.",
    "adapter": {"max_rounds": 6, "budget_s": 90},
    "tools": {
        "signoz_list_services": {"args": ["timeRange", "limit"],
                                 "hint": "service inventory in the window"},
        "signoz_search_logs": {"args": ["searchText", "service", "severity", "timeRange", "limit"],
                               "hint": "searchText = literal CONTAINS, NOT semantic"},
    },
    "probe": {"tool": "signoz_list_services", "ttl_ok_s": 60, "ttl_fail_s": 15, "timeout_s": 120},
    "timeout_s": 10,
    "query_tool": {"tool": "signoz_search_logs", "arg": "searchText"},
}

LEGACY_ENTRY = {
    "id": "codegraph",
    "transport": "app-mcp",
    "app_slug": "codegraph",
    "tools": {"codegraph_explore": {"args": ["query", "maxFiles"]}},
    "timeout_s": 10,
}


# ---------------------------------------------------------------------------
# Scripted seams
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _a_key_is_in_the_vault(monkeypatch):
    """Every test but the no-key one wants the credential to resolve. Autouse
    so no test can accidentally reach the real workspace secret store (and so
    none is silently green for the wrong reason, passing because the vault was
    unreachable rather than because the loop worked)."""
    async def _read(name, *, reason):
        return "test-api-key"

    monkeypatch.setattr(nl_adapter.key_vault, "read_secret", _read)


def _script_chat(monkeypatch, replies):
    """Drive the loop: each round pops the next scripted model reply. Records
    the messages and the HTTP timeout each round was given, which is how the
    §9.3 "budget is also the per-call timeout" claim gets asserted rather than
    assumed."""
    calls: list[dict] = []

    async def _chat(api_key, messages, timeout_s):
        calls.append({"api_key": api_key, "messages": [dict(m) for m in messages],
                      "timeout_s": timeout_s})
        if not replies:
            raise AssertionError("the loop asked for more rounds than the test scripted")
        item = replies.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(nl_adapter, "_chat", _chat)
    return calls


def _script_forward(monkeypatch, results):
    calls: list[dict] = []

    async def _forward(upstream_id, tool, args):
        calls.append({"upstream_id": upstream_id, "tool": tool, "args": args})
        return results.pop(0) if results else {"status": "ok", "result": "", "elapsed_ms": 1}

    monkeypatch.setattr(nl_adapter.forwarding, "forward", _forward)
    return calls


def _script_clock(monkeypatch, ticks):
    """A scripted ``time.monotonic``, last value repeating. Patches the name
    ``nl_adapter`` holds rather than the stdlib module, so nothing else in the
    suite (or in pytest itself) sees a frozen clock."""
    seq = list(ticks)

    def _monotonic():
        return seq.pop(0) if len(seq) > 1 else seq[0]

    monkeypatch.setattr(nl_adapter, "time", SimpleNamespace(monotonic=_monotonic))


def _action(tool, **args):
    return json.dumps({"tool": tool, "args": args, "why": "because"})


def _final(text, evidence=None):
    body = {"final": text}
    if evidence is not None:
        body["evidence"] = evidence
    return json.dumps(body)


# ---------------------------------------------------------------------------
# The declaration — §2's migration story
# ---------------------------------------------------------------------------


def test_absent_flag_is_nl_true_so_every_existing_entry_keeps_todays_path():
    assert forwarding.accepts_natural_language(LEGACY_ENTRY) is True
    assert forwarding.accepts_natural_language({"id": "x"}) is True


def test_only_an_explicit_false_activates_the_adapter():
    assert forwarding.accepts_natural_language({**LEGACY_ENTRY, "accepts_natural_language": True}) is True
    assert forwarding.accepts_natural_language(SIGNOZ_ENTRY) is False


def test_an_unknown_source_is_nl_true_so_it_still_falls_through_to_forward():
    """§3.1 — an unknown ``source_id`` must keep being rejected by name by
    ``forward()``, not diverted into an adapter that has no declaration to
    work from."""
    assert forwarding.accepts_natural_language(None) is True


def test_adapter_bounds_default_and_are_floored():
    """Part 6 §32 D3 recalibrated the defaults 6/90 → 8/105. The two knobs are
    calibrated to each other (≤7 tool rounds ≈ 84s ≈ 105 − the 20s final
    reserve) and move together or not at all, so they are pinned together."""
    assert forwarding.adapter_bounds({}) == (8, 105.0)
    assert forwarding.adapter_bounds({"adapter": {"max_rounds": 2, "budget_s": 30}}) == (2, 30.0)
    assert forwarding.adapter_bounds({"adapter": {"max_rounds": 0, "budget_s": 0}}) == (8, 105.0)
    assert forwarding.adapter_bounds({"adapter": {"max_rounds": "nope"}}) == (8, 105.0)


def test_the_recalibrated_inner_budget_still_fits_under_aw_knowledgeables_outer_wall():
    """§35's cross-repo invariant with no compiler, asserted on this side.

    aw-knowledgeable's `_per_source_timeout` gives an NL-false leg
    `min(min(budget_s, ADAPTER_BUDGET_S_CAP=120) + ADAPTER_ENVELOPE_GRACE_S=15,
    request budget)`, with `ADAPTER_DEFAULT_BUDGET_MS=135_000`. Raising the
    default inner budget past the cap, or past `request − grace`, silently
    clips the grace back off and re-blinds the instrument (N6a's whole point).
    """
    inner = forwarding.DEFAULT_ADAPTER_BUDGET_S
    cap, grace, request_budget_s = 120.0, 15.0, 135.0
    assert inner <= cap, "a default past the cap is capped, so the grace shrinks"
    outer = min(min(inner, cap) + grace, request_budget_s)
    assert outer > inner, f"outer wall {outer}s must outlive the inner budget {inner}s"
    assert outer == 120.0


def test_the_last_round_is_reserved_so_max_rounds_yields_one_fewer_tool_call():
    """§35 — `max_rounds: N` now means at most N−1 tool calls. Pinned because
    an operator tuning from §2's original description is off by one, and
    because this is what keeps the forced final from being squeezed out."""
    assert nl_adapter._forced_kind(0, 8, 105.0) == ""          # round 1: explore
    assert nl_adapter._forced_kind(6, 8, 60.0) == ""           # round 7: last tool call
    assert nl_adapter._forced_kind(7, 8, 60.0) == "rounds"     # round 8: landing
    assert nl_adapter._forced_kind(2, 8, 19.0) == "budget"     # seconds bind first
    # Never on the very first round: a budget smaller than the reserve must not
    # land before a single tool call has been made (§36.7c's `budget_s: 5`).
    assert nl_adapter._forced_kind(0, 8, 5.0) == ""


# ---------------------------------------------------------------------------
# The happy path and its telemetry
# ---------------------------------------------------------------------------


def test_the_loop_executes_proposed_calls_and_returns_the_final_answer(monkeypatch):
    chat = _script_chat(monkeypatch, [
        _action("signoz_list_services", timeRange="24h"),
        _action("signoz_search_logs", severity="ERROR", timeRange="24h"),
        _final("aw-knowledgeable logged 33,944 ERROR lines in 24h.",
               ["signoz_search_logs: 33944 rows, severity=ERROR"]),
    ])
    forwards = _script_forward(monkeypatch, [
        {"status": "ok", "result": "aw-knowledgeable\naw-backend", "elapsed_ms": 40},
        {"status": "ok", "result": "33944 rows", "elapsed_ms": 900},
    ])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="application errors"))

    assert out["status"] == "ok"
    assert out["result"].startswith("aw-knowledgeable logged 33,944 ERROR lines in 24h.")
    # Evidence rides INLINE in `result`, which stays a plain string — the
    # thing `_coerce_source_payload` passthrough and `sources-digest`'s
    # `answer: result` projection depend on.
    assert isinstance(out["result"], str)
    assert "signoz_search_logs: 33944 rows" in out["result"]
    assert out["adapter"] == {
        "rounds": 3,
        "tool_calls": [
            {"tool": "signoz_list_services", "ok": True},
            {"tool": "signoz_search_logs", "ok": True},
        ],
    }
    assert isinstance(out["elapsed_ms"], int)
    assert len(chat) == 3
    assert [c["tool"] for c in forwards] == ["signoz_list_services", "signoz_search_logs"]
    assert forwards[1]["args"] == {"severity": "ERROR", "timeRange": "24h"}


def test_the_adapter_gets_the_original_query_and_code_query_is_context_only(monkeypatch):
    """§3.1 — ``value = code_query or query`` on the legacy path is exactly
    what manufactured the garbage substring. The adapter must see the real
    problem, with the restatement demoted to a hint."""
    chat = _script_chat(monkeypatch, [_final("done")])

    _run(nl_adapter.solve(
        SIGNOZ_ENTRY,
        query="which services are erroring?",
        code_query="SigNoz recent errors exceptions logs traces service.name frequency",
    ))

    first_user = chat[0]["messages"][1]["content"]
    assert "which services are erroring?" in first_user
    assert "AUXILIARY HINT" in first_user
    assert "never a value to send to a tool verbatim" in first_user


def test_the_system_prompt_carries_the_declaration_and_the_arg_cap(monkeypatch):
    """The per-source knowledge is injected at call time — that is what lets
    ONE generic agent row serve every NL-false source (§3.3). And the prompt
    has to state MAX_ARG_CHARS, or long retries loop on rejections (§9.1)."""
    chat = _script_chat(monkeypatch, [_final("done")])

    _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    prompt = chat[0]["messages"][0]["content"]
    assert "observability for this workspace's own services" in prompt
    assert "searchText is literal CONTAINS, never a restated question." in prompt
    assert "`signoz_search_logs` — args: searchText, service, severity, timeRange, limit" in prompt
    assert "hint: searchText = literal CONTAINS, NOT semantic" in prompt
    assert str(forwarding.MAX_ARG_CHARS) in prompt


def test_a_codex_cli_banner_and_a_code_fence_do_not_defeat_the_parser(monkeypatch):
    """§9.4 — the codex runner prepends "Reading additional input from
    stdin..." to its replies. ``json.loads(reply)`` would raise on that; the
    brace slice skips it for free."""
    assert nl_adapter.json_object(
        'Reading additional input from stdin...\n```json\n{"final": "ok"}\n```'
    ) == {"final": "ok"}
    assert nl_adapter.json_object("no object here") is None
    assert nl_adapter.json_object("[1, 2, 3]") is None
    assert nl_adapter.json_object('{"final": "unterminated') is None


# ---------------------------------------------------------------------------
# §9.7 — the degraded-mode matrix
# ---------------------------------------------------------------------------


def test_no_key_in_the_vault_is_unavailable_naming_the_key_and_the_agent(monkeypatch):
    """The N1 end state, before N2 mints anything: visibly degraded, and the
    reason has to say WHICH missing thing, or an operator cannot act on it."""
    async def _no_key(name, *, reason):
        return None

    monkeypatch.setattr(nl_adapter.key_vault, "read_secret", _no_key)
    # Scripted so that a bug which called ap-mt anyway fails loudly here.
    _script_chat(monkeypatch, [])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="application errors"))

    assert out["status"] == "unavailable"
    assert out["reason"].startswith("nl adapter: ")
    assert nl_adapter.SECRET_NAME in out["reason"]
    assert nl_adapter.AGENT_SLUG in out["reason"]
    assert out["adapter"] == {"rounds": 0, "tool_calls": []}
    assert "result" not in out


def test_apmt_unreachable_is_unavailable_not_a_raise(monkeypatch):
    """The real transport, faked at the httpx seam rather than at ``_chat`` —
    so this also pins that ``_chat`` itself turns a transport error into the
    envelope instead of letting httpx escape through ``solve``."""
    class _Boom:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None, headers=None):
            raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(nl_adapter.httpx, "AsyncClient", _Boom)

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="application errors"))

    assert out["status"] == "unavailable"
    assert "ap-mt unreachable" in out["reason"]
    assert "ConnectError" in out["reason"]


def test_a_missing_agent_row_arrives_as_a_200_and_is_still_unavailable(monkeypatch):
    """ap-mt answers a FAILED run with HTTP 200 and the failure as prose. A
    2xx is not proof of a reply, and this is exactly how "the agent row does
    not exist yet" shows up — so the detail must survive into the reason."""
    class _FailedRun:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None, headers=None):
            return SimpleNamespace(
                status_code=200,
                text="",
                json=lambda: {"choices": [{"message": {
                    "content": "[agent knowledgeable-nl-adapter failed] no such agent"}}]},
            )

    monkeypatch.setattr(nl_adapter.httpx, "AsyncClient", _FailedRun)

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="application errors"))

    assert out["status"] == "unavailable"
    assert "ap-mt run failed" in out["reason"]
    assert "no such agent" in out["reason"]


def test_a_rejected_apikey_names_the_secret_and_the_agent_scope(monkeypatch):
    class _Rejected:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None, headers=None):
            return SimpleNamespace(status_code=401, text="unauthorized", json=lambda: {})

    monkeypatch.setattr(nl_adapter.httpx, "AsyncClient", _Rejected)

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert nl_adapter.SECRET_NAME in out["reason"]
    assert nl_adapter.AGENT_SLUG in out["reason"]


def test_rounds_exhausted_with_no_successful_tool_call_is_unavailable(monkeypatch):
    """The exhaustion reason operators already know, unchanged — reached now
    through the evidence gate rather than by falling out of the loop. Zero
    successful tool calls means there is nothing to land, so the forced final
    is not even issued (§36.5)."""
    entry = {**SIGNOZ_ENTRY, "adapter": {"max_rounds": 2, "budget_s": 90}}
    chat = _script_chat(monkeypatch, [
        _action("signoz_execute_builder_query", query="SELECT *"),
    ])
    _script_forward(monkeypatch, [
        {"status": "unavailable",
         "reason": "tool 'signoz_execute_builder_query' is not allow-listed for upstream 'signoz'",
         "elapsed_ms": 0},
    ])

    out = _run(nl_adapter.solve(entry, query="application errors"))

    assert out["status"] == "unavailable"
    assert "exhausted 2 round(s) without a final answer" in out["reason"]
    assert out["adapter"]["tool_calls"] == [
        {"tool": "signoz_execute_builder_query", "ok": False}]
    assert "partial" not in out["adapter"], "no partial may be fabricated from no evidence"
    assert len(chat) == 1, "a forced final the gate will reject is not worth a round"


def test_rounds_exhausted_lands_a_forced_final_instead_of_one_more_tool_call(monkeypatch):
    """§36.6 — rounds exhaust → forced final on the last round, not another
    tool call. `max_rounds: 2` therefore buys exactly one tool call."""
    entry = {**SIGNOZ_ENTRY, "adapter": {"max_rounds": 2, "budget_s": 90}}
    chat = _script_chat(monkeypatch, [
        _action("signoz_list_services", timeRange="24h"),
        _final("Partial answer (budget exhausted after 2 round(s)):\n\nTwo services ran in "
               "the window; their error counts were not established.",
               ["signoz_list_services: aw-knowledgeable, aw-backend"]),
    ])
    forwards = _script_forward(monkeypatch, [
        {"status": "ok", "result": "aw-knowledgeable\naw-backend", "elapsed_ms": 1},
    ])

    out = _run(nl_adapter.solve(entry, query="application errors"))

    assert out["status"] == "ok"
    assert out["result"].startswith("Partial answer (budget exhausted after 2 round(s)):")
    assert out["adapter"]["rounds"] == 2
    assert out["adapter"]["partial"] is True
    assert len(forwards) == 1, "the reserved round must not be spent on a tool call"
    forced = chat[1]["messages"][-1]["content"]
    assert forced.startswith("BUDGET EXHAUSTED")
    assert "round 2 of 2, the last one" in forced
    assert "no further tool call will be executed" in forced


def test_budget_exhausted_stops_the_loop_even_with_rounds_left(monkeypatch):
    """§9.3 — an ap-mt call can land minutes late, so the round counter alone
    does not bound this. Round 1 is allowed; round 2's pre-check sees the
    wall clock has gone past the budget and stops."""
    _script_clock(monkeypatch, [0.0, 0.0, 200.0, 200.0])
    chat = _script_chat(monkeypatch, [_action("signoz_list_services", timeRange="24h")])
    _script_forward(monkeypatch, [{"status": "ok", "result": "svc", "elapsed_ms": 1}])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="application errors"))

    assert out["status"] == "unavailable"
    assert "exhausted 90s without a final answer" in out["reason"]
    assert "1 round(s) run" in out["reason"]
    assert len(chat) == 1


def test_budget_exhaustion_lands_a_labelled_partial_when_a_tool_call_succeeded(monkeypatch):
    """§36.6, the case the whole card exists for: the seconds bind mid-question
    and the user gets the labelled partial instead of a timeout notice.

    Round 1 runs with the full budget; at round 2's pre-check only 15s remain,
    inside `FINAL_RESERVE_S`, so that round is the landing maneuver.
    """
    _script_clock(monkeypatch, [0.0, 0.0, 75.0, 75.0, 75.0])
    chat = _script_chat(monkeypatch, [
        _action("signoz_search_logs", severity="ERROR", timeRange="24h"),
        _final("Partial answer (budget exhausted after 2 round(s)):\n\n33944 ERROR lines in "
               "24h, mostly aw-knowledgeable. Which call site raised them is not established.",
               ["signoz_search_logs: 33944 rows, severity=ERROR"]),
    ])
    _script_forward(monkeypatch, [{"status": "ok", "result": "33944 rows", "elapsed_ms": 900}])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="what errors did mark_flow_done throw"))

    assert out["status"] == "ok"
    assert out["result"].startswith("Partial answer (budget exhausted after 2 round(s)):")
    assert "33944 ERROR lines" in out["result"]
    # `partial` rides INSIDE the adapter block (§36.4) — `_coerce_source_payload`
    # passes that dict through verbatim, so this needs zero aw-knowledgeable
    # changes, and `status: ok` alone no longer implies complete (§35).
    assert out["adapter"] == {
        "rounds": 2,
        "tool_calls": [{"tool": "signoz_search_logs", "ok": True}],
        "partial": True,
    }
    forced = chat[1]["messages"][-1]["content"]
    assert forced.startswith("BUDGET EXHAUSTED")
    assert "only about 15s of budget remain" in forced
    # §36.1 — the landing call's HTTP timeout is `remaining`, never `budget_s`:
    # reusing the full budget here recreates the §9.3 hang inside the maneuver.
    assert chat[1]["timeout_s"] == pytest.approx(15.0)


def test_budget_exhaustion_with_no_successful_tool_call_fabricates_nothing(monkeypatch):
    """The gate with teeth (§36.5): the timeout path survives, but only
    genuinely empty-handed. A user must never be handed a confident-sounding
    partial synthesised from nothing."""
    _script_clock(monkeypatch, [0.0, 0.0, 75.0, 75.0, 75.0])
    chat = _script_chat(monkeypatch, [
        _action("signoz_search_logs", filter="severity='ERROR'"),
    ])
    _script_forward(monkeypatch, [
        {"status": "unavailable",
         "reason": "argument(s) not allow-listed for signoz_search_logs: filter",
         "elapsed_ms": 0},
    ])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert "exhausted 90s without a final answer" in out["reason"]
    assert "1 round(s) run" in out["reason"]
    assert "result" not in out
    assert "partial" not in out["adapter"]
    assert len(chat) == 1


def test_a_forced_final_that_is_not_a_usable_answer_degrades_to_unavailable(monkeypatch):
    """The model may spend its one landing reply on another tool call, or on
    prose. There is no correction round left — by construction the budget or
    the rounds are gone — so this is the honest exhaustion envelope again,
    never an empty `ok`."""
    _script_clock(monkeypatch, [0.0, 0.0, 75.0, 75.0, 75.0])
    _script_chat(monkeypatch, [
        _action("signoz_search_logs", severity="ERROR"),
        _action("signoz_list_services", timeRange="24h"),
    ])
    forwards = _script_forward(monkeypatch, [{"status": "ok", "result": "rows", "elapsed_ms": 1}])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert "exhausted 90s without a final answer" in out["reason"]
    assert "forced final reply was not a usable answer" in out["reason"]
    assert len(forwards) == 1, "a tool call proposed in the reserved round is not executed"


def test_an_unlabelled_forced_final_is_labelled_in_code(monkeypatch):
    """The design calls the label prompt-enforced and therefore soft. It is
    cheap to make it hard on the way out, and the product guarantee is that a
    partial can never read as a complete answer."""
    _script_clock(monkeypatch, [0.0, 0.0, 75.0, 75.0, 75.0])
    _script_chat(monkeypatch, [
        _action("signoz_search_logs", severity="ERROR"),
        _final("33944 ERROR lines in 24h."),
    ])
    _script_forward(monkeypatch, [{"status": "ok", "result": "33944 rows", "elapsed_ms": 1}])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["result"] == (
        "Partial answer (budget exhausted after 2 round(s)):\n\n33944 ERROR lines in 24h."
    )
    assert out["adapter"]["partial"] is True


def test_the_model_is_told_its_round_and_remaining_budget_every_round(monkeypatch):
    """§32 D2's self-pacing. Today the model cannot know it is near a cliff —
    both failing legs were still mid-exploration at 90s."""
    _script_clock(monkeypatch, [0.0, 0.0, 12.0, 12.0, 12.0])
    chat = _script_chat(monkeypatch, [
        _action("signoz_list_services", timeRange="24h"),
        _final("done"),
    ])
    _script_forward(monkeypatch, [{"status": "ok", "result": "svc", "elapsed_ms": 1}])

    _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert "Round 1 of 6; about 90s of budget remain." in chat[0]["messages"][1]["content"]
    feedback = chat[1]["messages"][-1]["content"]
    assert "Round 2 of 6; about 78s of budget remain." in feedback
    assert "at most 4 more tool call(s) will be executed" in feedback


def test_a_tiny_budget_still_gets_one_exploring_round_before_it_lands(monkeypatch):
    """§36.7c's instrument probe is a scratch entry with `budget_s: 5` — below
    `FINAL_RESERVE_S`. The reserve must not swallow round 1, or that probe
    returns zero rounds and zero evidence and measures nothing."""
    entry = {**SIGNOZ_ENTRY, "adapter": {"max_rounds": 8, "budget_s": 5}}
    chat = _script_chat(monkeypatch, [
        _action("signoz_list_services", timeRange="24h"),
        _final("two services ran"),
    ])
    forwards = _script_forward(monkeypatch, [{"status": "ok", "result": "svc", "elapsed_ms": 1}])

    out = _run(nl_adapter.solve(entry, query="x"))

    assert len(forwards) == 1, "round 1 must still be a real exploring round"
    assert len(chat) == 2, "round 2 is then the landing maneuver"
    assert out["status"] == "ok"
    assert out["adapter"]["rounds"] == 2, "the instrument must report real rounds"
    assert out["adapter"]["partial"] is True


def test_the_remaining_budget_is_also_the_rounds_http_timeout(monkeypatch):
    """Checked before every round AND passed as the per-call timeout — a hung
    round would otherwise eat the whole budget invisibly (§9.3)."""
    _script_clock(monkeypatch, [0.0, 0.0, 30.0, 30.0, 30.0])
    chat = _script_chat(monkeypatch, [
        _action("signoz_list_services", timeRange="24h"),
        _final("done"),
    ])
    _script_forward(monkeypatch, [{"status": "ok", "result": "svc", "elapsed_ms": 1}])

    _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert chat[0]["timeout_s"] == pytest.approx(90.0)
    assert chat[1]["timeout_s"] == pytest.approx(60.0)


def test_an_unlisted_tool_is_fed_back_as_a_correction_and_the_next_round_succeeds(monkeypatch):
    """A by-name rejection from the one enforcement point is a correction
    signal, not a loop failure (§3.2) — the agent is structurally unable to
    reach anything outside this source's allow-list, and discovers that the
    same way it discovers anything else."""
    _script_chat(monkeypatch, [
        _action("signoz_execute_builder_query", query="SELECT *"),
        _action("signoz_list_services", timeRange="24h"),
        _final("two services, both healthy"),
    ])
    _script_forward(monkeypatch, [
        {"status": "unavailable",
         "reason": "tool 'signoz_execute_builder_query' is not allow-listed for upstream 'signoz'",
         "elapsed_ms": 0},
        {"status": "ok", "result": "aw-knowledgeable\naw-backend", "elapsed_ms": 20},
    ])
    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="which services exist?"))

    assert out["status"] == "ok"
    assert out["result"].startswith("two services, both healthy")
    assert out["adapter"]["tool_calls"] == [
        {"tool": "signoz_execute_builder_query", "ok": False},
        {"tool": "signoz_list_services", "ok": True},
    ]


def test_an_unlisted_argument_is_fed_back_verbatim(monkeypatch):
    chat = _script_chat(monkeypatch, [
        _action("signoz_search_logs", filter="severity='ERROR'"),
        _final("done"),
    ])
    _script_forward(monkeypatch, [
        {"status": "unavailable",
         "reason": "argument(s) not allow-listed for signoz_search_logs: filter",
         "elapsed_ms": 0},
    ])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "ok"
    feedback = chat[1]["messages"][-1]["content"]
    assert "REJECTED by the allow-list for `signoz_search_logs`" in feedback
    assert "argument(s) not allow-listed for signoz_search_logs: filter" in feedback


def test_an_oversized_argument_is_a_correction_not_a_terminal_failure(monkeypatch):
    """§9.1 — MAX_ARG_CHARS applies to every adapter-issued arg. The model
    gets told, rather than the call dying."""
    chat = _script_chat(monkeypatch, [
        _action("signoz_search_logs", searchText="x" * (forwarding.MAX_ARG_CHARS + 1)),
        _final("done"),
    ])
    _script_forward(monkeypatch, [
        {"status": "unavailable",
         "reason": f"argument(s) exceed {forwarding.MAX_ARG_CHARS} chars: searchText",
         "elapsed_ms": 0},
    ])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "ok"
    assert "exceed 2000 chars" in chat[1]["messages"][-1]["content"]


def test_a_transport_failure_is_terminal_for_the_whole_call(monkeypatch):
    """§9.2 — one transport failure populates the negative probe cache for
    ``ttl_fail_s`` (15s on signoz), so every remaining round would get an
    instant cached "unavailable". Spinning rounds against a cache is worse
    than stopping, so the whole call ends with the transport's own reason."""
    chat = _script_chat(monkeypatch, [
        _action("signoz_list_services", timeRange="24h"),
        _final("should never be asked for"),
    ])
    _script_forward(monkeypatch, [
        {"status": "unavailable",
         "reason": "signoz-connector mcp.json declares no server 'signoz-query'",
         "elapsed_ms": 3},
    ])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert "transport failure calling signoz_list_services on signoz" in out["reason"]
    assert "declares no server 'signoz-query'" in out["reason"]
    assert "negative probe cache" in out["reason"]
    assert len(chat) == 1, "no further round may be spent against the poisoned cache"


def test_two_consecutive_unparsable_replies_end_the_call(monkeypatch):
    _script_chat(monkeypatch, ["I think you should look at the logs.", "Still prose, sorry."])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert "2 consecutive replies" in out["reason"]
    assert out["adapter"]["rounds"] == 2


def test_one_unparsable_reply_is_corrected_and_the_streak_resets(monkeypatch):
    chat = _script_chat(monkeypatch, [
        "prose, not JSON",
        _action("signoz_list_services", timeRange="24h"),
        "prose again",
        _final("recovered"),
    ])
    _script_forward(monkeypatch, [{"status": "ok", "result": "svc", "elapsed_ms": 1}])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "ok"
    assert out["result"] == "recovered"
    assert "not a usable action" in chat[1]["messages"][-1]["content"]


def test_a_json_object_with_neither_final_nor_tool_counts_as_unparsable(monkeypatch):
    _script_chat(monkeypatch, ['{"thinking": "hmm"}', '{"notes": []}'])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert "not a usable JSON action object" in out["reason"]


def test_an_oversized_final_answer_is_truncated_and_flagged(monkeypatch):
    _script_chat(monkeypatch, [_final("z" * (nl_adapter.MAX_FINAL_CHARS + 500))])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "ok"
    assert len(out["result"]) == nl_adapter.MAX_FINAL_CHARS
    assert out["truncated"] is True


def test_a_fed_back_tool_result_is_truncated_so_six_rounds_cannot_run_away(monkeypatch):
    """``forward()`` already caps each result at 32KB; the adapter cuts the
    fed-back copy further so six rounds cannot build an unbounded prompt."""
    chat = _script_chat(monkeypatch, [
        _action("signoz_search_logs", severity="ERROR"),
        _final("done"),
    ])
    _script_forward(monkeypatch, [
        {"status": "ok", "result": "L" * (nl_adapter.MAX_FED_BACK_CHARS * 3), "elapsed_ms": 1},
    ])

    _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    fed_back = chat[1]["messages"][-1]["content"]
    assert "…[truncated]" in fed_back
    body = fed_back.split(":\n", 1)[1].split("\n…[truncated]", 1)[0]
    assert body == "L" * nl_adapter.MAX_FED_BACK_CHARS


def test_solve_never_raises_even_on_an_unexpected_bug(monkeypatch):
    """§9.5 — ``/source-query`` has never returned a 500 and must not start
    here. An unexpected exception degrades to a declared envelope."""
    async def _explode(api_key, messages, timeout_s):
        raise ZeroDivisionError("a bug nobody predicted")

    monkeypatch.setattr(nl_adapter, "_chat", _explode)

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert "unexpected failure: ZeroDivisionError" in out["reason"]


def test_a_missing_prompt_file_is_reported_by_name_not_as_a_bad_model(monkeypatch):
    monkeypatch.setattr(nl_adapter, "PROMPT_PATH", Path("/nope/nl-adapter.md"))
    _script_chat(monkeypatch, [])

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "unavailable"
    assert "nl-adapter.md is missing from the installed app" in out["reason"]


def test_the_shipped_prompt_states_the_protocol_and_the_arg_cap():
    """The prompt is a shipped artefact of this release (§5), so a packaging
    mistake has to fail here rather than in production."""
    text = nl_adapter.PROMPT_PATH.read_text(encoding="utf-8")
    assert '"tool"' in text and '"args"' in text and '"final"' in text
    assert str(forwarding.MAX_ARG_CHARS) in text


# ---------------------------------------------------------------------------
# ap-mt transport details
# ---------------------------------------------------------------------------


def test_the_call_is_bearer_scoped_to_the_adapter_agent(monkeypatch):
    """ap-mt's ``require_api_key`` does ``removeprefix("Bearer ")`` and looks
    the remainder up verbatim, so any other scheme name fails as a 401 that
    reads exactly like a revoked key. ``max_tokens`` is deliberately absent —
    ap-mt ignores it, the agent row owns its own ceiling."""
    seen: dict = {}

    class _Capture:
        def __init__(self, *a, **k):
            seen["timeout"] = k.get("timeout")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None, headers=None):
            seen.update(url=url, body=json, headers=headers)
            return SimpleNamespace(
                status_code=200, text="",
                json=lambda: {"choices": [{"message": {"content": '{"final": "ok"}'}}]},
            )

    monkeypatch.setattr(nl_adapter.httpx, "AsyncClient", _Capture)
    monkeypatch.setenv(nl_adapter.BASE_URL_ENV, "http://apmt.example/")

    out = _run(nl_adapter.solve(SIGNOZ_ENTRY, query="x"))

    assert out["status"] == "ok"
    assert seen["url"] == "http://apmt.example/v1/chat/completions"
    assert seen["headers"]["Authorization"] == "Bearer test-api-key"
    assert seen["body"]["model"] == "agent/knowledgeable-nl-adapter"
    assert "max_tokens" not in seen["body"]


def test_the_base_url_falls_back_through_env_then_runners_config_then_the_constant(
        monkeypatch, tmp_path):
    monkeypatch.setenv(nl_adapter.BASE_URL_ENV, "http://from-env:1/")
    assert nl_adapter.apmt_base_url() == "http://from-env:1"

    monkeypatch.delenv(nl_adapter.BASE_URL_ENV, raising=False)
    monkeypatch.setenv("AW_WORKSPACE_HOME", str(tmp_path))
    assert nl_adapter.apmt_base_url() == nl_adapter.DEFAULT_APMT_BASE_URL

    cfg_dir = tmp_path / "app-config"
    cfg_dir.mkdir()
    (cfg_dir / f"{nl_adapter.RUNNERS_APP_ID}.json").write_text(
        json.dumps({"config": {"agents_platform_base": "http://172.18.0.1:10014"}}),
        encoding="utf-8",
    )
    assert nl_adapter.apmt_base_url() == "http://172.18.0.1:10014"


# ---------------------------------------------------------------------------
# §4 / §9.6 — back-compat is a test, not a hope
# ---------------------------------------------------------------------------


class _FakeSecrets:
    def __init__(self) -> None:
        self._v: dict[str, str] = {}

    def read(self, name):
        return self._v.get(name)

    def write(self, name, value):
        self._v[name] = value

    def delete(self, name):
        self._v.pop(name, None)


@pytest.fixture()
def source_query_client():
    """Just enough ctx for ``POST /source-query`` — the route reads
    ``ctx.secrets`` and ``ctx.config`` and nothing else."""
    ctx = SimpleNamespace(
        secrets=_FakeSecrets(), config={},
        state=SimpleNamespace(kv=None), db=None,
    )
    ctx.secrets.write(routes_mod.SECRET_KEY, "shh")
    client.configure(lambda: client.DEFAULT_BASE_URL,
                     lambda: ctx.secrets.read(routes_mod.SECRET_KEY) or "")
    forwarding.configure(lambda: ctx.config or {})
    return TestClient(routes_mod.build_routes(ctx)), ctx


def test_an_entry_without_the_new_fields_takes_the_legacy_branch(source_query_client, monkeypatch):
    """§9.6 — the whole zero-regression claim for the only leg that works end
    to end today, asserted rather than hoped. ``nl_adapter`` is not merely
    unused on this path: it is never even imported, which is why
    ``routes.py`` defers that import into the NL-false branch."""
    tc, ctx = source_query_client
    ctx.config = {"forward_upstreams": [LEGACY_ENTRY]}
    sys.modules.pop("knowledgeable_app.nl_adapter", None)

    captured = {}

    async def fake_forward(upstream_id, tool, args):
        captured.update(upstream_id=upstream_id, tool=tool, args=args)
        return {"status": "ok", "result": "3847 files indexed", "elapsed_ms": 12}

    monkeypatch.setattr(routes_mod.forwarding, "forward", fake_forward)
    resp = tc.post(
        "/source-query",
        json={"source_id": "codegraph", "query": "how does auth work"},
        headers={"x-internal-secret": "shh"},
    )

    assert resp.status_code == 200
    # Byte-identical to pre-adapter: no `adapter` block, no new field at all.
    assert resp.json() == {"status": "ok", "result": "3847 files indexed", "elapsed_ms": 12}
    assert captured == {
        "upstream_id": "codegraph",
        "tool": "codegraph_explore",
        "args": {"query": "how does auth work"},
    }
    assert "knowledgeable_app.nl_adapter" not in sys.modules


def test_an_nl_true_entry_still_lets_code_query_override_the_forwarded_value(
        source_query_client, monkeypatch):
    """The legacy branch keeps ``value = code_query or query`` exactly — the
    adapter changed what an NL-FALSE source is asked, and nothing else."""
    tc, ctx = source_query_client
    ctx.config = {"forward_upstreams": [{**LEGACY_ENTRY, "accepts_natural_language": True}]}
    captured = {}

    async def fake_forward(upstream_id, tool, args):
        captured.update(args=args)
        return {"status": "ok", "result": "ok", "elapsed_ms": 1}

    monkeypatch.setattr(routes_mod.forwarding, "forward", fake_forward)
    tc.post(
        "/source-query",
        json={"source_id": "codegraph", "query": "how does auth work",
              "code_query": "AuthMiddleware"},
        headers={"x-internal-secret": "shh"},
    )

    assert captured["args"] == {"query": "AuthMiddleware"}


def test_an_unknown_source_id_still_reaches_forward_and_is_rejected_by_name(source_query_client):
    tc, ctx = source_query_client
    ctx.config = {"forward_upstreams": [SIGNOZ_ENTRY]}

    resp = tc.post(
        "/source-query",
        json={"source_id": "not-a-real-source", "query": "x"},
        headers={"x-internal-secret": "shh"},
    )

    assert resp.status_code == 200
    assert resp.json()["status"] == "unavailable"
    assert "no forward_upstreams entry" in resp.json()["reason"]


def test_an_nl_false_entry_routes_through_the_adapter_and_never_a_500(
        source_query_client, monkeypatch):
    """The card's own acceptance for N1: with neither the key nor the agent
    row minted, a flipped entry answers ``unavailable`` with a reason naming
    what is missing — never a 500, and never a silent fallback to the blind
    ``query_tool`` forward."""
    tc, ctx = source_query_client
    ctx.config = {"forward_upstreams": [SIGNOZ_ENTRY]}

    async def _no_key(name, *, reason):
        return None

    monkeypatch.setattr(nl_adapter.key_vault, "read_secret", _no_key)

    async def _must_not_forward(upstream_id, tool, args):
        raise AssertionError("the blind query_tool forward is the garbage path — never a fallback")

    monkeypatch.setattr(routes_mod.forwarding, "forward", _must_not_forward)
    monkeypatch.setattr(nl_adapter.forwarding, "forward", _must_not_forward)

    resp = tc.post(
        "/source-query",
        json={"source_id": "signoz", "query": "application errors"},
        headers={"x-internal-secret": "shh"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "unavailable"
    assert nl_adapter.SECRET_NAME in body["reason"]
    assert body["adapter"] == {"rounds": 0, "tool_calls": []}


# ---------------------------------------------------------------------------
# Part 4 / N5b — the transport wrapper. `stream_solve` plus the route change,
# and the one property the whole fix rests on: a streamed envelope parses to
# byte-for-byte what a buffered one parsed to, because the only thing allowed
# to precede it is JSON-legal whitespace (§22.1).
# ---------------------------------------------------------------------------


def _drain(agen) -> list[bytes]:
    """Every chunk `stream_solve` yields, in order."""

    async def _collect():
        return [chunk async for chunk in agen]

    return _run(_collect())


def test_stream_solve_commits_the_head_before_awaiting_anything(monkeypatch):
    """§25.2 — the first byte must leave before the loop is even started, so
    the head frame crosses the relay's 30s deadline (aw-backend
    workspace_tunnel_proxy.py:631) in <1s no matter how long the loop takes.
    Asserted by making `solve` itself fail if it is reached before the first
    chunk has been pulled."""
    pulled: list[str] = []

    async def _fake_solve(entry, *, query, code_query=""):
        pulled.append("solve")
        return {"status": "ok", "result": "x", "elapsed_ms": 1}

    monkeypatch.setattr(nl_adapter, "solve", _fake_solve)

    async def _first_chunk_only():
        agen = nl_adapter.stream_solve(SIGNOZ_ENTRY, query="errors?")
        first = await agen.__anext__()
        assert pulled == [], "solve must not have been started by the first chunk"
        await agen.aclose()
        return first

    assert _run(_first_chunk_only()) == nl_adapter._HEARTBEAT


def test_stream_solve_heartbeats_while_the_loop_runs_then_yields_the_envelope(monkeypatch):
    """The shape on the wire: heartbeat, heartbeats while it works, envelope.
    Cadence is driven down to make the test fast; the real 10s vs the relay's
    30s per-frame window is the headroom argument, not something a unit test
    can observe."""
    envelope = {"status": "ok", "result": "the real answer", "elapsed_ms": 41200,
                "adapter": {"rounds": 3, "tool_calls": [{"tool": "signoz_search_logs", "ok": True}]}}

    async def _slow_solve(entry, *, query, code_query=""):
        await asyncio.sleep(0.07)
        return envelope

    monkeypatch.setattr(nl_adapter, "solve", _slow_solve)
    monkeypatch.setattr(nl_adapter, "HEARTBEAT_INTERVAL_S", 0.01)

    chunks = _drain(nl_adapter.stream_solve(SIGNOZ_ENTRY, query="errors?"))

    assert chunks[0] == nl_adapter._HEARTBEAT
    assert len(chunks) > 2, f"expected heartbeats while the loop ran, got {chunks!r}"
    assert all(c == nl_adapter._HEARTBEAT for c in chunks[:-1])
    # The envelope is unchanged, and the whole body still parses as the same
    # object — that is what keeps the fan-out's `resp.json()`,
    # `_coerce_source_payload`, the 256KB guard and sources-digest untouched.
    assert json.loads(b"".join(chunks).decode()) == envelope


def test_every_chunk_before_the_envelope_is_json_leading_whitespace(monkeypatch):
    """The property the fix is cheap because of, pinned directly: emit one
    byte that is not JSON-leading whitespace and the receiving `resp.json()`
    stops parsing the same envelope."""
    async def _slow_solve(entry, *, query, code_query=""):
        await asyncio.sleep(0.05)
        return {"status": "ok", "result": "x", "elapsed_ms": 1}

    monkeypatch.setattr(nl_adapter, "solve", _slow_solve)
    monkeypatch.setattr(nl_adapter, "HEARTBEAT_INTERVAL_S", 0.01)

    chunks = _drain(nl_adapter.stream_solve(SIGNOZ_ENTRY, query="errors?"))
    prefix = b"".join(chunks[:-1])

    assert prefix, "there should be at least the immediate head byte"
    assert prefix.strip() == b"", f"non-whitespace reached the wire before the envelope: {prefix!r}"
    assert chunks[-1].lstrip().startswith(b"{")


def test_a_failure_after_the_head_is_committed_is_an_unavailable_envelope(monkeypatch):
    """§25.4, the sharpest edge in N5b: status 200 is already spent, so an
    exception from `solve` — which it is not supposed to be able to raise —
    must still arrive as a well-formed `unavailable` envelope in the BODY,
    not as an HTTP error and not as a truncated body that the fan-out leg
    would misreport as "response was not JSON"."""
    async def _exploding_solve(entry, *, query, code_query=""):
        raise RuntimeError("boom, after the head")

    monkeypatch.setattr(nl_adapter, "solve", _exploding_solve)

    chunks = _drain(nl_adapter.stream_solve(SIGNOZ_ENTRY, query="errors?"))
    body = json.loads(b"".join(chunks).decode())

    assert body["status"] == "unavailable"
    assert "after the response head was committed" in body["reason"]
    assert "RuntimeError: boom, after the head" in body["reason"]
    assert body["adapter"] == {"rounds": 0, "tool_calls": []}
    assert isinstance(body["elapsed_ms"], int)


def test_an_unserializable_envelope_still_arrives_as_an_unavailable_envelope(monkeypatch):
    """Same reason as above, one layer further out — the backstop in
    `_render_envelope`. A `TypeError` from `json.dumps` after the head is
    committed would truncate the body too."""
    async def _bad_envelope(entry, *, query, code_query=""):
        return {"status": "ok", "result": object(), "elapsed_ms": 1}

    monkeypatch.setattr(nl_adapter, "solve", _bad_envelope)

    body = json.loads(b"".join(_drain(nl_adapter.stream_solve(SIGNOZ_ENTRY, query="x"))).decode())

    assert body["status"] == "unavailable"
    assert "did not serialize" in body["reason"]


def test_closing_the_stream_cancels_the_adapter_loop(monkeypatch):
    """A client disconnect must abandon the in-flight `/v1` round rather than
    leave it running against a reader that is gone (§22.1)."""
    state: dict = {}

    async def _never_finishes(entry, *, query, code_query=""):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise
        return {}

    monkeypatch.setattr(nl_adapter, "solve", _never_finishes)
    monkeypatch.setattr(nl_adapter, "HEARTBEAT_INTERVAL_S", 0.01)

    async def _open_then_close():
        agen = nl_adapter.stream_solve(SIGNOZ_ENTRY, query="errors?")
        await agen.__anext__()          # head
        await agen.__anext__()          # first heartbeat — the loop is running
        await agen.aclose()
        await asyncio.sleep(0.01)       # let the cancellation land

    _run(_open_then_close())

    assert state.get("cancelled") is True


def test_the_adapter_branch_streams_and_parses_to_the_same_envelope(
        source_query_client, monkeypatch):
    """End to end through the route: NL-false answers 200 with a whitespace-
    prefixed body that `resp.json()` reads as the unchanged envelope."""
    tc, ctx = source_query_client
    ctx.config = {"forward_upstreams": [SIGNOZ_ENTRY]}
    envelope = {"status": "ok", "result": "real errors", "elapsed_ms": 45000,
                "adapter": {"rounds": 4, "tool_calls": []}}

    async def _fake_solve(entry, *, query, code_query=""):
        assert query == "quais erros estão acontecendo na aplicação"
        return envelope

    monkeypatch.setattr(nl_adapter, "solve", _fake_solve)

    resp = tc.post(
        "/source-query",
        json={"source_id": "signoz", "query": "quais erros estão acontecendo na aplicação"},
        headers={"x-internal-secret": "shh"},
    )

    assert resp.status_code == 200
    assert resp.content.startswith(b" "), f"the head byte should lead: {resp.content[:20]!r}"
    assert resp.json() == envelope


def test_the_legacy_branch_is_still_buffered_with_no_leading_whitespace(
        source_query_client, monkeypatch):
    """§25.3 — the cheap proxy for "byte-identical": codegraph is the leg that
    already works and its response must not change shape at all, so byte 0 is
    still `{` and there is no transfer-encoding change."""
    tc, ctx = source_query_client
    ctx.config = {"forward_upstreams": [LEGACY_ENTRY]}

    async def fake_forward(upstream_id, tool, args):
        return {"status": "ok", "result": "3847 files indexed", "elapsed_ms": 12}

    monkeypatch.setattr(routes_mod.forwarding, "forward", fake_forward)
    resp = tc.post(
        "/source-query",
        json={"source_id": "codegraph", "query": "how does auth work"},
        headers={"x-internal-secret": "shh"},
    )

    assert resp.status_code == 200
    assert resp.content[:1] == b"{"
    assert resp.headers.get("content-length") is not None
    assert resp.json() == {"status": "ok", "result": "3847 files indexed", "elapsed_ms": 12}


def test_auth_and_validation_still_answer_before_any_stream_begins(source_query_client):
    """§25.3 — only the adapter execution moved inside the generator; a bad
    secret is still a real 403 and a missing field still a real 400, not a
    200 with an envelope."""
    tc, ctx = source_query_client
    ctx.config = {"forward_upstreams": [SIGNOZ_ENTRY]}

    bad_secret = tc.post("/source-query", json={"source_id": "signoz", "query": "x"},
                         headers={"x-internal-secret": "wrong"})
    assert bad_secret.status_code == 403

    no_query = tc.post("/source-query", json={"source_id": "signoz"},
                       headers={"x-internal-secret": "shh"})
    assert no_query.status_code == 400
