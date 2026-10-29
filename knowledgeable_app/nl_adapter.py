"""The NL→Tool-Calling Adapter — ``docs/design/knowledgeable-nl-tool-adapter.md``
§3, card N1.

``POST /source-query`` forwards one generic natural-language query to every
source through that source's ``query_tool``. For codegraph that is a real
semantic search. For SigNoz it is ``signoz_search_logs.searchText``, which is
literal CONTAINS over raw log bodies — so the source is asked to find the
restated question *verbatim in a log line* and returns 0 rows, while
``signoz_search_logs(severity="ERROR", timeRange="24h")`` returns exactly the
data the question wanted. The forwarder assumed every source is NL-capable;
SigNoz structurally is not.

This module is the other branch. For an entry that declares
``accepts_natural_language: false`` it runs a **bounded tool-calling loop**:
one ap-mt ``POST /v1/chat/completions`` call per round against the stateless
``knowledgeable-nl-adapter`` agent, which proposes ``{tool, args}`` as a JSON
object and *this process* executes it through :func:`forwarding.forward`.

Three properties that are structural, not prompt-deep:

* **The agent cannot reach anything outside the source's allow-list.** It has
  no MCP access at all (``disable_harness_tools: true``, no servers): it is a
  text-in/JSON-out brain. The hands are ``forwarding.forward()``, the one
  enforcement point, which rejects an unlisted tool or argument by name with
  the registry as the single source of truth (§3.4). A rejection is fed back
  to the model as a correction signal, not treated as a loop failure.
* **There is no fallback to the blind ``query_tool`` forward.** That is the
  known-garbage path this whole design exists to kill, and a
  plausible-but-empty result is worse than an honest ``unavailable`` — which
  ``sources-digest`` already keeps visible to the synthesizer as
  status+reason. Every degraded mode (no ApiKey, ap-mt unreachable, two
  unparsable replies, rounds or budget exhausted) is declared in the envelope.
* **Never raises.** ``/source-query`` has never returned a 500 and must not
  start here (§9.5), so :func:`solve` catches everything and answers with an
  envelope instead.

Both bounds are enforced: ``max_rounds`` AND ``budget_s`` wall clock, checked
before every round and passed as that round's HTTP timeout — an ap-mt call can
land minutes late (``apmt-mcp-write-timeouts-can-land-minutes-late``), so a
round count alone does not bound this (§9.3).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import httpx

from . import forwarding, key_vault

log = logging.getLogger("aw_apps.knowledgeable.nl_adapter")

#: Fifth member of the connector's ap-mt key family, minted by N2 and scoped
#: to call only the adapter agent. Unlike the four ``*_key_push.py`` siblings
#: this key is consumed **locally** and never pushed to production
#: aw-knowledgeable — there is no push loop for it, only this read (§3.5).
SECRET_NAME = "knowledgeable-nl-adapter-apmt-key"

#: The stateless ap-mt agent row (created by N2). One generic adapter, not one
#: per source: the per-source knowledge lives in the registry entry and is
#: injected at call time, so the agent row carries zero source-specific
#: content (§3.3).
AGENT_SLUG = "knowledgeable-nl-adapter"

#: ap-mt's ``/v1`` model name for an agent — the ``agent/<slug>`` spelling
#: every other caller in this estate uses (aw-knowledgeable's
#: ``topic_label_apmt_agent`` and friends).
APMT_MODEL = f"agent/{AGENT_SLUG}"

#: Each round's fed-back tool result is truncated to this, on top of the 32KB
#: cap ``forward()`` already applies per result — six rounds must not be able
#: to build an unbounded prompt (§3.2).
MAX_FED_BACK_CHARS = 8_000

#: The adapter's own final answer cap, for envelope symmetry with
#: ``forward()``'s ``MAX_RESULT_CHARS``.
MAX_FINAL_CHARS = forwarding.MAX_RESULT_CHARS

#: Floor on a round's HTTP timeout. A budget with a sliver left should fail
#: the budget check below rather than issue a call that cannot possibly
#: complete, but if it gets here, a sub-second timeout is still honest.
MIN_ROUND_TIMEOUT_S = 1.0

#: Two *consecutive* unparsable replies end the call (§3.2). One is a
#: correction opportunity — the model is told what it did wrong and asked
#: again, which is the same tolerance the rest of this estate's JSON-action
#: callers extend.
MAX_CONSECUTIVE_UNPARSABLE = 2

#: Where the ap-mt base URL comes from, in order. The env override exists for
#: tests and for a workspace pointed at a different platform; the
#: agents-platform-runners app's own saved config is the locally-proven value
#: (``aw-app-research-search/research_search_app/apmt_credential.py`` reads the
#: same field for the same reason); the constant is aw-knowledgeable's own
#: ``apmt_base_url`` default, so a workspace with neither still reaches the
#: same door every other ap-mt caller here does.
BASE_URL_ENV = "KNOWLEDGEABLE_NL_ADAPTER_APMT_BASE_URL"
RUNNERS_APP_ID = "agents-platform-runners"
DEFAULT_APMT_BASE_URL = "https://agents-platform.aw.tekflox.com"

#: Shipped in this repo, beside the code that consumes it (§5) — a Tier-1
#: release is how a prompt change reaches production, deliberately.
PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "nl-adapter.md"

#: ap-mt's ``/v1`` answers a FAILED run with **HTTP 200** and the failure as
#: prose in ``choices[0].message.content``, shaped ``[<kind> <slug> <status>]
#: <error>`` (``openai_compat.py::_run``). A 2xx is therefore not proof of a
#: reply, and this is also the shape a *missing agent row* arrives as — which
#: is exactly the N1 end state, so the detail is carried into the ``reason``
#: verbatim rather than collapsed into "the model said nothing". Pattern
#: copied from aw-knowledgeable's ``core/apmt.py::_APMT_RUN_FAILED_RE``.
_APMT_RUN_FAILED_RE = re.compile(r"^\s*\[(?:(?:agent|workflow)\s+\S+\s+)?(\w+)\]\s*(.*)", re.DOTALL)

_FENCE_RE = re.compile(r"^```[a-zA-Z]*\s*|\s*```$")


class _Unavailable(Exception):
    """A degraded mode that ends this adapter call. Carries the operator-facing
    ``reason`` string verbatim — raised internally so every exit path funnels
    through the one envelope builder in :func:`solve`, rather than each failure
    site assembling its own."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# Reaching ap-mt
# ---------------------------------------------------------------------------


def _workspace_home() -> str:
    """Same resolution as ``sources_sync._workspace_home`` /
    ``bulk_ingest._workspace_home`` — deliberately a fourth copy rather than
    an import, for the reason those two already record: no shared library
    exists between these modules and this one must not depend on a module that
    owns background loops."""
    home = os.environ.get("AW_WORKSPACE_HOME")
    if home:
        return home
    root = os.path.realpath(os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace"))
    return os.path.join(root, ".aw-workspace")


def _runners_base_url() -> str:
    """Best-effort read of the agents-platform-runners app's saved
    ``agents_platform_base``. Tolerates the ``config`` wrapper and never
    raises — an absent or unreadable file just means "fall through to the
    constant"."""
    path = os.path.join(_workspace_home(), "app-config", f"{RUNNERS_APP_ID}.json")
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError):
        return ""
    if not isinstance(raw, dict):
        return ""
    cfg = raw.get("config") if isinstance(raw.get("config"), dict) else raw
    return str((cfg or {}).get("agents_platform_base") or "").strip()


def apmt_base_url() -> str:
    """See :data:`BASE_URL_ENV` for the resolution order and why."""
    env = (os.environ.get(BASE_URL_ENV) or "").strip()
    if env:
        return env.rstrip("/")
    runners = _runners_base_url()
    if runners:
        return runners.rstrip("/")
    return DEFAULT_APMT_BASE_URL


# ---------------------------------------------------------------------------
# Prompt composition
# ---------------------------------------------------------------------------


def _protocol_prompt() -> str:
    """The static JSON-action protocol, read fresh so a prompt edit needs no
    restart beyond the one the Tier-1 release already requires."""
    try:
        return PROMPT_PATH.read_text(encoding="utf-8").strip()
    except OSError as exc:
        # A packaging mistake, not a model problem — say so by name rather
        # than silently running with an empty system prompt, which would read
        # downstream as "the model is bad at this".
        raise _Unavailable(f"prompt file {PROMPT_PATH.name} is missing from the installed app: {exc}")


def _tool_catalog(entry: dict) -> str:
    lines = []
    for name, cfg in sorted((entry.get("tools") or {}).items()):
        args = ", ".join(str(a) for a in (cfg or {}).get("args") or []) or "(no arguments)"
        hint = str((cfg or {}).get("hint") or "").strip()
        lines.append(f"- `{name}` — args: {args}" + (f"\n  hint: {hint}" if hint else ""))
    return "\n".join(lines) or "(this source declares no tools — nothing can be called)"


def system_prompt(entry: dict) -> str:
    """Protocol + this source's own declaration. The per-source knowledge is
    injected here, at call time, which is what lets one generic agent row
    serve every NL-false source (§3.3)."""
    blocks = [
        _protocol_prompt(),
        "## This source",
        f"source_id: `{entry.get('id')}`",
        "",
        "### What it answers (problem_domain)",
        str(entry.get("problem_domain") or "").strip() or "(not declared)",
        "",
        "### How it must be queried (query_guidance)",
        str(entry.get("query_guidance") or "").strip() or "(not declared)",
        "",
        "### Tools you may propose (nothing else exists)",
        _tool_catalog(entry),
    ]
    return "\n".join(blocks)


def _first_user_message(query: str, code_query: str) -> str:
    """The adapter receives the **original** ``query``; ``code_query`` is
    auxiliary context only.

    ``value = code_query or query`` on the legacy path is exactly what
    manufactured the garbage substring — for a tool-calling agent the full
    natural-language problem is the input and the restatement is at most a
    hint (§3.1).
    """
    parts = [f"PROBLEM (natural language, from the caller):\n{query}"]
    if code_query:
        parts.append(
            "AUXILIARY HINT (the caller's own restatement — context only, "
            f"never a value to send to a tool verbatim):\n{code_query}"
        )
    parts.append("Reply with ONE JSON object: an action or a final answer.")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Reply parsing
# ---------------------------------------------------------------------------


def json_object(text: str) -> dict[str, Any] | None:
    """The outermost ``{...}`` in a reply, parsed; ``None`` if there is none or
    it does not parse.

    Brace-slicing, never ``json.loads(reply)`` — the codex runner prepends a
    "Reading additional input from stdin..." banner to its replies
    (live-confirmed 2026-10-24), and models wrap JSON in a code fence anyway.
    The slice skips past both for free without needing to know they exist
    (§9.4). The ap-mt-side banner strip is defense in depth, not a guarantee.
    Same idiom as aw-knowledgeable's ``core/apmt.py::json_object``.
    """
    body = (text or "").strip()
    if body.startswith("```"):
        body = _FENCE_RE.sub("", body)
    start, end = body.find("{"), body.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        parsed = json.loads(body[start : end + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _allowlist_violation(entry: dict, tool: str, args: dict) -> bool:
    """Would ``forward()`` have rejected this call on the allow-list, before
    reaching any transport at all?

    This is how a *correction signal* is told apart from a *terminal transport
    failure*, both of which arrive as the same ``{status: "unavailable",
    reason}`` envelope. The distinction matters because of the negative probe
    cache: one transport failure populates it for ``ttl_fail_s`` (15s on
    signoz), so every subsequent round would get an instant cached
    "unavailable" and the adapter would spin its whole budget against a cache
    rather than a source (§9.2). A transport failure is therefore terminal.

    Deliberately predicated on the entry's own declaration rather than on
    matching ``forward()``'s reason strings: the registry is the single source
    of truth, and a reworded reason must not silently reclassify a failure.
    """
    tool_cfg = (entry.get("tools") or {}).get(tool)
    if tool_cfg is None:
        return True
    allowed = set((tool_cfg or {}).get("args") or [])
    if any(key not in allowed for key in args):
        return True
    return any(
        isinstance(value, str) and len(value) > forwarding.MAX_ARG_CHARS
        for value in args.values()
    )


# ---------------------------------------------------------------------------
# The ap-mt round
# ---------------------------------------------------------------------------


async def _chat(api_key: str, messages: list[dict], timeout_s: float) -> str:
    """One ap-mt ``POST /v1/chat/completions``, shaped exactly like
    aw-knowledgeable's ``core/apmt.py::chat_complete``.

    ``Authorization: Bearer <key>`` specifically: ap-mt's ``require_api_key``
    does ``removeprefix("Bearer ")`` and looks the remainder up verbatim, so
    any other scheme name fails as a 401 that reads like a revoked key.
    ``max_tokens`` is deliberately not sent — ap-mt accepts and ignores it
    because the target agent's own row owns its ceiling.

    Every failure is an :class:`_Unavailable` naming what failed: this is the
    "declared, never silent" half of the contract, and in N1 — with neither
    the key nor the agent row minted yet — it is the *expected* path.
    """
    url = f"{apmt_base_url()}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "content-type": "application/json"}
    body = {"model": APMT_MODEL, "messages": messages}
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as http_client:
            resp = await http_client.post(url, json=body, headers=headers)
    except httpx.HTTPError as exc:
        raise _Unavailable(f"ap-mt unreachable at {url}: {type(exc).__name__}: {exc}")
    if resp.status_code in (401, 403):
        raise _Unavailable(
            f"ap-mt rejected the {SECRET_NAME} ApiKey (HTTP {resp.status_code}) — "
            f"it must exist and be scoped to agent {AGENT_SLUG}"
        )
    if resp.status_code >= 400:
        raise _Unavailable(f"ap-mt {APMT_MODEL}: HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise _Unavailable(f"ap-mt {APMT_MODEL}: reply was not JSON: {exc}")

    choices = (data or {}).get("choices") or []
    if not choices:
        raise _Unavailable(f"ap-mt {APMT_MODEL}: reply carried no choices")
    content = (choices[0].get("message") or {}).get("content")
    text = content if isinstance(content, str) else ""

    failed = _APMT_RUN_FAILED_RE.match(text)
    if failed:
        status, detail = failed.group(1), failed.group(2).strip()
        raise _Unavailable(f"ap-mt run {status} for {APMT_MODEL}: {detail or '(no detail)'}")
    return text


# ---------------------------------------------------------------------------
# The loop
# ---------------------------------------------------------------------------


def _final_text(action: dict) -> str:
    """``result`` stays a plain string so ``_coerce_source_payload``
    passthrough, the 256KB fan-out guard and ``sources-digest``'s
    ``answer: result`` projection all keep working with zero changes — so the
    model's ``evidence`` list rides inline rather than as a second field."""
    text = str(action.get("final") or "").strip()
    evidence = action.get("evidence")
    if isinstance(evidence, list):
        lines = [str(item).strip() for item in evidence if str(item).strip()]
        if lines:
            text = text + "\n\nEvidence:\n" + "\n".join(f"- {line}" for line in lines)
    return text


async def solve(entry: dict, *, query: str, code_query: str = "") -> dict[str, Any]:
    """Answer ``query`` against one NL-false source, and never raise.

    Returns the unchanged ``/source-query`` envelope — ``{status, result,
    elapsed_ms}`` or ``{status: "unavailable", reason, elapsed_ms}`` — plus an
    additive ``adapter`` telemetry block (``rounds``, ``tool_calls``) that is
    the meter for §8's standing cost concern.
    """
    start = time.monotonic()
    source_id = str(entry.get("id") or "")
    max_rounds, budget_s = forwarding.adapter_bounds(entry)
    tool_calls: list[dict[str, Any]] = []
    rounds = 0

    def _envelope(extra: dict[str, Any]) -> dict[str, Any]:
        return {
            **extra,
            "elapsed_ms": int((time.monotonic() - start) * 1000),
            "adapter": {"rounds": rounds, "tool_calls": tool_calls},
        }

    def _unavailable(reason: str) -> dict[str, Any]:
        log.warning("nl adapter %r: %s", source_id, reason)
        return _envelope({"status": "unavailable", "reason": f"nl adapter: {reason}"})

    try:
        api_key = await key_vault.read_secret(
            SECRET_NAME, reason=f"NL→tool-calling adapter answering a /source-query for {source_id}"
        )
        if not api_key:
            return _unavailable(
                f"the ApiKey {SECRET_NAME!r} is not readable from the workspace vault — "
                f"it must be minted and scoped to agent {AGENT_SLUG} before this source "
                "can answer"
            )

        messages: list[dict[str, str]] = [
            {"role": "system", "content": system_prompt(entry)},
            {"role": "user", "content": _first_user_message(query, code_query)},
        ]
        unparsable_streak = 0

        while rounds < max_rounds:
            # Wall clock BEFORE the round and as the round's own HTTP timeout
            # — §9.3. A hung round would otherwise eat the whole budget
            # invisibly and the round counter would still read 1.
            remaining = budget_s - (time.monotonic() - start)
            if remaining <= 0:
                return _unavailable(
                    f"adapter exhausted {budget_s:g}s without a final answer "
                    f"({rounds} round(s) run)"
                )
            rounds += 1
            reply = await _chat(api_key, messages, max(MIN_ROUND_TIMEOUT_S, remaining))
            action = json_object(reply)

            if action is None or not (action.get("final") or action.get("tool")):
                unparsable_streak += 1
                if unparsable_streak >= MAX_CONSECUTIVE_UNPARSABLE:
                    return _unavailable(
                        f"the model returned {unparsable_streak} consecutive replies that were "
                        "not a usable JSON action object"
                    )
                messages.append({"role": "assistant", "content": reply})
                messages.append({
                    "role": "user",
                    "content": (
                        "That was not a usable action. Reply with ONE JSON object and nothing "
                        'else: either {"tool": "<name>", "args": {...}, "why": "<one line>"} or '
                        '{"final": "<answer>", "evidence": ["<tool>: <what it showed>"]}.'
                    ),
                })
                continue
            unparsable_streak = 0

            if action.get("final"):
                text = _final_text(action)
                out: dict[str, Any] = {"status": "ok", "result": text}
                if len(text) > MAX_FINAL_CHARS:
                    out["result"] = text[:MAX_FINAL_CHARS]
                    out["truncated"] = True
                return _envelope(out)

            tool = str(action.get("tool") or "")
            args = action.get("args")
            args = args if isinstance(args, dict) else {}
            # forwarding.forward() is the enforcement point, for the adapter
            # exactly as for every other caller — this module never consults
            # the allow-list to DECIDE anything, only to classify a failure.
            result = await forwarding.forward(source_id, tool, args)
            ok = result.get("status") == "ok"
            tool_calls.append({"tool": tool, "ok": ok})

            if not ok:
                reason = str(result.get("reason") or "unavailable")
                if not _allowlist_violation(entry, tool, args):
                    return _unavailable(
                        f"transport failure calling {tool} on {source_id}: {reason} — "
                        "terminal for this call, the negative probe cache would serve it "
                        "to every remaining round"
                    )
                feedback = f"REJECTED by the allow-list for `{tool}`: {reason}"
            else:
                body = str(result.get("result") or "")
                if len(body) > MAX_FED_BACK_CHARS:
                    body = body[:MAX_FED_BACK_CHARS] + "\n…[truncated]"
                feedback = f"RESULT of `{tool}`:\n{body}"

            messages.append({"role": "assistant", "content": reply})
            messages.append({
                "role": "user",
                "content": feedback + "\n\nReply with ONE JSON object: another action, or the final answer.",
            })

        return _unavailable(
            f"adapter exhausted {max_rounds} round(s) without a final answer"
        )
    except _Unavailable as exc:
        return _unavailable(exc.reason)
    except Exception as exc:  # noqa: BLE001
        # §9.5 — `/source-query` has never returned a 500 and must not start
        # here. An unexpected bug degrades to a declared envelope, loudly in
        # the log, rather than crashing the fan-out leg.
        log.exception("nl adapter %r: unexpected failure", source_id)
        return _unavailable(f"unexpected failure: {type(exc).__name__}: {exc}")
