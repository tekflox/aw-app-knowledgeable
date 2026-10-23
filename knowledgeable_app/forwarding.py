"""Generic forwardable-upstream registry (docs/design/knowledgeable-connector-
tool-forwarder.md) — codegraph is the first entry, not a special case.

Each registry entry ({id, transport, app_slug, tools (tool allow-list + per-
tool arg allow-list), probe, timeout_s}) names an installed workspace app's
own ``/api/apps/<app_slug>/mcp`` route, reached by loopback HTTP under this
process's own ``AW_WORKSPACE_API_KEY`` — the exact URL+header every
self-registering app already writes into its own ``mcp.json`` for the
gateway (``aw-app-codegraph/codegraph_app/mcp/self_register.py``). Never
routed through aw-mcp-gateway (zombie-caching + no gateway credential held
here, design §4).

Config is resolved through a callable rather than read once, same
read-fresh-every-call rule ``mcp/client.py`` already follows for its secret
— installed once from ``plugin.activate`` via ``configure()``; a config save
takes effect on the next tool call with no restart.

``forward()`` is the only entry point that calls out: it enforces the
tool+argument allow-list at the boundary (an unlisted tool or argument is
rejected by name, never silently dropped — ``projectPath`` is excluded this
way, by simply never being in a tool's allow-list), then performs the
forwarded ``tools/call`` itself, which doubles as the availability probe
(design §6 — the forwarded call's own failure populates the negative cache;
no separate pre-flight check). ``status_block()`` is the one place that
*does* run a dedicated probe call (the registry's declared ``probe.tool``,
e.g. ``codegraph_status``) for ``GET /status`` operator visibility, sharing
the same per-worker cache.

Per-worker cache only (no cross-worker state, design §4/§12 — 5 workers, 5
``activate()``s): worst case is 5 probes/min, acceptable for a cache.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable

import httpx

from .workspace_env import workspace_env

log = logging.getLogger("aw_apps.knowledgeable")

#: Hard ceiling on any forwarded string argument value (design §7) — these
#: values are forwarded into FTS/SQL on the far side; cheap insurance against
#: an oversized query, rejected explicitly rather than silently truncated.
MAX_ARG_CHARS = 2000

#: Cap on the forwarded result text so a composed response never blows the
#: tunnel's practical payload limits for a remote caller (design §13 item 9).
MAX_RESULT_CHARS = 32_000

_config_resolver: Callable[[], dict] = lambda: {}

#: Per-worker availability cache: upstream_id -> (ok, reason, checked_at).
_availability_cache: dict[str, tuple[bool, str | None, float]] = {}

#: docs/design/knowledgeable-knowledge-sources-registry.md §4b — which
#: allow-listed tool answers a generic natural-language query for an
#: upstream whose registry entry carries no explicit ``query_tool``. Shipped
#: as codegraph's own default in ``aw-app.json`` too; this is the fallback
#: for an older saved config that predates the field, not a second source of
#: truth. Additive and ignored by ``compose_knowledge_search`` (which still
#: hardcodes its own codegraph call below) — `POST /source-query` (routes.py)
#: is the only caller.
DEFAULT_QUERY_TOOL = {"tool": "codegraph_explore", "arg": "query"}


def configure(config_resolver: Callable[[], dict]) -> None:
    """Installed once from ``plugin.activate``; every function below reads
    through this callable on every call — see module docstring."""
    global _config_resolver
    _config_resolver = config_resolver


def _registry() -> list[dict]:
    try:
        cfg = _config_resolver() or {}
    except Exception:
        # Forwarding stays off when config can't be read — but never
        # silently: an empty registry and a broken resolver are
        # indistinguishable to every caller below, and this estate's
        # failure mode is silent degradation, not crashes.
        log.exception("forward_upstreams: config resolver failed; forwarding disabled")
        return []
    raw = cfg.get("forward_upstreams")
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            log.warning("forward_upstreams config is not valid JSON; ignoring")
            return []
    if not isinstance(raw, list):
        return []
    return [entry for entry in raw if isinstance(entry, dict) and entry.get("id")]


def _find_upstream(upstream_id: str) -> dict | None:
    for entry in _registry():
        if entry.get("id") == upstream_id:
            return entry
    return None


def query_tool_for(upstream_id: str) -> dict:
    """``{tool, arg}`` for ``POST /source-query`` (routes.py) — the
    entry's own ``query_tool`` if it declares one, else
    :data:`DEFAULT_QUERY_TOOL`. Never ``None``: an unknown ``upstream_id``
    still gets a tool name to try, and ``forward()`` is what actually
    rejects it (by upstream, not by this helper guessing wrong)."""
    entry = _find_upstream(upstream_id)
    query_tool = (entry or {}).get("query_tool") or {}
    return {
        "tool": query_tool.get("tool") or DEFAULT_QUERY_TOOL["tool"],
        "arg": query_tool.get("arg") or DEFAULT_QUERY_TOOL["arg"],
    }


def _aw_port() -> str:
    return workspace_env("AW_PORT") or "9030"


def _api_key() -> str:
    return workspace_env("AW_WORKSPACE_API_KEY")


def _extract_text(result: dict) -> str:
    content = result.get("content") or []
    parts = [c.get("text", "") for c in content if isinstance(c, dict) and c.get("type") == "text"]
    return "\n".join(parts) if parts else json.dumps(result, ensure_ascii=False)


async def _call_mcp(app_slug: str, tool: str, args: dict, timeout_s: float) -> tuple[dict | None, str | None]:
    """One loopback ``tools/call`` against an installed app's own ``/mcp``
    route. Never raises — every failure mode becomes ``(None, reason)`` so a
    caller never has to guess which exceptions are possible (design's "never
    a 500" rule, same as ``http_handler.handle_request``'s own last-resort
    catch)."""
    url = f"http://127.0.0.1:{_aw_port()}/api/apps/{app_slug}/mcp"
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": args},
    }
    headers = {"X-Api-Key": _api_key(), "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as http_client:
            resp = await http_client.post(url, json=body, headers=headers)
    except httpx.TimeoutException:
        return None, f"timed out after {timeout_s:g}s"
    except httpx.HTTPError as exc:
        return None, f"{app_slug} app not installed or not responding: {exc}"
    except Exception as exc:  # noqa: BLE001 — last resort, must not 500 the route
        return None, f"{app_slug} app not installed or not responding: {exc}"

    if resp.status_code >= 400:
        return None, f"{app_slug} app not installed or not responding: HTTP {resp.status_code}"
    try:
        data = resp.json()
    except ValueError:
        return None, f"{app_slug} returned a non-JSON response"

    result = data.get("result")
    if result is None:
        err = (data.get("error") or {}).get("message") if isinstance(data, dict) else None
        return None, err or "unknown error"
    if result.get("isError"):
        return None, _extract_text(result) or f"{tool} failed"
    return result, None


def _cache_get(upstream_id: str, probe_cfg: dict) -> tuple[bool, str | None] | None:
    """Fresh cache hit, honouring the success/failure TTLs — ``None`` means
    "no fresh entry, go probe for real" (design §6's call path)."""
    cached = _availability_cache.get(upstream_id)
    if cached is None:
        return None
    ok, reason, checked_at = cached
    ttl = float(probe_cfg.get("ttl_ok_s", 60)) if ok else float(probe_cfg.get("ttl_fail_s", 15))
    if time.time() - checked_at < ttl:
        return ok, reason
    return None


def _cache_put(upstream_id: str, ok: bool, reason: str | None) -> None:
    _availability_cache[upstream_id] = (ok, reason, time.time())


async def forward(upstream_id: str, tool: str, args: dict) -> dict[str, Any]:
    """Enforce the allow-list, then perform the forwarded call — the call
    itself is the availability probe (design §6). Returns
    ``{status: "ok", result, elapsed_ms[, truncated]}`` or
    ``{status: "unavailable", reason, elapsed_ms}``, never raises."""
    start = time.monotonic()

    def _unavailable(reason: str) -> dict[str, Any]:
        return {"status": "unavailable", "reason": reason, "elapsed_ms": int((time.monotonic() - start) * 1000)}

    entry = _find_upstream(upstream_id)
    if entry is None:
        return _unavailable(f"no forward_upstreams entry named '{upstream_id}'")

    tools_cfg = entry.get("tools") or {}
    tool_cfg = tools_cfg.get(tool)
    if tool_cfg is None:
        return _unavailable(f"tool '{tool}' is not allow-listed for upstream '{upstream_id}'")

    allowed_args = set(tool_cfg.get("args") or [])
    unknown = sorted(k for k in args if k not in allowed_args)
    if unknown:
        return _unavailable(f"argument(s) not allow-listed for {tool}: {', '.join(unknown)}")
    oversized = sorted(
        k for k, v in args.items() if isinstance(v, str) and len(v) > MAX_ARG_CHARS
    )
    if oversized:
        return _unavailable(f"argument(s) exceed {MAX_ARG_CHARS} chars: {', '.join(oversized)}")

    probe_cfg = entry.get("probe") or {}
    cached = _cache_get(upstream_id, probe_cfg)
    if cached is not None and not cached[0]:
        return _unavailable(cached[1] or "unavailable")

    timeout_s = min(20.0, float(entry.get("timeout_s", 10)))
    result, err = await _call_mcp(entry.get("app_slug", upstream_id), tool, args, timeout_s)
    if err:
        _cache_put(upstream_id, False, err)
        return _unavailable(err)

    _cache_put(upstream_id, True, None)
    text = _extract_text(result)
    out: dict[str, Any] = {"status": "ok", "result": text, "elapsed_ms": int((time.monotonic() - start) * 1000)}
    if len(text) > MAX_RESULT_CHARS:
        out["result"] = text[:MAX_RESULT_CHARS]
        out["truncated"] = True
    return out


async def status_block() -> list[dict[str, Any]]:
    """``GET /status``'s ``forward_upstreams`` list — a dedicated
    ``probe.tool`` call per registry entry (never ``tools/list`` presence,
    design §6), sharing the same cache ``forward()`` populates."""
    out: list[dict[str, Any]] = []
    for entry in _registry():
        upstream_id = entry["id"]
        probe_cfg = entry.get("probe") or {}
        cached = _cache_get(upstream_id, probe_cfg)
        if cached is not None:
            ok, reason = cached
            checked_at = _availability_cache[upstream_id][2]
        else:
            tool = probe_cfg.get("tool") or "status"
            timeout_s = float(probe_cfg.get("timeout_s", 5))
            _, err = await _call_mcp(entry.get("app_slug", upstream_id), tool, {}, timeout_s)
            ok = err is None
            reason = err
            checked_at = time.time()
            _cache_put(upstream_id, ok, reason)
        out.append({"id": upstream_id, "available": ok, "checked_at": checked_at, "reason": reason})
    return out


async def compose_knowledge_search(args: dict) -> tuple[str, bool]:
    """``_DISPATCH["knowledge_search"]`` — additive ``include_code`` knob
    (design §5). Absent/false, delegates straight to ``client.knowledge_search``
    with no change to request/response shape whatsoever (back-compat is a
    test, not a hope). True, fans out the unchanged KG call and a forwarded
    ``codegraph_explore`` call concurrently (``asyncio.gather``, design §7 —
    the code leg must never extend the existing KG timeout envelope), and
    composes ``{knowledge, code}`` — the code answer never lands inside
    ``chunks``/``citations`` (P6 no-fusion semantics, design §2/§5).

    ``code_only`` (docs/design/search-knowledge-codegraph-p6-worker.md §2a),
    honored only when ``include_code`` is true, skips the KG leg entirely —
    a caller that only wants the code perspective would otherwise drag a
    full fused search into single-worker production aw-knowledgeable for
    nothing. ``code_only`` without ``include_code`` is ignored, same
    byte-identical-legacy rule as every other knob here."""
    import asyncio

    from .mcp import client

    if not args.get("include_code"):
        return await client.knowledge_search(args)

    query = (args.get("query") or "").strip()
    if not query:
        return "query is required", True
    code_query = (args.get("code_query") or query).strip()

    if args.get("code_only"):
        code_result = await forward("codegraph", "codegraph_explore", {"query": code_query})
        response = {
            "knowledge": None,
            "code": {
                **code_result,
                "source": "codegraph",
                "tool": "codegraph_explore",
                "mentions": [],
            },
        }
        return json.dumps(response, indent=2, ensure_ascii=False), False

    (kg_data, kg_err), code_result = await asyncio.gather(
        client.knowledge_search_data(args),
        forward("codegraph", "codegraph_explore", {"query": code_query}),
    )
    if kg_err:
        return f"knowledge_search failed: {kg_err}", True

    response = {
        "knowledge": kg_data,
        "code": {
            **code_result,
            "source": "codegraph",
            "tool": "codegraph_explore",
            "mentions": [],
        },
    }
    return json.dumps(response, indent=2, ensure_ascii=False), False
