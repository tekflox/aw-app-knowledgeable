"""Generic forwardable-upstream registry (docs/design/knowledgeable-connector-
tool-forwarder.md) — codegraph is the first entry, not a special case.

Each registry entry ({id, transport, app_slug, tools (tool allow-list + per-
tool arg allow-list), probe, timeout_s}) names an installed workspace app's
MCP surface. ``transport`` selects *how* that surface is reached, and is the
one field every call below branches on first:

``app-mcp`` (the default, and the only kind before 2026-10-27)
    The app's own ``/api/apps/<app_slug>/mcp`` route, reached by loopback
    HTTP under this process's own ``AW_WORKSPACE_API_KEY`` — the exact
    URL+header every self-registering app already writes into its own
    ``mcp.json`` for the gateway (``aw-app-codegraph/codegraph_app/mcp/
    self_register.py``). Only a Tier-1 app with ``routes:register`` can have
    such a route at all.

``app-mcp-json`` (docs/design/knowledgeable-forwarder-declared-mcp-transport.md)
    The URL, headers and enabled-state are resolved, fresh per call, from an
    installed app's **rendered** ``apps/<app_slug>/mcp.json`` — the file
    ``src/apps/mcp_template.py`` generates and aw-mcp-gateway's app-scan
    already trusts. The entry names only ``{app_slug, server}`` and can
    never carry a URL or a header, so this cannot be configured into an
    arbitrary-outbound-request primitive. **The workspace key is never
    attached on this transport** (it travels to ``127.0.0.1`` only); the
    connector sends exactly the headers the app declared. This is the
    transport a Tier-2 container app needs, because it runs no
    workspace-side code and therefore has no loopback route to dial.

Never routed through aw-mcp-gateway on either transport (zombie-caching + no
gateway credential held here, design §4).

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

import asyncio
import json
import logging
import os
import re
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

#: ``adapter.max_rounds`` / ``adapter.budget_s`` defaults for an NL-false
#: entry — ``docs/design/knowledgeable-nl-tool-adapter.md`` §2. The ``adapter``
#: block is optional on an entry precisely because these exist. Read through
#: :func:`adapter_bounds` below, never directly.
DEFAULT_ADAPTER_MAX_ROUNDS = 6
DEFAULT_ADAPTER_BUDGET_S = 90.0

#: The two ``transport`` kinds this module understands — see the module
#: docstring. ``app-mcp`` is also the implicit default for an entry saved
#: before the field was branched on anywhere.
TRANSPORT_APP_MCP = "app-mcp"
TRANSPORT_APP_MCP_JSON = "app-mcp-json"

#: Path-traversal guard applied to ``app_slug`` before it is ever joined
#: into the apps root (declared-transport design §3.1).
APP_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")

#: Protocol version announced on ``initialize`` — the same value the
#: gateway's own ``HttpUpstream`` sends (``aw-mcp-gateway/back/gateway/
#: upstream.py``'s ``DEFAULT_PROTOCOL``), which is the in-estate proof that
#: it is accepted by the servers this workspace actually dials.
MCP_PROTOCOL_VERSION = "2024-11-05"


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


def accepts_natural_language(entry: dict | None) -> bool:
    """Whether this registry entry's source can be asked a natural-language
    question directly — ``docs/design/knowledgeable-nl-tool-adapter.md`` §2.

    **Absent or truthy is today's path; only an explicit false activates the
    NL→tool-calling adapter.** That one rule is the whole migration story
    (§6): every existing entry, on this workspace and on every other one,
    keeps the exact current behavior with no edit. A missing entry (an unknown
    ``source_id``) is NL-true too, so it still falls through to
    :func:`forward`, which rejects it by name — unchanged (§3.1).

    Lives here rather than in ``nl_adapter`` because it reads a *registry*
    entry, and because ``/source-query``'s legacy branch must be able to
    decide without importing the adapter at all (§9.6).
    """
    if entry is None:
        return True
    return bool(entry.get("accepts_natural_language", True))


def adapter_bounds(entry: dict) -> tuple[int, float]:
    """``(max_rounds, budget_s)`` for an NL-false entry, defaulted and
    floored. Read by ``nl_adapter`` to bound its loop and by ``sources_sync``
    to tell aw-knowledgeable how long this source's fan-out leg may take
    (§3.6) — one reader of the declaration, not two that can disagree."""
    adapter = entry.get("adapter") or {}
    try:
        max_rounds = int(adapter.get("max_rounds") or DEFAULT_ADAPTER_MAX_ROUNDS)
    except (TypeError, ValueError):
        max_rounds = DEFAULT_ADAPTER_MAX_ROUNDS
    try:
        budget_s = float(adapter.get("budget_s") or DEFAULT_ADAPTER_BUDGET_S)
    except (TypeError, ValueError):
        budget_s = DEFAULT_ADAPTER_BUDGET_S
    return max(1, max_rounds), max(1.0, budget_s)


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


def _apps_root() -> str:
    """Mirror of ``src/apps/fetch.py``'s ``apps_root()`` — the same env var,
    the same default, so the forwarder reads the very file core rendered."""
    return workspace_env("AW_APPS_ROOT") or os.path.join(
        workspace_env("AW_WORKSPACE_CONTAINER_DIR") or "/opt/aw-workspace", "apps")


def _declared_server(app_slug: str, server: str) -> tuple[dict | None, str | None]:
    """Resolve one ``mcpServers`` entry out of an installed app's rendered
    ``apps/<app_slug>/mcp.json``, read fresh on every call (same
    read-fresh rule as :func:`_registry`).

    Every state has a declared reason rather than an empty success — the
    unconfigured case especially: ``mcp_template.py`` sets ``enabled: false``
    on any server with an unresolved ``${config.x}`` placeholder, so "the
    operator hasn't filled this in yet" arrives here as a fact, not as a
    mystery 401 from the far side.

    Stricter than the gateway's own scan on one point, deliberately: the
    gateway treats a *missing* ``enabled`` as enabled
    (``server.py``'s ``spec.get("enabled") is not False``), this treats any
    falsy/absent value as not configured. Every rendered file in this estate
    writes the key explicitly, so the two agree in practice; where they
    could differ, a forwarder that holds a credential should fail closed.
    """
    if not APP_SLUG_RE.match(app_slug or ""):
        return None, f"invalid app_slug '{app_slug}' for an app-mcp-json upstream"
    path = os.path.join(_apps_root(), app_slug, "mcp.json")
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except OSError:
        return None, f"app '{app_slug}' is not installed (no rendered mcp.json)"
    except ValueError:
        return None, f"app '{app_slug}' has an unreadable mcp.json"

    servers = doc.get("mcpServers") if isinstance(doc, dict) else None
    spec = servers.get(server) if isinstance(servers, dict) else None
    if not isinstance(spec, dict):
        return None, f"app '{app_slug}' declares no MCP server '{server}'"
    if not spec.get("enabled"):
        return None, (f"upstream '{server}' of app '{app_slug}' is declared but "
                      "disabled — not configured yet")
    if spec.get("type") != "http":
        return None, (f"upstream '{server}' of app '{app_slug}' declares type "
                      f"'{spec.get('type')}', not 'http'")
    url = spec.get("url")
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return None, (f"upstream '{server}' of app '{app_slug}' declares no "
                      "usable http(s) url")
    raw_headers = spec.get("headers")
    headers = ({str(k): str(v) for k, v in raw_headers.items()}
               if isinstance(raw_headers, dict) else {})
    return {"url": url, "headers": headers}, None


def _parse_declared_body(resp: Any) -> tuple[dict | None, str | None]:
    """A streamable-HTTP MCP response is either a JSON body or a one-event
    SSE body — the same two shapes the gateway's ``HttpUpstream._parse_body``
    handles (``upstream.py:455-466``)."""
    text = resp.text or ""
    content_type = resp.headers.get("content-type") or ""
    if "text/event-stream" in content_type or text.lstrip().startswith("event:"):
        for line in text.splitlines():
            if line.startswith("data:"):
                try:
                    return json.loads(line[len("data:"):].strip()), None
                except ValueError:
                    return None, "returned an SSE event that is not JSON"
        return None, "returned an SSE body with no data: event"
    try:
        return resp.json(), None
    except ValueError:
        return None, "returned a non-JSON response"


async def _call_declared(
    app_slug: str, server: str, tool: str, args: dict, timeout_s: float,
) -> tuple[dict | None, str | None]:
    """One ``tools/call`` against an app's own declared streamable-HTTP MCP
    endpoint, including the handshake that endpoint requires.

    The far side is a real MCP-SDK server (``signoz/signoz-mcp-server``),
    which rejects a bare ``tools/call`` POST: the sequence is ``initialize``
    → ``notifications/initialized`` → ``tools/call``, with
    ``Accept: application/json, text/event-stream`` throughout. Reference
    implementation: ``aw-mcp-gateway/back/gateway/upstream.py:418-510``.

    One :func:`asyncio.timeout` wraps the whole sequence, so the entry's
    budget is total rather than per-request — three requests each allowed
    the full ``timeout_s`` would silently treble it. Any ``Mcp-Session-Id``
    the server issues is honoured for the remaining two requests of *this
    call only* and never cached across calls (the gateway's recorded
    session-collision incident, ``upstream.py:113-115``, is the argument
    against getting clever here).

    Never raises: every failure mode becomes ``(None, reason)``, same rule
    as :func:`_call_mcp`. The headers are **exactly** the app's declared
    ones plus content negotiation — ``AW_WORKSPACE_API_KEY`` is never
    attached on this transport.
    """
    spec, reason = _declared_server(app_slug, server)
    if spec is None:
        return None, reason

    label = f"{app_slug}/{server}"
    url = spec["url"]
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        **spec["headers"],
    }
    try:
        async with asyncio.timeout(timeout_s):
            async with httpx.AsyncClient(timeout=timeout_s) as http_client:
                resp = await http_client.post(url, json={
                    "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "aw-app-knowledgeable", "version": "1"},
                    },
                }, headers=headers)
                if resp.status_code >= 400:
                    return None, f"{label} initialize failed: HTTP {resp.status_code}"
                init, parse_err = _parse_declared_body(resp)
                if parse_err:
                    return None, f"{label} {parse_err}"
                init_err = (init or {}).get("error") if isinstance(init, dict) else None
                if init_err:
                    msg = (init_err or {}).get("message") or "unknown error"
                    return None, f"{label} initialize failed: {msg}"
                session_id = resp.headers.get("mcp-session-id")
                if session_id:
                    headers = {**headers, "Mcp-Session-Id": session_id}

                # A notification has no response body to check, and a server
                # that answers it oddly is not evidence about tools/call —
                # the call itself is the verdict (design §6's "the forwarded
                # call is the probe" rule, one layer down).
                await http_client.post(url, json={
                    "jsonrpc": "2.0", "method": "notifications/initialized",
                }, headers=headers)

                resp = await http_client.post(url, json={
                    "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": tool, "arguments": args},
                }, headers=headers)
                if resp.status_code >= 400:
                    return None, f"{label} not responding: HTTP {resp.status_code}"
                data, parse_err = _parse_declared_body(resp)
                if parse_err:
                    return None, f"{label} {parse_err}"
    except (TimeoutError, httpx.TimeoutException):
        return None, f"timed out after {timeout_s:g}s"
    except httpx.HTTPError as exc:
        return None, f"{label} not responding: {exc}"
    except Exception as exc:  # noqa: BLE001 — last resort, must not 500 the route
        return None, f"{label} not responding: {exc}"

    result = data.get("result") if isinstance(data, dict) else None
    if result is None:
        err = (data.get("error") or {}).get("message") if isinstance(data, dict) else None
        return None, err or "unknown error"
    if result.get("isError"):
        return None, _extract_text(result) or f"{tool} failed"
    return result, None


async def _call_entry(
    entry: dict, tool: str, args: dict, timeout_s: float,
) -> tuple[dict | None, str | None]:
    """Transport dispatch — the one place ``transport`` is branched on.

    An unknown value is reported **by name** and never falls back to
    ``app-mcp``: that fallback would dial a loopback route a Tier-2 app
    cannot have and then misattribute the 404 as "app not installed",
    which is the exact misdiagnosis this transport exists to end.
    """
    transport = entry.get("transport") or TRANSPORT_APP_MCP
    if transport == TRANSPORT_APP_MCP:
        return await _call_mcp(
            entry.get("app_slug") or entry.get("id") or "", tool, args, timeout_s)
    if transport == TRANSPORT_APP_MCP_JSON:
        # No fall back to ``id`` for the slug here, unlike ``app-mcp``: this
        # slug selects which installed app's declaration (and therefore which
        # URL and credential) is read, so guessing it from the entry's own
        # name would silently resolve a *different* app. Absent is invalid,
        # and _call_declared says so by name.
        return await _call_declared(
            entry.get("app_slug") or "", entry.get("server") or "", tool, args, timeout_s)
    return None, (f"unknown transport '{transport}' for upstream "
                  f"'{entry.get('id')}' — not forwarded")


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
    result, err = await _call_entry(entry, tool, args, timeout_s)
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
    design §6), sharing the same cache ``forward()`` populates.

    Entries are probed **concurrently**: probe timeouts are deliberately
    generous (120s, so a slow-not-dead upstream doesn't poison the negative
    cache — ``bug:knowledgeable-codegraph-probe-timeout-too-low``), and
    serially that budget is additive — two cold entries would stack to four
    minutes on one ``GET /status``. No timeout value changes here, only the
    fact that they no longer queue. ``asyncio.gather`` preserves registry
    order, so the rendered list is unchanged.
    """
    async def _row(entry: dict) -> dict[str, Any]:
        upstream_id = entry["id"]
        probe_cfg = entry.get("probe") or {}
        cached = _cache_get(upstream_id, probe_cfg)
        if cached is not None:
            ok, reason = cached
            checked_at = _availability_cache[upstream_id][2]
        else:
            tool = probe_cfg.get("tool") or "status"
            timeout_s = float(probe_cfg.get("timeout_s", 5))
            _, err = await _call_entry(entry, tool, {}, timeout_s)
            ok = err is None
            reason = err
            checked_at = time.time()
            _cache_put(upstream_id, ok, reason)
        return {"id": upstream_id, "available": ok, "checked_at": checked_at, "reason": reason}

    return list(await asyncio.gather(*(_row(entry) for entry in _registry())))


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
