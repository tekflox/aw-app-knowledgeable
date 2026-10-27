"""forwarding.py — the generic forwardable-upstream registry
(docs/design/knowledgeable-connector-tool-forwarder.md). Deliberately no
real network: every test monkeypatches the relevant module's own
``httpx.AsyncClient`` with a fake that records the outbound request and
returns/raises a canned result — same pattern as ``test_client.py``'s
``_FakeAsyncClient``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import forwarding  # noqa: E402
from knowledgeable_app.mcp import client, http_handler  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


CODEGRAPH_ENTRY = {
    "id": "codegraph",
    "transport": "app-mcp",
    "app_slug": "codegraph",
    "tools": {
        "codegraph_explore": {"args": ["query", "maxFiles"]},
        "codegraph_status": {"args": []},
    },
    "probe": {"tool": "codegraph_status", "ttl_ok_s": 60, "ttl_fail_s": 15, "timeout_s": 120},
    "timeout_s": 10,
}


# ---------------------------------------------------------------------------
# Fakes for the forwarder's own loopback call (forwarding.httpx)
# ---------------------------------------------------------------------------


class _FakeForwardResponse:
    def __init__(self, status_code=200, json_body=None):
        self.status_code = status_code
        self._json = json_body if json_body is not None else {}

    def json(self):
        return self._json


class _FakeForwardAsyncClient:
    """``_QUEUE`` items are either a ``_FakeForwardResponse`` to return or an
    ``Exception`` instance to raise — same double-duty queue test_client.py
    doesn't need because it never has to simulate a transport failure this
    way for its own single-caller tests."""

    _CALLS: list = []
    _QUEUE: list = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, headers=None):
        _FakeForwardAsyncClient._CALLS.append({"url": url, "json": json, "headers": headers})
        item = _FakeForwardAsyncClient._QUEUE.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _ok_tools_call(text="codegraph says hi"):
    return _FakeForwardResponse(200, {
        "jsonrpc": "2.0", "id": 1,
        "result": {"content": [{"type": "text", "text": text}], "isError": False},
    })


def _error_tools_call(text="boom"):
    return _FakeForwardResponse(200, {
        "jsonrpc": "2.0", "id": 1,
        "result": {"content": [{"type": "text", "text": text}], "isError": True},
    })


@pytest.fixture(autouse=True)
def _fresh_forwarding(monkeypatch):
    _FakeForwardAsyncClient._CALLS = []
    _FakeForwardAsyncClient._QUEUE = []
    forwarding._availability_cache.clear()
    monkeypatch.setattr(forwarding, "httpx", type(
        "M", (), {
            "AsyncClient": _FakeForwardAsyncClient,
            "HTTPError": httpx.HTTPError,
            "TimeoutException": httpx.TimeoutException,
        },
    ))
    monkeypatch.setenv("AW_PORT", "9030")
    monkeypatch.setenv("AW_WORKSPACE_API_KEY", "test-key")
    forwarding.configure(lambda: {"forward_upstreams": [CODEGRAPH_ENTRY]})
    yield
    forwarding.configure(lambda: {})
    forwarding._availability_cache.clear()


# ---------------------------------------------------------------------------
# forward() — allow-lists, the forwarded call doubling as the probe, timeouts
# ---------------------------------------------------------------------------


def test_forward_happy_path():
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("3847 files indexed"))
    result = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "foo"}))
    assert result["status"] == "ok"
    assert "3847 files indexed" in result["result"]
    assert isinstance(result["elapsed_ms"], int)
    call = _FakeForwardAsyncClient._CALLS[0]
    assert call["url"] == "http://127.0.0.1:9030/api/apps/codegraph/mcp"
    assert call["headers"]["X-Api-Key"] == "test-key"
    assert call["json"]["method"] == "tools/call"
    assert call["json"]["params"] == {"name": "codegraph_explore", "arguments": {"query": "foo"}}


def test_forward_unknown_upstream():
    result = _run(forwarding.forward("not-a-real-upstream", "whatever", {}))
    assert result["status"] == "unavailable"
    assert "no forward_upstreams entry" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


def test_forward_disallowed_tool():
    result = _run(forwarding.forward("codegraph", "codegraph_impact", {}))
    assert result["status"] == "unavailable"
    assert "codegraph_impact" in result["reason"]
    assert "not allow-listed" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


def test_forward_disallowed_arg_rejects_projectpath():
    """`projectPath` is deliberately absent from codegraph's own arg
    allow-list (design §3) — rejected by name, never silently dropped, and
    never reaching the network (it would otherwise be a filesystem-read
    primitive over repos/**)."""
    result = _run(forwarding.forward(
        "codegraph", "codegraph_explore", {"query": "x", "projectPath": "/etc"}
    ))
    assert result["status"] == "unavailable"
    assert "projectPath" in result["reason"]
    assert "not allow-listed" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


def test_forward_uninstalled_app_error_shape():
    _FakeForwardAsyncClient._QUEUE.append(httpx.ConnectError("Connection refused"))
    result = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "x"}))
    assert result["status"] == "unavailable"
    assert "codegraph app not installed or not responding" in result["reason"]


def test_forward_timeout():
    _FakeForwardAsyncClient._QUEUE.append(httpx.TimeoutException("timed out"))
    result = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "x"}))
    assert result["status"] == "unavailable"
    assert "timed out after 10s" in result["reason"]


def test_forward_clamps_timeout_to_20s(monkeypatch):
    entry = {**CODEGRAPH_ENTRY, "timeout_s": 999}
    forwarding.configure(lambda: {"forward_upstreams": [entry]})
    captured = {}
    real_async_client = _FakeForwardAsyncClient

    class _Capturing(real_async_client):
        def __init__(self, *a, timeout=None, **k):
            captured["timeout"] = timeout
            super().__init__(*a, **k)

    monkeypatch.setattr(forwarding, "httpx", type(
        "M", (), {"AsyncClient": _Capturing, "HTTPError": httpx.HTTPError, "TimeoutException": httpx.TimeoutException},
    ))
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call())
    _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "x"}))
    assert captured["timeout"] == 20.0


def test_forward_tool_level_error_is_reported_verbatim():
    _FakeForwardAsyncClient._QUEUE.append(_error_tools_call("index not found"))
    result = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "x"}))
    assert result["status"] == "unavailable"
    assert "index not found" in result["reason"]


def test_forward_negative_cache_short_circuits():
    """A fresh negative cache entry skips the network call entirely (design
    §6's call path: "a fresh negative cache short-circuits to unavailable
    immediately") — the first failing call's own error populated it."""
    _FakeForwardAsyncClient._QUEUE.append(httpx.ConnectError("refused"))
    first = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "x"}))
    assert first["status"] == "unavailable"
    assert len(_FakeForwardAsyncClient._CALLS) == 1

    second = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "y"}))
    assert second["status"] == "unavailable"
    assert second["reason"] == first["reason"]
    assert len(_FakeForwardAsyncClient._CALLS) == 1  # no second network call


def test_forward_oversized_argument_rejected():
    result = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "x" * 2001}))
    assert result["status"] == "unavailable"
    assert "exceed" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


def test_forward_result_is_truncated_past_cap(monkeypatch):
    monkeypatch.setattr(forwarding, "MAX_RESULT_CHARS", 10)
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("x" * 100))
    result = _run(forwarding.forward("codegraph", "codegraph_explore", {"query": "x"}))
    assert result["status"] == "ok"
    assert result["truncated"] is True
    assert len(result["result"]) == 10


# ---------------------------------------------------------------------------
# transport dispatch + the app-mcp-json transport
# (docs/design/knowledgeable-forwarder-declared-mcp-transport.md)
# ---------------------------------------------------------------------------


SIGNOZ_ENTRY = {
    "id": "signoz",
    "transport": "app-mcp-json",
    "app_slug": "signoz-connector",
    "server": "signoz-query",
    "tools": {
        "signoz_list_services": {"args": ["timeRange", "limit"]},
        "signoz_search_logs": {"args": ["searchText", "service", "severity", "timeRange", "limit"]},
        "signoz_search_traces": {"args": ["service", "operation", "error", "timeRange", "limit"]},
    },
    "probe": {"tool": "signoz_list_services", "ttl_ok_s": 60, "ttl_fail_s": 15, "timeout_s": 120},
    "timeout_s": 10,
    "query_tool": {"tool": "signoz_search_logs", "arg": "searchText"},
}

DECLARED_URL = "https://signoz-mcp.example.test/mcp"


class _FakeDeclaredResponse:
    """Unlike ``_FakeForwardResponse`` this also carries ``headers``/``text``
    — the declared transport reads the ``Mcp-Session-Id`` response header and
    has to be able to parse an SSE body, neither of which the loopback
    ``app-mcp`` path ever touches."""

    def __init__(self, status_code=200, json_body=None, text=None, headers=None):
        self.status_code = status_code
        self._json = json_body
        self.text = text if text is not None else json.dumps(json_body or {})
        self.headers = headers or {"content-type": "application/json"}

    def json(self):
        if self._json is None:
            raise ValueError("not json")
        return self._json


def _declared_init(session_id=None):
    headers = {"content-type": "application/json"}
    if session_id:
        headers["mcp-session-id"] = session_id
    return _FakeDeclaredResponse(200, {
        "jsonrpc": "2.0", "id": 1,
        "result": {"protocolVersion": "2024-11-05", "serverInfo": {"name": "signoz-mcp-server"}},
    }, headers=headers)


def _declared_notification_ack():
    return _FakeDeclaredResponse(202, {}, text="")


def _declared_tools_call(text="signoz says hi", is_error=False):
    return _FakeDeclaredResponse(200, {
        "jsonrpc": "2.0", "id": 2,
        "result": {"content": [{"type": "text", "text": text}], "isError": is_error},
    })


def _declared_tools_call_sse(text="signoz over sse"):
    body = {
        "jsonrpc": "2.0", "id": 2,
        "result": {"content": [{"type": "text", "text": text}], "isError": False},
    }
    return _FakeDeclaredResponse(
        200, None,
        text="event: message\ndata: " + json.dumps(body) + "\n\n",
        headers={"content-type": "text/event-stream"},
    )


def _declared_handshake(final):
    """The three responses one successful declared call consumes."""
    return [_declared_init(), _declared_notification_ack(), final]


@pytest.fixture
def declared(tmp_path, monkeypatch):
    """Point the resolver at a throwaway apps root and register the signoz
    entry alongside codegraph — returns a writer for the rendered
    ``apps/<slug>/mcp.json`` so each test declares the state it is about."""
    monkeypatch.setenv("AW_APPS_ROOT", str(tmp_path))
    forwarding.configure(lambda: {"forward_upstreams": [CODEGRAPH_ENTRY, SIGNOZ_ENTRY]})

    def _write(slug="signoz-connector", doc=None, enabled=True):
        if doc is None:
            doc = {"mcpServers": {"signoz-query": {
                "enabled": enabled, "type": "http", "url": DECLARED_URL,
                "headers": {"SIGNOZ-API-KEY": "declared-signoz-key"},
            }}}
        app_dir = tmp_path / slug
        app_dir.mkdir(parents=True, exist_ok=True)
        (app_dir / "mcp.json").write_text(json.dumps(doc), encoding="utf-8")
        return app_dir / "mcp.json"

    return _write


def test_declared_happy_path_runs_the_full_handshake(declared):
    declared()
    _FakeForwardAsyncClient._QUEUE.extend(_declared_handshake(_declared_tools_call("12 services")))

    result = _run(forwarding.forward("signoz", "signoz_list_services", {"timeRange": "1h"}))
    assert result["status"] == "ok", result
    assert "12 services" in result["result"]

    calls = _FakeForwardAsyncClient._CALLS
    assert [c["json"]["method"] for c in calls] == [
        "initialize", "notifications/initialized", "tools/call",
    ]
    assert all(c["url"] == DECLARED_URL for c in calls)
    assert calls[2]["json"]["params"] == {
        "name": "signoz_list_services", "arguments": {"timeRange": "1h"},
    }
    # Declared headers, plus the content negotiation a streamable-HTTP MCP
    # endpoint requires.
    for call in calls:
        assert call["headers"]["SIGNOZ-API-KEY"] == "declared-signoz-key"
        assert call["headers"]["Accept"] == "application/json, text/event-stream"


def test_declared_never_sends_the_workspace_api_key(declared):
    """The header invariant, asserted rather than assumed (design §5): the
    workspace key travels to 127.0.0.1 only. On this transport the connector
    sends exactly what the app declared — a hostile-but-declared endpoint
    receives only its own credential."""
    declared()
    _FakeForwardAsyncClient._QUEUE.extend(_declared_handshake(_declared_tools_call()))
    _run(forwarding.forward("signoz", "signoz_search_logs", {"searchText": "oops"}))

    assert _FakeForwardAsyncClient._CALLS, "no outbound request was made"
    for call in _FakeForwardAsyncClient._CALLS:
        headers = call["headers"] or {}
        assert "X-Api-Key" not in headers
        assert not any(k.lower() == "x-api-key" for k in headers)
        assert "test-key" not in json.dumps(headers)  # the fixture's AW_WORKSPACE_API_KEY


def test_declared_parses_an_sse_response_body(declared):
    declared()
    _FakeForwardAsyncClient._QUEUE.extend(_declared_handshake(_declared_tools_call_sse()))
    result = _run(forwarding.forward("signoz", "signoz_search_logs", {"searchText": "x"}))
    assert result["status"] == "ok", result
    assert "signoz over sse" in result["result"]


def test_declared_honours_the_session_id_within_one_call(declared):
    """A session the server issues is carried on the remaining two requests
    of this call, and never cached for the next one."""
    declared()
    _FakeForwardAsyncClient._QUEUE.extend([
        _declared_init(session_id="sess-1"), _declared_notification_ack(), _declared_tools_call(),
    ])
    _run(forwarding.forward("signoz", "signoz_list_services", {}))
    calls = _FakeForwardAsyncClient._CALLS
    assert "Mcp-Session-Id" not in calls[0]["headers"]
    assert calls[1]["headers"]["Mcp-Session-Id"] == "sess-1"
    assert calls[2]["headers"]["Mcp-Session-Id"] == "sess-1"

    # Second call: fresh handshake, no leaked session.
    forwarding._availability_cache.clear()
    _FakeForwardAsyncClient._CALLS = []
    _FakeForwardAsyncClient._QUEUE.extend(_declared_handshake(_declared_tools_call()))
    _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert "Mcp-Session-Id" not in _FakeForwardAsyncClient._CALLS[0]["headers"]


def test_declared_app_not_installed(declared):
    """No rendered mcp.json at all — the app is not installed, said in those
    words rather than as a connection error from a URL we never had."""
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert result["reason"] == "app 'signoz-connector' is not installed (no rendered mcp.json)"
    assert _FakeForwardAsyncClient._CALLS == []


def test_declared_server_key_absent_is_named(declared):
    declared(doc={"mcpServers": {"something-else": {"enabled": True, "type": "http", "url": DECLARED_URL}}})
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert result["reason"] == "app 'signoz-connector' declares no MCP server 'signoz-query'"
    assert _FakeForwardAsyncClient._CALLS == []


def test_declared_disabled_reports_not_configured_yet(declared):
    """This card's known blocker state, made legible: mcp_template.py renders
    `enabled: false` for a server whose ${config.x} placeholder is unresolved,
    so "nobody filled in query_mcp_url yet" must arrive as a declared reason,
    never as an empty success or a mystery 401."""
    declared(enabled=False)
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert result["reason"] == (
        "upstream 'signoz-query' of app 'signoz-connector' is declared but "
        "disabled — not configured yet"
    )
    assert _FakeForwardAsyncClient._CALLS == []


def test_declared_non_http_type_is_named(declared):
    declared(doc={"mcpServers": {"signoz-query": {
        "enabled": True, "type": "stdio", "command": "signoz-mcp",
    }}})
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert "declares type 'stdio', not 'http'" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


def test_declared_non_http_url_is_named(declared):
    declared(doc={"mcpServers": {"signoz-query": {
        "enabled": True, "type": "http", "url": "file:///etc/passwd",
    }}})
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert "no usable http(s) url" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


@pytest.mark.parametrize("bad_slug", ["../../etc", "Signoz", "", "sig noz", "-signoz", "a" * 65])
def test_declared_rejects_an_invalid_app_slug_by_name(declared, bad_slug):
    """The path-traversal guard (design §3.1) — rejected before the slug is
    ever joined into the apps root."""
    forwarding.configure(lambda: {"forward_upstreams": [{**SIGNOZ_ENTRY, "app_slug": bad_slug}]})
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert result["reason"] == f"invalid app_slug '{bad_slug}' for an app-mcp-json upstream"
    assert _FakeForwardAsyncClient._CALLS == []


def test_unknown_transport_is_named_and_never_falls_back_to_loopback():
    """A silent fallback to `app-mcp` would dial a loopback route a Tier-2 app
    cannot have and then report "app not installed" — the exact misdiagnosis
    this transport exists to end."""
    forwarding.configure(lambda: {"forward_upstreams": [{**SIGNOZ_ENTRY, "transport": "gateway"}]})
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert result["reason"] == "unknown transport 'gateway' for upstream 'signoz' — not forwarded"
    assert _FakeForwardAsyncClient._CALLS == []


def test_declared_unlisted_tool_rejected_by_name(declared):
    """The allow-list is the only per-tool gate anywhere on this path — the
    upstream server exposes ~43 tools, 12 of them mutating."""
    declared()
    result = _run(forwarding.forward("signoz", "signoz_delete_alert", {}))
    assert result["status"] == "unavailable"
    assert "signoz_delete_alert" in result["reason"]
    assert "not allow-listed" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


@pytest.mark.parametrize("arg", ["filter", "selectFields", "start", "end", "offset", "minDuration"])
def test_declared_unlisted_argument_rejected_by_name(declared, arg):
    """`filter` is the `projectPath` analog — a raw query-expression
    passthrough. These are deliberately absent from the allow-list, so they
    are rejected by name and never reach the network."""
    declared()
    result = _run(forwarding.forward("signoz", "signoz_search_logs", {"searchText": "x", arg: "y"}))
    assert result["status"] == "unavailable"
    assert arg in result["reason"]
    assert "not allow-listed" in result["reason"]
    assert _FakeForwardAsyncClient._CALLS == []


def test_declared_handshake_timeout_is_one_deadline(declared):
    """One `asyncio.timeout` across the whole sequence — the entry's budget
    is total, not per-request."""
    declared()
    _FakeForwardAsyncClient._QUEUE.append(httpx.TimeoutException("timed out"))
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert result["reason"] == "timed out after 10s"


def test_declared_initialize_http_error_is_reported(declared):
    declared()
    _FakeForwardAsyncClient._QUEUE.append(_FakeDeclaredResponse(401, {}))
    result = _run(forwarding.forward("signoz", "signoz_list_services", {}))
    assert result["status"] == "unavailable"
    assert result["reason"] == "signoz-connector/signoz-query initialize failed: HTTP 401"


def test_declared_tool_level_error_is_reported_verbatim(declared):
    declared()
    _FakeForwardAsyncClient._QUEUE.extend(
        _declared_handshake(_declared_tools_call("key service.name not found", is_error=True)))
    result = _run(forwarding.forward("signoz", "signoz_search_logs", {"service": "ghost"}))
    assert result["status"] == "unavailable"
    assert "key service.name not found" in result["reason"]


def test_declared_query_tool_is_search_logs_on_searchtext():
    """`POST /source-query`'s generic natural-language leg for this upstream."""
    forwarding.configure(lambda: {"forward_upstreams": [CODEGRAPH_ENTRY, SIGNOZ_ENTRY]})
    assert forwarding.query_tool_for("signoz") == {
        "tool": "signoz_search_logs", "arg": "searchText",
    }


# ---------------------------------------------------------------------------
# status_block() — GET /status's forward_upstreams block
# ---------------------------------------------------------------------------


def test_status_block_reports_available_entry():
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("ok"))
    rows = _run(forwarding.status_block())
    assert rows == [{"id": "codegraph", "available": True, "checked_at": rows[0]["checked_at"], "reason": None}]
    call = _FakeForwardAsyncClient._CALLS[0]
    assert call["json"]["params"]["name"] == "codegraph_status"


def test_status_block_reports_unavailable_with_reason():
    _FakeForwardAsyncClient._QUEUE.append(httpx.ConnectError("refused"))
    rows = _run(forwarding.status_block())
    assert rows[0]["available"] is False
    assert "codegraph app not installed" in rows[0]["reason"]


def test_status_block_reports_the_declared_disabled_reason(declared):
    """Acceptance for this card while the sibling config card is open: the
    signoz upstream is LISTED on /status with the declared reason, which is
    what "declared unavailable-reason, never an empty success" means in
    practice. Only codegraph touches the network here — signoz fails at
    resolution, before any request."""
    declared(enabled=False)
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("ok"))

    rows = _run(forwarding.status_block())
    assert [r["id"] for r in rows] == ["codegraph", "signoz"]
    assert rows[0]["available"] is True
    assert rows[1]["available"] is False
    assert rows[1]["reason"] == (
        "upstream 'signoz-query' of app 'signoz-connector' is declared but "
        "disabled — not configured yet"
    )


def test_status_block_probes_entries_concurrently(monkeypatch):
    """Probe timeouts are deliberately generous (120s, so a slow-not-dead
    probe doesn't poison the negative cache); serially that budget is
    additive and two cold entries stack to four minutes on one GET /status.
    Both probes must be in flight at the same moment."""
    in_flight = {"now": 0, "max": 0}

    class _SlowClient(_FakeForwardAsyncClient):
        async def post(self, url, json=None, headers=None):
            in_flight["now"] += 1
            in_flight["max"] = max(in_flight["max"], in_flight["now"])
            await asyncio.sleep(0.05)
            in_flight["now"] -= 1
            return _ok_tools_call("ok")

    monkeypatch.setattr(forwarding, "httpx", type("M", (), {
        "AsyncClient": _SlowClient,
        "HTTPError": httpx.HTTPError,
        "TimeoutException": httpx.TimeoutException,
    }))
    forwarding.configure(lambda: {"forward_upstreams": [
        CODEGRAPH_ENTRY, {**CODEGRAPH_ENTRY, "id": "codegraph-2"},
    ]})

    rows = _run(forwarding.status_block())
    assert [r["available"] for r in rows] == [True, True]
    assert in_flight["max"] == 2, "probes ran serially, not concurrently"


def test_status_block_empty_registry_when_unconfigured():
    forwarding.configure(lambda: {})
    assert _run(forwarding.status_block()) == []


def test_registry_logs_when_the_config_resolver_raises(caplog):
    """A broken resolver and an unconfigured app both yield an empty
    registry, so the only thing that tells them apart is the log line.
    Pin it: forwarding may degrade, but never silently."""

    def _boom():
        raise RuntimeError("config store unreachable")

    forwarding.configure(_boom)
    with caplog.at_level(logging.ERROR, logger="aw_apps.knowledgeable"):
        assert forwarding._registry() == []
    assert any(
        "forwarding disabled" in r.message and r.exc_info for r in caplog.records
    ), caplog.text


# ---------------------------------------------------------------------------
# compose_knowledge_search — the knowledge_search dispatch row
# ---------------------------------------------------------------------------


class _FakeKgResponse:
    def __init__(self, status_code=200, json_body=None):
        self.status_code = status_code
        self._json = json_body if json_body is not None else {}

    def json(self):
        return self._json


class _FakeKgAsyncClient:
    """Mirrors test_client.py's own fake — compose_knowledge_search's KG leg
    goes through `client._post_json`, independent of `forwarding.httpx`."""

    _CALLS: list = []
    _QUEUE: list = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, headers=None, params=None, files=None, data=None):
        _FakeKgAsyncClient._CALLS.append({"url": url, "json": json, "params": params, "headers": headers})
        return _FakeKgAsyncClient._QUEUE.pop(0)


@pytest.fixture(autouse=True)
def _fresh_kg_client(monkeypatch):
    _FakeKgAsyncClient._CALLS = []
    _FakeKgAsyncClient._QUEUE = []
    monkeypatch.setattr(client, "httpx", type(
        "M", (), {"AsyncClient": _FakeKgAsyncClient, "HTTPError": httpx.HTTPError, "Response": httpx.Response},
    ))
    client.configure(lambda: "http://aw-knowledgeable:8090", lambda: "s3cr3t")
    yield
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: "")


def test_compose_include_code_absent_is_byte_identical_to_legacy():
    """`include_code` omitted → delegates straight to `client.knowledge_search`
    with no change to the response whatsoever (design §5 — back-compat is a
    test, not a hope)."""
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": [], "answer": None}))
    direct_text, direct_err = _run(client.knowledge_search({"query": "sushi"}))

    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": [], "answer": None}))
    composed_text, composed_err = _run(forwarding.compose_knowledge_search({"query": "sushi"}))

    assert direct_err is False
    assert composed_err is False
    assert composed_text == direct_text
    assert _FakeForwardAsyncClient._CALLS == []  # never touched the forwarder


def test_compose_include_code_false_is_also_byte_identical():
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": []}))
    direct_text, direct_err = _run(client.knowledge_search({"query": "sushi"}))

    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": []}))
    composed_text, composed_err = _run(forwarding.compose_knowledge_search({"query": "sushi", "include_code": False}))

    assert composed_text == direct_text
    assert composed_err == direct_err


def test_compose_include_code_true_returns_knowledge_and_code_sections():
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": [{"id": "doc-1"}], "answer": "answer text"}))
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("codegraph answer"))

    text, is_error = _run(forwarding.compose_knowledge_search(
        {"query": "how does auth work", "include_code": True}
    ))
    assert is_error is False
    body = json.loads(text)
    assert body["knowledge"]["chunks"] == [{"id": "doc-1"}]
    assert body["knowledge"]["answer"] == "answer text"
    assert body["code"]["status"] == "ok"
    assert "codegraph answer" in body["code"]["result"]
    assert body["code"]["source"] == "codegraph"
    assert body["code"]["tool"] == "codegraph_explore"
    assert body["code"]["mentions"] == []
    # Structurally separate — never fused into chunks/citations (P6 no-fusion).
    assert "code" not in body["knowledge"]
    assert "chunks" not in body["code"]


def test_compose_include_code_true_codegraph_absent_knowledge_still_returned():
    """The absence case (design §13 item 1) — the KG answer must survive a
    codegraph failure untouched, with the code section explicitly marked
    unavailable, never a 500 and never a silent empty."""
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": [{"id": "doc-1"}], "answer": "answer text"}))
    _FakeForwardAsyncClient._QUEUE.append(httpx.ConnectError("refused"))

    text, is_error = _run(forwarding.compose_knowledge_search(
        {"query": "how does auth work", "include_code": True}
    ))
    assert is_error is False
    body = json.loads(text)
    assert body["knowledge"]["answer"] == "answer text"
    assert body["code"]["status"] == "unavailable"
    assert "codegraph app not installed or not responding" in body["code"]["reason"]


def test_compose_code_query_defaults_to_query():
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": []}))
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("ok"))
    _run(forwarding.compose_knowledge_search({"query": "sushi recipe", "include_code": True}))
    call = _FakeForwardAsyncClient._CALLS[0]
    assert call["json"]["params"]["arguments"]["query"] == "sushi recipe"


def test_compose_code_query_overrides_query_for_the_code_leg_only():
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": []}))
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("ok"))
    _run(forwarding.compose_knowledge_search({
        "query": "how does auth work", "include_code": True, "code_query": "AuthMiddleware",
    }))
    forward_call = _FakeForwardAsyncClient._CALLS[0]
    assert forward_call["json"]["params"]["arguments"]["query"] == "AuthMiddleware"
    kg_call = _FakeKgAsyncClient._CALLS[0]
    assert kg_call["json"]["query"] == "how does auth work"


def test_compose_include_code_true_still_requires_query():
    text, is_error = _run(forwarding.compose_knowledge_search({"include_code": True}))
    assert is_error is True
    assert "query is required" in text
    assert _FakeKgAsyncClient._CALLS == []
    assert _FakeForwardAsyncClient._CALLS == []


def test_compose_code_only_happy_path_never_touches_the_kg_leg():
    """`code_only=True` must skip the KG leg entirely — a KG-client call
    would pop from an empty queue and raise `IndexError`, so an empty
    `_FakeKgAsyncClient._QUEUE` is itself the assertion that it was never
    touched."""
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("codegraph answer"))

    text, is_error = _run(forwarding.compose_knowledge_search(
        {"query": "where is RRF fusion implemented", "include_code": True, "code_only": True}
    ))
    assert is_error is False
    body = json.loads(text)
    assert body["knowledge"] is None
    assert body["code"]["status"] == "ok"
    assert "codegraph answer" in body["code"]["result"]
    assert body["code"]["source"] == "codegraph"
    assert _FakeKgAsyncClient._CALLS == []


def test_compose_code_only_codegraph_unavailable():
    _FakeForwardAsyncClient._QUEUE.append(httpx.ConnectError("refused"))

    text, is_error = _run(forwarding.compose_knowledge_search(
        {"query": "where is RRF fusion implemented", "include_code": True, "code_only": True}
    ))
    assert is_error is False
    body = json.loads(text)
    assert body["knowledge"] is None
    assert body["code"]["status"] == "unavailable"
    assert "codegraph app not installed or not responding" in body["code"]["reason"]
    assert _FakeKgAsyncClient._CALLS == []


def test_compose_code_only_without_include_code_is_ignored():
    """`code_only` with no `include_code` must stay byte-identical legacy
    (design §2a/§6.5) — the forwarder is never touched, the KG leg runs
    exactly as it would with no knobs at all."""
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": [], "answer": None}))
    direct_text, direct_err = _run(client.knowledge_search({"query": "sushi"}))

    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(200, {"chunks": [], "answer": None}))
    composed_text, composed_err = _run(forwarding.compose_knowledge_search(
        {"query": "sushi", "code_only": True}
    ))

    assert composed_err is direct_err
    assert composed_text == direct_text
    assert _FakeForwardAsyncClient._CALLS == []


def test_compose_kg_error_short_circuits_before_touching_code():
    """A KG-side error must still surface as an error — the code leg's
    result (if any) is irrelevant once the knowledge answer itself failed."""
    _FakeKgAsyncClient._QUEUE.append(_FakeKgResponse(400, {"detail": "unsupported filter key(s)"}))
    _FakeForwardAsyncClient._QUEUE.append(_ok_tools_call("ok"))
    text, is_error = _run(forwarding.compose_knowledge_search({"query": "sushi", "include_code": True}))
    assert is_error is True
    assert "unsupported filter key(s)" in text


# ---------------------------------------------------------------------------
# Dispatch wiring
# ---------------------------------------------------------------------------


def test_dispatch_points_knowledge_search_at_the_composer():
    assert http_handler._DISPATCH["knowledge_search"] is forwarding.compose_knowledge_search
