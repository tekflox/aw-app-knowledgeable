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
    "probe": {"tool": "codegraph_status", "ttl_ok_s": 60, "ttl_fail_s": 15, "timeout_s": 5},
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
