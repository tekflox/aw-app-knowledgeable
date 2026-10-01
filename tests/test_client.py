"""The tool layer against aw-knowledgeable: secret resolution, request shaping,
and the failure modes that would otherwise reach an agent as a raw traceback.

Deliberately no real network — every test monkeypatches ``httpx.AsyncClient``
with a fake that records the outbound request and returns a canned response,
same shape as ``aw-app-google-maps``'s ``test_places.py`` (which stubs
``urllib.request.urlopen`` for the same reason).
"""
from __future__ import annotations

import asyncio
import base64
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import mcp_config  # noqa: E402
from knowledgeable_app.mcp import client, http_handler  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeResponse:
    def __init__(self, status_code=200, json_body=None):
        self.status_code = status_code
        self._json = json_body if json_body is not None else {}

    def json(self):
        return self._json


class _FakeAsyncClient:
    """Records every request; ``_QUEUE`` (module-global, reset per test)
    supplies canned responses in call order."""

    _CALLS: list = []
    _QUEUE: list = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None, headers=None):
        _FakeAsyncClient._CALLS.append({"method": "GET", "url": url, "params": params, "headers": headers})
        return _FakeAsyncClient._QUEUE.pop(0)

    async def post(self, url, json=None, headers=None, files=None):
        _FakeAsyncClient._CALLS.append(
            {"method": "POST", "url": url, "json": json, "headers": headers, "files": files}
        )
        return _FakeAsyncClient._QUEUE.pop(0)


@pytest.fixture(autouse=True)
def _fresh_fake_client(monkeypatch):
    _FakeAsyncClient._CALLS = []
    _FakeAsyncClient._QUEUE = []
    monkeypatch.setattr(client, "httpx", type(
        "M", (), {"AsyncClient": _FakeAsyncClient, "HTTPError": httpx.HTTPError, "Response": httpx.Response},
    ))
    client.configure(lambda: "http://aw-knowledgeable:8090", lambda: "s3cr3t")
    yield
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: "")


def test_all_seven_tools_are_advertised_and_dispatchable():
    names = {t["name"] for t in client.TOOLS_SCHEMA}
    assert names == {
        "upload_document", "create_node", "create_link",
        "get_graph", "list_documents", "search_nodes", "search_graph",
    }
    assert names == set(http_handler._DISPATCH)


def test_secret_is_resolved_per_call_not_captured_at_import():
    """A rotated secret must take effect on the very next call — no restart
    on this Tier-1 process (D2 risk 5)."""
    box = {"s": ""}
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: box["s"])
    assert not client.configured()
    box["s"] = "new-secret"
    assert client.configured()


def test_secret_resolver_surviving_a_broken_callable():
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: 1 / 0)
    assert not client.configured()


def test_missing_secret_is_reported_before_calling_out():
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: "")
    resp = _run(http_handler.handle_request({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "list_documents", "arguments": {}},
    }))
    assert resp["result"]["isError"] is True
    assert "No service secret configured" in resp["result"]["content"][0]["text"]
    assert _FakeAsyncClient._CALLS == [], "must not reach aw-knowledgeable without a secret"


def test_list_documents_sends_the_secret_header():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"documents": [{"id": "doc-1"}]}))
    resp = _run(http_handler.handle_request({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "list_documents", "arguments": {}},
    }))
    assert resp["result"]["isError"] is False
    assert "doc-1" in resp["result"]["content"][0]["text"]
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/documents"
    assert call["headers"] == {"X-Internal-Secret": "s3cr3t"}


def test_get_graph_requires_focus():
    text, is_error = _run(client.get_graph({}))
    assert is_error is True
    assert "focus is required" in text
    assert _FakeAsyncClient._CALLS == []


def test_get_graph_passes_focus_and_clamped_depth():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"nodes": [], "edges": []}))
    _run(client.get_graph({"focus": "doc-1", "depth": "2"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"focus": "doc-1", "depth": 2}


def test_create_node_requires_label_and_type():
    text, is_error = _run(client.create_node({"label": "x"}))
    assert is_error is True and "type is required" in text
    text, is_error = _run(client.create_node({"type": "x"}))
    assert is_error is True and "label is required" in text


def test_create_node_only_forwards_known_fields():
    """A gateway-injected extra key (e.g. `_gateway_caller_run_id`, D2 risk 4)
    must never reach the outbound body — the request is built field-by-field,
    not passed through wholesale."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(201, {"id": "node-1"}))
    _run(client.create_node({"label": "Alice", "type": "person", "_gateway_caller_run_id": "run-xyz"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["json"] == {"label": "Alice", "type": "person"}


def test_create_link_requires_both_ids_and_type():
    text, is_error = _run(client.create_link({"from_id": "a", "to_id": "b"}))
    assert is_error is True and "type is required" in text
    text, is_error = _run(client.create_link({"type": "references"}))
    assert is_error is True and "from_id and to_id are required" in text


def test_create_link_reports_404_as_a_tool_error_not_a_crash():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(404, {"detail": "One of the nodes was not found"}))
    text, is_error = _run(client.create_link({"from_id": "a", "to_id": "ghost", "type": "references"}))
    assert is_error is True
    assert "One of the nodes was not found" in text


def test_search_nodes_builds_bounded_query_params():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": [], "relationship_types": []}))
    _run(client.search_nodes({"q": "sushi", "exclude": "doc-1", "limit": "5"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"q": "sushi", "exclude": "doc-1", "limit": 5}


def test_search_graph_defaults_mode_to_tree():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": [], "mode": "tree"}))
    _run(client.search_graph({"q": "sushi"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"q": "sushi", "mode": "tree"}


def test_search_graph_coerces_types_and_joins_related_vias():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    _run(client.search_graph({
        "q": "sushi", "mode": "tree", "limit": "10", "beam_width": "5",
        "min_score": "0.5", "related_vias": ["topic", "entity"], "bucket": "recipes",
    }))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {
        "q": "sushi", "mode": "tree", "limit": 10, "beam_width": 5,
        "min_score": 0.5, "related_vias": "topic,entity", "bucket": "recipes",
    }


def test_search_graph_only_forwards_known_fields():
    """Same rule as create_node — a gateway-injected extra key must never
    reach the outbound query params (D2 risk 4)."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    _run(client.search_graph({"q": "sushi", "_gateway_caller_run_id": "run-xyz"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"q": "sushi", "mode": "tree"}


def test_search_graph_forwards_include_traversal():
    """§12 traversal provenance
    (docs/design/aw-knowledgeable-v2-retrieval.md §12, card
    feature:aw-knowledgeable-retrieval-traversal-provenance) — plain
    passthrough, default false so an agent pays for it only when it asks."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    _run(client.search_graph({"q": "sushi", "mode": "tree", "include_traversal": True}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"q": "sushi", "mode": "tree", "include_traversal": True}


def test_search_graph_omits_include_traversal_when_absent():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    _run(client.search_graph({"q": "sushi"}))
    call = _FakeAsyncClient._CALLS[0]
    assert "include_traversal" not in call["params"]


def test_search_graph_surfaces_backend_400_verbatim():
    """§5/§12's declared-contract rule: the backend's validation matrix is the
    one source of truth for knob×mode mismatches — this tool must not
    pre-validate and must not swallow the detail."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(
        400, {"detail": "beam_width is only valid with mode=tree"},
    ))
    text, is_error = _run(client.search_graph({"q": "sushi", "mode": "lexical", "beam_width": "5"}))
    assert is_error is True
    assert "beam_width is only valid with mode=tree" in text


def test_upload_document_requires_filename_and_content():
    text, is_error = _run(client.upload_document({}))
    assert is_error is True and "filename is required" in text
    text, is_error = _run(client.upload_document({"filename": "x.pdf"}))
    assert is_error is True and "content_base64 is required" in text


def test_upload_document_rejects_invalid_base64():
    text, is_error = _run(client.upload_document({"filename": "x.pdf", "content_base64": "not-base64!!"}))
    assert is_error is True
    assert "not valid base64" in text
    assert _FakeAsyncClient._CALLS == []


def test_upload_document_sends_real_multipart_bytes():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(201, {"id": "doc-9", "link_count": 0}))
    raw = b"%PDF-1.4 fake pdf bytes"
    b64 = base64.b64encode(raw).decode()
    text, is_error = _run(client.upload_document({"filename": "notes.pdf", "content_base64": b64}))
    assert is_error is False
    assert "doc-9" in text
    call = _FakeAsyncClient._CALLS[0]
    assert call["method"] == "POST"
    assert call["files"]["file"] == ("notes.pdf", raw)


def test_push_playground_key_sends_the_value_and_secret_header():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"ok": True, "configured": True}))
    ok, err = _run(client.push_playground_key("apmt-scoped-key"))
    assert ok is True
    assert err is None
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/playground/key"
    assert call["json"] == {"api_key": "apmt-scoped-key"}
    assert call["headers"] == {"X-Internal-Secret": "s3cr3t"}


def test_push_playground_key_reports_a_backend_refusal():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(401, {"detail": "unauthorized"}))
    ok, err = _run(client.push_playground_key("apmt-scoped-key"))
    assert ok is False
    assert "unauthorized" in err


def test_push_playground_key_reports_an_ok_false_body_as_failure():
    """A 200 whose body says `{"ok": false}` must not read as success — the
    only thing this function's caller checks before logging "re-asserted"."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"ok": False}))
    ok, err = _run(client.push_playground_key("apmt-scoped-key"))
    assert ok is False


def test_push_ingest_key_sends_the_value_and_secret_header():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"ok": True, "configured": True}))
    ok, err = _run(client.push_ingest_key("apmt-scoped-key"))
    assert ok is True
    assert err is None
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/ingest/key"
    assert call["json"] == {"api_key": "apmt-scoped-key"}
    assert call["headers"] == {"X-Internal-Secret": "s3cr3t"}


def test_push_ingest_key_reports_a_backend_refusal():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(401, {"detail": "unauthorized"}))
    ok, err = _run(client.push_ingest_key("apmt-scoped-key"))
    assert ok is False
    assert "unauthorized" in err


def test_push_ingest_key_reports_an_ok_false_body_as_failure():
    """A 200 whose body says `{"ok": false}` must not read as success — the
    only thing this function's caller checks before logging "re-asserted"."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"ok": False}))
    ok, err = _run(client.push_ingest_key("apmt-scoped-key"))
    assert ok is False


def test_unreachable_host_is_a_tool_error_not_a_crash(monkeypatch):
    class _Boom(_FakeAsyncClient):
        async def get(self, *a, **k):
            raise httpx.HTTPError("connect failed")

    monkeypatch.setattr(client, "httpx", type(
        "M", (), {"AsyncClient": _Boom, "HTTPError": httpx.HTTPError, "Response": httpx.Response},
    ))
    text, is_error = _run(client.list_documents({}))
    assert is_error is True
    assert "could not reach aw-knowledgeable" in text


def test_initialize_and_tools_list():
    init = _run(http_handler.handle_request({"jsonrpc": "2.0", "id": 1, "method": "initialize"}))
    assert init["result"]["serverInfo"]["name"] == "aw-knowledgeable"
    listed = _run(http_handler.handle_request({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
    assert len(listed["result"]["tools"]) == 7


def test_initialized_notification_gets_no_response():
    assert _run(http_handler.handle_request({"jsonrpc": "2.0", "method": "notifications/initialized"})) is None


def test_unknown_tool_is_an_error_not_a_crash():
    resp = _run(http_handler.handle_request({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": {"name": "nope", "arguments": {}},
    }))
    assert resp["result"]["isError"] is True
    assert "Unknown tool" in resp["result"]["content"][0]["text"]


def test_unknown_method_is_a_jsonrpc_error():
    resp = _run(http_handler.handle_request({"jsonrpc": "2.0", "id": 4, "method": "resources/list"}))
    assert resp["error"]["code"] == -32601


def test_handler_exception_becomes_a_tool_error_not_a_500(monkeypatch):
    monkeypatch.setitem(http_handler._DISPATCH, "list_documents",
                        lambda args: (_ for _ in ()).throw(RuntimeError("boom")))
    resp = _run(http_handler.handle_request({
        "jsonrpc": "2.0", "id": 5, "method": "tools/call",
        "params": {"name": "list_documents", "arguments": {}},
    }))
    assert resp["result"]["isError"] is True
    assert "boom" in resp["result"]["content"][0]["text"]


def test_mcp_json_names_the_server_and_route(tmp_path):
    doc = mcp_config.write_mcp_json(str(tmp_path), 9030)
    entry = doc["mcpServers"]["aw-knowledgeable"]
    assert entry["type"] == "http"
    assert entry["url"].endswith(":9030/api/apps/knowledgeable/mcp")


def test_mcp_json_write_is_skipped_when_unchanged(tmp_path):
    mcp_config.write_mcp_json(str(tmp_path), 9030)
    before = (tmp_path / "mcp.json").stat().st_mtime_ns
    mcp_config.write_mcp_json(str(tmp_path), 9030)
    assert (tmp_path / "mcp.json").stat().st_mtime_ns == before
    mcp_config.write_mcp_json(str(tmp_path), 9999)
    assert (tmp_path / "mcp.json").stat().st_mtime_ns != before
