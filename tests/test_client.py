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

    async def post(self, url, json=None, headers=None, files=None, params=None, data=None):
        _FakeAsyncClient._CALLS.append(
            {
                "method": "POST",
                "url": url,
                "json": json,
                "headers": headers,
                "files": files,
                "params": params,
                "data": data,
            }
        )
        return _FakeAsyncClient._QUEUE.pop(0)

    async def put(self, url, json=None, headers=None, files=None, params=None, data=None):
        _FakeAsyncClient._CALLS.append(
            {
                "method": "PUT",
                "url": url,
                "json": json,
                "headers": headers,
                "files": files,
                "params": params,
                "data": data,
            }
        )
        return _FakeAsyncClient._QUEUE.pop(0)

    async def delete(self, url, params=None, headers=None):
        _FakeAsyncClient._CALLS.append({"method": "DELETE", "url": url, "params": params, "headers": headers})
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


def test_all_thirteen_tools_are_advertised_and_dispatchable():
    names = {t["name"] for t in client.TOOLS_SCHEMA}
    assert names == {
        "upload_document", "create_node", "create_link",
        "get_graph", "list_documents", "search_nodes", "search_graph",
        "knowledge_search", "search_execution_graph", "exec_run_detail",
        "list_pending_deletions", "approve_pending_deletions",
        "decline_pending_deletions",
    }
    # approve/decline need `db` (the audit trail) so they dispatch through
    # their own table — see http_handler.py's `_DISPATCH_WITH_DB` docstring.
    assert names == set(http_handler._DISPATCH) | set(http_handler._DISPATCH_WITH_DB)


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
    assert call["params"] is None, "omitted bucket must not be forced to a literal 'default'"


def test_list_documents_passes_an_explicit_bucket_through():
    """bug:knowledgeable-list-documents-pinned-to-default-bucket — the fix's
    optional narrowing knob, mirroring search_graph's own `bucket` param."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"documents": [{"id": "doc-2"}]}))
    text, is_error = _run(client.list_documents({"bucket": "main"}))
    assert is_error is False
    assert "doc-2" in text
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"bucket": "main"}


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


def test_get_graph_passes_an_explicit_bucket_through():
    """bug:knowledgeable-get-graph-search-nodes-bucket-default — the fix's
    optional narrowing knob, mirroring list_documents' own `bucket` param."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"nodes": [], "edges": []}))
    _run(client.get_graph({"focus": "doc-1", "bucket": "main"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"focus": "doc-1", "depth": 1, "bucket": "main"}


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


def test_search_nodes_passes_an_explicit_bucket_through():
    """bug:knowledgeable-get-graph-search-nodes-bucket-default — the fix's
    optional narrowing knob, mirroring list_documents' own `bucket` param."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": [], "relationship_types": []}))
    _run(client.search_nodes({"q": "sushi", "bucket": "main"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {"q": "sushi", "bucket": "main"}


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


def test_search_graph_forwards_collection_anchor_and_anchor_depth():
    """§15.6 — coerce-and-forward, same pattern as every other knob: the
    backend's own validation matrix is the single source of truth."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    _run(client.search_graph({
        "q": "sushi", "mode": "semantic", "bucket": "recipes",
        "collection": "notion/kanban/done/", "anchor": "doc-1", "anchor_depth": "2",
    }))
    call = _FakeAsyncClient._CALLS[0]
    assert call["params"] == {
        "q": "sushi", "mode": "semantic", "bucket": "recipes",
        "collection": "notion/kanban/done/", "anchor": "doc-1", "anchor_depth": 2,
    }


def test_search_graph_omits_collection_and_anchor_when_absent():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    _run(client.search_graph({"q": "sushi"}))
    call = _FakeAsyncClient._CALLS[0]
    assert "collection" not in call["params"]
    assert "anchor" not in call["params"]
    assert "anchor_depth" not in call["params"]


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


def test_lucene_wildcard_terms_fixes_the_measured_underscore_miss():
    """docs/design/execution-history-in-production-neo4j.md §9 step 4 card —
    verified empirically against a real Neo4j fulltext index (not asserted
    here, just pinned): a bare term for 'approve_pending_deletions' misses
    the qualified name entirely because Lucene's standard analyser does not
    split on underscores; a leading+trailing wildcard recovers it."""
    assert client._lucene_wildcard_terms("approve_pending_deletions") == "*approve_pending_deletions*"
    assert (
        client._lucene_wildcard_terms("pending deletions")
        == "*pending* OR *deletions*"
    )
    assert client._lucene_wildcard_terms("   ") == ""


def test_lucene_wildcard_terms_escapes_lucene_syntax_inside_each_word():
    """A caller's query must not be able to break out of the wildcard and
    inject Lucene query syntax (e.g. a field qualifier or boolean operator)."""
    assert client._lucene_wildcard_terms("a:b") == '*a\\:b*'
    assert client._lucene_wildcard_terms("x*y") == "*x\\*y*"


def test_search_execution_graph_requires_q():
    text, is_error = _run(client.search_execution_graph({}))
    assert is_error is True and "q is required" in text


def test_search_execution_graph_rejects_an_unknown_target():
    text, is_error = _run(client.search_execution_graph({"q": "x", "target": "bogus"}))
    assert is_error is True and "target must be one of" in text


def test_search_execution_graph_both_hits_runs_and_tool_calls():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": [{"run_id": "r1"}]}))
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": [{"name": "approve_pending_deletions"}]}))
    text, is_error = _run(client.search_execution_graph({"q": "approve_pending_deletions"}))
    assert is_error is False
    assert "r1" in text and "approve_pending_deletions" in text

    runs_call, tc_call = _FakeAsyncClient._CALLS
    assert runs_call["url"] == "http://aw-knowledgeable:8090/api/exec/search/runs"
    assert runs_call["params"] == {"q": "approve_pending_deletions", "limit": 10}, (
        "run-text queries must stay literal, unwildcarded — the measurement's v2 "
        "pass hit 10/10 on prose questions with plain fulltext"
    )
    assert tc_call["url"] == "http://aw-knowledgeable:8090/api/exec/search/tool-calls"
    assert tc_call["params"] == {"q": "*approve_pending_deletions*", "limit": 10}, (
        "tool-call queries must be wildcard-wrapped to find a bare tool name "
        "inside its full qualified, underscore-joined form"
    )


def test_search_execution_graph_target_runs_only_makes_one_call():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    text, is_error = _run(client.search_execution_graph({"q": "redis acl", "target": "runs"}))
    assert is_error is False
    assert len(_FakeAsyncClient._CALLS) == 1
    assert "tool_calls" not in text


def test_search_execution_graph_tool_calls_carry_the_scope_caveat_and_name_counts():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": [
        {"name": "approve_pending_deletions"},
        {"name": "approve_pending_deletions"},
        {"name": "aw-gateway.aw__aw_knowledgeable__approve_pending_deletions"},
    ]}))
    text, is_error = _run(
        client.search_execution_graph({"q": "approve_pending_deletions", "target": "tool_calls"})
    )
    assert is_error is False
    assert "Covers only named MCP tool-call events" in text
    assert '"approve_pending_deletions": 2' in text
    assert '"aw-gateway.aw__aw_knowledgeable__approve_pending_deletions": 1' in text


def test_search_execution_graph_flags_possible_truncation():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": [{"id": str(i)} for i in range(3)]}))
    text, is_error = _run(
        client.search_execution_graph({"q": "x", "target": "runs", "limit": 3})
    )
    assert is_error is False
    assert '"possibly_truncated": true' in text


def test_search_execution_graph_clamps_limit_to_the_backend_floor_and_ceiling():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"results": []}))
    _run(client.search_execution_graph({"q": "x", "target": "runs", "limit": 9999}))
    assert _FakeAsyncClient._CALLS[0]["params"]["limit"] == 100


def test_search_execution_graph_surfaces_a_backend_error_verbatim():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(503, {"detail": "index not ready"}))
    text, is_error = _run(client.search_execution_graph({"q": "x", "target": "runs"}))
    assert is_error is True
    assert "index not ready" in text


def test_exec_run_detail_requires_run_id():
    text, is_error = _run(client.exec_run_detail({}))
    assert is_error is True and "run_id is required" in text


def test_exec_run_detail_sends_the_secret_header_and_run_id_in_the_path():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {
        "run": {"id": "run-1"}, "tool_calls": [], "children": [],
        "caveat": "CHILD_OF understates lineage across wakeups",
    }))
    text, is_error = _run(client.exec_run_detail({"run_id": "run-1"}))
    assert is_error is False
    assert "CHILD_OF understates lineage across wakeups" in text, (
        "the backend's own lineage caveat must reach the agent verbatim"
    )
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/exec/runs/run-1"
    assert call["headers"] == {"X-Internal-Secret": "s3cr3t"}
    assert call["params"] is None


def test_exec_run_detail_forwards_optional_limits():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"run": {}, "tool_calls": [], "children": []}))
    _run(client.exec_run_detail({"run_id": "run-1", "toolcall_limit": "5", "children_limit": "2"}))
    assert _FakeAsyncClient._CALLS[0]["params"] == {"toolcall_limit": 5, "children_limit": 2}


def test_exec_run_detail_reports_404_as_a_tool_error_not_a_crash():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(404, {"detail": "run 'ghost' not found"}))
    text, is_error = _run(client.exec_run_detail({"run_id": "ghost"}))
    assert is_error is True
    assert "run 'ghost' not found" in text


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


def test_create_bucket_sends_the_name():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(201, {"bucket": "kb-crispal", "name": "kb-crispal"}))
    data, err = _run(client.create_bucket("kb-crispal"))
    assert err is None
    assert data["bucket"] == "kb-crispal"
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/buckets"
    assert call["json"] == {"name": "kb-crispal"}


def test_create_bucket_treats_a_409_as_already_existing_not_an_error():
    """§13.1 — the driver calls this every tick; "already exists" has to
    read as steady state, not a failure, even though the route itself
    answers a real 409 on a name collision."""
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(409, {"detail": "bucket 'kb-crispal' already exists"})
    )
    data, err = _run(client.create_bucket("kb-crispal"))
    assert err is None
    assert data == {"bucket": "kb-crispal", "already_existed": True}


def test_create_bucket_surfaces_a_genuine_error():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(500, {"detail": "internal error"}))
    data, err = _run(client.create_bucket("kb-crispal"))
    assert data is None
    assert "internal error" in err


def test_get_ingest_status_forwards_the_bucket():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"extraction": {"claiming": False}}))
    data, err = _run(client.get_ingest_status("kb-crispal"))
    assert err is None
    assert data["extraction"]["claiming"] is False
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/ingest/status"
    assert call["params"] == {"bucket": "kb-crispal"}


def test_get_ingest_status_omits_bucket_param_when_absent():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"extraction": {}}))
    _run(client.get_ingest_status())
    assert _FakeAsyncClient._CALLS[0]["params"] is None


def test_upload_bytes_sends_real_bytes_bucket_and_source_path():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(201, {"id": "doc-1", "deduplicated": False}))
    data, err = _run(
        client.upload_bytes(
            "notes.md", b"kb body", bucket="kb-crispal", source_path="crispal/notes.md"
        )
    )
    assert err is None
    assert data == {"id": "doc-1", "deduplicated": False}
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/documents"
    assert call["params"] == {"bucket": "kb-crispal"}
    assert call["data"] == {"source_path": "crispal/notes.md"}
    assert call["files"] == {"file": ("notes.md", b"kb body")}


def test_upload_bytes_surfaces_a_backend_error():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(413, {"detail": "File exceeds the 10MB limit"}))
    data, err = _run(client.upload_bytes("huge.md", b"x", bucket="kb-crispal"))
    assert data is None
    assert "10MB" in err


def test_put_document_by_path_sends_bucket_as_a_query_param():
    """fs-sync-upload-bucket-missing — PUT /api/documents/by-path must carry
    an explicit `bucket`, the same way `upload_bytes` already does above.
    `require_bucket_write` reads `bucket` off the query string (never the
    multipart body), so an omitted bucket resolves server-side to "the
    tenant's sole writable bucket" — which breaks the moment a second
    writable bucket exists. This assertion is the guard against that
    silently coming back."""
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"id": "doc-1", "rev": 2, "content_hash": "h", "outcome": "updated"})
    )
    data, err = _run(
        client.put_document_by_path(
            "notes.md", b"kb body", bucket="main",
            source_path="authored/notes.md", workspace_slug="aw", base_hash="old-hash",
        )
    )
    assert err is None
    assert data == {"id": "doc-1", "rev": 2, "content_hash": "h", "outcome": "updated"}
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/documents/by-path"
    assert call["params"] == {"bucket": "main"}
    assert call["data"] == {
        "source_path": "authored/notes.md", "workspace_slug": "aw", "base_hash": "old-hash",
    }
    assert call["files"] == {"file": ("notes.md", b"kb body")}


def test_mark_delete_by_path_sends_bucket_as_a_query_param():
    """fs-sync-delete-mark-bucket-missing — the sibling of
    test_put_document_by_path_sends_bucket_as_a_query_param above.
    POST /api/documents/mark-delete-by-path is gated by the same
    `require_bucket_write` dependency: an omitted `bucket` resolves
    server-side to "the tenant's sole writable bucket", 400ing as
    ambiguous the moment a second writable bucket exists. This was the
    second call site carrying the exact defect `put_document_by_path` was
    fixed for — missed because that fix was scoped to the upload call
    only."""
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"source_path": "notion/x.md", "marked": ["doc-1"], "count": 1})
    )
    data, err = _run(
        client.mark_delete_by_path("notion/x.md", "aw", bucket="main", reason="2 consecutive misses")
    )
    assert err is None
    assert data == {"source_path": "notion/x.md", "marked": ["doc-1"], "count": 1}
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/documents/mark-delete-by-path"
    assert call["params"] == {"bucket": "main"}
    assert call["json"] == {
        "source_path": "notion/x.md", "workspace_slug": "aw", "reason": "2 consecutive misses",
    }


def test_get_sync_paths_sends_bucket_as_a_query_param():
    """The read-side sibling of test_mark_delete_by_path_sends_bucket_as_a_
    query_param above — GET /api/documents/sync-paths is gated by
    `require_bucket_read`, not `require_bucket_write`, but the same defect
    applies: an omitted `bucket` resolves server-side to "the tenant's sole
    readable bucket", 400ing as ambiguous the moment a second readable
    bucket exists. Found live: this call's own caller, `fs_sync.
    _adopt_paths`, only logs the resulting error and returns `sync_paths_
    seen=None`, which silently no-ops adoption, GC and the restore-
    reconcile catch-up every single tick — not a crash, so nothing short
    of this assertion would have caught it."""
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"paths": [{"source_path": "notion/x.md", "claimant_count": 1}], "cursor": "notion/x.md"})
    )
    data, err = _run(client.get_sync_paths("aw", after="notion/a.md", limit=500, bucket="main"))
    assert err is None
    assert data == {"paths": [{"source_path": "notion/x.md", "claimant_count": 1}], "cursor": "notion/x.md"}
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/documents/sync-paths"
    assert call["params"] == {"workspace_slug": "aw", "limit": 500, "after": "notion/a.md", "bucket": "main"}


# ---------------------------------------------------------------------------
# fs-sync-delete-mark-bucket-missing's audit — EVERY client method that
# hits a `require_bucket_write`-gated route and can name a bucket at all
# (i.e. not `upload_document`/`create_node`/`create_link`, whose MCP tool
# schemas have no `bucket` argument for an agent to fill in — a structural
# inability to name one, not an omission; see
# `_resolve_implicit_write_bucket`'s own docstring in identity.py) MUST
# send it explicitly. A test that only pins one method's call shape — the
# mistake that let this exact defect ship twice — can't catch the next
# missed call site; this one is table-driven so adding a new bucket-
# requiring method means adding a row here, not writing a new test file.
# ---------------------------------------------------------------------------

_BUCKET_REQUIRING_WRITE_CALLS = [
    (
        "upload_bytes",
        lambda: client.upload_bytes("notes.md", b"body", bucket="main", source_path="x.md"),
    ),
    (
        "put_document_by_path",
        lambda: client.put_document_by_path(
            "notes.md", b"body", bucket="main", source_path="authored/x.md", workspace_slug="aw",
        ),
    ),
    (
        "mark_delete_by_path",
        lambda: client.mark_delete_by_path("notion/x.md", "aw", bucket="main"),
    ),
    (
        "delete_document",
        lambda: client.delete_document("doc-1", bucket="main"),
    ),
    (
        "restore_document",
        lambda: client.restore_document("doc-1", bucket="main"),
    ),
]


@pytest.mark.parametrize(
    "name,call", _BUCKET_REQUIRING_WRITE_CALLS, ids=[row[0] for row in _BUCKET_REQUIRING_WRITE_CALLS]
)
def test_every_bucket_requiring_write_call_sends_an_explicit_bucket(name, call):
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {
        "id": "doc-1", "rev": 1, "content_hash": "h", "outcome": "created",
        "deduplicated": False, "source_path": "x.md", "marked": [], "count": 0,
    }))
    _run(call())
    sent_params = _FakeAsyncClient._CALLS[0]["params"] or {}
    assert sent_params.get("bucket") == "main", (
        f"{name} must send bucket explicitly as a query param — "
        "require_bucket_write resolves an omitted bucket to the tenant's "
        "sole writable bucket, which 400s the moment a second one exists "
        "(fs-sync-delete-mark-bucket-missing)"
    )


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
    assert len(listed["result"]["tools"]) == 13


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


# ---------------------------------------------------------------------------
# knowledge_search (feature:knowledge-search-tunable-endpoint) — POSTs to
# /api/knowledge/search with a JSON body plus bucket/collection as query
# params, same split the route itself uses.
# ---------------------------------------------------------------------------


def test_knowledge_search_requires_a_query():
    text, is_error = _run(client.knowledge_search({}))
    assert is_error
    assert "query is required" in text
    assert _FakeAsyncClient._CALLS == []


def test_knowledge_search_posts_query_as_the_only_required_body_field():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"chunks": []}))
    _run(client.knowledge_search({"query": "how do I restart the ingest worker"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["method"] == "POST"
    assert call["url"].endswith("/api/knowledge/search")
    assert call["json"] == {"query": "how do I restart the ingest worker"}
    assert call["params"] == {}


def test_knowledge_search_forwards_every_tunable_knob():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"chunks": []}))
    _run(client.knowledge_search({
        "query": "sushi", "mode": "fused", "radius": 2, "limit": 20,
        "subqueries": 5, "rounds": 1, "budget_ms": 15000, "synthesize": False,
        "filters": {"document_ids": ["doc-1"]},
        "bucket": "recipes", "collection": "notion/kanban/",
    }))
    call = _FakeAsyncClient._CALLS[0]
    assert call["json"] == {
        "query": "sushi", "mode": "fused", "radius": 2, "limit": 20,
        "subqueries": 5, "rounds": 1, "budget_ms": 15000, "synthesize": False,
        "filters": {"document_ids": ["doc-1"]},
    }
    assert call["params"] == {"bucket": "recipes", "collection": "notion/kanban/"}


def test_knowledge_search_only_forwards_known_fields():
    """Same D2-risk-4 rule as search_graph/create_node — a gateway-injected
    extra key must never reach the outbound body."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"chunks": []}))
    _run(client.knowledge_search({"query": "sushi", "_gateway_caller_run_id": "run-xyz"}))
    call = _FakeAsyncClient._CALLS[0]
    assert call["json"] == {"query": "sushi"}


def test_knowledge_search_reports_the_backend_error_text():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(400, {"detail": "unsupported filter key(s): ['author']"}))
    text, is_error = _run(client.knowledge_search({"query": "sushi", "filters": {"author": "x"}}))
    assert is_error
    assert "unsupported filter key(s)" in text
