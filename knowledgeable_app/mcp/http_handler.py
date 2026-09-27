"""The ``knowledgeable`` MCP server, over Streamable HTTP (``POST /mcp``).

JSON-RPC 2.0 envelope + dispatch, same shape as ``apps/kb/kb_app/mcp_http.py``
and ``aw-app-google-maps``'s ``mcp/http_handler.py`` — the wire protocol is
what aw-mcp-gateway's own ``HttpUpstream`` speaks, and none of it is specific
to aw-knowledgeable. The actual tool logic lives in ``client.py``.
"""
from __future__ import annotations

import logging

from . import client

log = logging.getLogger("aw_apps.knowledgeable")

SERVER_NAME = "aw-knowledgeable"
SERVER_VERSION = "1.0.0"

TOOLS_SCHEMA = client.TOOLS_SCHEMA

_DISPATCH = {
    "upload_document": client.upload_document,
    "create_node": client.create_node,
    "create_link": client.create_link,
    "get_graph": client.get_graph,
    "list_documents": client.list_documents,
    "search_nodes": client.search_nodes,
}

_NO_SECRET = (
    "No service secret configured for this connector. Open the Knowledgeable "
    "Connector app's Settings in this workspace and save one — it must match "
    "aw-knowledgeable's own KNOWLEDGEABLE_SERVICE_SECRET."
)


def _result(req_id, text: str, is_error: bool) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {"content": [{"type": "text", "text": text}], "isError": is_error},
    }


async def handle_request(request: dict) -> dict | None:
    method = request.get("method", "")
    req_id = request.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        }
    if method == "notifications/initialized":
        return None
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS_SCHEMA}}
    if method != "tools/call":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Unknown method: {method}"},
        }

    params = request.get("params") or {}
    name = params.get("name", "")
    args = params.get("arguments") or {}

    handler = _DISPATCH.get(name)
    if not handler:
        return _result(req_id, f"Unknown tool: {name}", True)

    if not client.configured():
        return _result(req_id, _NO_SECRET, True)

    try:
        text, is_error = await handler(args)
    except Exception as exc:  # noqa: BLE001 — last resort, must not 500 the route
        log.exception("knowledgeable MCP tool %s failed", name)
        return _result(req_id, f"{name} failed: {exc}", True)

    return _result(req_id, text, is_error)
