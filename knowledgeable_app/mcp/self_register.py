"""Entry describing this app's own ``/mcp`` endpoint, for aw-mcp-gateway's
app-scan (``scan_app_mcp_servers()``, which reads ``<app dir>/mcp.json``).

Tier-1 (in-process): this *is* the aw-workspace process, so
``socket.gethostname()`` is exactly the value ContainerSupervisor injects into
sibling containers as ``AW_WORKSPACE_HOST`` (not ``AW_APP_SELF_HOST`` —
that env var is the Tier-2 variant, see ``apps/kb/kb_app/self_register.py``),
and ``AW_WORKSPACE_API_KEY`` is already in this process's environment —
nothing has to be provisioned. The header is required because Tier-1 routes
sit behind IdentityGuard (docs/design/aw-knowledgeable-infra.md §10, D2 Q1).
"""
from __future__ import annotations

import os
import socket

MCP_SERVER_NAME = "aw-knowledgeable"
ROUTE_PATH = "/api/apps/knowledgeable/mcp"


def build_self_entry(port: int | None = None) -> dict:
    host = socket.gethostname()
    port = port or int(os.environ.get("AW_PORT") or 9030)
    entry: dict = {
        "type": "http",
        "url": f"http://{host}:{port}{ROUTE_PATH}",
        "enabled": True,
    }
    api_key = os.environ.get("AW_WORKSPACE_API_KEY")
    if api_key:
        entry["headers"] = {"X-Api-Key": api_key}
    return entry
