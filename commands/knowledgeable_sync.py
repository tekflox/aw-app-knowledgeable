"""``aw-workspace-cli knowledgeable-sync`` — this app's fs-sync CLI (card
B of docs/design/aw-knowledgeable-fs-sync.md).

Same two-doors shape as ``knowledgeable_ingest.py``'s own docstring: the
§3 engine (``knowledgeable_app/fs_sync.py``) runs inside the workspace
server process (it reuses this app's already-configured ``mcp/client.py``
and the shared bulk-ingest journal), so this CLI is a thin client over
``/api/apps/knowledgeable/fs-sync/*``.

Usage:
    aw-workspace-cli knowledgeable-sync run [--max-uploads N]  # one bounded tick (default 200)
    aw-workspace-cli knowledgeable-sync status                 # journal counts: synced/missing/pending-delete
    aw-workspace-cli knowledgeable-sync report                 # per-subtree §3/§7 deliverable report

Exit codes for `run`: 0 progressed or nothing to do, 1 the tick was
blocked — the §6 workspace_slug refusal, another tick already in
progress, or the §7 mass-delete valve tripped (check `status` for
`valve_tripped` and verify the tree before clearing it or approving in
the UI).
"""
from __future__ import annotations

import json
import sys

COMMAND = "knowledgeable-sync"
DESCRIPTION = "fs-sync upsync engine (§3/§7) — run/status/report"

_BASE = "/api/apps/knowledgeable/fs-sync"


def _usage() -> int:
    print(__doc__.split("Usage:")[1].strip())
    return 2


def run(args: list[str] | None = None) -> int:
    args = list(args or [])
    if not args or args[0] in ("-h", "--help"):
        return _usage()

    from src.cli import local_client

    sub, rest = args[0], args[1:]

    if sub == "run":
        payload = {}
        if "--max-uploads" in rest:
            idx = rest.index("--max-uploads")
            payload["max_uploads"] = int(rest[idx + 1])
        status, body = local_client.request("POST", f"{_BASE}/run", payload)
        if status != 200:
            print(f"run failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if body.get("blocked"):
            print(f"blocked: {body.get('reason')}", file=sys.stderr)
            return 1
        return 0

    if sub == "status":
        status, body = local_client.request("GET", f"{_BASE}/status")
        if status != 200:
            print(f"status failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        return 0

    if sub == "report":
        status, body = local_client.request("GET", f"{_BASE}/report")
        if status != 200:
            print(f"report failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        return 0

    print(f"unknown subcommand {sub!r}", file=sys.stderr)
    return _usage()
