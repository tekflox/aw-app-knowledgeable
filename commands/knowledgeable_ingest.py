"""``aw-workspace-cli knowledgeable-ingest`` — this app's own CLI command.

Auto-discovered by aw-workspace-cli from this app's installed directory
(``<apps_root>/knowledgeable/commands/``, since this file lives at
``commands/`` in this repo's root — see aw-workspace's ``src/cli/
discovery.py``).

A thin client over ``/api/apps/knowledgeable/bulk-ingest/*`` (same shape as
aw-app-architecture's own ``commands/architecture.py``), because the §13
engine (``knowledgeable_app/bulk_ingest.py``) has to run inside the
workspace server process to reuse this app's already-configured
``mcp/client.py`` (base_url + X-Internal-Secret) — a CLI invocation is a
separate OS process and holds none of that.

Door 1 of the §13 driver's "one engine, two doors": the operator runs this
by hand to start/resume/report; door 2 is the contributed scheduled task,
which runs the exact `run --max-uploads N` form below as its `command` — so
whatever this prints and whatever exit code it returns is also what the
unattended task sees.

Usage:
    aw-workspace-cli knowledgeable-ingest scan                # (re)walk the KB tree, hash + canonicalize
    aw-workspace-cli knowledgeable-ingest run [--max-uploads N]  # one bounded tick (default 200)
    aw-workspace-cli knowledgeable-ingest status               # journal counts by subtree/status
    aw-workspace-cli knowledgeable-ingest report                # the §13 deliverable report

Exit codes for `run`: 0 progressed or nothing pending, 1 the ignition guard
blocked the tick (extraction is claiming, or the status check itself
failed) — the only code the contributed scheduled task escalates to an
agent.
"""
from __future__ import annotations

import json
import sys

COMMAND = "knowledgeable-ingest"
DESCRIPTION = "KB bulk-ingest driver (§13) — scan/run/status/report"

_BASE = "/api/apps/knowledgeable/bulk-ingest"


def _usage() -> int:
    print(__doc__.split("Usage:")[1].strip())
    return 2


def run(args: list[str] | None = None) -> int:
    args = list(args or [])
    if not args or args[0] in ("-h", "--help"):
        return _usage()

    from src.cli import local_client

    sub, rest = args[0], args[1:]

    if sub == "scan":
        status, body = local_client.request("POST", f"{_BASE}/scan")
        if status != 200:
            print(f"scan failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        return 0

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
