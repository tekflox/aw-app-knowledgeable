"""``aw-workspace-cli knowledgeable-exec-sync`` — the execution-history sync
CLI (docs/design/execution-history-in-production-neo4j.md §4/§8).

Same two-doors shape as ``knowledgeable_sync.py``: the engine
(``knowledgeable_app/exec_sync.py``) runs inside the workspace server process
— it reuses this app's configured ``mcp/client.py`` and the workspace
``SecretStore`` the scrubber's exact-match pass reads — so this CLI is a thin
client over ``/api/apps/knowledgeable/exec-sync/*``.

Usage:
    aw-workspace-cli knowledgeable-exec-sync run [--max-runs N]   # one bounded tick
    aw-workspace-cli knowledgeable-exec-sync status               # sentinel + staleness + counts
    aw-workspace-cli knowledgeable-exec-sync reconcile            # SQL vs graph count table
    aw-workspace-cli knowledgeable-exec-sync retention [--days N] # the args_text valve

Exit codes for `run`: 0 progressed or nothing to do, 1 the tick was blocked —
the flag is off, no DSN, no source tenant, no service secret, another tick in
flight, or an ingest batch failed (the watermark then only advanced over the
prefix that landed, so the next tick resumes at the gap rather than past it).

`status` exits 1 when the sync is ENABLED and degraded — the §8 staleness
dead-man's switch, in a form `aw-workspace-cli doctor` and the scheduled
task's `notify_exit_codes` can both read. A disabled sync is exit 0: off is
not degraded.

`reconcile` exits 1 on any count mismatch. Read the table before concluding
drift: a `CHILD_OF`/`QA_OF` mismatch is often correct behaviour (an endpoint
outside the loaded window creates no edge by design), whereas a missing
`ExecToolCall` is a batch that failed.

`retention` is §8's disk valve, not a schedule to run casually: it nulls
`ExecToolCall.args_text` older than 180 days. The text is rebuildable from
Postgres at any time (Postgres is the system of record), so this is a cache
eviction — but it is also irreversible in the graph until a re-backfill.
"""
from __future__ import annotations

import json
import sys

COMMAND = "knowledgeable-exec-sync"
DESCRIPTION = "execution-history projection sync — run/status/reconcile/retention"

_BASE = "/api/apps/knowledgeable/exec-sync"


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
        if "--max-runs" in rest:
            payload["max_runs"] = int(rest[rest.index("--max-runs") + 1])
        status, body = local_client.request("POST", f"{_BASE}/run", payload)
        if status != 200:
            print(f"run failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if body.get("blocked"):
            print(f"blocked: {body.get('reason')}", file=sys.stderr)
            return 1
        # A tick that completed but reported an error advanced the watermark
        # only over the prefix that landed. Non-zero so the scheduled task
        # notices, rather than reading a partial pass as a clean one.
        if body.get("reason"):
            print(f"partial: {body['reason']}", file=sys.stderr)
            return 1
        return 0

    if sub == "status":
        status, body = local_client.request("GET", f"{_BASE}/status")
        if status != 200:
            print(f"status failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if body.get("degraded"):
            print("degraded: the exec sync is enabled but stale or unreachable", file=sys.stderr)
            return 1
        return 0

    if sub == "reconcile":
        status, body = local_client.request("POST", f"{_BASE}/reconcile", {})
        if status != 200:
            print(f"reconcile failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if not body.get("ok"):
            print(f"reconcile not ok: {body.get('error') or 'count mismatch'}", file=sys.stderr)
            return 1
        return 0

    if sub == "retention":
        payload = {}
        if "--days" in rest:
            payload["days"] = int(rest[rest.index("--days") + 1])
        status, body = local_client.request("POST", f"{_BASE}/retention", payload)
        if status != 200:
            print(f"retention failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if not body.get("ok"):
            print(f"retention: {body.get('error')}", file=sys.stderr)
            return 1
        return 0

    print(f"unknown subcommand {sub!r}", file=sys.stderr)
    return _usage()
