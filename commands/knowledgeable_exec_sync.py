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
    aw-workspace-cli knowledgeable-exec-sync purge-secret-args    # §13.2 secret-tool purge
    aw-workspace-cli knowledgeable-exec-sync reset-watermark      # §13.8 backfill: re-read every run

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

`reset-watermark` is §13.8's per-slice backfill trigger: it clears the
freshness watermark so the next bounded ticks re-read every run from the
beginning, which is the only way a projection added by a new slice reaches
runs that were already synced. Nothing is deleted and nothing can be lost —
every statement behind the ingest route is a MERGE, so a re-read is free
while a skip is permanent. What it costs is TICKS: ~14 bounded passes over
~28k runs, sharing the single production worker with live search, so run it
off-hours and never from a scheduled task. After it, call `run` repeatedly
until `runs` comes back 0 / "no runs due".

`purge-secret-args` is §13.2's remediation: it nulls `args_text` on every
`ExecToolCall` whose name's terminal segment is a secret-store tool, however
the gateway spelled the prefix. It exists because the drop used to be an
exact match on the bare name and therefore never fired for a real
MCP-routed call, so prefixed `write_secret` arguments — vault-only values,
which are both unmatchable by the exact pass and shapeless to the pattern
pass — landed in the graph. Idempotent, and all zeroes once it has run; run
it again after adding a name to `SECRET_TOOL_NAMES`.
"""
from __future__ import annotations

import json
import sys

COMMAND = "knowledgeable-exec-sync"
DESCRIPTION = "execution-history projection sync — run/status/reconcile/retention"

_BASE = "/api/apps/knowledgeable/exec-sync"

#: Read timeout for the subcommands that do real work, in seconds.
#:
#: `local_client.request`'s own default is 30 s, which is right for a status
#: read and wrong for every mutating call here. Measured 2026-10-19, during
#: §13 slice 1's backfill: a 2,000-run tick that also fetches and posts
#: events takes **longer than 30 s**, so the CLI raised `httpx.ReadTimeout`
#: while the route went on to finish normally server-side — the watermark
#: advanced, the events landed, and the caller was told the tick had failed.
#:
#: That is the worst available failure shape for this particular command,
#: twice over: `knowledgeable-exec-sync run` is also the body of the
#: contributed `KB exec-history sync — tick` scheduled task, whose
#: `notify_exit_codes: [1]` then dispatches an agent to diagnose a tick that
#: actually worked; and a backfill loop reading that exit code cannot tell
#: "drained" from "timed out", which is what made the first slice-1 backfill
#: attempt unobservable.
#:
#: 30 minutes, not a tuned number: the tick is already bounded by
#: `MAX_RUNS_PER_TICK` and `BATCH_BYTES` on the engine side (§13.12 point
#: 10), so this only has to be longer than the slowest bounded tick rather
#: than a budget of its own. `status` deliberately keeps the 30 s default —
#: a doctor surface that hangs for half an hour is its own failure.
_WORK_TIMEOUT_S = 1800.0


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
        status, body = local_client.request(
            "POST", f"{_BASE}/run", payload, timeout=_WORK_TIMEOUT_S
        )
        if status != 200:
            print(f"run failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if body.get("blocked"):
            print(f"blocked: {body.get('reason')}", file=sys.stderr)
            return 1
        # A tick that completed but reported an error advanced the watermark
        # only over the prefix that landed. Non-zero so the scheduled task
        # notices, rather than reading a partial pass as a clean one. "no
        # runs due" is the engine's own benign no-op (it still stamps the
        # sentinel) and must stay exit 0, matching this file's own
        # docstring — it is not a partial pass.
        if body.get("reason") and body["reason"] != "no runs due":
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
        status, body = local_client.request(
            "POST", f"{_BASE}/reconcile", {}, timeout=_WORK_TIMEOUT_S
        )
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
        status, body = local_client.request(
            "POST", f"{_BASE}/retention", payload, timeout=_WORK_TIMEOUT_S
        )
        if status != 200:
            print(f"retention failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if not body.get("ok"):
            print(f"retention: {body.get('error')}", file=sys.stderr)
            return 1
        return 0

    if sub == "reset-watermark":
        status, body = local_client.request("POST", f"{_BASE}/reset-watermark", {})
        if status != 200:
            print(f"reset-watermark failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if not body.get("ok"):
            print(f"reset-watermark: {body.get('error')}", file=sys.stderr)
            return 1
        print(
            "watermark cleared — now run `knowledgeable-exec-sync run` repeatedly "
            "until it reports 0 runs",
            file=sys.stderr,
        )
        return 0

    if sub == "purge-secret-args":
        status, body = local_client.request(
            "POST", f"{_BASE}/purge-secret-args", {}, timeout=_WORK_TIMEOUT_S
        )
        if status != 200:
            print(f"purge-secret-args failed: HTTP {status} {body}", file=sys.stderr)
            return 1
        print(json.dumps(body, indent=2))
        if not body.get("ok"):
            print(f"purge-secret-args: {body.get('error')}", file=sys.stderr)
            return 1
        return 0

    print(f"unknown subcommand {sub!r}", file=sys.stderr)
    return _usage()
