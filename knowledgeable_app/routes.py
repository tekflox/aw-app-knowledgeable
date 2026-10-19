"""This app's backend sub-app, mounted by the runtime at
``/api/apps/knowledgeable``.

The service secret goes to ``ctx.secrets`` via ``POST /settings``, never
through the generic config path — that would land it in plain,
cloud-syncable app config. The ``x-secret`` flag on ``service_secret`` in
the manifest exists only so the settings UI renders a password field. Same
split as ``aw-app-google-maps``'s API key.

**This app's config must carry ``auth_required: false``** — without it, the
workspace's own ``IdentityGuard`` 401s every request to this mount before any
handler below runs, and ``GET /keys/{slot}`` (below) exists precisely to be
called by production aw-knowledgeable, which holds neither a signed-in
session nor the workspace API key, only the shared ``X-Internal-Secret``.
``auth_required`` is an ALL-OR-NOTHING flag on the whole mount
(``aw-autoskill-app-auth-required-routing``), so every OTHER route here now
carries its own explicit ``_require_identity`` dependency — the protection
the framework gate used to provide for free. Flipping that config key is a
deliberate operational step outside this repo (``POST
/api/apps/knowledgeable/config``); this file only makes it SAFE to flip.
"""
from __future__ import annotations

import hmac

from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response

from . import (
    bulk_ingest,
    exec_sync,
    forwarding,
    fs_sync,
    git_backend,
    ingest_key_push,
    key_vault,
    label_key_push,
    mcp_config,
    orphan_reconcile,
    pending_deletions,
    playground_key_push,
)
from .mcp import client
from .workspace_env import workspace_env

SECRET_KEY = "service_secret"

#: `GET /keys/{slot}` — docs/design/knowledgeable-apmt-key-pull-through.md
#: §3.1. Each slot is one of the three ap-mt keys production already pushes
#: for, named by the aw-knowledgeable-side endpoint that consumes it, mapped
#: to the vault secret + push module that already know how to fetch it. One
#: table rather than three near-identical route functions.
_PULL_SLOTS = {
    "playground": (
        playground_key_push.SECRET_NAME,
        "pull-through: aw-knowledgeable's Playground ap-mt key on miss/401",
    ),
    "ingest": (
        ingest_key_push.SECRET_NAME,
        "pull-through: aw-knowledgeable's extractor ap-mt key on miss/401",
    ),
    "topics": (
        label_key_push.SECRET_NAME,
        "pull-through: aw-knowledgeable's topic labeller ap-mt key on miss/401",
    ),
}
GIT_TOKEN_KEY = git_backend.GIT_TOKEN_SECRET

#: docs/design/execution-history-in-production-neo4j.md §4 — the AP-MT
#: Postgres DSN the exec sync reads from. In the SECRET store, not app
#: config, for the same reason `service_secret` is: a DSN carries a password,
#: and app config is plain and cloud-syncable. `x-secret` on the manifest
#: field only makes the settings UI render a password input.
APMT_DSN_KEY = "apmt_database_url"
#: `ctx.state.kv` name the last tick's git_state/git_detail is cached under,
#: so `/status` can show it between ticks (§16.3 point 3) without re-running
#: git. 1 day: generous enough to span the 15-minute schedule across a
#: quiet weekend without going stale forever.
GIT_STATE_KV_NAME = "git_last_state"
GIT_STATE_KV_TTL = 60 * 60 * 24

#: docs/design/aw-knowledgeable-fs-sync.md §7 — the mass-delete valve's
#: own "journal flag", cached the same way `git_last_state` is: a tripped
#: valve is a rare, deliberately sticky signal an operator must notice and
#: clear (verify the tree, then either resolve it or approve in the UI).
#:
#: §21.2 promotes this latch from reporting-only to a HARD GATE on
#: unattended deletion approval, which makes a silent expiry a silent
#: re-arm of that gate — this workspace's named failure mode. The 30-day
#: TTL is gone for that reason; `ctx.state.kv` has no immortal-key concept
#: (`put` requires `ttl > 0`), so this is the largest practical stand-in
#: for "never" rather than a literal absence of one. The only way this key
#: now goes away is the audited `POST /fs-sync/valve/clear` below.
FS_SYNC_VALVE_KV_NAME = "fs_sync_valve_tripped"
FS_SYNC_VALVE_KV_TTL = 60 * 60 * 24 * 365 * 100  # ~100 years — see note above


def _caller_actor(request: Request) -> str:
    """Best-effort "who did this" for the pending-deletion audit trail
    (docs/design/aw-knowledgeable-fs-sync.md §7 approval). `x-aw-caller-agent`
    / `x-aw-caller-run-id` are the headers aw-mcp-gateway's `HttpUpstream`
    attaches to every forwarded tool call (`gateway/upstream.py`'s
    `_client_headers`) — present whenever this route is reached through the
    gateway, which is every agent call. A direct, non-gateway caller carries
    neither, so this falls back to the authenticated identity `_require_identity`
    already proved is real, rather than silently going blank — exactly the
    "no record of who cleared it" gap this audit trail exists to close."""
    agent = request.headers.get("x-aw-caller-agent") or ""
    run_id = request.headers.get("x-aw-caller-run-id") or ""
    if agent or run_id:
        return f"{agent or 'agent:?'} run={run_id or '?'}"
    identity = request.scope.get("aw_identity")
    return f"identity:{identity}" if identity else "unknown"


def _require_identity(request: Request) -> None:
    """401 unless the framework's own ``IdentityGuard`` authenticated this
    caller (a signed-in workspace session, or the workspace API key) —
    re-implemented per-route because ``auth_required: false`` (see module
    docstring) stops the guard from enforcing it for the whole mount. Mirrors
    ``aw-app-xiaomi``'s own ``_require_credential``; no loopback carve-out
    here, since this app has no local-agent caller that needs one.
    """
    if request.scope.get("aw_identity") is not None:
        return
    raise HTTPException(status_code=401, detail="unauthorized")


def build_routes(ctx) -> FastAPI:
    app = FastAPI(title="knowledgeable")

    @app.get("/status", dependencies=[Depends(_require_identity)])
    async def status() -> dict:
        secret = ctx.secrets.read(SECRET_KEY) or ""
        base = client.base_url()
        git_remote_url = ctx.config.get("git_remote_url") or ""
        git_last_state = await ctx.state.kv.get(GIT_STATE_KV_NAME)
        return {
            # "logged_in" is what windows/main.json's auth_status widget binds to.
            "logged_in": bool(secret),
            "configured": bool(secret),
            "base_url": base,
            "tools": [t["name"] for t in client.TOOLS_SCHEMA],
            "mcp_server": mcp_config.SERVER_NAME,
            # feature:knowledgeable-connector-forwards-to-codegraph —
            # operator visibility into each forward_upstreams entry's
            # availability (design §6), greppable by doctor tooling, not
            # only inside tool responses.
            "forward_upstreams": await forwarding.status_block(),
            # §16.3 point 3 — the git backend's own failure surface. `enabled`
            # mirrors git_backend.sync_tick's own "no git_remote_url, nothing
            # to do" contract; `state`/`detail` are whatever the last tick
            # recorded (None before the first tick ever runs).
            "git": {
                "enabled": bool(git_remote_url),
                "configured": bool(git_remote_url and ctx.secrets.read(GIT_TOKEN_KEY)),
                "state": (git_last_state or {}).get("state"),
                "detail": (git_last_state or {}).get("detail"),
            },
        }

    @app.post("/settings", dependencies=[Depends(_require_identity)])
    async def save_settings(data: dict = Body(...)) -> dict:
        secret = (data.get(SECRET_KEY) or "").strip()
        if not secret:
            return JSONResponse({"ok": False, "error": f"{SECRET_KEY} is required"}, status_code=400)
        ctx.secrets.write(SECRET_KEY, secret)
        # git_token is optional on this same form (§16.3 — "the exact
        # service_secret pattern"): present and non-empty writes it,
        # present and empty clears it, absent entirely leaves it untouched.
        if GIT_TOKEN_KEY in data:
            git_token = (data.get(GIT_TOKEN_KEY) or "").strip()
            if git_token:
                ctx.secrets.write(GIT_TOKEN_KEY, git_token)
            else:
                ctx.secrets.delete(GIT_TOKEN_KEY)
        # Same present/empty/absent contract as `git_token` above, for the
        # AP-MT Postgres DSN the exec sync reads from. Memory
        # knowledgeable-connector-settings-route-clobbers-service-secret is
        # the scar this shape avoids: a key the caller did not send must be
        # left untouched, never cleared.
        if APMT_DSN_KEY in data:
            dsn = (data.get(APMT_DSN_KEY) or "").strip()
            if dsn:
                ctx.secrets.write(APMT_DSN_KEY, dsn)
            else:
                ctx.secrets.delete(APMT_DSN_KEY)
        # No restart, no gateway reload: client.py resolves the secret through
        # a per-call callable, so the very next tool call already uses it.
        return {"ok": True, "logged_in": True}

    @app.post("/logout", dependencies=[Depends(_require_identity)])
    async def clear_secret() -> dict:
        ctx.secrets.delete(SECRET_KEY)
        return {"ok": True, "logged_in": False, "configured": False}

    @app.post("/test", dependencies=[Depends(_require_identity)])
    async def test_connection() -> dict:
        """Prove reachability + auth for real, from the actual owning
        process — not from a sandbox or an agent container, per D2 risk 6
        ("verify from inside aw-app-mcp-gateway", the same rule applied here
        to this app's own runtime)."""
        result, is_error = await client.list_documents({})
        return {"ok": not is_error, "base_url": client.base_url(), "result": result[:2000]}

    @app.get("/mcp.json", dependencies=[Depends(_require_identity)])
    async def mcp_json() -> dict:
        # build_self_entry() embeds AW_WORKSPACE_API_KEY in this body when
        # set (mcp/self_register.py) — exactly the credential this whole
        # file's reasoning says an external, credential-less caller must
        # never get. This MUST stay gated even with auth_required: false.
        return {"mcpServers": mcp_config.build_mcp_servers()}

    # ------------------------------------------------------------------
    # §3.1 of the pull-through design — production fetches its ap-mt key
    # from HERE (at boot, on an empty slot, on a 401) instead of waiting for
    # the push loops above. Same `X-Internal-Secret` channel those loops'
    # pushes already travel over, just inbound instead of outbound: this
    # route must enforce that auth itself rather than trust whatever gate
    # the workspace server wraps app routes in — a workspace API key must
    # not be enough to extract an LLM credential (the same "never a human"
    # reasoning `require_service` already applies on the push side).
    # ------------------------------------------------------------------

    @app.get("/keys/{slot}")
    async def pull_key(slot: str, request: Request):
        provided = request.headers.get("x-internal-secret", "")
        expected = ctx.secrets.read(SECRET_KEY) or ""
        if not provided or not expected or not hmac.compare_digest(expected, provided):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        entry = _PULL_SLOTS.get(slot)
        if entry is None:
            return JSONResponse({"detail": f"unknown key slot {slot!r}"}, status_code=404)
        secret_name, reason = entry
        value = await key_vault.read_secret(secret_name, reason=reason)
        if not value:
            return JSONResponse(
                {"detail": f"vault read for {secret_name!r} unavailable", "reason": "vault_unavailable"},
                status_code=503,
            )
        return {"api_key": value}

    # ------------------------------------------------------------------
    # §13 — the KB bulk-ingest driver. Both doors (the `knowledgeable-ingest`
    # CLI command and the contributed scheduled task, which just runs that
    # same CLI command) reach the engine through these routes, never by
    # importing `bulk_ingest` into a separate process — the engine has to
    # run here, in-process, to reuse this app's already-configured
    # `mcp/client.py` (base_url + X-Internal-Secret).
    # ------------------------------------------------------------------

    @app.post("/bulk-ingest/scan", dependencies=[Depends(_require_identity)])
    async def bulk_ingest_scan() -> dict:
        return await run_in_threadpool(bulk_ingest.scan, ctx.db)

    @app.post("/bulk-ingest/run", dependencies=[Depends(_require_identity)])
    async def bulk_ingest_run(data: dict = Body(default={})) -> dict:
        max_uploads = data.get("max_uploads")
        git_config = {
            "remote_url": ctx.config.get("git_remote_url") or "",
            "branch": ctx.config.get("git_branch") or "main",
            "token": ctx.secrets.read(GIT_TOKEN_KEY) or "",
            "workspace_slug": workspace_env("AW_WORKSPACE"),
        }
        kwargs = {
            "git_config": git_config,
            # Card `3fa5bf3b` — this driver uploads by path now, so the slug
            # is a quarter of the document identity here exactly as it
            # already is for `fs_sync_run` below. Same `workspace_env`
            # source (§6); `run_tick` refuses rather than defaulting it.
            "workspace_slug": workspace_env("AW_WORKSPACE"),
        }
        if max_uploads is not None:
            kwargs["max_uploads"] = int(max_uploads)
        result = await bulk_ingest.run_tick(ctx.db, ctx.state.lease, **kwargs)
        if "git_state" in result:
            await ctx.state.kv.put(
                GIT_STATE_KV_NAME,
                {"state": result["git_state"], "detail": result.get("git_detail")},
                ttl=GIT_STATE_KV_TTL,
            )
        return result

    @app.get("/bulk-ingest/status", dependencies=[Depends(_require_identity)])
    async def bulk_ingest_status() -> dict:
        return await run_in_threadpool(bulk_ingest.status, ctx.db)

    @app.get("/bulk-ingest/report", dependencies=[Depends(_require_identity)])
    async def bulk_ingest_report() -> dict:
        return await run_in_threadpool(bulk_ingest.report, ctx.db)

    # ------------------------------------------------------------------
    # docs/design/aw-knowledgeable-fs-sync.md §3/§7 (card B) — the fs-sync
    # upsync engine. Same one-engine-reused-through-routes shape as the
    # bulk-ingest doors above: `knowledgeable-sync` CLI is door 1, a future
    # scheduled task would be door 2 (not wired up by this card — enabling
    # it live is an operator decision after the restart this Tier-1 code
    # change needs, same as card A's deploy-then-verify split).
    # ------------------------------------------------------------------

    @app.post("/fs-sync/run", dependencies=[Depends(_require_identity)])
    async def fs_sync_run(data: dict = Body(default={})) -> dict:
        max_uploads = data.get("max_uploads")
        kwargs = {
            "workspace_slug": workspace_env("AW_WORKSPACE"),
            # §20.5 — the single config seam: absent (None) behaves exactly
            # as before this amendment (30s debounce everywhere).
            "quiet_rules": ctx.config.get("sync_quiet_rules"),
            # §21.2 — same seam for the auto-approve pass, plus the ONE
            # place `valve_latched` is ever proven rather than defaulted:
            # this call always resolves to a real bool, never `None`, so
            # `run_tick`'s own fail-closed default only protects a caller
            # that skips this kwarg entirely (see that function's docstring).
            "delete_policy": ctx.config.get("delete_policy"),
            "valve_latched": bool(await ctx.state.kv.get(FS_SYNC_VALVE_KV_NAME)),
        }
        if max_uploads is not None:
            kwargs["max_uploads"] = int(max_uploads)
        result = await fs_sync.run_tick(ctx.db, ctx.state.lease, **kwargs)
        if result.get("valve_tripped"):
            await ctx.state.kv.put(
                FS_SYNC_VALVE_KV_NAME,
                {"reason": result.get("reason")},
                ttl=FS_SYNC_VALVE_KV_TTL,
            )
        return result

    @app.post("/fs-sync/valve/clear", dependencies=[Depends(_require_identity)])
    async def fs_sync_valve_clear(request: Request) -> dict:
        """§21.2 — the only way the §7 valve latch ever clears now that its
        TTL is gone (see `FS_SYNC_VALVE_KV_NAME`'s own comment above): an
        explicit, audited, out-of-tick decision. Writes a `valve_clear` row
        to the same card-D audit table `pending_deletions.py` already owns
        — one audit trail for every action on the deletion gate, not a
        second one for this route."""
        actor = _caller_actor(request)
        was_latched = bool(await ctx.state.kv.get(FS_SYNC_VALVE_KV_NAME))
        await ctx.state.kv.delete(FS_SYNC_VALVE_KV_NAME)
        await pending_deletions.record_valve_clear(ctx.db, actor, bucket=bulk_ingest.BUCKET)
        return {"cleared": True, "was_latched": was_latched, "actor": actor}

    @app.get("/fs-sync/status", dependencies=[Depends(_require_identity)])
    async def fs_sync_status() -> dict:
        payload = await run_in_threadpool(
            fs_sync.status, ctx.db, ctx.config.get("sync_quiet_rules"), ctx.config.get("delete_policy"),
        )
        payload["valve_tripped"] = await ctx.state.kv.get(FS_SYNC_VALVE_KV_NAME)
        return payload

    @app.get("/fs-sync/report", dependencies=[Depends(_require_identity)])
    async def fs_sync_report() -> dict:
        return await run_in_threadpool(
            fs_sync.report, ctx.db, ctx.config.get("sync_quiet_rules"), ctx.config.get("delete_policy"),
        )

    @app.post("/fs-sync/reconcile-orphans", dependencies=[Depends(_require_identity)])
    async def fs_sync_reconcile_orphans(request: Request, data: dict = Body(default={})) -> dict:
        """Card `3fa5bf3b` — mark the journal-orphaned duplicate documents
        the two-identity gap stranded. `apply` defaults to FALSE: an
        un-parameterized call is a dry-run measurement, and marking is the
        explicit opt-in. `_caller_actor` is the same audit identity the MCP
        approve/decline tools already record, so a mark and its later
        approval appear in one log with the same shape."""
        kwargs: dict = {"apply": bool(data.get("apply")), "actor": _caller_actor(request)}
        if data.get("limit") is not None:
            kwargs["limit"] = int(data["limit"])
        if data.get("bucket"):
            kwargs["bucket"] = str(data["bucket"])
        return await orphan_reconcile.reconcile(ctx.db, **kwargs)

    @app.post("/fs-sync/unmark-not-ready", dependencies=[Depends(_require_identity)])
    async def fs_sync_unmark_not_ready(request: Request, data: dict = Body(default={})) -> dict:
        """Card `3fa5bf3b-9510-8112-8f3f-d9fdaadd8263`, deliverable 2: clear
        `pending_delete` (never decline — see `orphan_reconcile.
        unmark_not_ready`'s own docstring for why) on every pending
        document whose live twin is not yet `ready`. No `apply` flag: every
        candidate this finds is, by construction, already wrongly marked."""
        kwargs: dict = {"actor": _caller_actor(request)}
        if data.get("bucket"):
            kwargs["bucket"] = str(data["bucket"])
        return await orphan_reconcile.unmark_not_ready(ctx.db, **kwargs)

    # ------------------------------------------------------------------
    # docs/design/execution-history-in-production-neo4j.md §4/§8 — the
    # exec-history sync engine. Same one-engine-reached-through-routes shape
    # as bulk-ingest and fs-sync above: the engine has to run here,
    # in-process, to reuse this app's configured `mcp/client.py` (base_url +
    # X-Internal-Secret) and the workspace `SecretStore` the scrubber's
    # exact-match pass reads.
    #
    # `exec_sync_enabled` ships FALSE (§9 step 2). Turning it on is an
    # operator decision after the schema-only step 1 is deployed.
    # ------------------------------------------------------------------

    def _exec_sync_kwargs() -> dict:
        return {
            "dsn": ctx.secrets.read(APMT_DSN_KEY) or "",
            "source_tenant": (ctx.config.get("exec_sync_source_tenant") or "").strip(),
            "enabled": bool(ctx.config.get("exec_sync_enabled")),
        }

    @app.post("/exec-sync/run", dependencies=[Depends(_require_identity)])
    async def exec_sync_run(data: dict = Body(default={})) -> dict:
        kwargs = _exec_sync_kwargs()
        if data.get("max_runs") is not None:
            kwargs["max_runs"] = int(data["max_runs"])
        return await exec_sync.run_tick(ctx.state.lease, **kwargs)

    async def _exec_sync_status_payload() -> dict:
        """The §8 doctor surface: the sentinel, its staleness verdict, the
        live graph counts, the §6 point 1 / §12 risk 8 scrub-coverage dict —
        and this side's own config, so "the sync is not running" and "the
        sync is running against nothing" are distinguishable without reading
        two places.

        Shared by `GET /exec-sync/status` (identity-gated, what the CLI
        reads) and `GET /exec-sync/doctor` (the unauthenticated
        `contributes.doctor` check below) so the two can never disagree on
        what `degraded` means."""
        cfg = _exec_sync_kwargs()
        payload: dict = {
            "enabled": cfg["enabled"],
            "source_tenant": cfg["source_tenant"] or None,
            "dsn_configured": bool(cfg["dsn"]),
            "secret_configured": bool(ctx.secrets.read(SECRET_KEY)),
        }
        remote, err = await client.get_exec_status()
        payload["graph"] = remote
        payload["error"] = err
        scrub = await exec_sync.scrub_status()
        payload["scrub"] = scrub
        # §13.6's open-set alarm, read off the sentinel this tick stamped.
        # DISTINCT kinds, so a burst of one new kind reads as one decision
        # owed (§13.12 point 8) — the `vault_names_uncovered` mechanism, for
        # event kinds instead of vault secrets.
        payload["event_kinds_unreviewed"] = exec_sync.event_kinds_unreviewed(remote)
        # The same mechanism for tool identities (exec-index-per-tool-policy.md
        # §3.5/§11): observed names that parsed as MCP-routed, resolved to no
        # installed app, and therefore had their ARGUMENTS DROPPED. Distinct
        # raw spellings, so a fourth gateway prefix is diagnosed from this
        # line rather than from a future incident — which is the whole
        # mitigation for fail-closed resolution being silent otherwise.
        payload["policy_unresolved"] = exec_sync.policy_unresolved(remote)
        # §13.7's disk preflight, reported here as well as enforced on
        # `reset-watermark`: an operator about to run a backfill should be
        # able to see the verdict BEFORE the route refuses them, and the
        # trajectory (36 GB free and falling, on a host at 92 %) is worth
        # being visible on the status surface rather than only at the moment
        # it blocks something. Deliberately NOT folded into `degraded`
        # below: low disk is a platform condition this feature did not
        # create (§13.10), and a red doctor check for it would blame the
        # sync for someone else's trajectory while telling nobody anything
        # the host-disk card does not already say.
        payload["disk"] = exec_sync.disk_preflight(*exec_sync.free_disk_bytes())
        # Degraded is computed on THIS side too, not only read off the
        # sentinel: a sentinel we cannot reach is itself degraded, and a
        # status surface that reports `None` for that reads as healthy.
        # The scrub-coverage signal (vault_names_uncovered non-empty,
        # vault_coverage_unknown, exact_match_unavailable — §6 point 1
        # correction, §12 risk 8) is folded into the SAME `enabled` gate as
        # the staleness checks: a coverage gap only matters while the sync it
        # protects is actually running, same as "a disabled sync is not
        # degraded" already meant here.
        staleness = ((remote or {}).get("staleness") or {})
        payload["degraded"] = bool(
            cfg["enabled"] and (
                err or not remote or staleness.get("degraded")
                or exec_sync.scrub_degraded(scrub)
                or payload["event_kinds_unreviewed"]
                or payload["policy_unresolved"]
            )
        )
        return payload

    @app.get("/exec-sync/status", dependencies=[Depends(_require_identity)])
    async def exec_sync_status() -> dict:
        return await _exec_sync_status_payload()

    @app.get("/exec-sync/doctor")
    async def exec_sync_doctor() -> dict:
        """``contributes.doctor``'s check for exec-sync (§8, §11 trigger #3).

        Deliberately carries no `_require_identity`: `_app_doctor_checks`
        dials this in-process over `httpx.ASGITransport` with no headers at
        all, bypassing the outer `IdentityGuard` the same way the module
        docstring already describes for `auth_required: false` — a gated
        route here would 401 on every poll regardless of real health, same
        trap `_require_identity`'s own docstring warns about. Precedent:
        aw-app-architecture's `/provision/check` and aw-app-call-agent's
        `/credentials/check` are unauthenticated for the identical reason.
        Nothing in `detail` is a secret value — `scrub` carries vault secret
        NAMES and counts only, never a value (§12 risk 8's list-never-read
        contract)."""
        payload = await _exec_sync_status_payload()
        return {"ok": not payload["degraded"], "detail": payload}

    @app.post("/exec-sync/reset-watermark", dependencies=[Depends(_require_identity)])
    async def exec_sync_reset_watermark() -> dict:
        """§13.8's per-slice backfill trigger: clear the freshness watermark
        so the next bounded ticks re-read every run from the beginning.

        **Why this is wired rather than a one-off.** Each slice of the §13
        timeline adds a projection to runs that were already synced, and the
        only way to reach them is from a null watermark — the design calls
        for exactly one watermark-reset backfill per slice (§13.8), so slices
        2 and 3 need this button too. Leaving it unwired means the next coder
        hand-rolls an HTTP call to a service-authed route with the internal
        secret, which is the shape this app exists to avoid.

        Cheap to get wrong in only one direction, and it is the harmless one:
        re-reading a run is free (every statement behind the ingest route is
        a MERGE, so a re-read is a no-op on identity and a re-SET on the
        mutable fields), while skipping one is permanent. What it DOES cost
        is ticks — ~14 bounded passes over ~28k runs, sharing the single
        production worker with live search — which is why it is a deliberate
        operator action and the scheduled task never calls it.

        Nothing is deleted. `counts` is cleared because the previous tick's
        numbers would otherwise describe a pass that no longer corresponds to
        the watermark, and `last_error` records WHY the sentinel looks like a
        first-ever pass, so an operator reading a null watermark can tell a
        deliberate reset from a lost sentinel.
        """
        cfg = _exec_sync_kwargs()
        if not cfg["source_tenant"]:
            return {
                "ok": False,
                "error": (
                    "exec_sync_source_tenant is not set — the sentinel is per "
                    "source tenant and this route will not guess it"
                ),
            }
        # §13.7's disk preflight. This route IS "the backfill starts" — it is
        # the only thing that makes the next ticks re-read ~28k runs and
        # re-project every result — so this is where the floor belongs,
        # rather than on the 10-minute tick (see `MIN_FREE_DISK_GB`: gating
        # the tick would trade bounded growth for silent staleness). It
        # refuses when the measurement FAILED too, not only when it came
        # back low.
        disk = exec_sync.disk_preflight(*exec_sync.free_disk_bytes())
        if not disk["ok"]:
            return {"ok": False, "error": disk["reason"], "disk": disk}
        ok, err = await client.put_exec_sync_state(
            source_tenant=cfg["source_tenant"],
            watermark=None,
            counts={},
            last_error="watermark reset for a §13.8 backfill",
        )
        return {"ok": ok, "error": err, "watermark": None, "disk": disk}

    @app.post("/exec-sync/reconcile", dependencies=[Depends(_require_identity)])
    async def exec_sync_reconcile() -> dict:
        cfg = _exec_sync_kwargs()
        return await exec_sync.reconcile(
            dsn=cfg["dsn"], source_tenant=cfg["source_tenant"]
        )

    @app.post("/exec-sync/retention", dependencies=[Depends(_require_identity)])
    async def exec_sync_retention(data: dict = Body(default={})) -> dict:
        days = data.get("days")
        result, err = await client.post_exec_retention(None if days is None else int(days))
        return {"ok": err is None, "error": err, "result": result}

    @app.post("/exec-sync/purge-secret-args", dependencies=[Depends(_require_identity)])
    async def exec_sync_purge_secret_args() -> dict:
        """§13.2's purge trigger — nulls `args_text` on every already-landed
        secret-tool call. Remediation for the rows that landed while the
        drop was an exact match on the bare name and therefore never fired
        for a gateway-prefixed call. Idempotent."""
        result, err = await client.post_exec_purge_secret_args()
        return {"ok": err is None, "error": err, "result": result}

    # ------------------------------------------------------------------
    # MCP — Streamable HTTP, auto-discovered by aw-mcp-gateway's app-scan.
    # ------------------------------------------------------------------

    @app.post("/mcp", dependencies=[Depends(_require_identity)])
    async def mcp_post(request: Request, data: dict | list = Body(...)):
        from .mcp.http_handler import handle_request

        actor = _caller_actor(request)
        messages = data if isinstance(data, list) else [data]
        responses = []
        for m in messages:
            r = await handle_request(m, db=ctx.db, actor=actor)
            if r is not None:
                responses.append(r)
        if not responses:
            return Response(status_code=202)
        return JSONResponse(responses if isinstance(data, list) else responses[0])

    @app.get("/mcp", dependencies=[Depends(_require_identity)])
    async def mcp_get():
        return Response(status_code=405)

    return app
