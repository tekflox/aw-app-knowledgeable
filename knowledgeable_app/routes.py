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

from . import bulk_ingest, fs_sync, git_backend, ingest_key_push, key_vault, label_key_push, mcp_config, playground_key_push
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
#: `ctx.state.kv` name the last tick's git_state/git_detail is cached under,
#: so `/status` can show it between ticks (§16.3 point 3) without re-running
#: git. 1 day: generous enough to span the 15-minute schedule across a
#: quiet weekend without going stale forever.
GIT_STATE_KV_NAME = "git_last_state"
GIT_STATE_KV_TTL = 60 * 60 * 24

#: docs/design/aw-knowledgeable-fs-sync.md §7 — the mass-delete valve's
#: own "journal flag", cached the same way `git_last_state` is: a tripped
#: valve is a rare, deliberately sticky signal an operator must notice and
#: clear (verify the tree, then either resolve it or approve in the UI),
#: so it gets a long TTL rather than the git state's 1-day one.
FS_SYNC_VALVE_KV_NAME = "fs_sync_valve_tripped"
FS_SYNC_VALVE_KV_TTL = 60 * 60 * 24 * 30


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
        kwargs = {"git_config": git_config}
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

    @app.get("/fs-sync/status", dependencies=[Depends(_require_identity)])
    async def fs_sync_status() -> dict:
        payload = await run_in_threadpool(fs_sync.status, ctx.db, ctx.config.get("sync_quiet_rules"))
        payload["valve_tripped"] = await ctx.state.kv.get(FS_SYNC_VALVE_KV_NAME)
        return payload

    @app.get("/fs-sync/report", dependencies=[Depends(_require_identity)])
    async def fs_sync_report() -> dict:
        return await run_in_threadpool(fs_sync.report, ctx.db, ctx.config.get("sync_quiet_rules"))

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
