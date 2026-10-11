"""This app's backend sub-app, mounted by the runtime at
``/api/apps/knowledgeable`` behind the workspace's IdentityGuard.

The service secret goes to ``ctx.secrets`` via ``POST /settings``, never
through the generic config path — that would land it in plain,
cloud-syncable app config. The ``x-secret`` flag on ``service_secret`` in
the manifest exists only so the settings UI renders a password field. Same
split as ``aw-app-google-maps``'s API key.
"""
from __future__ import annotations

from fastapi import Body, FastAPI
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response

from . import bulk_ingest, git_backend, mcp_config
from .mcp import client
from .workspace_env import workspace_env

SECRET_KEY = "service_secret"
GIT_TOKEN_KEY = git_backend.GIT_TOKEN_SECRET
#: `ctx.state.kv` name the last tick's git_state/git_detail is cached under,
#: so `/status` can show it between ticks (§16.3 point 3) without re-running
#: git. 1 day: generous enough to span the 15-minute schedule across a
#: quiet weekend without going stale forever.
GIT_STATE_KV_NAME = "git_last_state"
GIT_STATE_KV_TTL = 60 * 60 * 24


def build_routes(ctx) -> FastAPI:
    app = FastAPI(title="knowledgeable")

    @app.get("/status")
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

    @app.post("/settings")
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

    @app.post("/logout")
    async def clear_secret() -> dict:
        ctx.secrets.delete(SECRET_KEY)
        return {"ok": True, "logged_in": False, "configured": False}

    @app.post("/test")
    async def test_connection() -> dict:
        """Prove reachability + auth for real, from the actual owning
        process — not from a sandbox or an agent container, per D2 risk 6
        ("verify from inside aw-app-mcp-gateway", the same rule applied here
        to this app's own runtime)."""
        result, is_error = await client.list_documents({})
        return {"ok": not is_error, "base_url": client.base_url(), "result": result[:2000]}

    @app.get("/mcp.json")
    async def mcp_json() -> dict:
        return {"mcpServers": mcp_config.build_mcp_servers()}

    # ------------------------------------------------------------------
    # §13 — the KB bulk-ingest driver. Both doors (the `knowledgeable-ingest`
    # CLI command and the contributed scheduled task, which just runs that
    # same CLI command) reach the engine through these routes, never by
    # importing `bulk_ingest` into a separate process — the engine has to
    # run here, in-process, to reuse this app's already-configured
    # `mcp/client.py` (base_url + X-Internal-Secret).
    # ------------------------------------------------------------------

    @app.post("/bulk-ingest/scan")
    async def bulk_ingest_scan() -> dict:
        return await run_in_threadpool(bulk_ingest.scan, ctx.db)

    @app.post("/bulk-ingest/run")
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

    @app.get("/bulk-ingest/status")
    async def bulk_ingest_status() -> dict:
        return await run_in_threadpool(bulk_ingest.status, ctx.db)

    @app.get("/bulk-ingest/report")
    async def bulk_ingest_report() -> dict:
        return await run_in_threadpool(bulk_ingest.report, ctx.db)

    # ------------------------------------------------------------------
    # MCP — Streamable HTTP, auto-discovered by aw-mcp-gateway's app-scan.
    # ------------------------------------------------------------------

    @app.post("/mcp")
    async def mcp_post(data: dict | list = Body(...)):
        from .mcp.http_handler import handle_request

        messages = data if isinstance(data, list) else [data]
        responses = []
        for m in messages:
            r = await handle_request(m)
            if r is not None:
                responses.append(r)
        if not responses:
            return Response(status_code=202)
        return JSONResponse(responses if isinstance(data, list) else responses[0])

    @app.get("/mcp")
    async def mcp_get():
        return Response(status_code=405)

    return app
