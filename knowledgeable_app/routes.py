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

from . import bulk_ingest, mcp_config
from .mcp import client

SECRET_KEY = "service_secret"


def build_routes(ctx) -> FastAPI:
    app = FastAPI(title="knowledgeable")

    @app.get("/status")
    async def status() -> dict:
        secret = ctx.secrets.read(SECRET_KEY) or ""
        base = client.base_url()
        return {
            # "logged_in" is what windows/main.json's auth_status widget binds to.
            "logged_in": bool(secret),
            "configured": bool(secret),
            "base_url": base,
            "tools": [t["name"] for t in client.TOOLS_SCHEMA],
            "mcp_server": mcp_config.SERVER_NAME,
        }

    @app.post("/settings")
    async def save_settings(data: dict = Body(...)) -> dict:
        secret = (data.get(SECRET_KEY) or "").strip()
        if not secret:
            return JSONResponse({"ok": False, "error": f"{SECRET_KEY} is required"}, status_code=400)
        ctx.secrets.write(SECRET_KEY, secret)
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
        return await run_in_threadpool(bulk_ingest.scan)

    @app.post("/bulk-ingest/run")
    async def bulk_ingest_run(data: dict = Body(default={})) -> dict:
        max_uploads = data.get("max_uploads")
        if max_uploads is not None:
            return await bulk_ingest.run_tick(max_uploads=int(max_uploads))
        return await bulk_ingest.run_tick()

    @app.get("/bulk-ingest/status")
    async def bulk_ingest_status() -> dict:
        return await run_in_threadpool(bulk_ingest.status)

    @app.get("/bulk-ingest/report")
    async def bulk_ingest_report() -> dict:
        return await run_in_threadpool(bulk_ingest.report)

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
