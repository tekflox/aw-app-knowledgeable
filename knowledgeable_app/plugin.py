"""Entrypoint referenced by ``aw-app.json``'s ``runtime.entrypoint``
("knowledgeable_app.plugin:KnowledgeableAppPlugin").

This app is a pure HTTP client of aw-knowledgeable's M6 API — no subprocess,
no venv, no port of its own to manage. The service secret is resolved
through a callable rather than read once, so saving one in Settings takes
effect on the next tool call with no restart and no gateway reload (D2 risk
5). ``base_url`` is read from ``ctx.config`` the same way, so pointing the
connector at a different aw-knowledgeable deployment is also restart-free.
"""
from __future__ import annotations

import logging
import os

from . import mcp_config, routes as routes_mod
from .mcp import client

log = logging.getLogger("aw_apps.knowledgeable")


class KnowledgeableAppPlugin:
    async def activate(self, ctx) -> None:
        self.ctx = ctx

        client.configure(
            base_url_resolver=lambda: (ctx.config or {}).get("base_url") or client.DEFAULT_BASE_URL,
            secret_resolver=lambda: ctx.secrets.read(routes_mod.SECRET_KEY) or "",
        )

        ctx.routes.register(routes_mod.build_routes(ctx))

        port = int(os.environ.get("AW_PORT") or 9030)
        # Rebuilt every boot rather than persisted: the entry embeds this
        # process's hostname and API key, both of which change when the
        # workspace container is recreated.
        doc = mcp_config.write_mcp_json(ctx.package_dir, port)

        log.info(
            "aw-app-knowledgeable activated: mcp server=%s, tools=%s, base_url=%s, secret=%s",
            sorted(doc["mcpServers"]),
            len(client.TOOLS_SCHEMA),
            client.base_url(),
            "saved" if client.configured() else "NOT SET (tools will explain how)",
        )

    async def deactivate(self) -> None:
        log.info("aw-app-knowledgeable deactivated")
