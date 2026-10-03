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

import asyncio
import logging
import os
from contextlib import suppress

from . import ingest_key_push, label_key_push, mcp_config, playground_key_push, routes as routes_mod
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

        # Card 3ea5bf3b: re-assert the Playground's ap-mt key into production
        # aw-knowledgeable on a loop, starting now (the boot re-assert) —
        # see playground_key_push.py's module docstring for why this has to
        # be a loop rather than a push at install time only.
        self._playground_key_task = asyncio.create_task(playground_key_push.run_forever())

        # Card 3ec5bf3b: same re-assert loop for the extractor's ap-mt key —
        # POST /api/ingest/key shipped in aw-knowledgeable with no caller,
        # so EXTRACTION_ENABLED=true lit nothing. See
        # ingest_key_push.py's module docstring.
        self._ingest_key_task = asyncio.create_task(ingest_key_push.run_forever())

        # 2026-10-03 — the third re-assert loop, for the topic labeller's
        # ap-mt key. Closes the LLM-key veto: topics/label.py was the last
        # caller still going straight at a provider. See
        # label_key_push.py's module docstring.
        self._label_key_task = asyncio.create_task(label_key_push.run_forever())

        log.info(
            "aw-app-knowledgeable activated: mcp server=%s, tools=%s, base_url=%s, secret=%s, "
            "playground-key re-assert every %ss, ingest-key re-assert every %ss, "
            "label-key re-assert every %ss",
            sorted(doc["mcpServers"]),
            len(client.TOOLS_SCHEMA),
            client.base_url(),
            "saved" if client.configured() else "NOT SET (tools will explain how)",
            playground_key_push.PUSH_INTERVAL_S,
            ingest_key_push.PUSH_INTERVAL_S,
            label_key_push.PUSH_INTERVAL_S,
        )

    async def deactivate(self) -> None:
        for attr in ("_playground_key_task", "_ingest_key_task", "_label_key_task"):
            task = getattr(self, attr, None)
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        log.info("aw-app-knowledgeable deactivated")
