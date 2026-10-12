"""Re-asserts the Playground's ap-mt ApiKey into production aw-knowledgeable
by PUSH, instead of that service pulling it from the vault itself.

Frederico's decision (kanban card 3ea5bf3b, 2026-09-30) replaces the earlier
design (production holding its own ``AW_WORKSPACE_HOST_TOKEN``, ap-mt-key-
shaped variants of it) outright rather than choosing between its options:
production never gets a credential onto the vault at all. This connector,
which already reaches the workspace vault, reads the one narrow secret
(``knowledgeable-playground-apmt-key``, an ApiKey scoped to
``agent_slugs=[knowledgeable-playground]`` — not a credential that opens
anything else) and pushes it to aw-knowledgeable's
``POST /api/playground/key`` over the ``X-Internal-Secret`` channel
``mcp/client.py`` already authenticates every other call with. Production
ends up holding one narrow, revocable value instead of a key to the whole
vault.

**Why re-assert on a loop, not push once at install.** aw-knowledgeable's own
deploy recreates its container, which wipes the in-memory key
(``api/playground.py``'s ``_resolved_key`` — never disk, by that module's own
design) — and `resolve_api_key()` there never raises, so the service stays up
with synthesis silently paused (``paused_reason: "secret_unavailable"``
answered as a declared 503, not a crash anyone would notice booting). A
one-shot push at install time cannot recover from that; a background loop
that re-pushes on a fixed interval turns "silently paused until someone
notices" into "self-heals within one interval", with no signal needed from
aw-knowledgeable's side — this process has no way to know when that
container was last recreated, so it does not try to, it just keeps asserting.

**The secret never touches disk here.** Read from the workspace vault into a
local variable, POSTed to aw-knowledgeable, discarded — this module never
logs the value, never writes it to config, never returns it from a function
a caller could accidentally print.
"""
from __future__ import annotations

import asyncio
import logging
import os

from . import key_vault
from .mcp import client

log = logging.getLogger("aw_apps.knowledgeable.playground_key_push")

SECRET_NAME = "knowledgeable-playground-apmt-key"

#: How often to re-push, regardless of whether the last push looked like it
#: worked — see module docstring: this process cannot know when
#: aw-knowledgeable's container was last recreated, so it does not try to
#: notice, it just keeps asserting. Overridable for tests / a future tighter
#: SLO without a code change.
PUSH_INTERVAL_S = int(os.environ.get("KNOWLEDGEABLE_PLAYGROUND_KEY_PUSH_INTERVAL_S") or 300)


async def push_once() -> bool:
    """Read the key from the vault and push it to production. Never raises —
    every failure is logged and this waits for the next tick, the same
    never-fail-loud shape aw-knowledgeable's own `resolve_api_key()` uses."""
    value = await key_vault.read_secret(
        SECRET_NAME,
        reason="re-assert aw-knowledgeable Playground's ap-mt key in production",
    )
    if not value:
        return False
    ok, err = await client.push_playground_key(value)
    if not ok:
        log.warning("playground key push: aw-knowledgeable refused the push: %s", err)
        return False
    log.info(
        "playground key push: re-asserted a %d-char key into %s",
        len(value), client.base_url(),
    )
    return True


async def run_forever() -> None:
    """Push once immediately (the boot re-assert), then on a fixed interval
    forever — see module docstring for why this is a loop and not a
    one-shot. Started as a background task from `plugin.activate()`,
    cancelled from `plugin.deactivate()`."""
    while True:
        await push_once()
        await asyncio.sleep(PUSH_INTERVAL_S)
