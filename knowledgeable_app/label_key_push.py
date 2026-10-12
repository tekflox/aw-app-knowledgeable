"""Re-asserts the topic labeller's ap-mt ApiKey into production
aw-knowledgeable by PUSH, instead of that service pulling it from the vault
itself.

The third of this connector's three pushed credentials (2026-10-03, closing
the LLM-key veto — `topics/label.py:229` was the last caller still going
straight at a provider's own Messages API). Twin of `ingest_key_push.py` and
`playground_key_push.py` (card 3ec5bf3b / 3ea5bf3b): same design, same
reasoning, deliberately not generalised into one shared loop — the labeller,
the extractor and the Playground reach three different ap-mt agents
(``knowledgeable-labeller`` vs ``knowledgeable-extractor-runner`` vs
``knowledgeable-playground``), so any one credential may be revoked, rotated
or scoped differently without touching the others. See
``api/topics.py::push_key``'s own docstring on the aw-knowledgeable side for
why that split is kept all the way through.

This connector, which already reaches the workspace vault, reads the one
narrow secret (``knowledgeable-labeller-apmt-key``, an ApiKey scoped to
``agent_slugs=[knowledgeable-labeller]`` — not a credential that opens
anything else) and pushes it to aw-knowledgeable's `POST /api/topics/key`
over the ``X-Internal-Secret`` channel ``mcp/client.py`` already
authenticates every other call with. Production ends up holding one narrow,
revocable value instead of a key to the whole vault.

**Why re-assert on a loop, not push once at install.** Same reason as the
other two: aw-knowledgeable's own deploy recreates its container, which
wipes the in-memory key (`topics/label.py`'s pushed-key slot — never disk,
by that module's own design). A one-shot push at install time cannot recover
from that; a background loop that re-pushes on a fixed interval turns
"silently paused until someone notices" into "self-heals within one
interval", with no signal needed from aw-knowledgeable's side — this process
has no way to know when that container was last recreated, so it does not
try to, it just keeps asserting.

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

log = logging.getLogger("aw_apps.knowledgeable.label_key_push")

SECRET_NAME = "knowledgeable-labeller-apmt-key"

#: How often to re-push, regardless of whether the last push looked like it
#: worked — see module docstring: this process cannot know when
#: aw-knowledgeable's container was last recreated, so it does not try to
#: notice, it just keeps asserting. Overridable for tests / a future tighter
#: SLO without a code change.
PUSH_INTERVAL_S = int(os.environ.get("KNOWLEDGEABLE_LABEL_KEY_PUSH_INTERVAL_S") or 300)


async def push_once() -> bool:
    """Read the key from the vault and push it to production. Never raises —
    every failure is logged and this waits for the next tick, the same
    never-fail-loud shape aw-knowledgeable's own `resolve_api_key()` uses."""
    value = await key_vault.read_secret(
        SECRET_NAME,
        reason="re-assert aw-knowledgeable topic labeller's ap-mt key in production",
    )
    if not value:
        return False
    ok, err = await client.push_label_key(value)
    if not ok:
        log.warning("label key push: aw-knowledgeable refused the push: %s", err)
        return False
    log.info(
        "label key push: re-asserted a %d-char key into %s",
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
