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

import httpx

from .mcp import client
from .workspace_env import workspace_env

log = logging.getLogger("aw_apps.knowledgeable.label_key_push")

SECRET_NAME = "knowledgeable-labeller-apmt-key"

#: Sent as the `X-Aw-Caller-Agent` header aw-app-secrets' REST route reads —
#: same stable caller identity the other two pushes use, since all three
#: originate from this same connector process. See `playground_key_push.py`'s
#: own comment for the verification this string is grounded on
#: (`caller.py::agent_identity()`).
CALLER_AGENT_HEADER_VALUE = "aw-app-knowledgeable"

#: How often to re-push, regardless of whether the last push looked like it
#: worked — see module docstring: this process cannot know when
#: aw-knowledgeable's container was last recreated, so it does not try to
#: notice, it just keeps asserting. Overridable for tests / a future tighter
#: SLO without a code change.
PUSH_INTERVAL_S = int(os.environ.get("KNOWLEDGEABLE_LABEL_KEY_PUSH_INTERVAL_S") or 300)

TIMEOUT_S = 20.0


def _secrets_api_base() -> tuple[str, str] | None:
    """This workspace's own loopback API root + the key that opens it, or
    None when neither is published yet (a workspace that has not finished
    booting — never raises, matches `resolve_api_key()`'s own contract on
    the aw-knowledgeable side)."""
    api_url = workspace_env("AW_WORKSPACE_API_URL")
    api_key = workspace_env("AW_WORKSPACE_API_KEY")
    if not api_url or not api_key:
        return None
    return api_url.rstrip("/"), api_key


async def _read_from_vault() -> str | None:
    """The ApiKey, read through aw-app-secrets' own REST surface (this
    workspace's `/api/apps/secrets/secrets/{name}/read`) — not a second,
    hand-rolled implementation of the approval protocol. `auto_approve_for`
    on this one secret names this connector's caller identity, so the read
    returns synchronously with a value; anything else (not yet approved,
    denied, the vault unreachable) is logged and treated as "nothing to push
    this tick", never raised."""
    base = _secrets_api_base()
    if base is None:
        log.warning(
            "label key push: AW_WORKSPACE_API_URL/AW_WORKSPACE_API_KEY "
            "not published yet — skipping this tick"
        )
        return None
    api_url, api_key = base
    url = f"{api_url}/api/apps/secrets/secrets/{SECRET_NAME}/read"
    headers = {"X-Api-Key": api_key, "X-Aw-Caller-Agent": CALLER_AGENT_HEADER_VALUE}
    body = {
        "reason": "re-assert aw-knowledgeable topic labeller's ap-mt key in production",
    }
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S) as c:
            resp = await c.post(url, json=body, headers=headers)
    except httpx.HTTPError as exc:
        log.warning("label key push: could not reach the workspace secret store: %s", exc)
        return None
    if resp.status_code >= 400:
        log.warning(
            "label key push: secret read failed (%s): %s",
            resp.status_code, resp.text[:300],
        )
        return None
    data = resp.json()
    if data.get("status") != "approved" or not data.get("value"):
        log.warning(
            "label key push: %r was not auto-approved for caller %r "
            "(status=%s) — that caller must be on the secret's "
            "auto_approve_for allowlist, or every tick opens a Telegram "
            "prompt instead of pushing anything",
            SECRET_NAME, CALLER_AGENT_HEADER_VALUE, data.get("status"),
        )
        return None
    return data["value"]


async def push_once() -> bool:
    """Read the key from the vault and push it to production. Never raises —
    every failure is logged and this waits for the next tick, the same
    never-fail-loud shape aw-knowledgeable's own `resolve_api_key()` uses."""
    value = await _read_from_vault()
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
