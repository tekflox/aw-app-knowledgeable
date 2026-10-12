"""The one vault-read implementation behind every ap-mt key this connector
hands to production — the three push loops (``playground_key_push.py`` and
its twins ``ingest_key_push.py``, ``label_key_push.py``) and, since the
pull-through design (``docs/design/knowledgeable-apmt-key-pull-through.md``),
the ``GET /keys/{slot}`` pull route in ``routes.py``.

Factored out rather than left as three (now four) copies of the same
``httpx`` call: the read itself — this workspace's own
``/api/apps/secrets/secrets/{name}/read``, with the one allowlisted caller
identity both the push loops and the pull route present — has nothing
caller-specific about it. What differs per caller is only which secret name
and which human-readable ``reason`` string lands on the resulting approval
row.
"""
from __future__ import annotations

import logging

import httpx

from .workspace_env import workspace_env

log = logging.getLogger("aw_apps.knowledgeable.key_vault")

#: Sent as the `X-Aw-Caller-Agent` header aw-app-secrets' REST route reads.
#: aw-app-secrets' own `caller.py::agent_identity()` always prefixes with
#: "agent:" (it does not special-case an "app:"-shaped caller — a plain app
#: is, to that allowlist, just another stable non-session identity), so the
#: string a per-secret `auto_approve_for` must name is `agent:aw-app-
#: knowledgeable`, not this literal — verified against that function's body
#: rather than assumed. Shared by every reader below: push loop or pull
#: route, this connector process is always the same caller.
CALLER_AGENT_HEADER_VALUE = "aw-app-knowledgeable"

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


async def read_secret(name: str, *, reason: str) -> str | None:
    """The ApiKey named `name`, read through aw-app-secrets' own REST
    surface (this workspace's `/api/apps/secrets/secrets/{name}/read`) — not
    a second, hand-rolled implementation of the approval protocol.
    `auto_approve_for` on each of these secrets names this connector's
    caller identity, so the read returns synchronously with a value;
    anything else (not yet approved, denied, the vault unreachable) is
    logged and returned as `None`, never raised — every caller of this
    function (a push loop's tick, the pull route's request) has its own
    well-defined "nothing to hand over this time" behaviour.
    """
    base = _secrets_api_base()
    if base is None:
        log.warning(
            "key vault read %r: AW_WORKSPACE_API_URL/AW_WORKSPACE_API_KEY "
            "not published yet",
            name,
        )
        return None
    api_url, api_key = base
    url = f"{api_url}/api/apps/secrets/secrets/{name}/read"
    headers = {"X-Api-Key": api_key, "X-Aw-Caller-Agent": CALLER_AGENT_HEADER_VALUE}
    body = {"reason": reason}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S) as c:
            resp = await c.post(url, json=body, headers=headers)
    except httpx.HTTPError as exc:
        log.warning("key vault read %r: could not reach the workspace secret store: %s", name, exc)
        return None
    if resp.status_code >= 400:
        log.warning("key vault read %r: secret read failed (%s): %s", name, resp.status_code, resp.text[:300])
        return None
    data = resp.json()
    if data.get("status") != "approved" or not data.get("value"):
        log.warning(
            "key vault read %r: not auto-approved for caller %r (status=%s) — "
            "that caller must be on the secret's auto_approve_for allowlist, "
            "or every read opens a Telegram prompt instead of returning",
            name, CALLER_AGENT_HEADER_VALUE, data.get("status"),
        )
        return None
    return data["value"]
