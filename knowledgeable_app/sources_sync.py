"""Pushes this workspace's ``forward_upstreams`` inventory up to
aw-knowledgeable's registry (docs/design/knowledgeable-knowledge-sources-
registry.md §4b) — the WorkspaceSkillSync twin, pointed at aw-knowledgeable
instead of agents-platform-multitenant (the Connector already authenticates
to the former; it holds no token for the latter, §6.3 of the design).

Two watchdog-style loops, started from ``plugin.activate()``:

* delta (~180s, first tick immediate): no-op if the local source-id set is
  unchanged since the last ack; a full sync if there is no ack yet (first
  run, or after a 409/failure cleared it).
* reconcile (~360s, unconditional): always a full sync, so the registry
  can't silently drift even if every delta happened to look like a no-op.

The acked state (just the hash — this side always re-reads the live
registry+availability on every tick, so there is nothing else worth
persisting) lives in an atomic JSON file under this app's own data dir, the
same resolution ``bulk_ingest._workspace_home()`` uses. ``_clear_ack()`` on a
409 or any push failure forces the next tick to full-sync rather than
stacking another delta on a base the server may have already rejected.

**The service secret is re-read every tick**, via ``mcp.client``'s own
fresh-per-call resolver — never captured here, the "23 consecutive 401s
forever" lesson (memory) this estate has already paid for once.

**Tolerates 404** (`PUT /api/sources/sync` not deployed yet) exactly like
the three key-push loops already do for their own endpoints: logged as an
honest, visible "not deployed yet" status rather than spammed as an error,
and the ack stays untouched so the next tick tries again.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Callable

import httpx

from . import forwarding
from .mcp import client
from .workspace_env import workspace_env

log = logging.getLogger("aw_apps.knowledgeable.sources_sync")

#: Cadences copied from `agents_platform_runners_app/plugin.py`'s own
#: skills-sync watchdog registration (design §4b) — same interval family as
#: every other decentralized-index sync in this estate. Overridable for
#: tests / a future tighter SLO without a code change.
DELTA_INTERVAL_S = int(os.environ.get("KNOWLEDGEABLE_SOURCES_DELTA_INTERVAL_S") or 180)
RECONCILE_INTERVAL_S = int(os.environ.get("KNOWLEDGEABLE_SOURCES_RECONCILE_INTERVAL_S") or 360)

_config_resolver: Callable[[], dict] = lambda: {}
_package_dir: str | None = None


def configure(config_resolver: Callable[[], dict], package_dir: str | None) -> None:
    """Installed once from ``plugin.activate``. ``config_resolver`` is read
    fresh every tick (same rule as ``forwarding.configure``) so a saved
    ``source_callback_url`` edit takes effect on the next cycle with no
    restart; ``package_dir`` only locates this app's own ``aw-app.json`` for
    ``connector_version`` and never changes at runtime."""
    global _config_resolver, _package_dir
    _config_resolver = config_resolver
    _package_dir = package_dir


def _config() -> dict:
    try:
        return _config_resolver() or {}
    except Exception:
        log.exception("sources_sync: config resolver failed")
        return {}


def _workspace_home() -> str:
    """Same resolution as ``bulk_ingest._workspace_home`` / ``skills_sync.
    _data_dir`` — deliberately not an import of either, this module has no
    dependency on the other app repo and no shared library exists between
    them."""
    home = os.environ.get("AW_WORKSPACE_HOME")
    if home:
        return home
    root = os.path.realpath(os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace"))
    return os.path.join(root, ".aw-workspace")


def _ack_path() -> str:
    d = os.path.join(_workspace_home(), "data", "knowledgeable")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "sources_sync_ack.json")


def state_hash(source_ids) -> str:
    """sha256 of the sorted, de-duplicated source-id list — MUST stay
    byte-identical to aw-knowledgeable's own ``source_store.state_hash``
    (the `skills_sync.py:76-81` rule, copied again here). A differently
    shaped hash makes every delta 409 and every tick a full sync — it
    degrades quietly rather than loudly, which is exactly why this function
    exists as a named, single-purpose thing rather than inlined once per
    call site."""
    joined = "\n".join(sorted(set(source_ids)))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _load_ack() -> dict:
    try:
        with open(_ack_path(), encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "hash" in data:
            return data
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return {}


def _save_ack(hash_: str) -> None:
    path = _ack_path()
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"hash": hash_}, f)
    os.replace(tmp, path)  # atomic


def _clear_ack() -> None:
    """Force the next cycle to full-sync (called on 409 / any push
    failure)."""
    try:
        os.remove(_ack_path())
    except FileNotFoundError:
        pass


def _connector_version() -> str | None:
    if not _package_dir:
        return None
    try:
        with open(os.path.join(_package_dir, "aw-app.json"), encoding="utf-8") as f:
            manifest = json.load(f)
        return manifest.get("version") if isinstance(manifest, dict) else None
    except (OSError, ValueError):
        return None


def _iso(epoch) -> str | None:
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


async def _current_sources() -> list[dict]:
    """This workspace's offered sources, shaped exactly as C1's
    ``PUT /api/sources/sync`` wire contract wants: registry names
    (``forwarding._registry()``) joined with the forwarder's own real
    availability probe (``forwarding.status_block()``, the same cache
    ``forward()`` populates) — a synced row is not a live source (design §9
    risk 3), but both facts travel in one push so the server can store them
    as the two separate columns it already has."""
    registry = forwarding._registry()
    status_rows = {row["id"]: row for row in await forwarding.status_block()}
    sources: list[dict] = []
    for entry in registry:
        source_id = entry.get("id")
        if not source_id:
            continue
        status = status_rows.get(source_id) or {}
        sources.append(
            {
                "id": source_id,
                "app_slug": entry.get("app_slug") or source_id,
                "tool_names": sorted((entry.get("tools") or {}).keys()),
                "available": bool(status.get("available")),
                "reason": status.get("reason"),
                "checked_at": _iso(status.get("checked_at")),
            }
        )
    return sources


async def _put_sync(body: dict) -> tuple[dict | None, int, str | None]:
    """One ``PUT /api/sources/sync``. Returns ``(data, status_code, err)``
    rather than collapsing every non-2xx into one error string the way
    ``mcp.client``'s ``_post_json``/``_get`` do — the caller here needs to
    tell a 404 (C1 not deployed yet, tolerate) apart from a 409 (hash drift,
    clear the ack) apart from everything else (also clear the ack, but log
    louder). ``client.base_url()``/``client._headers()`` are reused rather
    than re-resolved here, so this module shares the exact same base URL and
    the exact same fresh-every-call secret every other outbound call in this
    connector already uses."""
    url = f"{client.base_url()}/api/sources/sync"
    headers = {**client._headers(), "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=20) as http_client:
            resp = await http_client.put(url, json=body, headers=headers)
    except httpx.HTTPError as exc:
        return None, 0, f"could not reach aw-knowledgeable at {url}: {exc}"
    try:
        data = resp.json()
    except ValueError:
        data = None
    if resp.status_code >= 400:
        detail = (data or {}).get("detail") if isinstance(data, dict) else None
        return data, resp.status_code, (str(detail) if detail else f"HTTP {resp.status_code}")
    return data, resp.status_code, None


async def _push(mode: str, sources: list[dict], new_hash: str, prev_hash: str | None) -> dict:
    cfg = _config()
    callback_url = (cfg.get("source_callback_url") or "").strip() or None
    body = {
        "workspace_slug": workspace_env("AW_WORKSPACE") or "aw",
        "callback_url": callback_url,
        "connector_version": _connector_version(),
        "mode": mode,
        "prev_hash": prev_hash,
        "state_hash": new_hash,
        "sources": sources,
    }
    data, status, err = await _put_sync(body)

    if status == 404:
        # Same precedent the key-push loops already set for their own
        # endpoints (card C2's instructions): a route that doesn't exist
        # yet is an honest, visible "not deployed" state, not an error —
        # and not a reason to clear an ack this side never got to use.
        log.info("sources_sync: PUT /api/sources/sync is 404 (C1 not deployed yet) — will retry next tick")
        return {"mode": mode, "status": "not_deployed"}
    if status == 409:
        _clear_ack()
        log.warning("sources_sync: %s rejected (409 hash_mismatch) — forcing full sync next cycle", mode)
        return {"mode": mode, "status": "hash_mismatch", "forced_full": True}
    if err:
        _clear_ack()
        log.warning("sources_sync: %s push failed: %s", mode, err)
        return {"mode": mode, "status": "error", "error": err}

    _save_ack(new_hash)
    return {"mode": mode, "status": "ok", "state_hash": new_hash, "source_count": len(sources), "server": data}


async def sync_incremental() -> dict:
    """Delta cadence: no-op if unchanged since the ack; a full sync if there
    is no ack yet (first run / after a 409 or failure cleared it)."""
    sources = await _current_sources()
    new_hash = state_hash(s["id"] for s in sources)
    ack = _load_ack()
    if not ack:
        return await _push("full", sources, new_hash, prev_hash=None)
    if ack.get("hash") == new_hash:
        return {"mode": "delta", "status": "unchanged", "state_hash": new_hash}
    return await _push("delta", sources, new_hash, prev_hash=ack.get("hash"))


async def sync_full() -> dict:
    """Reconcile cadence: always ship the complete current state,
    unconditionally — the registry can't silently drift even if every delta
    in between happened to look like a no-op."""
    sources = await _current_sources()
    new_hash = state_hash(s["id"] for s in sources)
    return await _push("full", sources, new_hash, prev_hash=None)


async def run_delta_forever() -> None:
    """Started as a background task from ``plugin.activate()``, cancelled
    from ``plugin.deactivate()``. First tick runs immediately — with no ack
    yet, that first tick IS the boot full sync."""
    while True:
        try:
            result = await sync_incremental()
            log.info("sources_sync delta: %s", result)
        except Exception:  # noqa: BLE001 — a tick must never kill the loop
            log.exception("sources_sync: delta tick failed")
        await asyncio.sleep(DELTA_INTERVAL_S)


async def run_reconcile_forever() -> None:
    """Started as a background task from ``plugin.activate()``, cancelled
    from ``plugin.deactivate()``. Sleeps first — the delta loop's own first
    tick already covers the boot full sync, so this loop's first real push
    is the first reconcile."""
    while True:
        await asyncio.sleep(RECONCILE_INTERVAL_S)
        try:
            result = await sync_full()
            log.info("sources_sync reconcile: %s", result)
        except Exception:  # noqa: BLE001 — a tick must never kill the loop
            log.exception("sources_sync: reconcile tick failed")
