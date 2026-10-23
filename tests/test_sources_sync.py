"""sources_sync.py — the WorkspaceSkillSync-twin push loop
(docs/design/knowledgeable-knowledge-sources-registry.md §4b). Deliberately
no real network: every test monkeypatches the module's own
``httpx.AsyncClient`` with a fake that records the outbound PUT and
returns/raises a canned result — same pattern ``test_forwarding.py`` already
uses for ``forwarding.httpx``.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import sources_sync  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeSyncResponse:
    def __init__(self, status_code=200, json_body=None):
        self.status_code = status_code
        self._json = json_body if json_body is not None else {}

    def json(self):
        return self._json


class _FakeSyncAsyncClient:
    """``_QUEUE`` items are either a ``_FakeSyncResponse`` to return or an
    ``Exception`` instance to raise."""

    _CALLS: list = []
    _QUEUE: list = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def put(self, url, json=None, headers=None):
        _FakeSyncAsyncClient._CALLS.append({"url": url, "json": json, "headers": headers})
        item = _FakeSyncAsyncClient._QUEUE.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture(autouse=True)
def _fresh_sources_sync(monkeypatch, tmp_path):
    _FakeSyncAsyncClient._CALLS = []
    _FakeSyncAsyncClient._QUEUE = []
    monkeypatch.setenv("AW_WORKSPACE_HOME", str(tmp_path))
    monkeypatch.delenv("AW_WORKSPACE", raising=False)
    monkeypatch.setattr(sources_sync, "httpx", type(
        "M", (), {"AsyncClient": _FakeSyncAsyncClient, "HTTPError": httpx.HTTPError},
    ))
    client.configure(lambda: "http://aw-knowledgeable:8090", lambda: "shh")
    sources_sync.configure(lambda: {}, None)
    yield
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: "")


def _no_sources(monkeypatch):
    monkeypatch.setattr(sources_sync.forwarding, "_registry", lambda: [])

    async def fake_status_block():
        return []

    monkeypatch.setattr(sources_sync.forwarding, "status_block", fake_status_block)


def _one_codegraph_source(monkeypatch):
    monkeypatch.setattr(sources_sync.forwarding, "_registry", lambda: [
        {"id": "codegraph", "app_slug": "codegraph", "tools": {"codegraph_explore": {}}},
    ])

    async def fake_status_block():
        return [{"id": "codegraph", "available": True, "reason": None, "checked_at": 1700000000.0}]

    monkeypatch.setattr(sources_sync.forwarding, "status_block", fake_status_block)


# ---------------------------------------------------------------------------
# state_hash — byte-identical rule
# ---------------------------------------------------------------------------


def test_state_hash_is_sorted_deduplicated_sha256():
    expected = hashlib.sha256("a\nb".encode("utf-8")).hexdigest()
    assert sources_sync.state_hash(["b", "a", "a"]) == expected


def test_state_hash_of_empty_set_is_the_empty_strings_hash():
    assert sources_sync.state_hash([]) == hashlib.sha256(b"").hexdigest()


# ---------------------------------------------------------------------------
# sync_incremental / sync_full — the two cadences
# ---------------------------------------------------------------------------


def test_sync_incremental_with_no_ack_does_a_full_sync(monkeypatch):
    _one_codegraph_source(monkeypatch)
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True, "mode": "full"}))

    result = _run(sources_sync.sync_incremental())

    assert result["mode"] == "full"
    assert result["status"] == "ok"
    call = _FakeSyncAsyncClient._CALLS[0]
    assert call["url"] == "http://aw-knowledgeable:8090/api/sources/sync"
    assert call["json"]["mode"] == "full"
    assert call["json"]["prev_hash"] is None
    assert call["json"]["sources"] == [
        {
            "id": "codegraph",
            "app_slug": "codegraph",
            "tool_names": ["codegraph_explore"],
            "available": True,
            "reason": None,
            "checked_at": sources_sync._iso(1700000000.0),
        }
    ]
    assert call["headers"]["X-Internal-Secret"] == "shh"
    assert sources_sync._load_ack()["hash"] == sources_sync.state_hash(["codegraph"])


def test_sync_incremental_unchanged_hash_is_a_noop(monkeypatch):
    _one_codegraph_source(monkeypatch)
    sources_sync._save_ack(sources_sync.state_hash(["codegraph"]))

    result = _run(sources_sync.sync_incremental())

    assert result == {
        "mode": "delta",
        "status": "unchanged",
        "state_hash": sources_sync.state_hash(["codegraph"]),
    }
    assert _FakeSyncAsyncClient._CALLS == []


def test_sync_incremental_changed_hash_pushes_a_delta_with_prev_hash(monkeypatch):
    _one_codegraph_source(monkeypatch)
    sources_sync._save_ack("a-stale-hash-from-before")
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))

    result = _run(sources_sync.sync_incremental())

    assert result["mode"] == "delta"
    assert result["status"] == "ok"
    call = _FakeSyncAsyncClient._CALLS[0]
    assert call["json"]["mode"] == "delta"
    assert call["json"]["prev_hash"] == "a-stale-hash-from-before"


def test_sync_full_always_pushes_even_when_unchanged(monkeypatch):
    """The reconcile cadence never no-ops — it's the whole point of having
    two cadences instead of one."""
    _one_codegraph_source(monkeypatch)
    sources_sync._save_ack(sources_sync.state_hash(["codegraph"]))
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))

    result = _run(sources_sync.sync_full())

    assert result["mode"] == "full"
    assert result["status"] == "ok"
    assert len(_FakeSyncAsyncClient._CALLS) == 1


def test_empty_registry_pushes_an_empty_sources_list_visibly(monkeypatch):
    """§9.1's mandatory test: a sync with an empty registry still runs and
    carries `sources: []` — the server is what deletes everything, this
    side just reports the truth without special-casing it."""
    _no_sources(monkeypatch)
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True, "deleted": ["codegraph"]}))

    result = _run(sources_sync.sync_full())

    assert result["status"] == "ok"
    call = _FakeSyncAsyncClient._CALLS[0]
    assert call["json"]["sources"] == []
    assert call["json"]["state_hash"] == hashlib.sha256(b"").hexdigest()


# ---------------------------------------------------------------------------
# 404 tolerance, 409 hash-mismatch, transport failure — all three clear or
# preserve the ack per §4b / the key-push loops' own precedent.
# ---------------------------------------------------------------------------


def test_push_404_is_tolerated_and_the_ack_is_left_untouched(monkeypatch):
    """C1 not deployed yet — same precedent the key-push loops already set:
    an honest, visible 'not deployed' status, never an error, and the ack
    (if any) survives so the next tick's delta logic is unaffected."""
    _no_sources(monkeypatch)
    sources_sync._save_ack("some-previous-hash")
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(404, {"detail": "Not Found"}))

    result = _run(sources_sync.sync_incremental())

    assert result == {"mode": "delta", "status": "not_deployed"}
    assert sources_sync._load_ack()["hash"] == "some-previous-hash"


def test_push_409_clears_the_ack_and_forces_full_next_tick(monkeypatch):
    _one_codegraph_source(monkeypatch)
    sources_sync._save_ack("a-stale-hash-from-before")
    _FakeSyncAsyncClient._QUEUE.append(
        _FakeSyncResponse(409, {"error": "hash_mismatch", "server_hash": "real-hash"})
    )

    result = _run(sources_sync.sync_incremental())

    assert result["status"] == "hash_mismatch"
    assert result["forced_full"] is True
    assert sources_sync._load_ack() == {}


def test_push_transport_failure_clears_the_ack(monkeypatch):
    _no_sources(monkeypatch)
    sources_sync._save_ack("whatever")
    _FakeSyncAsyncClient._QUEUE.append(httpx.ConnectError("refused"))

    result = _run(sources_sync.sync_full())

    assert result["status"] == "error"
    assert sources_sync._load_ack() == {}


# ---------------------------------------------------------------------------
# The secret is re-read every tick — never captured at configure() time.
# ---------------------------------------------------------------------------


def test_secret_is_read_fresh_every_tick_not_captured_at_configure_time(monkeypatch):
    _no_sources(monkeypatch)
    secret_box = {"value": "first"}
    client.configure(lambda: "http://aw-knowledgeable:8090", lambda: secret_box["value"])

    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))
    _run(sources_sync.sync_full())
    assert _FakeSyncAsyncClient._CALLS[0]["headers"]["X-Internal-Secret"] == "first"

    secret_box["value"] = "rotated"
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))
    _run(sources_sync.sync_full())
    assert _FakeSyncAsyncClient._CALLS[1]["headers"]["X-Internal-Secret"] == "rotated"


# ---------------------------------------------------------------------------
# callback_url / connector_version / workspace_slug — the request body's
# other fields.
# ---------------------------------------------------------------------------


def test_callback_url_comes_from_config_and_empty_becomes_none(monkeypatch):
    _no_sources(monkeypatch)
    sources_sync.configure(lambda: {"source_callback_url": "https://example.test/cb"}, None)
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))
    _run(sources_sync.sync_full())
    assert _FakeSyncAsyncClient._CALLS[0]["json"]["callback_url"] == "https://example.test/cb"

    sources_sync.configure(lambda: {"source_callback_url": ""}, None)
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))
    _run(sources_sync.sync_full())
    assert _FakeSyncAsyncClient._CALLS[1]["json"]["callback_url"] is None


def test_connector_version_is_read_from_the_installed_manifest(tmp_path, monkeypatch):
    _no_sources(monkeypatch)
    (tmp_path / "aw-app.json").write_text(json.dumps({"version": "0.73.0"}))
    sources_sync.configure(lambda: {}, str(tmp_path))

    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))
    _run(sources_sync.sync_full())
    assert _FakeSyncAsyncClient._CALLS[0]["json"]["connector_version"] == "0.73.0"


def test_workspace_slug_defaults_to_aw_when_unset(monkeypatch):
    _no_sources(monkeypatch)
    _FakeSyncAsyncClient._QUEUE.append(_FakeSyncResponse(200, {"synced": True}))
    _run(sources_sync.sync_full())
    assert _FakeSyncAsyncClient._CALLS[0]["json"]["workspace_slug"] == "aw"
