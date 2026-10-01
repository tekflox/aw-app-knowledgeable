"""The re-assert loop: read the extractor's ap-mt key from this workspace's
own vault (over aw-app-secrets' loopback REST, never a hand-rolled second
implementation of the approval protocol) and push it to production
aw-knowledgeable — never once at install, always on a fixed interval, and
never touching disk (card 3ec5bf3b, see ingest_key_push.py's own docstring
for the design this locks in, and test_playground_key_push.py for the twin
this mirrors).

No real network, no real event loop plugin: same style as test_client.py —
async calls are driven through the ``_run`` helper from plain ``def`` tests,
because this repo runs under plain pytest with no pytest-asyncio.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import ingest_key_push as ikp  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeResponse:
    def __init__(self, status_code=200, json_body=None, text=""):
        self.status_code = status_code
        self._json = json_body if json_body is not None else {}
        self.text = text or ""

    def json(self):
        return self._json


class _FakeAsyncClient:
    _CALLS: list = []
    _QUEUE: list = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, headers=None):
        _FakeAsyncClient._CALLS.append({"url": url, "json": json, "headers": headers})
        return _FakeAsyncClient._QUEUE.pop(0)


@pytest.fixture(autouse=True)
def _fresh_fake_client(monkeypatch):
    _FakeAsyncClient._CALLS = []
    _FakeAsyncClient._QUEUE = []
    monkeypatch.setattr(ikp, "httpx", type(
        "M", (), {"AsyncClient": _FakeAsyncClient, "HTTPError": httpx.HTTPError},
    ))
    monkeypatch.setattr(ikp, "workspace_env", lambda name: {
        "AW_WORKSPACE_API_URL": "http://workspace:9030",
        "AW_WORKSPACE_API_KEY": "ws-api-key",
    }.get(name, ""))
    yield


# ---------------------------------------------------------------------------
# _secrets_api_base — nothing to reach before the workspace has published it
# ---------------------------------------------------------------------------


def test_secrets_api_base_is_none_without_both_vars(monkeypatch):
    monkeypatch.setattr(ikp, "workspace_env", lambda name: "")
    assert ikp._secrets_api_base() is None


def test_secrets_api_base_needs_the_key_too(monkeypatch):
    monkeypatch.setattr(ikp, "workspace_env", lambda name: (
        "http://workspace:9030" if name == "AW_WORKSPACE_API_URL" else ""
    ))
    assert ikp._secrets_api_base() is None


# ---------------------------------------------------------------------------
# _read_from_vault — the loopback read, with the exact caller identity
# ---------------------------------------------------------------------------


def test_read_from_vault_sends_the_stable_caller_identity_header():
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"status": "approved", "value": "apmt-key-from-vault"})
    )
    value = _run(ikp._read_from_vault())
    assert value == "apmt-key-from-vault"
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == (
        "http://workspace:9030/api/apps/secrets/secrets/"
        "knowledgeable-extractor-apmt-key/read"
    )
    assert call["headers"] == {"X-Api-Key": "ws-api-key", "X-Aw-Caller-Agent": "aw-app-knowledgeable"}
    assert call["json"]["reason"]


def test_read_from_vault_returns_none_without_workspace_publication(monkeypatch):
    monkeypatch.setattr(ikp, "workspace_env", lambda name: "")
    value = _run(ikp._read_from_vault())
    assert value is None
    assert _FakeAsyncClient._CALLS == []


def test_read_from_vault_returns_none_when_not_auto_approved():
    """The caller identity is unlisted, or the secret's policy was reset —
    either way this must not raise or hang, it just has nothing to push this
    tick. Also proves the allowlist gate is real: an un-approved read yields
    no value even though the HTTP call itself succeeded."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"status": "pending", "request_id": "req-1"}))
    value = _run(ikp._read_from_vault())
    assert value is None


def test_read_from_vault_returns_none_on_backend_error():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(500, text="internal error"))
    value = _run(ikp._read_from_vault())
    assert value is None


def test_read_from_vault_returns_none_on_connection_failure(monkeypatch):
    class _Boom(_FakeAsyncClient):
        async def post(self, *a, **k):
            raise httpx.HTTPError("connect failed")

    monkeypatch.setattr(ikp, "httpx", type(
        "M", (), {"AsyncClient": _Boom, "HTTPError": httpx.HTTPError},
    ))
    value = _run(ikp._read_from_vault())
    assert value is None


# ---------------------------------------------------------------------------
# push_once — the two hops chained, and what "nothing to push" looks like
# ---------------------------------------------------------------------------


def test_push_once_reads_then_pushes_and_reports_success(monkeypatch):
    calls = []

    async def fake_read():
        return "the-real-key"

    async def fake_push(value):
        calls.append(value)
        return True, None

    monkeypatch.setattr(ikp, "_read_from_vault", fake_read)
    monkeypatch.setattr(client, "push_ingest_key", fake_push)

    ok = _run(ikp.push_once())

    assert ok is True
    assert calls == ["the-real-key"], "push_once must forward exactly the value it read"


def test_push_once_is_a_noop_when_the_vault_has_nothing(monkeypatch):
    async def fake_read():
        return None

    pushed = []

    async def fake_push(value):
        pushed.append(value)
        return True, None

    monkeypatch.setattr(ikp, "_read_from_vault", fake_read)
    monkeypatch.setattr(client, "push_ingest_key", fake_push)

    ok = _run(ikp.push_once())

    assert ok is False
    assert pushed == [], "nothing to push must not call aw-knowledgeable at all"


def test_push_once_reports_failure_when_aw_knowledgeable_refuses(monkeypatch):
    async def fake_read():
        return "the-real-key"

    async def fake_push(value):
        return False, "HTTP 401: unauthorized"

    monkeypatch.setattr(ikp, "_read_from_vault", fake_read)
    monkeypatch.setattr(client, "push_ingest_key", fake_push)

    ok = _run(ikp.push_once())

    assert ok is False


# ---------------------------------------------------------------------------
# run_forever — a LOOP, not a one-shot at install (the card's own point)
# ---------------------------------------------------------------------------


def test_run_forever_pushes_more_than_once_without_being_restarted(monkeypatch):
    """The whole reason this exists as a background task instead of a call
    in `activate()`: a one-shot push cannot recover from aw-knowledgeable's
    own container being recreated later. If `run_forever` ever regresses to
    `await push_once()` with no loop around it, this test must fail."""
    count = 0

    async def fake_push_once():
        nonlocal count
        count += 1
        return True

    monkeypatch.setattr(ikp, "push_once", fake_push_once)
    monkeypatch.setattr(ikp, "PUSH_INTERVAL_S", 0)

    async def drive():
        task = asyncio.ensure_future(ikp.run_forever())
        try:
            for _ in range(200):
                if count >= 3:
                    break
                await asyncio.sleep(0.01)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    _run(drive())

    assert count >= 3, f"run_forever only pushed {count} time(s) — it must keep looping"
