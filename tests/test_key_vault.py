"""The one vault-read implementation shared by the three push loops and the
pull-through route (docs/design/knowledgeable-apmt-key-pull-through.md
§3.1) — moved here from test_playground_key_push.py (and its two twins) when
`_read_from_vault`/`_secrets_api_base` were factored out of those modules
into `key_vault.py`.

No real network, no real event loop plugin: async calls are driven through
the ``_run`` helper from plain ``def`` tests, same style as the rest of this
suite — this repo runs under plain pytest with no pytest-asyncio.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import key_vault  # noqa: E402


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
    monkeypatch.setattr(key_vault, "httpx", type(
        "M", (), {"AsyncClient": _FakeAsyncClient, "HTTPError": httpx.HTTPError},
    ))
    monkeypatch.setattr(key_vault, "workspace_env", lambda name: {
        "AW_WORKSPACE_API_URL": "http://workspace:9030",
        "AW_WORKSPACE_API_KEY": "ws-api-key",
    }.get(name, ""))
    yield


# ---------------------------------------------------------------------------
# _secrets_api_base — nothing to reach before the workspace has published it
# ---------------------------------------------------------------------------


def test_secrets_api_base_is_none_without_both_vars(monkeypatch):
    monkeypatch.setattr(key_vault, "workspace_env", lambda name: "")
    assert key_vault._secrets_api_base() is None


def test_secrets_api_base_needs_the_key_too(monkeypatch):
    monkeypatch.setattr(key_vault, "workspace_env", lambda name: (
        "http://workspace:9030" if name == "AW_WORKSPACE_API_URL" else ""
    ))
    assert key_vault._secrets_api_base() is None


# ---------------------------------------------------------------------------
# read_secret — the loopback read, with the exact caller identity
# ---------------------------------------------------------------------------


def test_read_secret_sends_the_stable_caller_identity_header():
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"status": "approved", "value": "apmt-key-from-vault"})
    )
    value = _run(key_vault.read_secret("knowledgeable-playground-apmt-key", reason="because"))
    assert value == "apmt-key-from-vault"
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == (
        "http://workspace:9030/api/apps/secrets/secrets/"
        "knowledgeable-playground-apmt-key/read"
    )
    assert call["headers"] == {"X-Api-Key": "ws-api-key", "X-Aw-Caller-Agent": "aw-app-knowledgeable"}
    assert call["json"]["reason"] == "because"


def test_read_secret_returns_none_without_workspace_publication(monkeypatch):
    monkeypatch.setattr(key_vault, "workspace_env", lambda name: "")
    value = _run(key_vault.read_secret("any-secret", reason="because"))
    assert value is None
    assert _FakeAsyncClient._CALLS == []


def test_read_secret_returns_none_when_not_auto_approved():
    """The caller identity is unlisted, or the secret's policy was reset —
    either way this must not raise or hang, it just has nothing to hand
    over. Also proves the allowlist gate is real: an un-approved read yields
    no value even though the HTTP call itself succeeded."""
    _FakeAsyncClient._QUEUE.append(_FakeResponse(200, {"status": "pending", "request_id": "req-1"}))
    value = _run(key_vault.read_secret("any-secret", reason="because"))
    assert value is None


def test_read_secret_returns_none_on_backend_error():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(500, text="internal error"))
    value = _run(key_vault.read_secret("any-secret", reason="because"))
    assert value is None


def test_read_secret_returns_none_on_connection_failure(monkeypatch):
    class _Boom(_FakeAsyncClient):
        async def post(self, *a, **k):
            raise httpx.HTTPError("connect failed")

    monkeypatch.setattr(key_vault, "httpx", type(
        "M", (), {"AsyncClient": _Boom, "HTTPError": httpx.HTTPError},
    ))
    value = _run(key_vault.read_secret("any-secret", reason="because"))
    assert value is None
