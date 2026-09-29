"""The app's own HTTP surface (/status, /settings, /logout, /test, /mcp),
via FastAPI's TestClient — same pattern as the rest of this repo family
(aw-app-google-maps has no equivalent file because its routes are trivial;
this one is worth its own test because /settings routes into a fake
ctx.secrets, not the generic config path)."""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import routes as routes_mod  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


class _FakeSecrets:
    def __init__(self):
        self._store: dict[str, str] = {}

    def read(self, key):
        return self._store.get(key)

    def write(self, key, value):
        self._store[key] = value

    def delete(self, key):
        self._store.pop(key, None)


@pytest.fixture()
def app_client():
    ctx = SimpleNamespace(secrets=_FakeSecrets(), config={})
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: ctx.secrets.read(routes_mod.SECRET_KEY) or "")
    app = routes_mod.build_routes(ctx)
    return TestClient(app), ctx


def test_status_starts_logged_out(app_client):
    tc, _ = app_client
    resp = tc.get("/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["logged_in"] is False
    assert body["tools"] == [t["name"] for t in client.TOOLS_SCHEMA]


def test_settings_requires_a_secret(app_client):
    tc, _ = app_client
    resp = tc.post("/settings", json={})
    assert resp.status_code == 400


def test_settings_saves_the_secret_into_ctx_secrets_not_plain_config(app_client):
    tc, ctx = app_client
    resp = tc.post("/settings", json={"service_secret": "topsecret"})
    assert resp.status_code == 200
    assert resp.json()["logged_in"] is True
    assert ctx.secrets.read("service_secret") == "topsecret"
    assert "service_secret" not in ctx.config

    status = tc.get("/status").json()
    assert status["logged_in"] is True


def test_logout_clears_the_secret(app_client):
    tc, ctx = app_client
    tc.post("/settings", json={"service_secret": "topsecret"})
    resp = tc.post("/logout")
    assert resp.status_code == 200
    assert resp.json()["logged_in"] is False
    assert ctx.secrets.read("service_secret") is None


def test_mcp_get_is_405(app_client):
    tc, _ = app_client
    resp = tc.get("/mcp")
    assert resp.status_code == 405


def test_mcp_post_tools_list(app_client):
    tc, _ = app_client
    resp = tc.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert resp.status_code == 200
    tools = resp.json()["result"]["tools"]
    assert len(tools) == 7


def test_mcp_json_endpoint(app_client):
    tc, _ = app_client
    resp = tc.get("/mcp.json")
    assert resp.status_code == 200
    assert "aw-knowledgeable" in resp.json()["mcpServers"]
