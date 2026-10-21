"""The app's own HTTP surface (/status, /settings, /logout, /test, /mcp),
via FastAPI's TestClient — same pattern as the rest of this repo family
(aw-app-google-maps has no equivalent file because its routes are trivial;
this one is worth its own test because /settings routes into a fake
ctx.secrets, not the generic config path)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import (  # noqa: E402
    exec_policy,
    exec_policy_store,
    exec_scrub,
    exec_sync,
    forwarding,
    pending_deletions,
    routes as routes_mod,
    vault_coverage,
)
from knowledgeable_app.mcp import client  # noqa: E402


async def _default_coverage_status():
    return {"vault_coverage_unknown": None, "vault_names_uncovered": []}


@pytest.fixture(autouse=True)
def _wire_scrub_fakes(monkeypatch):
    """Same reasoning as `test_exec_sync.py`'s own `_wire_fakes`: keep this
    file's exec-sync route tests off the real workspace secret store and off
    whatever `AW_WORKSPACE_API_URL`/`KEY` happen to be set in the ambient
    environment, now that `GET /exec-sync/status` and `/exec-sync/doctor`
    compute `exec_sync.scrub_status()` on every call."""
    monkeypatch.setattr(exec_scrub, "workspace_secret_values", lambda: (["invented-secret"], None))
    monkeypatch.setattr(vault_coverage, "coverage_status", _default_coverage_status)


@pytest.fixture(autouse=True)
def _fresh_pending_deletion_table_flag(monkeypatch):
    """Each test's `app_client`/`ungated_app_client` builds a brand new
    `_FakeDb` — without resetting this module-level flag, the first test to
    call approve/decline would set it forever and every later test's own
    fresh `:memory:` db would never get `TABLE` created (same trap
    `test_fs_sync.py`'s `_fresh_table_flag` guards against for
    `bulk_ingest._table_ensured`)."""
    monkeypatch.setattr(pending_deletions, "_table_ensured", False)


class _FakeDb:
    """Double for ``ctx.db`` — same shape as `test_fs_sync.py`/
    `test_bulk_ingest.py`'s identical class, needed here now that
    `mcp_post` threads `ctx.db` through to the pending-deletion audit
    trail on every `/mcp` call, not just `tools/call` against those two
    tools."""

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    def create(self, name: str, columns_sql: str) -> str:
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({columns_sql})")
        self._conn.commit()
        return name

    def execute(self, name: str, sql: str, params: dict | None = None):
        stmt = sql.replace("{table}", name)
        cur = self._conn.execute(stmt, params or {})
        if stmt.strip().lower().startswith("select"):
            rows = cur.fetchall()
            self._conn.commit()
            return rows
        self._conn.commit()
        return cur


class _FakeSecrets:
    def __init__(self):
        self._store: dict[str, str] = {}

    def read(self, key):
        return self._store.get(key)

    def write(self, key, value):
        self._store[key] = value

    def delete(self, key):
        self._store.pop(key, None)


class _FakeKv:
    """Double for `ctx.state.kv` — plain dict, no TTL semantics (not
    needed for these route-level tests; TTL behavior itself is
    state_facade.py's own concern, not this app's)."""

    def __init__(self):
        self._store: dict[str, object] = {}

    async def put(self, name, value, ttl):
        self._store[name] = value

    async def get(self, name):
        return self._store.get(name)


@pytest.fixture()
def app_client():
    """Every route except `GET /keys/{slot}` now carries its own
    `_require_identity` dependency — re-implemented per-route because this
    app's config must run with `auth_required: false` for the pull route to
    be reachable at all (see routes.py's module docstring). The REAL
    `IdentityGuard` middleware that would normally populate
    `request.scope["aw_identity"]` sits outside this sub-app and is not part
    of what `build_routes(ctx)` returns, so every test below that exercises
    ordinary app behaviour overrides the dependency to simulate "the caller
    already passed that gate" — `test_identity_is_still_required.*` below
    removes the override to prove the gate is real.
    """
    ctx = SimpleNamespace(
        secrets=_FakeSecrets(), config={}, state=SimpleNamespace(kv=_FakeKv()), db=_FakeDb()
    )
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: ctx.secrets.read(routes_mod.SECRET_KEY) or "")
    # Same manual-wiring pattern as `client.configure` above — `build_routes`
    # itself calls neither; only `plugin.activate` does in production. See
    # feature:knowledgeable-connector-forwards-to-codegraph.
    forwarding.configure(lambda: ctx.config or {})
    app = routes_mod.build_routes(ctx)
    app.dependency_overrides[routes_mod._require_identity] = lambda: None
    return TestClient(app), ctx


@pytest.fixture(autouse=True)
def _reset_forwarding_registry():
    forwarding._availability_cache.clear()
    yield
    forwarding.configure(lambda: {})
    forwarding._availability_cache.clear()


def test_status_starts_logged_out(app_client):
    tc, _ = app_client
    resp = tc.get("/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["logged_in"] is False
    assert body["tools"] == [t["name"] for t in client.TOOLS_SCHEMA]


def test_status_forward_upstreams_empty_by_default(app_client):
    """No `forward_upstreams` configured → the block is present and empty,
    never missing (design §6's "always an explicit verdict" contract)."""
    tc, _ = app_client
    resp = tc.get("/status")
    assert resp.json()["forward_upstreams"] == []


def test_status_forward_upstreams_reports_a_configured_entry(app_client, monkeypatch):
    tc, ctx = app_client
    ctx.config = {
        "forward_upstreams": [
            {
                "id": "codegraph",
                "transport": "app-mcp",
                "app_slug": "codegraph",
                "tools": {"codegraph_status": {"args": []}},
                "probe": {"tool": "codegraph_status", "ttl_ok_s": 60, "ttl_fail_s": 15, "timeout_s": 5},
                "timeout_s": 10,
            }
        ]
    }

    class _FakeResponse:
        status_code = 200

        def json(self):
            return {
                "jsonrpc": "2.0", "id": 1,
                "result": {"content": [{"type": "text", "text": "ok"}], "isError": False},
            }

    class _FakeAsyncClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, json=None, headers=None):
            return _FakeResponse()

    monkeypatch.setattr(forwarding, "httpx", type("M", (), {"AsyncClient": _FakeAsyncClient}))
    resp = tc.get("/status")
    rows = resp.json()["forward_upstreams"]
    assert rows == [{"id": "codegraph", "available": True, "checked_at": rows[0]["checked_at"], "reason": None}]


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


def test_status_reports_git_disabled_by_default(app_client):
    tc, _ = app_client
    body = tc.get("/status").json()
    assert body["git"] == {
        "enabled": False, "configured": False, "state": None, "detail": None,
    }


def test_settings_saves_git_token_alongside_service_secret(app_client):
    tc, ctx = app_client
    resp = tc.post("/settings", json={"service_secret": "topsecret", "git_token": "ghp_abc"})
    assert resp.status_code == 200
    assert ctx.secrets.read("git_token") == "ghp_abc"
    assert "git_token" not in ctx.config


def test_settings_omitting_git_token_leaves_it_untouched(app_client):
    tc, ctx = app_client
    tc.post("/settings", json={"service_secret": "topsecret", "git_token": "ghp_abc"})
    tc.post("/settings", json={"service_secret": "topsecret"})
    assert ctx.secrets.read("git_token") == "ghp_abc"


def test_settings_an_empty_git_token_clears_it(app_client):
    tc, ctx = app_client
    tc.post("/settings", json={"service_secret": "topsecret", "git_token": "ghp_abc"})
    tc.post("/settings", json={"service_secret": "topsecret", "git_token": ""})
    assert ctx.secrets.read("git_token") is None


def test_status_reports_git_configured_once_remote_url_and_token_are_set(app_client):
    tc, ctx = app_client
    ctx.config["git_remote_url"] = "https://github.com/f/k.git"
    tc.post("/settings", json={"service_secret": "topsecret", "git_token": "ghp_abc"})
    body = tc.get("/status").json()
    assert body["git"]["enabled"] is True
    assert body["git"]["configured"] is True


def test_mcp_get_is_405(app_client):
    tc, _ = app_client
    resp = tc.get("/mcp")
    assert resp.status_code == 405


def test_mcp_post_tools_list(app_client):
    tc, _ = app_client
    resp = tc.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert resp.status_code == 200
    tools = resp.json()["result"]["tools"]
    assert len(tools) == 13


def test_mcp_post_threads_actor_and_db_into_approve_pending_deletions(app_client, monkeypatch):
    """The new plumbing this card adds to `mcp_post`: `ctx.db` reaches the
    audit trail, and the gateway's `x-aw-caller-*` headers become the
    recorded actor — the exact gap the 12-to-zero-with-no-record incident
    exposed."""
    tc, ctx = app_client
    tc.post("/settings", json={"service_secret": "topsecret"})

    async def fake_pending(bucket=None):
        return {"documents": [{"id": "doc-1", "source_path": "notion/a.md"}]}, None

    async def fake_delete(node_id, *, bucket):
        return True, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)
    monkeypatch.setattr(client, "delete_document", fake_delete)

    resp = tc.post(
        "/mcp",
        json={
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "approve_pending_deletions",
                "arguments": {"bucket": "main", "path_prefix": "notion/", "expected_count": 1},
            },
        },
        headers={"x-aw-caller-agent": "agent:coder-sonnet", "x-aw-caller-run-id": "run-42"},
    )
    assert resp.status_code == 200
    assert resp.json()["result"]["isError"] is False

    rows = [dict(r) for r in ctx.db.execute(pending_deletions.TABLE, "SELECT * FROM {table}")]
    assert len(rows) == 1
    assert rows[0]["actor"] == "agent:coder-sonnet run=run-42"
    assert rows[0]["outcome"] == "ok"


def test_mcp_post_actor_falls_back_to_unknown_without_gateway_headers(app_client, monkeypatch):
    tc, ctx = app_client
    tc.post("/settings", json={"service_secret": "topsecret"})

    async def fake_pending(bucket=None):
        return {"documents": []}, None

    monkeypatch.setattr(client, "get_documents_pending_delete", fake_pending)

    resp = tc.post(
        "/mcp",
        json={
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "decline_pending_deletions", "arguments": {"bucket": "main"}},
        },
    )
    assert resp.status_code == 200
    rows = [dict(r) for r in ctx.db.execute(pending_deletions.TABLE, "SELECT * FROM {table}")]
    assert rows[0]["actor"] == "unknown"


def test_mcp_json_endpoint(app_client):
    tc, _ = app_client
    resp = tc.get("/mcp.json")
    assert resp.status_code == 200
    assert "aw-knowledgeable" in resp.json()["mcpServers"]


# ---------------------------------------------------------------------------
# GET /keys/{slot} — the pull-through route (docs/design/
# knowledgeable-apmt-key-pull-through.md §3.1). Auth is this route's own
# X-Internal-Secret check against ctx.secrets, not the workspace's gate.
# ---------------------------------------------------------------------------


def test_pull_key_requires_the_internal_secret(app_client):
    tc, ctx = app_client
    ctx.secrets.write("service_secret", "shh")
    resp = tc.get("/keys/playground")
    assert resp.status_code == 401


def test_pull_key_rejects_a_wrong_secret(app_client):
    tc, ctx = app_client
    ctx.secrets.write("service_secret", "shh")
    resp = tc.get("/keys/playground", headers={"x-internal-secret": "wrong"})
    assert resp.status_code == 401


def test_pull_key_401s_when_no_service_secret_is_configured_yet(app_client):
    tc, _ = app_client
    resp = tc.get("/keys/playground", headers={"x-internal-secret": "anything"})
    assert resp.status_code == 401


def test_pull_key_404s_on_an_unknown_slot(app_client):
    tc, ctx = app_client
    ctx.secrets.write("service_secret", "shh")
    resp = tc.get("/keys/bogus", headers={"x-internal-secret": "shh"})
    assert resp.status_code == 404


def test_pull_key_returns_the_vault_value_on_success(app_client, monkeypatch):
    tc, ctx = app_client
    ctx.secrets.write("service_secret", "shh")

    async def fake_read_secret(name, *, reason):
        assert name == "knowledgeable-playground-apmt-key"
        return "sk-live-123"

    monkeypatch.setattr(routes_mod.key_vault, "read_secret", fake_read_secret)
    resp = tc.get("/keys/playground", headers={"x-internal-secret": "shh"})
    assert resp.status_code == 200
    assert resp.json() == {"api_key": "sk-live-123"}


def test_pull_key_503s_on_a_vault_miss(app_client, monkeypatch):
    tc, ctx = app_client
    ctx.secrets.write("service_secret", "shh")

    async def fake_read_secret(name, *, reason):
        return None

    monkeypatch.setattr(routes_mod.key_vault, "read_secret", fake_read_secret)
    resp = tc.get("/keys/ingest", headers={"x-internal-secret": "shh"})
    assert resp.status_code == 503
    assert resp.json()["reason"] == "vault_unavailable"


# ---------------------------------------------------------------------------
# `_require_identity` — every OTHER route must still 401 an anonymous
# caller even with `auth_required: false` on the whole mount (routes.py's
# module docstring). `app_client` above overrides this dependency for every
# ordinary-behaviour test; these rebuild the app WITHOUT that override.
# ---------------------------------------------------------------------------


@pytest.fixture()
def ungated_app_client():
    """Same app as `app_client`, but with NO `_require_identity` override —
    the shape a real deploy has until the real `IdentityGuard` (outside this
    sub-app) attaches `scope["aw_identity"]` for an authenticated caller."""
    ctx = SimpleNamespace(
        secrets=_FakeSecrets(), config={}, state=SimpleNamespace(kv=_FakeKv()), db=_FakeDb()
    )
    client.configure(lambda: client.DEFAULT_BASE_URL, lambda: ctx.secrets.read(routes_mod.SECRET_KEY) or "")
    app = routes_mod.build_routes(ctx)
    return TestClient(app), ctx


def test_identity_is_still_required_for_status(ungated_app_client):
    tc, _ = ungated_app_client
    assert tc.get("/status").status_code == 401


def test_identity_is_still_required_for_settings(ungated_app_client):
    tc, _ = ungated_app_client
    assert tc.post("/settings", json={"service_secret": "x"}).status_code == 401


def test_identity_is_still_required_for_logout(ungated_app_client):
    tc, _ = ungated_app_client
    assert tc.post("/logout").status_code == 401


def test_identity_is_still_required_for_test_connection(ungated_app_client):
    tc, _ = ungated_app_client
    assert tc.post("/test").status_code == 401


def test_identity_is_still_required_for_mcp_json(ungated_app_client):
    """The route that embeds AW_WORKSPACE_API_KEY in its body when set
    (mcp/self_register.py) — the one existing route that MUST NOT become
    reachable by the same caller `GET /keys/{slot}` now is."""
    tc, _ = ungated_app_client
    assert tc.get("/mcp.json").status_code == 401


def test_identity_is_still_required_for_bulk_ingest_routes(ungated_app_client):
    tc, _ = ungated_app_client
    assert tc.post("/bulk-ingest/scan").status_code == 401
    assert tc.post("/bulk-ingest/run").status_code == 401
    assert tc.get("/bulk-ingest/status").status_code == 401
    assert tc.get("/bulk-ingest/report").status_code == 401


def test_identity_is_still_required_for_fs_sync_routes(ungated_app_client):
    tc, _ = ungated_app_client
    assert tc.post("/fs-sync/run").status_code == 401
    assert tc.get("/fs-sync/status").status_code == 401
    assert tc.get("/fs-sync/report").status_code == 401


def test_identity_is_still_required_for_mcp(ungated_app_client):
    tc, _ = ungated_app_client
    assert tc.get("/mcp").status_code == 401
    assert tc.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).status_code == 401


def test_pull_key_needs_no_identity_at_all(ungated_app_client):
    """The one deliberate exception — `GET /keys/{slot}` must answer a
    caller with NO identity, only the shared `X-Internal-Secret`, which is
    the entire reason this app's `auth_required` has to go `false`."""
    tc, ctx = ungated_app_client
    ctx.secrets.write("service_secret", "shh")
    resp = tc.get("/keys/bogus", headers={"x-internal-secret": "shh"})
    assert resp.status_code == 404, "reached the handler, past any identity gate"


# ---------------------------------------------------------------------------
# docs/design/execution-history-in-production-neo4j.md §4/§8 — the exec-sync
# doors. What is worth testing at the ROUTE level (the engine's own decisions
# are test_exec_sync.py's): that the DSN lands in the secret store and not in
# plain config, that the absent/present/empty contract on `/settings` does
# not clobber a key the caller did not send, and that the status surface
# reports degraded for an unreachable sentinel instead of a cheerful `None`.
# ---------------------------------------------------------------------------


class _FakeLease:
    def __init__(self):
        self.claims = []

    def claim(self, name):
        self.claims.append(name)
        return True

    def release(self, name):
        pass


@pytest.fixture()
def exec_app_client(app_client):
    tc, ctx = app_client
    ctx.state.lease = _FakeLease()
    return tc, ctx


def test_the_apmt_dsn_goes_to_the_secret_store_never_plain_config(exec_app_client):
    """A DSN carries a password, and app config is plain and cloud-syncable
    — same split `service_secret` already uses."""
    tc, ctx = exec_app_client
    resp = tc.post(
        "/settings",
        json={"service_secret": "s", "apmt_database_url": "postgresql://u:p@h/db"},
    )
    assert resp.status_code == 200
    assert ctx.secrets.read(routes_mod.APMT_DSN_KEY) == "postgresql://u:p@h/db"
    assert "apmt_database_url" not in ctx.config


def test_saving_settings_without_the_dsn_key_leaves_it_untouched(exec_app_client):
    """The clobber scar (memory knowledgeable-connector-settings-route-
    clobbers-service-secret): a key the caller did not send must be left
    alone, never cleared. An ordinary secret-rotation POST from the settings
    form sends only `service_secret`."""
    tc, ctx = exec_app_client
    ctx.secrets.write(routes_mod.APMT_DSN_KEY, "postgresql://kept@h/db")
    resp = tc.post("/settings", json={"service_secret": "rotated"})
    assert resp.status_code == 200
    assert ctx.secrets.read(routes_mod.APMT_DSN_KEY) == "postgresql://kept@h/db"


def test_sending_an_empty_dsn_clears_it(exec_app_client):
    tc, ctx = exec_app_client
    ctx.secrets.write(routes_mod.APMT_DSN_KEY, "postgresql://old@h/db")
    tc.post("/settings", json={"service_secret": "s", "apmt_database_url": ""})
    assert ctx.secrets.read(routes_mod.APMT_DSN_KEY) is None


def test_exec_sync_run_is_a_no_op_while_the_flag_is_off(exec_app_client):
    """§9 step 2 — the flag ships false and the tick must do nothing, not
    "nothing, successfully"."""
    tc, _ = exec_app_client
    body = tc.post("/exec-sync/run", json={}).json()
    assert body["blocked"] is True
    assert "exec_sync_enabled is false" in body["reason"]


def test_exec_sync_status_distinguishes_off_from_running_against_nothing(
    exec_app_client, monkeypatch
):
    """"The sync is not running" and "the sync is running against nothing"
    are different problems and must not read the same."""
    tc, ctx = exec_app_client

    async def _unreachable():
        return None, "HTTP 503"

    monkeypatch.setattr(client, "get_exec_status", _unreachable)

    off = tc.get("/exec-sync/status").json()
    assert off["enabled"] is False
    assert off["dsn_configured"] is False
    # Off is NOT degraded — a disabled feature must not page anyone.
    assert off["degraded"] is False

    ctx.config["exec_sync_enabled"] = True
    ctx.config["exec_sync_source_tenant"] = "tenant-bootstrap"
    ctx.secrets.write(routes_mod.APMT_DSN_KEY, "postgresql://u:p@h/db")
    on = tc.get("/exec-sync/status").json()
    assert on["enabled"] is True
    assert on["dsn_configured"] is True
    assert on["source_tenant"] == "tenant-bootstrap"
    # An unreachable sentinel IS degraded. A status surface that reported
    # `None` here would read as healthy, which is this workspace's named
    # failure mode.
    assert on["degraded"] is True
    assert on["error"] == "HTTP 503"


def test_exec_sync_status_is_degraded_when_the_sentinel_says_stale(
    exec_app_client, monkeypatch
):
    tc, ctx = exec_app_client
    ctx.config["exec_sync_enabled"] = True

    async def _stale():
        return {"staleness": {"state": "stale", "degraded": True, "age_seconds": 9999}}, None

    monkeypatch.setattr(client, "get_exec_status", _stale)
    assert tc.get("/exec-sync/status").json()["degraded"] is True

    async def _fresh():
        return {"staleness": {"state": "fresh", "degraded": False, "age_seconds": 30}}, None

    monkeypatch.setattr(client, "get_exec_status", _fresh)
    assert tc.get("/exec-sync/status").json()["degraded"] is False


async def _fresh_sentinel():
    return {"staleness": {"state": "fresh", "degraded": False, "age_seconds": 30}}, None


def _enable_exec_sync(ctx, monkeypatch):
    ctx.config["exec_sync_enabled"] = True
    ctx.config["exec_sync_source_tenant"] = "tenant-bootstrap"
    ctx.secrets.write(routes_mod.APMT_DSN_KEY, "postgresql://u:p@h/db")
    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)


def test_exec_sync_status_folds_the_scrub_dict_in(exec_app_client, monkeypatch):
    """routes.py's own docstring calls this `/exec-sync/status` endpoint the
    §8 doctor surface, and the CLI's `knowledgeable-exec-sync status` reads
    it — before this card it omitted `scrub` entirely."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)
    scrub = tc.get("/exec-sync/status").json()["scrub"]
    assert scrub == {
        "exact_values": 1,
        "exact_match_unavailable": None,
        "pattern_families": len(exec_scrub._PATTERNS),
        "vault_coverage_unknown": None,
        "vault_names_uncovered": [],
    }


def test_exec_sync_status_is_degraded_by_an_uncovered_vault_secret(exec_app_client, monkeypatch):
    """§6 point 1 / §12 risk 8: a vault secret with no argued pattern
    coverage must degrade this endpoint visibly. Verified in BOTH
    directions — registering the gap degrades it, clearing the gap clears
    it — because the whole finding behind this card is that a clean-looking
    report hid an absent check."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)

    async def _one_uncovered():
        return {"vault_coverage_unknown": None, "vault_names_uncovered": ["some-new-vault-secret"]}

    monkeypatch.setattr(vault_coverage, "coverage_status", _one_uncovered)
    body = tc.get("/exec-sync/status").json()
    assert body["degraded"] is True
    assert body["scrub"]["vault_names_uncovered"] == ["some-new-vault-secret"]

    monkeypatch.setattr(vault_coverage, "coverage_status", _default_coverage_status)
    assert tc.get("/exec-sync/status").json()["degraded"] is False


def test_exec_sync_status_is_degraded_when_vault_coverage_is_unknown(exec_app_client, monkeypatch):
    """The unreachable-secrets-app case must surface as degraded, never as
    a clean pass — the same property `test_vault_coverage.py`'s
    `test_coverage_status_degrades_visibly_when_the_secrets_app_is_unreachable`
    already pins at the function level, now pinned at this endpoint too."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)

    async def _unreachable_secrets_app():
        return {"vault_coverage_unknown": "could not reach the secrets app: boom",
                "vault_names_uncovered": None}

    monkeypatch.setattr(vault_coverage, "coverage_status", _unreachable_secrets_app)
    body = tc.get("/exec-sync/status").json()
    assert body["degraded"] is True
    assert body["scrub"]["vault_coverage_unknown"] == "could not reach the secrets app: boom"


def test_exec_sync_status_is_degraded_when_exact_match_is_unavailable(exec_app_client, monkeypatch):
    """`exact_match_unavailable` has had this exact hole since it was added
    — never once surfaced to a human. Same fix, same pass, same test shape
    as the vault-coverage cases above."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)
    monkeypatch.setattr(
        exec_scrub, "workspace_secret_values", lambda: ([], "secret store read failed: OSError")
    )
    body = tc.get("/exec-sync/status").json()
    assert body["degraded"] is True
    assert body["scrub"]["exact_match_unavailable"] == "secret store read failed: OSError"


def test_exec_sync_status_scrub_degradation_is_gated_on_enabled(exec_app_client, monkeypatch):
    """Mirrors what `degraded` already meant here before this card: "off is
    not degraded". A coverage gap only matters while the sync it protects is
    actually running."""
    tc, ctx = exec_app_client
    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)

    async def _one_uncovered():
        return {"vault_coverage_unknown": None, "vault_names_uncovered": ["some-new-vault-secret"]}

    monkeypatch.setattr(vault_coverage, "coverage_status", _one_uncovered)
    body = tc.get("/exec-sync/status").json()
    assert body["enabled"] is False
    assert body["degraded"] is False


def test_exec_sync_status_is_degraded_by_an_unreviewed_event_kind(
    exec_app_client, monkeypatch
):
    """§13.6's open-set alarm, at the endpoint the CLI and the doctor check
    both read. Same mechanism as `vault_names_uncovered` above, and verified
    in BOTH directions for the same reason: the finding behind that card was
    a clean-looking report hiding an absent check.

    The list rides the sentinel's `reported_counts` because the sweep itself
    is a 253 ms scan over 1.3M rows and this endpoint is polled far more
    often than the 10-minute tick."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)

    async def _with_new_kind():
        return (
            {
                "staleness": {"state": "fresh", "degraded": False, "age_seconds": 30},
                "reported_counts": {"event_kinds_unreviewed": ["brand.new.kind"]},
            },
            None,
        )

    monkeypatch.setattr(client, "get_exec_status", _with_new_kind)
    body = tc.get("/exec-sync/status").json()
    assert body["degraded"] is True
    assert body["event_kinds_unreviewed"] == ["brand.new.kind"]
    assert tc.get("/exec-sync/doctor").json()["ok"] is False

    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)
    cleared = tc.get("/exec-sync/status").json()
    assert cleared["degraded"] is False
    assert cleared["event_kinds_unreviewed"] == []


def test_exec_sync_status_is_degraded_by_an_unresolved_tool_identity(
    exec_app_client, monkeypatch
):
    """The per-tool-policy alarm, at the same endpoint
    (docs/design/exec-index-per-tool-policy.md §3.5/§8, slice 2).

    These names had their ARGUMENTS DROPPED because nothing could attribute
    them to an installed app — fail closed. §11 states the cost of that
    direction plainly: a gateway rename stops indexing arguments
    workspace-wide until someone reads the doctor, so this line IS the
    mitigation and has to be able to go red. Verified in both directions,
    same as the event-kind alarm above.
    """
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)

    async def _with_unresolved():
        return (
            {
                "staleness": {"state": "fresh", "degraded": False, "age_seconds": 30},
                "reported_counts": {
                    "policy_unresolved": ["mcp__a-new-gateway__aw__kb__search_nodes"],
                    "policy_denied_calls": 12,
                },
            },
            None,
        )

    monkeypatch.setattr(client, "get_exec_status", _with_unresolved)
    body = tc.get("/exec-sync/status").json()
    assert body["degraded"] is True
    assert body["policy_unresolved"] == ["mcp__a-new-gateway__aw__kb__search_nodes"]
    assert tc.get("/exec-sync/doctor").json()["ok"] is False

    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)
    cleared = tc.get("/exec-sync/status").json()
    assert cleared["degraded"] is False
    assert cleared["policy_unresolved"] == []


def test_a_sentinel_without_the_key_is_not_degraded(exec_app_client, monkeypatch):
    """A sentinel stamped by a connector older than §13 slice 1 has no
    `event_kinds_unreviewed` key at all. That must read as "nothing owed",
    not as a gap — otherwise the doctor check goes red on every deployment
    up to the one that fixes it, and a permanently red check detects
    nothing."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)

    async def _old_sentinel():
        return (
            {
                "staleness": {"state": "fresh", "degraded": False, "age_seconds": 30},
                "reported_counts": {"runs": 8, "tool_calls": 137},
            },
            None,
        )

    monkeypatch.setattr(client, "get_exec_status", _old_sentinel)
    body = tc.get("/exec-sync/status").json()
    assert body["event_kinds_unreviewed"] == []
    # Same contract for the policy alarm, for the same reason.
    assert body["policy_unresolved"] == []
    assert body["degraded"] is False


def test_reset_watermark_clears_the_frontier_and_refuses_without_a_source_tenant(
    exec_app_client, monkeypatch
):
    """§13.8's per-slice backfill trigger. Wired rather than left as a
    one-off, because slices 2 and 3 need the same button and the
    alternative is hand-rolling an HTTP call to a service-authed route with
    the internal secret.

    It refuses without `exec_sync_source_tenant` for the same reason
    `run_tick` does: the sentinel is per source tenant, and guessing it
    would stamp the wrong one — which reads as a clean reset and syncs
    nothing."""
    tc, ctx = exec_app_client
    posted: list[dict] = []

    async def _put_state(*, source_tenant, watermark, counts=None, last_error=None):
        posted.append(
            {
                "source_tenant": source_tenant,
                "watermark": watermark,
                "counts": counts,
                "last_error": last_error,
            }
        )
        return True, None

    monkeypatch.setattr(client, "put_exec_sync_state", _put_state)

    refused = tc.post("/exec-sync/reset-watermark", json={}).json()
    assert refused["ok"] is False
    assert "exec_sync_source_tenant" in refused["error"]
    assert posted == [], "it stamped a sentinel without knowing whose it was"

    _enable_exec_sync(ctx, monkeypatch)
    monkeypatch.setattr(client, "put_exec_sync_state", _put_state)
    body = tc.post("/exec-sync/reset-watermark", json={}).json()
    assert body["ok"] is True
    assert body["watermark"] is None
    assert posted[0]["source_tenant"] == "tenant-bootstrap"
    assert posted[0]["watermark"] is None
    assert posted[0]["counts"] == {}
    # The reason is recorded on the sentinel so an operator reading a null
    # watermark can tell a deliberate reset from a lost sentinel.
    assert "backfill" in posted[0]["last_error"]


def test_exec_sync_doctor_is_ok_when_nothing_is_degraded(exec_app_client, monkeypatch):
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)
    resp = tc.get("/exec-sync/doctor")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["detail"]["degraded"] is False


def test_exec_sync_doctor_is_not_ok_when_a_vault_secret_is_uncovered(exec_app_client, monkeypatch):
    """The sharper half of this card: §11 reopen trigger #3 is "the
    coverage registry rotting in doctor output" — this is the test that
    proves that trigger can now actually fire. Verified in both directions:
    registering the gap trips it, clearing the gap clears it."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)

    async def _one_uncovered():
        return {"vault_coverage_unknown": None, "vault_names_uncovered": ["some-new-vault-secret"]}

    monkeypatch.setattr(vault_coverage, "coverage_status", _one_uncovered)
    body = tc.get("/exec-sync/doctor").json()
    assert body["ok"] is False
    assert body["detail"]["scrub"]["vault_names_uncovered"] == ["some-new-vault-secret"]

    monkeypatch.setattr(vault_coverage, "coverage_status", _default_coverage_status)
    assert tc.get("/exec-sync/doctor").json()["ok"] is True


def test_exec_sync_doctor_is_not_ok_when_vault_coverage_is_unknown(exec_app_client, monkeypatch):
    """Same property as `test_exec_sync_status_is_degraded_when_vault_coverage_is_unknown`
    above, but at `doctor` — the unreachable-secrets-app case must surface as
    a failing check, never a clean pass, in the surface `aw-workspace-cli
    doctor` actually polls."""
    tc, ctx = exec_app_client
    _enable_exec_sync(ctx, monkeypatch)

    async def _unreachable_secrets_app():
        return {"vault_coverage_unknown": "could not reach the secrets app: boom",
                "vault_names_uncovered": None}

    monkeypatch.setattr(vault_coverage, "coverage_status", _unreachable_secrets_app)
    body = tc.get("/exec-sync/doctor").json()
    assert body["ok"] is False
    assert body["detail"]["scrub"]["vault_coverage_unknown"] == "could not reach the secrets app: boom"


def test_exec_sync_doctor_needs_no_identity_at_all(ungated_app_client, monkeypatch):
    """`contributes.doctor` dials this in-process with no headers at all
    (`_app_doctor_checks` in aw-workspace core) — a `_require_identity` gate
    here would 401 on every poll regardless of real health."""
    tc, ctx = ungated_app_client
    ctx.state.lease = _FakeLease()
    assert tc.get("/exec-sync/doctor").status_code == 200


def test_the_exec_sync_routes_require_identity(app_client):
    """`auth_required: false` on this mount means the framework gate does not
    protect these; each route carries its own `_require_identity`. Proven by
    REMOVING the override, same shape as the existing identity tests."""
    tc, ctx = app_client
    ctx.state.lease = _FakeLease()
    tc.app.dependency_overrides.clear()
    for method, path in (
        ("post", "/exec-sync/run"),
        ("get", "/exec-sync/status"),
        ("post", "/exec-sync/reconcile"),
        ("post", "/exec-sync/retention"),
    ):
        resp = getattr(tc, method)(path, json={}) if method == "post" else tc.get(path)
        assert resp.status_code == 401, f"{method.upper()} {path} -> {resp.status_code}"


# ---------------------------------------------------------------------------
# exec-index-per-tool-policy.md §9 slice 4 — the retro-purge door.
#
# Worth testing at the ROUTE level because this is the one route in the
# feature that takes a caller-supplied TOOL NAME, which §13.2's own purge
# deliberately refuses to do. The relaxation is argued (it shares ingest
# auth, and an ingest-secret holder can already overwrite both text
# properties through the MERGE upsert), but the shape of it is load-bearing:
# name-at-a-time, never a pattern, never a list. The server owns the
# predicate and therefore its validation; what these pin is that the door
# does not widen on the way through.
# ---------------------------------------------------------------------------


def test_the_retro_purge_passes_one_name_and_both_axis_flags_through(
    exec_app_client, monkeypatch
):
    """The wiring, and that neither axis is invented by the connector. An
    `output: false` opt-out must be able to purge results WITHOUT nulling
    arguments that were never the problem."""
    seen = {}

    async def _fake(*, name, args, result):
        seen.update(name=name, args=args, result=result)
        return {"ok": True, "dropped": {"result_text": 341}, "total": 341}, None

    monkeypatch.setattr(client, "post_exec_purge_tool_text", _fake)
    tc, _ = exec_app_client
    body = tc.post(
        "/exec-sync/purge-tool-text",
        json={"name": "remote_host_read_file", "result": True},
    ).json()
    assert body["ok"] is True
    assert seen == {"name": "remote_host_read_file", "args": False, "result": True}
    assert body["result"]["dropped"] == {"result_text": 341}


def test_the_retro_purge_refuses_a_missing_name_without_calling_the_service(
    exec_app_client, monkeypatch
):
    """A nameless purge must never reach the service. The failure mode this
    guards is not a crash — it is a request that the service might interpret
    as "everything", which is the one thing this route must never be able to
    express."""

    async def _never(**_kw):
        pytest.fail("called the purge with no name")

    monkeypatch.setattr(client, "post_exec_purge_tool_text", _never)
    tc, _ = exec_app_client
    for payload in ({}, {"name": ""}, {"name": "   "}, {"name": None}, {"result": True}):
        body = tc.post("/exec-sync/purge-tool-text", json=payload).json()
        assert body["ok"] is False, payload
        assert "name is required" in body["error"], payload


def test_the_retro_purge_does_not_validate_the_name_itself(exec_app_client, monkeypatch):
    """§12 risk 6's lesson on the write side: the SERVER owns the predicate,
    so the server owns its validation. A prefixed spelling is forwarded and
    400s there rather than being silently re-segmented here — a second copy
    of that rule in the connector is how the two drift and one of them
    re-learns `endswith`."""
    forwarded = []

    async def _fake(*, name, args, result):
        forwarded.append(name)
        return None, "HTTP 400: name must be a BARE tool name (one segment, no '__')"

    monkeypatch.setattr(client, "post_exec_purge_tool_text", _fake)
    tc, _ = exec_app_client
    body = tc.post(
        "/exec-sync/purge-tool-text",
        json={"name": "aw__aw_secrets__write_secret", "args": True},
    ).json()
    assert forwarded == ["aw__aw_secrets__write_secret"]
    assert body["ok"] is False
    assert "BARE tool name" in body["error"]


# ---------------------------------------------------------------------------
# exec-index-per-tool-policy.md §7 — the maintenance surface (slice 3).
#
# The half of the card Frederico asked for in his own words: a UI inside the
# connector's settings where the effective table can be seen and maintained by
# hand. Slices 1, 2 and 4 built a policy that enforces; none of them gave a
# human any way to look at it or disagree with it.
# ---------------------------------------------------------------------------


async def _sentinel_with(**reported):
    async def _get():
        return (
            {
                "staleness": {"state": "fresh", "degraded": False, "age_seconds": 30},
                "reported_counts": reported,
            },
            None,
        )
    return _get


def _policy_client(app_client, monkeypatch, tmp_path, tree=None):
    """A client whose policy table reads a FIXTURE tree, never the real
    `/opt/aw-workspace/apps` (§12 risk 10 / `tests/conftest.py`'s guard — a
    test that reads the machine it runs on is green only here and red in CI,
    and that has already reddened one release of this very module)."""
    import json as _json

    tc, ctx = app_client
    ctx.state.lease = _FakeLease()
    root = tree or (tmp_path / "apps")
    kb = root / "kb"
    kb.mkdir(parents=True, exist_ok=True)
    (kb / "mcp.json").write_text(
        _json.dumps({"mcpServers": {"kb": {"type": "http"}}}), encoding="utf-8")
    (kb / "aw-app.json").write_text(
        _json.dumps({
            "id": "kb",
            "contributes": {"mcp": {
                "indexing": [
                    {"tool": "update_knowledge_base", "call": False, "output": True,
                     "reason": "arguments carry whole documents"},
                    {"tool": "search_knowledge_base", "call": True, "output": True},
                ],
                "provides": ["search_knowledge_base", "list_documents"],
            }},
        }),
        encoding="utf-8",
    )
    secrets_dir = root / "secrets"
    secrets_dir.mkdir(parents=True, exist_ok=True)
    (secrets_dir / "mcp.json").write_text(
        _json.dumps({"mcpServers": {"aw-secrets": {"type": "http"}}}), encoding="utf-8")
    (secrets_dir / "aw-app.json").write_text(
        _json.dumps({"id": "secrets"}), encoding="utf-8")
    monkeypatch.setattr(exec_policy, "_apps_root", lambda: str(root))
    return tc, ctx


def test_exec_policy_table_renders_the_five_states(app_client, monkeypatch, tmp_path):
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)
    body = tc.get("/exec-policy").json()
    by_tool = {(r["app"], r["tool"]): r for r in body["rows"]}
    assert by_tool[(None, "write_secret")]["state"] == "floor"
    assert by_tool[(None, "write_secret")]["locked"] is True
    assert by_tool[("kb", "update_knowledge_base")]["state"] == "declared_restricted"
    assert by_tool[("kb", "search_knowledge_base")]["state"] == "declared_indexable"
    assert by_tool[("kb", "list_documents")]["state"] == "never_declared"
    # ...and the two that behave identically are still two different states.
    assert (
        by_tool[("kb", "search_knowledge_base")]["call"]
        == by_tool[("kb", "list_documents")]["call"]
        is True
    )


def test_put_override_rejects_write_secret(app_client, monkeypatch, tmp_path):
    """**The route-level floor refusal** (§2, §3 of the card's scope). The
    engine already wins against a lying override row — slice 4 pins that — but
    the invariant has to hold at the EDGE too, so the store's contents can be
    read at face value by a human. Nothing is written."""
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    resp = tc.request(
        "PUT", "/exec-policy/override",
        json={"app": "secrets", "tool": "write_secret", "call": True,
              "output": True, "note": "please index it"},
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["ok"] is False
    assert body["floor"] is True
    assert "floor" in body["error"]
    assert ctx.state.kv._store == {}, "nothing may reach the store"


@pytest.mark.parametrize("tool", sorted(exec_scrub.SECRET_TOOL_NAMES))
def test_put_override_rejects_every_floor_name(app_client, monkeypatch, tmp_path, tool):
    tc, _ctx = _policy_client(app_client, monkeypatch, tmp_path)
    resp = tc.request(
        "PUT", "/exec-policy/override",
        json={"app": "secrets", "tool": tool, "call": True, "output": True},
    )
    assert resp.status_code == 400
    assert resp.json()["floor"] is True


def test_put_override_rejects_a_prefixed_spelling_of_a_floor_name(
    app_client, monkeypatch, tmp_path
):
    """The 138-call defect was a name that did not match. A refusal keyed on
    string equality would be the same bug wearing this slice's hat."""
    tc, _ctx = _policy_client(app_client, monkeypatch, tmp_path)
    resp = tc.request(
        "PUT", "/exec-policy/override",
        json={"app": "secrets", "tool": "mcp__aw-gateway__aw__aw_secrets__write_secret",
              "call": True, "output": True},
    )
    assert resp.status_code == 400
    assert resp.json()["floor"] is True


def test_override_round_trips_through_the_state_facade(app_client, monkeypatch, tmp_path):
    """PUT -> the table reflects it -> DELETE -> it falls back to the manifest.

    And it rides `ctx.state.kv`, never `ctx.config`: §6 and §10 both rule the
    config blob out, because this app's own `POST /settings` has clobbered a
    sibling config key before.
    """
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)

    resp = tc.request(
        "PUT", "/exec-policy/override",
        json={"app": "kb", "tool": "search_knowledge_base", "call": False,
              "output": False, "note": "queries carry customer names"},
    )
    assert resp.status_code == 200
    assert resp.json()["override"]["note"] == "queries carry customer names"

    rows = {(r["app"], r["tool"]): r for r in tc.get("/exec-policy").json()["rows"]}
    row = rows[("kb", "search_knowledge_base")]
    assert row["state"] == "override"
    assert (row["call"], row["output"]) == (False, False)
    # What it overrode, shown beside it.
    assert row["declared"] == {"call": True, "output": True}
    assert row["override"]["actor"]

    # Nowhere near the config blob.
    assert ctx.config == {} or "exec_policy" not in str(ctx.config)
    assert any("exec_policy" in k for k in ctx.state.kv._store)

    resp = tc.request("DELETE", "/exec-policy/override",
                      json={"app": "kb", "tool": "search_knowledge_base"})
    assert resp.json() == {"ok": True, "cleared": True}
    rows = {(r["app"], r["tool"]): r for r in tc.get("/exec-policy").json()["rows"]}
    assert rows[("kb", "search_knowledge_base")]["state"] == "declared_indexable"


def test_an_override_reaches_the_sync_tick(app_client, monkeypatch, tmp_path):
    """The parameter slice 2 carried for exactly this moment. An override that
    the page can show but the tick does not consult would be the §11 failure
    in its purest form — a human edits a layer the system ignores."""
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    tc.request(
        "PUT", "/exec-policy/override",
        json={"app": "kb", "tool": "search_knowledge_base", "call": False,
              "output": False, "note": "n"},
    )
    seen = {}

    async def _capture(lease, **kwargs):
        seen.update(kwargs)
        return {"blocked": True, "reason": "captured"}

    monkeypatch.setattr(exec_sync, "run_tick", _capture)
    tc.post("/exec-sync/run", json={})
    assert seen["policy_overrides"] == {("kb", "search_knowledge_base"): (False, False)}
    assert seen["policy_overrides_error"] is None


def test_the_tick_records_the_identities_it_observed(app_client, monkeypatch, tmp_path):
    """§7 row population source (c) — and `policy_observed` is popped off the
    result rather than returned to the caller: a few hundred `(app, tool)`
    pairs are a UI row list, not part of a tick's report."""
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)

    async def _tick(lease, **kwargs):
        return {"blocked": False, "reason": None,
                "policy_observed": [["kb", "search_graph"], [None, "Bash"]]}

    monkeypatch.setattr(exec_sync, "run_tick", _tick)
    result = tc.post("/exec-sync/run", json={}).json()
    assert "policy_observed" not in result

    rows = {(r["app"], r["tool"]): r for r in tc.get("/exec-policy").json()["rows"]}
    assert "observed" in rows[("kb", "search_graph")]["listed_because"]
    assert (None, "Bash") in rows


def test_acknowledging_an_upstream_clears_the_doctor_without_permitting_it(
    app_client, monkeypatch, tmp_path
):
    """**The escalation §9 left to this slice.** Two of this gateway's 40 live
    upstream segments are BYOD linked-workspace federations with no
    `apps/<slug>/mcp.json`; they fail closed correctly and will NEVER resolve,
    so the doctor went red the first time a tick saw one and stayed red
    forever. An always-red check detects nothing.

    So: red before, green after acknowledging, and the name is still on the
    full `policy_unresolved` list — which is what proves the acknowledgement
    silenced a counter rather than opening a door.
    """
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    _enable_exec_sync(ctx, monkeypatch)
    name = "mcp__aw-gateway__aw__a_future_federation__x__browser_click"

    async def _sentinel():
        return (
            {
                "staleness": {"state": "fresh", "degraded": False, "age_seconds": 30},
                "reported_counts": {"policy_unresolved": [name]},
            },
            None,
        )

    monkeypatch.setattr(client, "get_exec_status", _sentinel)
    before = tc.get("/exec-sync/status").json()
    assert before["degraded"] is True
    assert before["policy_unresolved_actionable"] == [name]
    assert tc.get("/exec-sync/doctor").json()["ok"] is False

    resp = tc.request(
        "PUT", "/exec-policy/ack",
        json={"upstream": "a_future_federation",
              "reason": "BYOD linked-workspace federation — no apps/<slug>/mcp.json"},
    )
    assert resp.status_code == 200

    # The sentinel is what the doctor reads, and only a TICK restamps it. So
    # the next tick's own split is what turns the check green — simulated here
    # by the sentinel now reporting the name as known, which is exactly what
    # `PolicyTable.unresolved_split()` will put there.
    async def _sentinel_after():
        return (
            {
                "staleness": {"state": "fresh", "degraded": False, "age_seconds": 30},
                "reported_counts": {
                    "policy_unresolved": [name],
                    "policy_unresolved_known": [name],
                    "policy_unresolved_actionable": 0,
                },
            },
            None,
        )

    monkeypatch.setattr(client, "get_exec_status", _sentinel_after)
    after = tc.get("/exec-sync/status").json()
    assert after["policy_unresolved_actionable"] == []
    assert after["degraded"] is False
    assert tc.get("/exec-sync/doctor").json()["ok"] is True
    # Acknowledging is NOT permitting: the name is still reported as
    # unresolved, which means its arguments and result were still dropped.
    assert after["policy_unresolved"] == [name]
    assert after["policy_unresolved_known"] == [name]


def test_the_ack_reaches_the_tick_and_is_withdrawable(app_client, monkeypatch, tmp_path):
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    tc.request("PUT", "/exec-policy/ack",
               json={"upstream": "a_future_federation", "reason": "byod"})
    seen = {}

    async def _capture(lease, **kwargs):
        seen.update(kwargs)
        return {"blocked": True, "reason": "captured"}

    monkeypatch.setattr(exec_sync, "run_tick", _capture)
    tc.post("/exec-sync/run", json={})
    assert seen["policy_acknowledged"] == {"a_future_federation": "byod"}

    resp = tc.request("DELETE", "/exec-policy/ack",
                      json={"upstream": "a_future_federation"})
    assert resp.json() == {"ok": True, "cleared": True}
    tc.post("/exec-sync/run", json={})
    assert seen["policy_acknowledged"] == {}


def test_an_ack_without_a_reason_is_refused(app_client, monkeypatch, tmp_path):
    """It silences an alarm, so why it is safe has to be readable by the next
    person — §4's rule for a manifest opt-out, applied here."""
    tc, _ctx = _policy_client(app_client, monkeypatch, tmp_path)
    resp = tc.request("PUT", "/exec-policy/ack", json={"upstream": "x", "reason": ""})
    assert resp.status_code == 400
    assert "reason is required" in resp.json()["error"]


def test_an_unreadable_override_store_degrades_the_doctor(
    app_client, monkeypatch, tmp_path
):
    """A tick that could not read the override store enforced less than the
    operator configured, and under a blacklist that cannot be inferred from
    "zero overrides". Read live, so the answer does not depend on the sync
    having run."""
    tc, ctx = _policy_client(app_client, monkeypatch, tmp_path)
    _enable_exec_sync(ctx, monkeypatch)
    monkeypatch.setattr(client, "get_exec_status", _fresh_sentinel)
    assert tc.get("/exec-sync/doctor").json()["ok"] is True

    async def _degraded_get(name):
        return None

    monkeypatch.setattr(ctx.state.kv, "get", _degraded_get)
    monkeypatch.setattr(exec_policy_store, "_facade_degraded", lambda: True)
    body = tc.get("/exec-sync/status").json()
    assert body["policy_overrides_unreadable"] == exec_policy_store.UNREACHABLE
    assert body["degraded"] is True
    assert tc.get("/exec-sync/doctor").json()["ok"] is False


def test_the_policy_page_is_served_as_html(app_client, monkeypatch, tmp_path):
    tc, _ctx = _policy_client(app_client, monkeypatch, tmp_path)
    resp = tc.get("/policy-ui")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert "Exec-indexing policy" in resp.text


def test_the_policy_page_uses_no_modals(app_client, monkeypatch, tmp_path):
    """§12 risk 8 / the host's sandbox: the iframe is `allow-scripts
    allow-forms allow-same-origin` and NOT `allow-modals`, so
    `confirm()`/`prompt()`/`alert()` silently no-op. A confirmation that is a
    dead button is worse than none, so every edit here is an inline row."""
    from knowledgeable_app import policy_panel

    for banned in ("confirm(", "prompt(", "alert("):
        assert banned not in policy_panel.PANEL_HTML, (
            f"{banned} silently no-ops inside this iframe — use an inline row"
        )


def test_the_policy_page_carries_no_credential(app_client, monkeypatch, tmp_path):
    """It loads from the API origin, so relative fetches are same-origin and
    the apex cookie authorises them. A page that carries a token in its markup
    is a page that leaks one."""
    from knowledgeable_app import policy_panel

    for banned in ("X-Api-Key", "x-internal-secret", "Authorization", "Bearer "):
        assert banned not in policy_panel.PANEL_HTML


def test_the_policy_page_is_not_under_the_reserved_ui_prefix():
    """Core serves app ESM bundles from `/ui` and would shadow the route —
    which is why §7 names `/policy-ui` explicitly."""
    import json as _json

    spec = _json.loads(
        (Path(__file__).resolve().parent.parent / "windows" / "main.json").read_text()
    )
    srcs = [
        w["src"]
        for region in spec["regions"]
        for w in region["widgets"]
        if w.get("type") == "iframe"
    ]
    assert srcs == ["/api/apps/knowledgeable/policy-ui"]
    assert not any("/api/apps/knowledgeable/ui" in s for s in srcs)


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("GET", "/exec-policy", None),
        ("PUT", "/exec-policy/override", {"app": "kb", "tool": "t", "call": True, "output": True}),
        ("DELETE", "/exec-policy/override", {"app": "kb", "tool": "t"}),
        ("PUT", "/exec-policy/ack", {"upstream": "u", "reason": "r"}),
        ("DELETE", "/exec-policy/ack", {"upstream": "u"}),
        ("GET", "/policy-ui", None),
    ],
)
def test_every_policy_route_requires_identity(ungated_app_client, method, path, body):
    """`auth_required: false` on this app's config means the framework gate
    enforces nothing for the mount, so each route carries its own dependency
    (see routes.py's module docstring). An unauthenticated caller must not be
    able to read — let alone edit — what gets indexed."""
    tc, _ctx = ungated_app_client
    resp = tc.request(method, path, json=body)
    assert resp.status_code == 401
