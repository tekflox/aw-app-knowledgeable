"""The §6 point 1 correction's monitored replacement guarantee
(docs/design/execution-history-in-production-neo4j.md §6 point 1, §12 risk
8): a vault secret with no argued, TESTED pattern coverage must be visible
as `vault_names_uncovered`, not assumed covered.

Two things this file proves, and why both matter:

* The registry cannot drift from the pattern catalog — every declared
  family is a real `exec_scrub` kind, and every declared family is proven
  here to actually redact a representative INVENTED shape of the
  credential it claims to cover. A registry entry with no such proof is an
  argument, not a measurement — exactly the standard
  `tests/test_exec_scrub.py` already holds the pattern catalog to.
* The list-never-read contract (§12 risk 8): `coverage_status()` only ever
  calls a names-only endpoint, and degrades to `vault_coverage_unknown`
  rather than silent success when that endpoint cannot be reached.

No real network, no real event loop plugin — same style as
`test_key_vault.py`: async calls driven through the `_run` helper from plain
`def` tests, this repo runs under plain pytest with no pytest-asyncio.

**No real credential appears anywhere in this file.** Every value below is
invented for the test, same discipline as `test_exec_scrub.py`.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import exec_scrub, key_vault, vault_coverage  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# The registry itself — cannot drift from the pattern catalog unnoticed.
# ---------------------------------------------------------------------------

#: Representative INVENTED shape per registered vault secret name, one per
#: declared family. Proves each registry entry's declared coverage actually
#: redacts today, so the registry cannot drift from the pattern catalog
#: without this test going red.
_REPRESENTATIVE_SHAPES = {
    "aw-stack-neo4j-auth": {
        "env-assignment": "NEO4J_AUTH=neo4j/invented-registry-test-pw",
        "url-userinfo": "bolt://neo4j:invented-registry-test-pw@aw-stack-aw-neo4j-1:7687",
    },
}


def test_every_registry_entry_only_names_real_pattern_families():
    for name, families in vault_coverage.VAULT_COVERAGE_REGISTRY.items():
        for family in families:
            assert family in exec_scrub.PATTERN_KINDS, (
                f"{name!r} claims coverage from {family!r}, which is not a real "
                "exec_scrub pattern kind — the registry has drifted from the catalog"
            )


def test_every_registry_entry_has_a_representative_shape_fixture():
    """A registry entry with no fixture here would be an unverified claim —
    this is what stops that from shipping silently."""
    missing = set(vault_coverage.VAULT_COVERAGE_REGISTRY) - set(_REPRESENTATIVE_SHAPES)
    assert not missing, (
        f"{missing} has no representative-shape test — add one before "
        "registering coverage for it"
    )


@pytest.mark.parametrize("name", sorted(vault_coverage.VAULT_COVERAGE_REGISTRY))
def test_every_declared_family_actually_redacts_its_representative_shape(name):
    families = vault_coverage.VAULT_COVERAGE_REGISTRY[name]
    shapes = _REPRESENTATIVE_SHAPES[name]
    scrubber = exec_scrub.Scrubber([])  # pattern pass only — no store values
    for family in families:
        text = shapes[family]
        out = scrubber.scrub(text)
        assert out != text, (
            f"{name!r} declares {family!r} covers it, but this shape was not "
            f"redacted: {text!r}"
        )
        assert f"<REDACTED:{family}>" in out, out


# ---------------------------------------------------------------------------
# uncovered_names — pure, no I/O.
# ---------------------------------------------------------------------------


def test_uncovered_names_excludes_registered_names():
    names = ["aw-stack-neo4j-auth", "some-new-vault-secret"]
    assert vault_coverage.uncovered_names(names) == ["some-new-vault-secret"]


def test_uncovered_names_is_empty_when_everything_is_covered():
    assert vault_coverage.uncovered_names(["aw-stack-neo4j-auth"]) == []


def test_uncovered_names_is_sorted_and_deduplicated():
    names = ["zzz-secret", "aaa-secret", "zzz-secret"]
    assert vault_coverage.uncovered_names(names) == ["aaa-secret", "zzz-secret"]


def test_uncovered_names_ignores_blank_entries():
    assert vault_coverage.uncovered_names(["", None, "a-secret"]) == ["a-secret"]


# ---------------------------------------------------------------------------
# _list_secret_names — the loopback call. List, never read (§12 risk 8): this
# is the only HTTP call this module ever makes.
# ---------------------------------------------------------------------------


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

    async def get(self, url, headers=None):
        _FakeAsyncClient._CALLS.append({"url": url, "headers": headers})
        return _FakeAsyncClient._QUEUE.pop(0)


@pytest.fixture(autouse=True)
def _fresh_fake_client(monkeypatch):
    _FakeAsyncClient._CALLS = []
    _FakeAsyncClient._QUEUE = []
    monkeypatch.setattr(vault_coverage, "httpx", type(
        "M", (), {"AsyncClient": _FakeAsyncClient, "HTTPError": httpx.HTTPError},
    ))
    monkeypatch.setattr(key_vault, "workspace_env", lambda name: {
        "AW_WORKSPACE_API_URL": "http://workspace:9030",
        "AW_WORKSPACE_API_KEY": "ws-api-key",
    }.get(name, ""))
    yield


def test_list_secret_names_sends_the_stable_caller_identity_and_no_body():
    """Names only, no approval — the route takes no request body at all."""
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"secrets": [{"name": "aw-stack-neo4j-auth"}], "count": 1})
    )
    names, error = _run(vault_coverage._list_secret_names())
    assert error is None
    assert names == ["aw-stack-neo4j-auth"]
    call = _FakeAsyncClient._CALLS[0]
    assert call["url"] == "http://workspace:9030/api/apps/secrets/secrets"
    assert call["headers"] == {"X-Api-Key": "ws-api-key", "X-Aw-Caller-Agent": "aw-app-knowledgeable"}


def test_list_secret_names_drops_entries_with_no_name():
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"secrets": [{"name": "a"}, {"description": "no name field"}], "count": 2})
    )
    names, error = _run(vault_coverage._list_secret_names())
    assert error is None
    assert names == ["a"]


def test_list_secret_names_returns_error_without_workspace_publication(monkeypatch):
    monkeypatch.setattr(key_vault, "workspace_env", lambda name: "")
    names, error = _run(vault_coverage._list_secret_names())
    assert names is None
    assert error is not None
    assert _FakeAsyncClient._CALLS == []


def test_list_secret_names_returns_error_on_backend_error():
    _FakeAsyncClient._QUEUE.append(_FakeResponse(500, text="internal error"))
    names, error = _run(vault_coverage._list_secret_names())
    assert names is None
    assert "500" in error


def test_list_secret_names_returns_error_on_connection_failure(monkeypatch):
    class _Boom(_FakeAsyncClient):
        async def get(self, *a, **k):
            raise httpx.HTTPError("connect failed")

    monkeypatch.setattr(vault_coverage, "httpx", type(
        "M", (), {"AsyncClient": _Boom, "HTTPError": httpx.HTTPError},
    ))
    names, error = _run(vault_coverage._list_secret_names())
    assert names is None
    assert "could not reach" in error


# ---------------------------------------------------------------------------
# coverage_status — the combined result a sync tick reports.
# ---------------------------------------------------------------------------


def test_coverage_status_reports_uncovered_names():
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(
            200,
            {
                "secrets": [
                    {"name": "aw-stack-neo4j-auth"},
                    {"name": "some-new-vault-secret"},
                ],
                "count": 2,
            },
        )
    )
    status = _run(vault_coverage.coverage_status())
    assert status == {
        "vault_coverage_unknown": None,
        "vault_names_uncovered": ["some-new-vault-secret"],
    }


def test_coverage_status_is_fully_covered_when_every_name_is_registered():
    _FakeAsyncClient._QUEUE.append(
        _FakeResponse(200, {"secrets": [{"name": "aw-stack-neo4j-auth"}], "count": 1})
    )
    status = _run(vault_coverage.coverage_status())
    assert status == {"vault_coverage_unknown": None, "vault_names_uncovered": []}


def test_coverage_status_degrades_visibly_when_the_secrets_app_is_unreachable(monkeypatch):
    """Degrade visibly, never silently — an unreachable check must not look
    like a clean, fully-covered pass."""
    monkeypatch.setattr(key_vault, "workspace_env", lambda name: "")
    status = _run(vault_coverage.coverage_status())
    assert status["vault_coverage_unknown"] is not None
    assert status["vault_names_uncovered"] is None
