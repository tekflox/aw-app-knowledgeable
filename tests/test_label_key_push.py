"""The re-assert loop: read the topic labeller's ap-mt key from this
workspace's own vault (via `key_vault.read_secret` — see test_key_vault.py
for that module's own tests, since the vault read was factored out of here
and its two twins, `playground_key_push.py`/`ingest_key_push.py`, in the
pull-through design) and push it to production aw-knowledgeable — never once
at install, always on a fixed interval, and never touching disk (2026-10-03,
closing the LLM-key veto; see label_key_push.py's own docstring for the
design this locks in).

No real network, no real event loop plugin: same style as test_client.py —
async calls are driven through the ``_run`` helper from plain ``def`` tests,
because this repo runs under plain pytest with no pytest-asyncio.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import key_vault  # noqa: E402
from knowledgeable_app import label_key_push as lkp  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# push_once — the two hops chained, and what "nothing to push" looks like
# ---------------------------------------------------------------------------


def test_push_once_reads_then_pushes_and_reports_success(monkeypatch):
    calls = []

    async def fake_read(name, *, reason):
        return "the-real-key"

    async def fake_push(value):
        calls.append(value)
        return True, None

    monkeypatch.setattr(key_vault, "read_secret", fake_read)
    monkeypatch.setattr(client, "push_label_key", fake_push)

    ok = _run(lkp.push_once())

    assert ok is True
    assert calls == ["the-real-key"], "push_once must forward exactly the value it read"


def test_push_once_is_a_noop_when_the_vault_has_nothing(monkeypatch):
    async def fake_read(name, *, reason):
        return None

    pushed = []

    async def fake_push(value):
        pushed.append(value)
        return True, None

    monkeypatch.setattr(key_vault, "read_secret", fake_read)
    monkeypatch.setattr(client, "push_label_key", fake_push)

    ok = _run(lkp.push_once())

    assert ok is False
    assert pushed == [], "nothing to push must not call aw-knowledgeable at all"


def test_push_once_reports_failure_when_aw_knowledgeable_refuses(monkeypatch):
    async def fake_read(name, *, reason):
        return "the-real-key"

    async def fake_push(value):
        return False, "HTTP 401: unauthorized"

    monkeypatch.setattr(key_vault, "read_secret", fake_read)
    monkeypatch.setattr(client, "push_label_key", fake_push)

    ok = _run(lkp.push_once())

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

    monkeypatch.setattr(lkp, "push_once", fake_push_once)
    monkeypatch.setattr(lkp, "PUSH_INTERVAL_S", 0)

    async def drive():
        task = asyncio.ensure_future(lkp.run_forever())
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
