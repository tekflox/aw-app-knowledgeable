"""The execution-history sync engine (docs/design/execution-history-in-
production-neo4j.md §4/§8).

No real Postgres and no real HTTP: the engine's own decisions are what this
file is about — what it refuses to do, how it bounds a batch, where the
watermark lands when a batch fails, and that every text field goes through
the scrubber on the way out. A fake connection object stands in for psycopg2,
driven from plain ``def`` tests through the ``_run`` helper, same style as the
rest of this suite (this repo runs under plain pytest with no pytest-asyncio).

The scrubber itself is tested next door, adversarially, in
``test_exec_scrub.py``. What is tested HERE is that the sync actually calls
it on every field that carries text — a scrubber nothing routes through is
the same as no scrubber.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import event_kinds, exec_scrub, exec_sync, vault_coverage  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


SOURCE_TENANT = "tenant-bootstrap"
DSN = "postgresql://aw:invented@aw-remote-host-postgres:5432/agents_platform_multitenant"
#: Invented, as in test_exec_scrub.py. Long enough to clear
#: MIN_EXACT_MATCH_LEN so the exact-match pass actually engages.
STORE_SECRET = "invented-store-secret-for-the-sync-test"


class _FakeLease:
    def __init__(self, claimable=True):
        self.claimable = claimable
        self.claimed = []
        self.released = []

    def claim(self, name):
        self.claimed.append(name)
        return self.claimable

    def release(self, name):
        self.released.append(name)


class _FakeCursor:
    def __init__(self, conn, name=None):
        self._conn = conn
        self._rows: list = []
        self.closed = False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        if "FROM runs r" in sql and "count(" not in sql:
            self._rows = list(self._conn.runs)
        elif "row_number()" in sql:
            ids = set((params or {}).get("run_ids") or [])
            self._rows = [r for r in self._conn.tool_calls if r[0] in ids]
        elif "e.kind = %(kind)s" in sql:
            # §13 slice 3's `SQL_TOOL_RESULTS`. Matched on the bound-kind
            # predicate rather than on the word "tool_result", because the
            # whole point of binding `event_kinds.RESULT_KIND` is that the
            # kind is NOT a literal in the SQL — a fake that matched on the
            # literal would go green the day someone put one back.
            ids = set((params or {}).get("run_ids") or [])
            assert (params or {}).get("kind") == "tool_result", (
                f"the result query bound kind={(params or {}).get('kind')!r} — it "
                f"must come from the registry, not from a literal"
            )
            self._rows = [r for r in self._conn.tool_results if r[0] in ids]
        elif "SELECT DISTINCT e.kind" in sql:
            # §13.6's open-set sweep. Returns whatever kinds this fake tenant
            # has "emitted", including ones the registry has never seen — that
            # is the whole point of the alarm.
            self._rows = [(k,) for k in self._conn.event_kinds]
        elif "e.kind = ANY" in sql and "count(" not in sql:
            ids = set((params or {}).get("run_ids") or [])
            kinds = set((params or {}).get("kinds") or [])
            # The fake enforces what the SQL claims: an unshipped kind must
            # never come back from this query, because the query binds the
            # registry's own shipped set.
            self._rows = [
                r for r in self._conn.events if r[1] in ids and r[2] in kinds
            ]
        elif "payload->>" in sql or "payload::text" in sql:
            # The fake refuses what production refuses. `run_events.payload` is
            # `json` (text-backed) and holds literal `\u0000`, so ANY
            # server-side extraction parses the whole document and fails. A
            # rollup query that came back here would pass a mock and crash the
            # real backfill — which is exactly what happened on 2026-10-18.
            raise AssertionError(
                "server-side payload extraction is not usable on this corpus: "
                f"{sql.strip()[:160]}"
            )
        elif "count(" in sql:
            self._rows = [(self._conn.counts.pop(0),)]
        else:  # pragma: no cover - a query this fake was not taught
            raise AssertionError(f"unexpected SQL: {sql[:120]}")

    def __iter__(self):
        return iter(self._rows)

    def fetchone(self):
        return self._rows[0]

    def close(self):
        self.closed = True


class _FakeConn:
    def __init__(
        self,
        runs=(),
        tool_calls=(),
        counts=(),
        events=(),
        event_kinds=None,
        tool_results=(),
    ):
        self.runs = list(runs)
        self.tool_calls = list(tool_calls)
        #: §13 slice 3 — `(run_id, payload, ts)` rows in `SQL_TOOL_RESULTS`'
        #: column order. Empty by default, so every pre-slice-3 test in this
        #: file goes on describing a corpus with no results in it.
        self.tool_results = list(tool_results)
        self.counts = list(counts)
        self.events = list(events)
        # Default: exactly the registry's own shipped set, so an ordinary
        # tick reports no unreviewed kind. A test that wants the alarm passes
        # its own list including something unregistered.
        self.event_kinds = (
            list(event_kinds)
            if event_kinds is not None
            else list(exec_sync.event_kinds.shipped_kinds())
        )
        self.executed: list = []
        self.closed = False

    def cursor(self, name=None):
        return _FakeCursor(self, name)

    def close(self):
        self.closed = True


def _pg_row(
    run_id="run-1",
    *,
    status="success",
    started="2026-10-18T10:00:00+00:00",
    ended="2026-10-18T10:10:00+00:00",
    run_input=None,
    run_output=None,
    parent=None,
    qa=None,
    session="sess-1",
    card="card-1",
):
    """One row in `SQL_RUNS`' column order — the order is part of the
    contract between the SQL and `build_run_row`, so the fixture mirrors it
    positionally rather than by name, exactly as the real cursor does."""
    return (
        run_id, "coder-opus", status, "claude-opus-5", "software-engineering",
        "success", 3, 1.25, 1000, 2000,
        datetime.fromisoformat(started), datetime.fromisoformat(ended) if ended else None,
        parent, qa, session, card,
        run_input if run_input is not None else {"prompt": "do the thing"},
        run_output if run_output is not None else {"text": "done"},
    )


async def _default_coverage_status():
    return {"vault_coverage_unknown": None, "vault_names_uncovered": []}


@pytest.fixture(autouse=True)
def _wire_fakes(monkeypatch):
    """Point the engine at fakes for Postgres, the HTTP client and the
    secret store. The secret-store fake is what keeps a real credential out
    of this test entirely. The vault-coverage fake keeps this suite from
    depending on whatever AW_WORKSPACE_API_URL/KEY happen to be set in the
    ambient environment — the real check is exercised in
    test_vault_coverage.py."""
    monkeypatch.setattr(
        exec_scrub, "workspace_secret_values", lambda: ([STORE_SECRET], None)
    )
    monkeypatch.setattr(client, "configured", lambda: True)
    monkeypatch.setattr(client, "_secret_resolver", lambda: "fake-secret")
    monkeypatch.setattr(vault_coverage, "coverage_status", _default_coverage_status)


class _Calls:
    def __init__(self):
        self.ingest: list[dict] = []
        self.sync_state: list[dict] = []
        self.status_result = ({"watermark": None, "graph_counts": {}}, None)
        self.ingest_result = (True, None)


@pytest.fixture()
def calls(monkeypatch):
    rec = _Calls()

    async def _get_status():
        return rec.status_result

    async def _post_ingest(*, runs, tool_calls, called, events=None):
        rec.ingest.append(
            {
                "runs": runs,
                "tool_calls": tool_calls,
                "called": called,
                "events": events or [],
            }
        )
        return rec.ingest_result

    async def _put_state(*, source_tenant, watermark, counts=None, last_error=None):
        rec.sync_state.append(
            {
                "source_tenant": source_tenant,
                "watermark": watermark,
                "counts": counts,
                "last_error": last_error,
            }
        )
        return True, None

    monkeypatch.setattr(client, "get_exec_status", _get_status)
    monkeypatch.setattr(client, "post_exec_ingest", _post_ingest)
    monkeypatch.setattr(client, "put_exec_sync_state", _put_state)
    return rec


def _tick(conn, monkeypatch, **kwargs):
    monkeypatch.setattr(exec_sync, "connect", lambda dsn: conn)
    params = {"dsn": DSN, "source_tenant": SOURCE_TENANT, "enabled": True}
    params.update(kwargs)
    return _run(exec_sync.run_tick(_FakeLease(), **params))


# ---------------------------------------------------------------------------
# What it refuses. Each of these would otherwise be a silent wrong answer
# rather than a visible failure, which is the only reason they are separate
# tests instead of one.
# ---------------------------------------------------------------------------


def test_the_flag_ships_off_and_a_disabled_tick_is_a_no_op(calls, monkeypatch):
    monkeypatch.setattr(exec_sync, "connect", lambda dsn: pytest.fail("connected while disabled"))
    result = _run(
        exec_sync.run_tick(_FakeLease(), dsn=DSN, source_tenant=SOURCE_TENANT, enabled=False)
    )
    assert result["blocked"] is True
    assert "exec_sync_enabled is false" in result["reason"]
    assert calls.ingest == [] and calls.sync_state == []


def test_a_missing_dsn_blocks_rather_than_syncing_nothing_successfully(calls):
    result = _run(
        exec_sync.run_tick(_FakeLease(), dsn="", source_tenant=SOURCE_TENANT, enabled=True)
    )
    assert result["blocked"] is True
    assert "apmt_database_url" in result["reason"]


def test_a_missing_source_tenant_blocks_rather_than_defaulting(calls):
    """§12 risk 7 — the sharp one. A defaulted AP-MT tenant reads ZERO rows
    and reports a clean tick, so this failure has to be loud or it is
    invisible. Never `or "tenant-bootstrap"`."""
    result = _run(exec_sync.run_tick(_FakeLease(), dsn=DSN, source_tenant="", enabled=True))
    assert result["blocked"] is True
    assert "exec_sync_source_tenant" in result["reason"]
    assert "tenant-bootstrap" not in str(result.get("watermark"))


def test_a_missing_service_secret_blocks(calls, monkeypatch):
    monkeypatch.setattr(client, "configured", lambda: False)
    result = _run(
        exec_sync.run_tick(_FakeLease(), dsn=DSN, source_tenant=SOURCE_TENANT, enabled=True)
    )
    assert result["blocked"] is True
    assert "service secret" in result["reason"]


def test_an_overlapping_tick_is_refused_and_the_lease_is_not_released_twice(calls):
    lease = _FakeLease(claimable=False)
    result = _run(
        exec_sync.run_tick(lease, dsn=DSN, source_tenant=SOURCE_TENANT, enabled=True)
    )
    assert result["blocked"] is True
    assert "already in progress" in result["reason"]
    assert lease.released == [], "released a lease it never claimed"


def test_an_unreachable_sentinel_blocks_rather_than_syncing_from_scratch(calls, monkeypatch):
    """A tick that cannot read the watermark must NOT fall back to "no
    watermark" — that would re-read and re-post the entire 28k-run history
    on every transient blip, against a single-worker service."""
    calls.status_result = (None, "HTTP 503")
    monkeypatch.setattr(exec_sync, "connect", lambda dsn: pytest.fail("connected without a watermark"))
    result = _run(
        exec_sync.run_tick(_FakeLease(), dsn=DSN, source_tenant=SOURCE_TENANT, enabled=True)
    )
    assert result["blocked"] is True
    assert "sentinel" in result["reason"]


# ---------------------------------------------------------------------------
# The happy path, and the scrubber actually being in it.
# ---------------------------------------------------------------------------


def test_a_tick_posts_runs_tool_calls_and_called_then_stamps_the_sentinel(calls, monkeypatch):
    conn = _FakeConn(
        runs=[_pg_row("run-1"), _pg_row("run-2", started="2026-10-18T11:00:00+00:00",
                                        ended="2026-10-18T11:05:00+00:00")],
        tool_calls=[
            ("run-1", {"name": "Bash", "input": {"command": "ls"}},
             datetime(2026, 10, 18, 10, 1, tzinfo=timezone.utc), 1),
        ],
    )
    result = _tick(conn, monkeypatch)

    assert result["blocked"] is False
    assert result["runs"] == 2
    assert result["tool_calls"] == 1
    assert result["called"] == 1
    assert conn.closed is True

    assert len(calls.ingest) == 1
    posted = calls.ingest[0]
    assert [r["id"] for r in posted["runs"]] == ["run-1", "run-2"]
    # `agent_slug` <- `source_slug`: §12 risk 2, encoded in exactly one place.
    assert posted["runs"][0]["agent_slug"] == "coder-opus"
    assert posted["runs"][0]["card_page_id"] == "card-1"

    # The watermark is the max over BOTH timestamp columns, and is stamped
    # once at the end.
    assert len(calls.sync_state) == 1
    assert calls.sync_state[0]["watermark"] == "2026-10-18T11:05:00+00:00"
    assert calls.sync_state[0]["source_tenant"] == SOURCE_TENANT
    assert calls.sync_state[0]["last_error"] is None


def test_every_text_field_goes_through_the_scrubber(calls, monkeypatch):
    """A scrubber nothing routes through is the same as no scrubber. Four
    fields, four real shapes: a stored secret in run input, an inline
    credential in run output, a stored secret inside a Bash argument, and a
    secret-store tool call whose arguments must be dropped entirely."""
    conn = _FakeConn(
        runs=[
            _pg_row(
                "run-1",
                run_input={"prompt": f"the password is {STORE_SECRET}"},
                run_output={"text": "ran docker run -e NEO4J_AUTH=neo4j/invented-inline-pw x"},
            )
        ],
        tool_calls=[
            ("run-1", {"name": "Bash", "input": {"command": f"psql '{STORE_SECRET}'"}},
             datetime(2026, 10, 18, 10, 1, tzinfo=timezone.utc), 1),
            ("run-1", {"name": "read_secret", "input": {"name": "neo4j_password"}},
             datetime(2026, 10, 18, 10, 2, tzinfo=timezone.utc), 2),
        ],
    )
    result = _tick(conn, monkeypatch)
    assert result["blocked"] is False

    posted = calls.ingest[0]
    blob = str(posted)
    assert STORE_SECRET not in blob, "a stored secret reached the ingest payload"
    assert "invented-inline-pw" not in blob, "an inline credential reached the ingest payload"
    # Retrieval value survives.
    assert "docker run" in blob and "NEO4J_AUTH" in blob
    # The secret-store tool's NAME loads; its arguments do not.
    by_seq = {tc["seq"]: tc for tc in posted["tool_calls"]}
    assert by_seq[2]["name"] == "read_secret"
    assert by_seq[2]["args_text"] is None
    assert by_seq[1]["args_text"] is not None

    # And the degraded/healthy state of the scrub itself is reported, not
    # assumed — §8's observability.
    assert result["scrub"]["exact_values"] == 1
    assert result["scrub"]["exact_match_unavailable"] is None


def test_a_degraded_scrub_is_reported_on_the_tick_result(calls, monkeypatch):
    """Silent degradation is this workspace's named failure mode. A tick
    whose exact-match pass could not load must still run — and must say so."""
    monkeypatch.setattr(
        exec_scrub, "workspace_secret_values", lambda: ([], "secret store read failed: OSError")
    )
    conn = _FakeConn(runs=[_pg_row("run-1")])
    result = _tick(conn, monkeypatch)
    assert result["scrub"]["exact_match_unavailable"] == "secret store read failed: OSError"


def test_vault_names_uncovered_is_reported_alongside_exact_match_unavailable(calls, monkeypatch):
    """§6 point 1 correction / §12 risk 8 — the monitored replacement for the
    vault's lost exact-match guarantee lives in the same status dict as
    `exact_match_unavailable`, checked on every tick."""
    async def _one_uncovered():
        return {"vault_coverage_unknown": None, "vault_names_uncovered": ["some-new-vault-secret"]}

    monkeypatch.setattr(vault_coverage, "coverage_status", _one_uncovered)
    conn = _FakeConn(runs=[_pg_row("run-1")])
    result = _tick(conn, monkeypatch)
    assert result["scrub"]["vault_names_uncovered"] == ["some-new-vault-secret"]
    assert result["scrub"]["vault_coverage_unknown"] is None
    # Unrelated to and does not displace the exact-match status it sits beside.
    assert result["scrub"]["exact_match_unavailable"] is None


def test_vault_coverage_unknown_is_reported_when_the_secrets_app_is_unreachable(calls, monkeypatch):
    """Degrade visibly, never silently (§12 risk 8): a tick that could not
    even ask `list_secrets` must not look like a clean, fully-covered pass."""
    async def _unreachable():
        return {"vault_coverage_unknown": "could not reach the secrets app: boom", "vault_names_uncovered": None}

    monkeypatch.setattr(vault_coverage, "coverage_status", _unreachable)
    conn = _FakeConn(runs=[_pg_row("run-1")])
    result = _tick(conn, monkeypatch)
    assert result["scrub"]["vault_coverage_unknown"] == "could not reach the secrets app: boom"
    assert result["scrub"]["vault_names_uncovered"] is None


def test_nul_bytes_are_stripped_client_side(calls, monkeypatch):
    """`run_events.payload` contains literal NUL (§12 risk 2). psycopg has
    already parsed the document client-side, so this is reachable here — and
    a NUL reaching Neo4j fails the whole transaction, not one row."""
    conn = _FakeConn(
        runs=[_pg_row("run-1", run_output={"text": "before\x00after"})],
        tool_calls=[
            ("run-1", {"name": "Bash", "input": {"command": "echo a\x00b"}},
             datetime(2026, 10, 18, 10, 1, tzinfo=timezone.utc), 1),
        ],
    )
    _tick(conn, monkeypatch)
    posted = calls.ingest[0]
    # No real NUL BYTE anywhere in the payload — that is the property that
    # matters, because it is the one Neo4j rejects.
    assert "\x00" not in str(posted)
    # A `json` column arrives as a dict, so it is re-serialised and the NUL
    # comes out as the six-character escape `\u0000` rather than a byte. That
    # is already safe (it is text, not a NUL), so it is asserted as the
    # expected shape rather than "fixed" into a strip that would have to
    # re-parse every payload to reach it.
    assert posted["runs"][0]["output_text"] == '{"text": "before\\u0000after"}'
    assert posted["tool_calls"][0]["args_text"] == '{"command": "echo a\\u0000b"}'


def test_nul_bytes_are_stripped_when_the_column_is_already_text(calls, monkeypatch):
    """The other arm of the same rule: a `json` column that comes back as a
    STRING (psycopg configuration, or a text column) carries a real NUL byte,
    and `_clean` is what removes it before it can fail a whole transaction."""
    conn = _FakeConn(runs=[_pg_row("run-1", run_output="before\x00after")])
    _tick(conn, monkeypatch)
    assert calls.ingest[0]["runs"][0]["output_text"] == "beforeafter"


def test_a_quiet_tick_still_stamps_the_sentinel(calls, monkeypatch):
    """Freshness means "the sync ran", not "the sync wrote". Without this a
    quiet night reads as six consecutive missed passes and trips §8's
    staleness alarm for no reason — the classic dead-man's-switch
    false positive."""
    result = _tick(_FakeConn(runs=[]), monkeypatch)
    assert result["blocked"] is False
    assert result["reason"] == "no runs due"
    assert len(calls.sync_state) == 1


# ---------------------------------------------------------------------------
# Batching (§12 risk 4) — bounded by BYTES, because run text is wildly
# variable and an over-large batch does not fail loudly: it starves live
# search on a single-worker service and then gets cut mid-request.
# ---------------------------------------------------------------------------


def test_batches_are_bounded_by_bytes_not_row_count():
    rows = [{"id": f"run-{i}", "input_text": "x" * 500} for i in range(10)]
    batches = exec_sync.plan_batches(rows, batch_bytes=1500)
    assert len(batches) > 1, "everything landed in one batch — the byte bound did nothing"
    assert sum(len(b) for b in batches) == 10, "a row was dropped while batching"
    for batch in batches:
        assert batch, "an empty batch was emitted"


def test_one_oversized_row_gets_its_own_batch_rather_than_being_dropped():
    """A run whose output is 5 MB is exactly the one worth having in the
    graph, and a node cannot be split. So the bound is advisory for that one
    row and exact for every other — never a reason to drop it."""
    rows = [
        {"id": "small-1", "input_text": "x" * 10},
        {"id": "huge", "input_text": "y" * 5000},
        {"id": "small-2", "input_text": "x" * 10},
    ]
    batches = exec_sync.plan_batches(rows, batch_bytes=1000)
    flat = [r["id"] for b in batches for r in b]
    assert flat == ["small-1", "huge", "small-2"], "a row was dropped or reordered"
    ids_per_batch = [[r["id"] for r in b] for b in batches]
    assert ["huge"] in ids_per_batch, (
        f"the oversized row was not given a batch of its own: {ids_per_batch}"
    )


def _pg_event(run_id="run-1", *, event_id="ev-1", kind="finalizing", payload=None, ts=None):
    """One row in `SQL_EVENTS`' column order — positional, like the real
    cursor, because that order is the contract with `build_event_row`."""
    return (
        event_id,
        run_id,
        kind,
        ts or datetime(2026, 10, 18, 10, 9, tzinfo=timezone.utc),
        None,
        payload if payload is not None else {"tokens_in": 1, "tokens_out": 2, "cost_usd": 0.5},
    )


def test_a_tick_posts_events_alongside_runs_and_tool_calls(calls, monkeypatch):
    """§13 slice 1's happy path, end to end through the tick: the event rows
    are fetched for the batch's own runs, built per their registry rows, and
    posted in the SAME request as those runs (the ingest route refuses an
    event whose run is absent)."""
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        events=[
            _pg_event("run-1", event_id="ev-1", kind="node_start",
                      payload={"label": "Coder", "agent": "coder-opus",
                               "provider": "runner", "model": "m", "model_slug": "m"}),
            _pg_event("run-1", event_id="ev-2", kind="finalizing"),
        ],
    )
    result = _tick(conn, monkeypatch)
    assert result["events"] == 2
    assert len(calls.ingest) == 1
    posted = calls.ingest[0]["events"]
    assert [e["id"] for e in posted] == ["ev-1", "ev-2"]
    assert [e["run_id"] for e in posted] == ["run-1", "run-1"]
    assert posted[0]["agent"] == "coder-opus"
    assert posted[1]["cost_usd"] == 0.5
    # The sentinel carries the event count, so the weekly reconcile has both
    # sides of the comparison without a second round trip.
    assert calls.sync_state[0]["counts"]["events"] == 2


def test_the_event_query_never_touches_the_payload_server_side(calls, monkeypatch):
    """§13.12 point 5 / the `rollup_called` precedent. `run_events.payload`
    is `json`, not `jsonb`, and holds literal NULs — so the event SQL may
    filter on `kind` (a varchar column) and must select `payload` RAW. The
    fake raises on any server-side payload extraction, so this test is the
    guard; what is asserted here is the positive half: the kind list that
    crossed into the query is the registry's own shipped set, not a literal."""
    conn = _FakeConn(runs=[_pg_row("run-1")], events=[_pg_event("run-1")])
    _tick(conn, monkeypatch)
    event_queries = [
        (sql, params)
        for sql, params in conn.executed
        if "e.kind = ANY" in sql and "count(" not in sql
    ]
    assert event_queries, "the tick never queried for events"
    sql, params = event_queries[0]
    assert "payload" in sql and "payload->>" not in sql and "payload::text" not in sql
    assert params["kinds"] == list(exec_sync.event_kinds.shipped_kinds())


def test_a_nul_bearing_event_payload_syncs_instead_of_crashing_the_pass(calls, monkeypatch):
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        events=[_pg_event("run-1", kind="log", payload={"msg": "boom\x00 here"})],
    )
    result = _tick(conn, monkeypatch)
    assert result["blocked"] is False
    assert result["reason"] is None
    assert "\x00" not in calls.ingest[0]["events"][0]["text"]


def test_an_unregistered_kind_is_reported_on_the_tick_and_the_sentinel(calls, monkeypatch):
    """§13.6's open-set alarm, through the tick. The kind is NOT projected
    (the SQL binds the shipped set), and the tick says so on its own result
    and on the sentinel's counts, which is where the doctor check reads it
    from."""
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        events=[_pg_event("run-1")],
        event_kinds=list(exec_sync.event_kinds.shipped_kinds())
        + ["llm_token", "brand.new.kind", "another.new.kind"],
    )
    result = _tick(conn, monkeypatch)
    # DISTINCT kinds, sorted; `llm_token` has a registry row (verdict
    # `excluded`) so it is a decision, not a gap.
    assert result["event_kinds_unreviewed"] == ["another.new.kind", "brand.new.kind"]
    assert calls.sync_state[0]["counts"]["event_kinds_unreviewed"] == [
        "another.new.kind",
        "brand.new.kind",
    ]
    assert exec_sync.event_kinds_unreviewed(
        {"reported_counts": calls.sync_state[0]["counts"]}
    ) == ["another.new.kind", "brand.new.kind"]


def test_a_quiet_tick_still_reports_an_unregistered_kind(calls, monkeypatch):
    """The sweep runs BEFORE the "no runs due" early return, deliberately: a
    new kind must degrade the doctor check on the next tick whether or not
    that tick had runs to project. Deriving the kind set from the fetched
    rows instead would make the alarm go quiet exactly when the sync is
    quiet."""
    conn = _FakeConn(runs=[], event_kinds=["done", "brand.new.kind"])
    result = _tick(conn, monkeypatch)
    assert result["reason"] == "no runs due"
    assert result["event_kinds_unreviewed"] == ["brand.new.kind"]
    assert calls.sync_state[0]["counts"]["event_kinds_unreviewed"] == ["brand.new.kind"]


def test_the_byte_bound_covers_events_not_only_the_run_rows(calls, monkeypatch):
    """§12 risk 4, extended by §13. Two small runs whose EVENTS are large:
    the old bound measured run rows only, so both would have ridden one POST
    and the request would have been several times the stated ceiling. A
    workflow run carries a node_start/node_end/finalizing triple per node,
    so this is the normal case, not a pathological one.

    MUTATION: bound on `_batch_bytes([run])` alone -> one POST here, and the
    assertion below goes red."""
    big = {"msg": "m" * 4000}
    conn = _FakeConn(
        runs=[
            _pg_row("run-1"),
            _pg_row("run-2", started="2026-10-18T11:00:00+00:00",
                    ended="2026-10-18T11:05:00+00:00"),
        ],
        events=[
            _pg_event("run-1", event_id="ev-1", kind="log", payload=big),
            _pg_event("run-2", event_id="ev-2", kind="log", payload=big),
        ],
    )
    _tick(conn, monkeypatch, batch_bytes=5000)
    assert len(calls.ingest) == 2, (
        "the byte bound ignored the event rows — both runs rode one oversized POST"
    )
    for post in calls.ingest:
        run_ids = {r["id"] for r in post["runs"]}
        for event in post["events"]:
            assert event["run_id"] in run_ids, (
                "an event was posted in a request that does not carry its run — the "
                "ingest route refuses that, and the template would silently write "
                "nothing"
            )


def test_each_batch_carries_its_own_tool_calls(calls, monkeypatch):
    """The ingest route refuses a tool call whose run is not in the same
    request (the template MATCHes the run and would otherwise silently write
    nothing), so the fetch is per batch rather than one sweep. Pinned,
    because batching the runs and sweeping the calls separately is the
    obvious "optimisation" that breaks it."""
    conn = _FakeConn(
        runs=[
            _pg_row("run-1", run_input={"p": "a" * 2000}),
            _pg_row("run-2", run_input={"p": "b" * 2000},
                    started="2026-10-18T11:00:00+00:00", ended="2026-10-18T11:05:00+00:00"),
        ],
        tool_calls=[
            ("run-1", {"name": "Bash", "input": {}},
             datetime(2026, 10, 18, 10, 1, tzinfo=timezone.utc), 1),
            ("run-2", {"name": "Read", "input": {}},
             datetime(2026, 10, 18, 11, 1, tzinfo=timezone.utc), 1),
        ],
    )
    _tick(conn, monkeypatch, batch_bytes=2500)
    assert len(calls.ingest) == 2, "the byte bound did not split these two runs"
    for batch in calls.ingest:
        run_ids = {r["id"] for r in batch["runs"]}
        for tc in batch["tool_calls"]:
            assert tc["run_id"] in run_ids, (
                "a tool call was posted in a batch that does not carry its run — "
                "the ingest route refuses that, and silently writing nothing is "
                "the failure it refuses to allow"
            )


# ---------------------------------------------------------------------------
# The watermark under failure — the one place a wrong answer is permanent.
# ---------------------------------------------------------------------------


def test_a_failed_batch_stops_the_pass_and_the_watermark_only_covers_what_landed(
    calls, monkeypatch
):
    """The sharp case. If the watermark advanced past a failed batch, every
    run in that batch would be skipped FOREVER — the projection would have a
    permanent hole that only the weekly count-reconcile could even detect,
    and nothing could localise.

    So: stop at the first failure, advance only over the prefix that landed,
    and record the error on the sentinel.
    """
    conn = _FakeConn(
        runs=[
            _pg_row("run-1", run_input={"p": "a" * 2000},
                    started="2026-10-18T10:00:00+00:00", ended="2026-10-18T10:05:00+00:00"),
            _pg_row("run-2", run_input={"p": "b" * 2000},
                    started="2026-10-18T11:00:00+00:00", ended="2026-10-18T11:05:00+00:00"),
            _pg_row("run-3", run_input={"p": "c" * 2000},
                    started="2026-10-18T12:00:00+00:00", ended="2026-10-18T12:05:00+00:00"),
        ],
    )

    posted = {"n": 0}
    original = client.post_exec_ingest

    async def _flaky(*, runs, tool_calls, called, events=None):
        posted["n"] += 1
        if posted["n"] == 2:
            return False, "HTTP 500"
        return await original(
            runs=runs, tool_calls=tool_calls, called=called, events=events
        )

    monkeypatch.setattr(client, "post_exec_ingest", _flaky)
    result = _tick(conn, monkeypatch, batch_bytes=2500)

    assert result["blocked"] is False
    assert result["runs"] == 1, "counted runs from a batch that failed"
    assert "batch 2 failed" in result["reason"]
    # Only run-1 landed, so the watermark must not pass run-2's timestamps.
    # It is run-1's STARTED_AT, not its ended_at: a partial pass left rows
    # above the frontier unread, and `ended_at` can sit days past `started_at`
    # on this corpus (see test_the_watermark_never_passes_an_unread_run).
    assert result["watermark"] == "2026-10-18T10:00:00+00:00"
    assert calls.sync_state[0]["last_error"] and "HTTP 500" in calls.sync_state[0]["last_error"]


def test_the_watermark_never_passes_an_unread_run_when_a_pass_is_truncated(
    calls, monkeypatch
):
    """The 2026-10-18 production gap, as a test.

    `max_runs` truncates at run-2, whose `ended_at` is FOUR DAYS after its
    `started_at` — the real shape of a watchdog-killed or host-stalled agent
    run (`245b745a82004a34a65407f4d4df8d15`: started 2026-09-02 12:40, ended
    2026-09-06 07:12, status `error`). run-3 starts inside that span and has
    already finished, so a watermark taken as the max across both columns puts
    run-3 permanently below it: never selected again by the watermark arm,
    never re-swept either, because the re-sweep only looks at non-terminal
    runs.

    Measured before the fix: the watermark ran ahead of the loaded data by a
    growing margin — 2 runs, then 1,716, then 5,806, then 5,966 of 27,853.
    """
    conn = _FakeConn(
        runs=[
            _pg_row("run-1", started="2026-09-02T12:00:00+00:00",
                    ended="2026-09-02T12:05:00+00:00"),
            _pg_row("run-2", started="2026-09-02T12:40:00+00:00",
                    ended="2026-09-06T07:12:00+00:00", status="error"),
        ],
    )
    # Truncated: the fetch returned exactly `max_runs` rows, so rows above
    # run-2's started_at exist and were not read.
    result = _tick(conn, monkeypatch, max_runs=2)

    assert result["runs"] == 2
    assert result["watermark"] == "2026-09-02T12:40:00+00:00", (
        "the watermark passed runs that were never read"
    )
    # Concretely: a run that started inside the long run's span is still due.
    assert result["watermark"] < "2026-09-06T07:12:00+00:00"


def test_a_complete_pass_may_advance_over_ended_at_so_a_long_run_is_not_re_read(
    calls, monkeypatch
):
    """The other side of the trade, and why this is a condition rather than
    just "always use started_at".

    When a pass consumed everything the predicate matched — the steady-state
    10-minute tick, where the batch is nowhere near `max_runs` — nothing was
    left behind, so advancing over `ended_at` is sound and is what stops a
    long-running run from being re-selected by the `ended_at > watermark` arm
    on every tick for as long as it runs.
    """
    conn = _FakeConn(
        runs=[
            _pg_row("run-1", started="2026-10-18T10:00:00+00:00",
                    ended="2026-10-18T10:05:00+00:00"),
            _pg_row("run-2", started="2026-10-18T10:01:00+00:00",
                    ended="2026-10-18T14:30:00+00:00"),
        ],
    )
    result = _tick(conn, monkeypatch, max_runs=500)

    assert result["runs"] == 2
    assert result["watermark"] == "2026-10-18T14:30:00+00:00"


def test_a_still_running_run_does_not_stall_the_watermark(calls, monkeypatch):
    """`ended_at` is NULL for a run in flight. It must simply be ignored, not
    treated as a frontier of its own — and the pass must still advance over
    the runs it did see."""
    conn = _FakeConn(
        runs=[
            _pg_row("run-1", started="2026-10-18T10:00:00+00:00",
                    ended="2026-10-18T10:05:00+00:00"),
            _pg_row("run-2", started="2026-10-18T10:10:00+00:00", ended=None,
                    status="running"),
        ],
    )
    result = _tick(conn, monkeypatch, max_runs=500)
    assert result["watermark"] == "2026-10-18T10:10:00+00:00"


def test_the_first_ever_pass_has_a_null_watermark_and_is_still_bounded(calls, monkeypatch):
    """A backfill is the same route run repeatedly, not a special path — so
    the only thing standing between "first pass" and "one unbounded request
    over 28k runs" is `max_runs`, which is therefore passed into the SQL
    rather than applied after the fetch."""
    calls.status_result = ({"watermark": None, "graph_counts": {}}, None)
    conn = _FakeConn(runs=[_pg_row("run-1")])
    _tick(conn, monkeypatch, max_runs=7)
    runs_sql = [(sql, params) for sql, params in conn.executed if "FROM runs r" in sql]
    assert runs_sql, "no run query was issued"
    _sql, params = runs_sql[0]
    assert params["max_runs"] == 7
    assert params["watermark"] is None


def test_the_resweep_window_is_in_the_query_because_runs_has_no_updated_at(calls, monkeypatch):
    """§8 told the Coder to check for `runs.updated_at` and tighten the
    re-sweep to it if present. VERIFIED ABSENT against
    agents-platform-multitenant/backend/app/models.py (`class Run`, lines
    474-720, 2026-10-18) — `started_at`/`ended_at` are the only timestamps.

    So the fallback shape is load-bearing, not a placeholder: every
    non-terminal run inside the window is re-read every pass. Pinned here so
    a future "optimise to a watermark-only pass" change has to confront that
    a running run mutates after insert and would otherwise freeze mid-flight
    in the graph forever.
    """
    conn = _FakeConn(runs=[_pg_row("run-1")])
    now = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
    _tick(conn, monkeypatch, now=now)
    _sql, params = next((s, p) for s, p in conn.executed if "FROM runs r" in s)
    assert params["terminal"] == exec_sync.TERMINAL_STATUSES
    assert params["resweep_since"] == datetime(2026, 10, 16, 12, 0)
    assert exec_sync.RESWEEP_HOURS == 48


def test_terminal_statuses_match_the_apmt_schema():
    """`models.py`'s own comment on `Run.status`:
    "pending|queued|running|success|error|cancelled". A status missing from
    the terminal list is re-swept forever (wasteful but correct); a
    non-terminal status wrongly listed as terminal freezes that run's text
    in the graph at whatever it was mid-flight, which is a silent wrong
    answer."""
    assert set(exec_sync.TERMINAL_STATUSES) == {"success", "error", "cancelled"}


# ---------------------------------------------------------------------------
# §8's weekly count-reconcile.
# ---------------------------------------------------------------------------


def test_reconcile_reports_every_row_not_just_a_verdict(calls, monkeypatch):
    """A reconcile that only says "drift" sends the next person to re-derive
    which layer drifted — and the layers fail for different reasons."""
    calls.status_result = (
        {
            "graph_counts": {
                "ExecRun": 10, "ExecSession": 3, "ExecCard": 2, "ExecToolCall": 40,
                "ExecEvent": 55, "EMITTED": 55,
                "IN_SESSION": 10, "ON_CARD": 2, "CHILD_OF": 4, "QA_OF": 1,
            }
        },
        None,
    )
    conn = _FakeConn(counts=[10, 3, 2, 40, 55, 55, 10, 2, 4, 1])
    # Positional, in SQL_RECONCILE dict order — ExecEvent and EMITTED sit
    # right after ExecToolCall (§13 slice 1) and carry the same number, since
    # every event hangs off exactly one run.
    monkeypatch.setattr(exec_sync, "connect", lambda dsn: conn)
    result = _run(exec_sync.reconcile(dsn=DSN, source_tenant=SOURCE_TENANT))
    assert result["ok"] is True
    assert result["degraded"] is False
    assert {r["entity"] for r in result["rows"]} == set(exec_sync.SQL_RECONCILE)
    assert all(r["match"] for r in result["rows"])


def test_reconcile_names_the_layer_that_drifted(calls, monkeypatch):
    calls.status_result = (
        {
            "graph_counts": {
                "ExecRun": 10, "ExecSession": 3, "ExecCard": 2, "ExecToolCall": 37,
                "ExecEvent": 55, "EMITTED": 55,
                "IN_SESSION": 10, "ON_CARD": 2, "CHILD_OF": 4, "QA_OF": 1,
            }
        },
        None,
    )
    conn = _FakeConn(counts=[10, 3, 2, 40, 55, 55, 10, 2, 4, 1])
    # Positional, in SQL_RECONCILE dict order — ExecEvent and EMITTED sit
    # right after ExecToolCall (§13 slice 1) and carry the same number, since
    # every event hangs off exactly one run.
    monkeypatch.setattr(exec_sync, "connect", lambda dsn: conn)
    result = _run(exec_sync.reconcile(dsn=DSN, source_tenant=SOURCE_TENANT))
    assert result["ok"] is False
    assert result["degraded"] is True
    drifted = [r for r in result["rows"] if not r["match"]]
    assert [r["entity"] for r in drifted] == ["ExecToolCall"]
    assert drifted[0] == {"entity": "ExecToolCall", "sql": 40, "graph": 37, "match": False}


def test_reconcile_counts_child_of_as_a_join_not_a_not_null_count():
    """An endpoint outside the loaded window creates no edge (the ingest
    template OPTIONAL MATCHes it), which is correct behaviour — so counting
    `parent_run_id IS NOT NULL` would report drift that is not drift, every
    week, until someone stopped reading the report."""
    for entity in ("CHILD_OF", "QA_OF"):
        sql = exec_sync.SQL_RECONCILE[entity]
        assert "JOIN runs" in sql, f"{entity} is not counted as a join"
    assert "JOIN" not in exec_sync.SQL_RECONCILE["ON_CARD"]


def test_reconcile_refuses_without_config():
    result = _run(exec_sync.reconcile(dsn="", source_tenant=""))
    assert result["ok"] is False
    assert "required" in result["error"]


# ---------------------------------------------------------------------------
# The SQL itself — the facts that cost the measurement real time, pinned so
# they cannot be re-derived wrongly.
# ---------------------------------------------------------------------------


def test_the_run_query_selects_source_slug_and_never_agent_slug():
    """§12 risk 2: `runs.agent_slug` does not exist on the AP-MT schema."""
    assert "r.source_slug" in exec_sync.SQL_RUNS
    assert "r.agent_slug" not in exec_sync.SQL_RUNS


def test_the_called_rollup_counts_per_run_and_per_tool_name():
    """`CALLED {n}` is derived from the tool-call rows already fetched, so it
    describes exactly the same row set the `MADE_CALL` layer does — the old
    second aggregate query could drift from it, this cannot."""
    rows = [
        {"run_id": "run-1", "seq": 1, "name": "Bash", "args_text": "ls"},
        {"run_id": "run-1", "seq": 2, "name": "Bash", "args_text": "pwd"},
        {"run_id": "run-1", "seq": 3, "name": "Read", "args_text": "f"},
        {"run_id": "run-2", "seq": 1, "name": "Bash", "args_text": "ls"},
    ]
    rollup = {(r["run_id"], r["name"]): r["n"] for r in exec_sync.rollup_called(rows)}
    assert rollup == {("run-1", "Bash"): 2, ("run-1", "Read"): 1, ("run-2", "Bash"): 1}
    # The total must equal the number of named tool calls, or MADE_CALL and
    # CALLED describe different things.
    assert sum(rollup.values()) == len([r for r in rows if r["name"]])


def test_the_called_rollup_drops_rows_with_no_tool_name():
    """A `CALLED` edge needs a tool to point at. The removed `GROUP BY` had a
    `if name` filter for this; losing it when the rollup moved client-side
    would have produced an edge to a nameless node."""
    rows = [
        {"run_id": "run-1", "seq": 1, "name": None, "args_text": "x"},
        {"run_id": "run-1", "seq": 2, "name": "", "args_text": "x"},
        {"run_id": "run-1", "seq": 3, "name": "Bash", "args_text": "x"},
    ]
    assert exec_sync.rollup_called(rows) == [{"run_id": "run-1", "name": "Bash", "n": 1}]


def test_a_nul_bearing_payload_syncs_instead_of_crashing_the_pass(calls, monkeypatch):
    """The 2026-10-18 backfill crash, as a test.

    A `tool_call` payload carrying a literal NUL — a screenshot byte stream
    quoted into a tool result is the real shape — used to reach a server-side
    `payload->>'name'` and abort the whole tick with
    `UntranslatableCharacter`. Now the payload is only ever selected raw and
    psycopg has already parsed it client-side by the time it gets here, so the
    NUL is simply stripped and the pass completes.
    """
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[
            (
                "run-1",
                {"name": "Bash\x00", "input": {"command": "200 36707 image/png\x00PNG"}},
                datetime(2026, 10, 18, 10, 1, tzinfo=timezone.utc),
                1,
            ),
        ],
    )
    result = _tick(conn, monkeypatch)

    assert result["blocked"] is False
    assert result["reason"] is None
    assert result["tool_calls"] == 1
    assert result["called"] == 1

    posted = calls.ingest[0]
    # No NUL survives into anything heading for the graph, in any field.
    assert "\x00" not in str(posted)
    assert posted["tool_calls"][0]["name"] == "Bash"
    assert posted["called"][0] == {"run_id": "run-1", "name": "Bash", "n": 1}
    # And the watermark advanced, i.e. the pass really did complete.
    assert calls.sync_state[0]["last_error"] is None


def test_no_query_extracts_the_event_payload_server_side():
    """`run_events.payload` is `json` (text-backed) and holds literal
    `\\u0000`, so EVERY server-side extraction fails — a `payload::text` cast
    of the document and a `payload->>'key'` over a single key alike, because a
    text-backed `json` parses the whole document to reach any part of it.

    This used to allow `payload->>'name'` for the CALLED rollup. That crashed
    the production backfill after ~21.4k runs with
    `UntranslatableCharacter: unsupported Unicode escape sequence`; the first
    nine 2000-run ticks passed only because no payload in those windows
    carried a NUL. Verified against the production table directly:
    `SELECT count(*) ... WHERE e.payload->>'name' IS NOT NULL` fails with
    "\\u0000 cannot be converted to text".
    """
    for name in dir(exec_sync):
        if not name.startswith("SQL_"):
            continue
        sql = getattr(exec_sync, name)
        statements = sql.values() if isinstance(sql, dict) else [sql]
        for statement in statements:
            assert "payload::text" not in statement, name
            assert "payload->>" not in statement, name
    assert "e.payload," in exec_sync.SQL_TOOL_CALLS, "the payload must come back unparsed"
    # And the rollup has no SQL of its own left to get this wrong in.
    assert not hasattr(exec_sync, "SQL_CALLED")


def test_every_query_is_scoped_to_one_source_tenant_or_to_explicit_run_ids():
    """§2 — a pass binds exactly one source tenant. The two per-batch queries
    are scoped by `run_ids` instead, which are themselves the output of the
    tenant-scoped run query, so neither can read another tenant's rows."""
    assert "r.tenant_id = %(tenant)s" in exec_sync.SQL_RUNS
    assert "e.run_id = ANY(%(run_ids)s)" in exec_sync.SQL_TOOL_CALLS
    for entity, sql in exec_sync.SQL_RECONCILE.items():
        assert "%(tenant)s" in sql, f"{entity}'s reconcile SQL is not tenant-scoped"
    assert "e.run_id = ANY(%(run_ids)s)" in exec_sync.SQL_TOOL_RESULTS


# ---------------------------------------------------------------------------
# §13 slice 3 — result pairing, the result drop, and the disk preflight.
#
# The pairing is the one mechanism in this slice that can fail SILENTLY and
# still look healthy: a join on the wrong key produces zero matches, every
# tick reports success, and the graph simply has no results in it — which is
# indistinguishable from "Postgres has no results yet" unless the unpaired
# count is observable. So these tests are about the key, the count, and the
# drop, in that order.
# ---------------------------------------------------------------------------


def _pg_result(run_id="run-1", *, tool_use_id="toolu_1", content="ok", ts=None):
    """One row in `SQL_TOOL_RESULTS`' column order — positional, like the
    real cursor. Note `tool_use_id`, NOT `id`: that asymmetry with the
    tool_call payload is §13.12 point 2 and it is the whole hazard here, so
    the fixture spells it the way production does rather than the way the
    other side does."""
    return (
        run_id,
        {"tool_use_id": tool_use_id, "content": content},
        ts or datetime(2026, 10, 18, 10, 1, 3, tzinfo=timezone.utc),
    )


def _pg_call(run_id="run-1", *, tool_use_id="toolu_1", name="Bash", seq=1, args=None):
    """One row in `SQL_TOOL_CALLS`' column order, with the payload `id` that
    is the pairing key's other half."""
    return (
        run_id,
        {"id": tool_use_id, "name": name, "input": args or {"command": "ls"}},
        datetime(2026, 10, 18, 10, 1, tzinfo=timezone.utc),
        seq,
    )


def test_a_result_is_paired_onto_its_own_call_across_the_two_key_names(calls, monkeypatch):
    """§13.12 point 2, as the test that would have caught the obvious bug:
    `tool_call.payload` keys its id as `id` and `tool_result.payload` keys
    the SAME value as `tool_use_id`.

    MUTATION: change either side to read the other's key name and this goes
    red with `result_text` absent — which, without this test, is exactly
    what "results are not in Postgres yet" looks like from the outside.
    """
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[
            _pg_call("run-1", tool_use_id="toolu_a", seq=1),
            _pg_call("run-1", tool_use_id="toolu_b", name="Read", seq=2),
        ],
        tool_results=[
            # Deliberately NOT in call order: the join is by id, never
            # positional, and a reversed fixture is what proves it.
            _pg_result("run-1", tool_use_id="toolu_b", content="file contents"),
            _pg_result("run-1", tool_use_id="toolu_a", content="a.txt b.txt"),
        ],
    )
    result = _tick(conn, monkeypatch)

    assert result["tool_results"] == 2
    assert result["orphan_results"] == 0
    posted = {c["name"]: c for c in calls.ingest[0]["tool_calls"]}
    assert posted["Bash"]["result_text"] == "a.txt b.txt"
    assert posted["Read"]["result_text"] == "file contents"
    assert posted["Bash"]["result_ts"] == "2026-10-18T10:01:03+00:00"
    assert posted["Bash"]["result_len"] == len("a.txt b.txt")
    assert posted["Bash"]["result_truncated"] is False
    # The counts ride the sentinel, like every other number a tick observed.
    assert calls.sync_state[0]["counts"]["tool_results"] == 2
    assert calls.sync_state[0]["counts"]["orphan_results"] == 0


def test_an_unpaired_result_is_counted_and_never_crashes_the_pass(calls, monkeypatch):
    """§13.10 names "unpaired results stop being rare" as the condition that
    reopens the separate-nodes decision. A condition nobody can observe is
    not a condition, so the count is on the tick result AND on the sentinel
    — and an unpaired row is neither fatal nor silently discarded."""
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_a")],
        tool_results=[
            _pg_result("run-1", tool_use_id="toolu_a", content="paired"),
            # A result whose call is not in this batch at all.
            _pg_result("run-1", tool_use_id="toolu_nowhere", content="orphan"),
            # A result with no `tool_use_id` — nothing to join on.
            ("run-1", {"content": "keyless"}, datetime(2026, 10, 18, 10, 2, tzinfo=timezone.utc)),
        ],
    )
    result = _tick(conn, monkeypatch)

    assert result["blocked"] is False, "an unpaired result must not abort the pass"
    assert result["orphan_results"] == 2
    assert result["tool_results"] == 1
    posted = calls.ingest[0]["tool_calls"]
    assert len(posted) == 1
    assert posted[0]["result_text"] == "paired"
    assert calls.sync_state[0]["counts"]["orphan_results"] == 2


def test_a_result_never_pairs_across_runs_even_on_the_same_tool_use_id(calls, monkeypatch):
    """The index is keyed `(run_id, tool_use_id)`, not by the bare id.

    Nothing upstream promises that id is unique across runs, and the failure
    this prevents is the worst shape available in this slice: one run's
    OUTPUT attached to a different run's call, which no count would ever
    reveal — both sides would look fully paired.
    """
    conn = _FakeConn(
        runs=[_pg_row("run-1"), _pg_row("run-2", started="2026-10-18T11:00:00+00:00",
                                        ended="2026-10-18T11:05:00+00:00")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_collide")],
        tool_results=[
            _pg_result("run-2", tool_use_id="toolu_collide", content="run-2 output"),
        ],
    )
    result = _tick(conn, monkeypatch)

    assert result["orphan_results"] == 1, (
        "a result from run-2 paired onto run-1's call — the index must be scoped "
        "per run, and this mis-pairing is invisible to every count"
    )
    posted = calls.ingest[0]["tool_calls"]
    assert posted[0].get("result_text") is None


@pytest.mark.parametrize(
    "name",
    [
        "collect_secret",
        "read_secret",
        "mcp__aw-gateway__aw__aw_secrets__collect_secret",
        "aw-gateway.aw__aw_secrets__read_secret",
        "mcp__workspace-gateway__aw__aw_secrets__write_secret",
    ],
)
def test_a_secret_tools_result_is_dropped_not_scrubbed(name, calls, monkeypatch):
    """§13.2's own closing paragraph, delivered: for `read_secret`,
    `delete_secret` and `collect_secret`, the ARGUMENTS carry a name or a
    request id and "the value lives in the RESULT, which slice 3 is what
    drops".

    Dropped, not scrubbed: the value is a credential by definition, so there
    is nothing for a pattern to recognise and nothing a redaction marker
    would add. Over the bare name plus all three measured gateway spellings,
    because §13.12 point 1 is that a tool's identity is never its full
    prefixed name.

    MUTATION: swap `scrub_tool_result` for `scrub` in `attach_results` and
    every case here goes red — the planted value has no shape and is not in
    the fake secret store, so the scrubber returns it verbatim. That is the
    point: this channel cannot be defended by scrubbing.
    """
    planted = "invented-vault-value-with-no-recognisable-shape-at-all"
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_s", name=name)],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_s", content=planted)],
    )
    result = _tick(conn, monkeypatch)

    assert result["orphan_results"] == 0, "the row paired — the drop is not a pairing miss"
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] is None
    assert posted["result_dropped"] is True, (
        "the drop has to be RECORDED — an absent result that does not say it was "
        "dropped is indistinguishable from a call that never returned"
    )
    assert planted not in str(calls.ingest), "the planted value reached the ingest payload"


def test_an_ordinary_tools_result_is_not_marked_dropped(calls, monkeypatch):
    """The other half of the signal: if `result_dropped` were set on every
    row it would mean nothing. `None` rather than `False` so the property is
    simply absent on the node."""
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_a", name="Bash")],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_a", content="ok")],
    )
    _tick(conn, monkeypatch)
    assert calls.ingest[0]["tool_calls"][0]["result_dropped"] is None


def test_a_result_goes_through_the_scrubber_on_the_way_out(calls, monkeypatch):
    """A scrubber the build path does not call is the same as no scrubber —
    the standard this file already holds the run and tool-call paths to,
    applied to the channel where credentials actually surface."""
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_a", name="Bash")],
        tool_results=[
            _pg_result(
                "run-1",
                tool_use_id="toolu_a",
                # Four result-shaped fixtures in one payload, all INVENTED
                # (§13.12 point 9): a `cat .env` line, a `docker inspect`
                # env entry, a git remote with an embedded token, and the
                # fake store's own value.
                content=(
                    "$ cat .env\n"
                    "DATABASE_PASSWORD=invented-result-test-pw\n"
                    '  "Env": ["GITHUB_TOKEN=invented-result-test-token"],\n'
                    "origin\thttps://x-access-token:invented-result-test-git@github.com/o/r\n"
                    f"stored={STORE_SECRET}\n"
                ),
            )
        ],
    )
    _tick(conn, monkeypatch)
    text = calls.ingest[0]["tool_calls"][0]["result_text"]
    assert "<REDACTED:" in text, text
    for leaked in (
        "invented-result-test-pw",
        "invented-result-test-token",
        "invented-result-test-git",
        STORE_SECRET,
    ):
        assert leaked not in text, f"{leaked!r} survived into {text!r}"
    # The non-credential context survives — a scrubber that shreds the
    # corpus is one that gets turned off (see `exec_scrub`'s own `-p` scar).
    assert "cat .env" in text
    assert "DATABASE_PASSWORD=" in text, "the env var NAME is the retrieval value"
    assert '"Env":' in text
    # MEASURED, not assumed, and recorded because it contradicts a promise
    # `exec_scrub`'s `url-userinfo` family makes in its own comment ("the
    # host survives; it is the useful half"): for a git remote whose
    # USERNAME contains the word `token`
    # (`https://x-access-token:SECRET@github.com/o/r`), the
    # `credential-field` family runs first — it is ordered ahead of
    # `url-userinfo` — matches `x-access-token:` as a key/value pair, and
    # eats the rest of the line including the host. The credential is gone
    # either way, which is what matters here; what is lost is the repo name.
    # Pinned so it is a known behaviour rather than a surprise, and so a
    # future reordering of `_PATTERNS` shows up as a change in this
    # assertion instead of silently.
    assert "github.com/o/r" not in text, (
        "if this now passes, the pattern ordering changed and the git-remote "
        "shape keeps its host — update this assertion and say so rather than "
        "deleting the case"
    )


def test_a_planted_credential_straddling_the_result_cap_is_still_redacted(calls, monkeypatch):
    """§13.12 point 6 for `result_text`, in the straddle shape slice 2
    generalised — and this is the kind the cap actually fires on in
    production (measured maximum 20.7 KB against a 16,384 ceiling), so the
    boundary is where this slice's credentials really sit.

    The assertion is on the FRAGMENT, not the whole value: cap-then-scrub
    leaves a leading half that matches no stored value and has no pattern
    shape, so it survives in the graph and stays greppable. Scrub-then-cap
    can only ever bisect the redaction MARKER.
    """
    straddle = len(STORE_SECRET) // 2
    padding = "p" * (event_kinds.RESULT_TEXT_MAX - straddle)
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_a", name="Bash")],
        tool_results=[
            _pg_result(
                "run-1",
                tool_use_id="toolu_a",
                content=f"{padding}{STORE_SECRET}{'t' * 100}",
            )
        ],
    )
    _tick(conn, monkeypatch)
    posted = calls.ingest[0]["tool_calls"][0]
    fragment = STORE_SECRET[:straddle]
    assert len(fragment) >= 8, "the fragment must be long enough to be a real leak"
    assert fragment not in posted["result_text"], (
        "a leading FRAGMENT of the planted credential survived the cut — the cap "
        "ran before the scrubber (§13.12 point 6)"
    )
    assert STORE_SECRET not in posted["result_text"]
    assert len(posted["result_text"]) == event_kinds.RESULT_TEXT_MAX
    assert posted["result_len"] > event_kinds.RESULT_TEXT_MAX
    assert posted["result_truncated"] is True


def test_nul_bytes_are_stripped_from_result_text(calls, monkeypatch):
    """§13.12 point 5. The NUL is legal in the client-side-parsed payload and
    illegal as a Neo4j property, and it arrives when it arrives — the first
    N ticks passing proves nothing."""
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_a")],
        tool_results=[
            _pg_result("run-1", tool_use_id="toolu_a", content="before\x00after")
        ],
    )
    _tick(conn, monkeypatch)
    assert "\x00" not in calls.ingest[0]["tool_calls"][0]["result_text"]


def test_tool_result_is_not_an_event_kind_and_cannot_flow_through_the_event_path():
    """The constant slice 3 must NOT change: `tool_result` rides
    `ExecToolCall`, so its registry row stays `shipped=False`, which keeps
    it out of `shipped_kinds()` and therefore out of `SQL_EVENTS`.

    This matters more for this kind than for any other: `build_event_row`
    applies neither the secret-tool result drop nor the result cap, so a
    `tool_result` that reached the event path would land unscrubbed-class
    output text through the one route that handles none of it."""
    assert "tool_result" not in event_kinds.shipped_kinds()
    assert event_kinds.EVENT_KIND_REGISTRY["tool_result"].shipped is False
    row, dropped = exec_sync.build_event_row(
        (
            "ev-1",
            "run-1",
            "tool_result",
            "2026-10-18T10:00:00+00:00",
            None,
            {"tool_use_id": "toolu_a", "content": "secret output"},
        ),
        exec_scrub.Scrubber([STORE_SECRET]),
    )
    assert dropped == 0
    assert "text" not in row, (
        "the event path produced text for tool_result — `shipped` is part of the "
        "build-path condition precisely so this cannot happen"
    )
    assert "secret output" not in str(row)


# ---------------------------------------------------------------------------
# §13.7's disk preflight. Exercised with invented numbers, never by filling a
# host: a guard whose trigger condition is a disaster can only be proven
# honestly by injecting the measurement.
# ---------------------------------------------------------------------------


def test_the_preflight_refuses_below_the_floor_and_says_the_numbers():
    verdict = exec_sync.disk_preflight(3 * 1024**3)
    assert verdict["ok"] is False
    assert verdict["free_gb"] == 3.0
    assert verdict["min_free_gb"] == exec_sync.MIN_FREE_DISK_GB
    assert "3.0 GB free" in verdict["reason"]
    assert str(exec_sync.MIN_FREE_DISK_GB) in verdict["reason"]
    # It has to say what to do and what is NOT blocked, or an operator reads
    # a refusal as the whole sync being down.
    assert "retention" in verdict["reason"]
    assert "10-minute tick is NOT blocked" in verdict["reason"]


def test_the_preflight_passes_with_headroom():
    verdict = exec_sync.disk_preflight(40 * 1024**3)
    assert verdict["ok"] is True
    assert verdict["free_gb"] == 40.0
    assert verdict["reason"] is None


def test_the_preflight_refuses_when_the_MEASUREMENT_failed_not_only_when_disk_is_low():
    """"I could not measure the disk" must not read like "the disk is fine" —
    silent degradation is this estate's named failure mode and a preflight
    that fails open is worse than none, because it reports a verdict."""
    verdict = exec_sync.disk_preflight(None, "could not measure free disk space: OSError")
    assert verdict["ok"] is False
    assert verdict["free_gb"] is None
    assert "could not measure" in verdict["reason"]


def test_the_preflight_floor_is_the_designs_number():
    """§13.7 named 25 GB against a host measured at 92% used / 36 GB free.
    Pinned, so a future "it keeps refusing, lower it" is a deliberate edit
    with the design in front of whoever makes it."""
    assert exec_sync.MIN_FREE_DISK_GB == 25


def test_free_disk_bytes_measures_the_host_mounted_workspace_tree():
    """The probe path has to be the host-mounted tree, not `/` (a container
    overlay in some topologies here) and not the Neo4j data dir, which this
    process cannot see at all — it never speaks Bolt and has no mount into
    that container (§4)."""
    assert exec_sync._DISK_PROBE_PATH == "/opt/aw-workspace"
    free, err = exec_sync.free_disk_bytes()
    assert err is None, err
    assert isinstance(free, int) and free > 0


# ---------------------------------------------------------------------------
# exec-index-per-tool-policy.md §9 slice 4 — the OUTPUT axis, where the
# `result_text` §13 slice 3 projects meets per-tool policy.
#
# What these tests are actually defending, since the floor already covers the
# four secret-store tools: the MIDDLE GROUND. A tool that is not a
# secret-store tool but whose OUTPUT carries a credential anyway —
# `remote_host_read_file` on a `.env`, a bash tool with a token in stdout, a
# config route echoing a key. Before this slice those were governed by the
# pattern scrubber alone, which is the weakest tier in this system and is the
# designed FALLBACK for vault-only credentials rather than a policy. So every
# test below drives the FULL tick, not `attach_results` in isolation: a gate
# the tick does not call is the same as no gate, which is the standard this
# file already holds the run and tool-call paths to.
# ---------------------------------------------------------------------------

import json  # noqa: E402

from knowledgeable_app import exec_policy  # noqa: E402


def _policy_tree(tmp_path, monkeypatch, apps):
    """Point the tick's own `PolicyTable.from_workspace()` at a fake installed
    tree. Patched at `_apps_root` rather than by passing a table in, so the
    test exercises the real build-per-tick path (§5).

    **Every policy test in this block must call this, including the ones
    whose expectation happens to match the live workspace.** The blind spot
    it closes, paid for once in CI on slice 4's own release: a test that read
    the REAL `/opt/aw-workspace/apps` was green for every human and every
    agent running the suite inside this workspace, and red only in CI, where
    that directory does not exist — so `from_workspace()` degrades, every
    routed name fails closed, and "a never-declared tool's result lands"
    becomes false. Two independent full-suite verifications both reported
    715 passed and neither could see it. `tests/conftest.py`'s autouse guard
    now makes the dependency impossible to acquire by accident; this helper
    is what a test uses instead.

    `apps` is `{slug: {"servers": [...], "indexing": [...] | None}}`. Server
    keys must be the REAL ones (`aw-app-remote-host-cli` for slug
    `remote-host-cli`, `aw-kanban` for slug `notion` — both key != slug
    joins), because a fixture that invented them would pass against itself
    and resolve nothing in production (§12 risk 1). `indexing: None` writes
    a manifest with no `indexing` key at all, which is the "never declared"
    state ~36 installed apps are really in.
    """
    root = tmp_path / "apps"
    for slug, spec in apps.items():
        app_dir = root / slug
        app_dir.mkdir(parents=True)
        (app_dir / "mcp.json").write_text(
            json.dumps({"mcpServers": {k: {"type": "http"} for k in spec["servers"]}}),
            encoding="utf-8",
        )
        contributes = {}
        if spec.get("indexing") is not None:
            contributes["mcp"] = {"indexing": spec["indexing"]}
        (app_dir / "aw-app.json").write_text(
            json.dumps({"id": slug, "contributes": contributes}), encoding="utf-8"
        )
    monkeypatch.setattr(exec_policy, "_apps_root", lambda: str(root))
    return root


def _rhc_tree(tmp_path, monkeypatch, indexing):
    """The common case: just `remote-host-cli`, with the given declaration."""
    return _policy_tree(
        tmp_path,
        monkeypatch,
        {"remote-host-cli": {"servers": ["aw-app-remote-host-cli"], "indexing": indexing}},
    )


_RHC = "mcp__aw-gateway__aw__aw_app_remote_host_cli__remote_host_read_file"


def test_a_declared_output_false_empties_the_result_the_floor_would_have_kept(
    calls, monkeypatch, tmp_path
):
    """The gap slice 4 closes, as one test. `remote_host_read_file` is NOT in
    `SECRET_TOOL_NAMES`, so `scrub_tool_result` keeps its result; the planted
    value has no recognisable shape and is not in the secret store, so the
    pattern pass keeps it too. Only the output axis stops it.

    MUTATION: delete the `apply_output_axis` call in `attach_results` and this
    goes red with the planted value in the ingest payload — which is exactly
    the state the connector shipped in between §13 slice 3 and this slice.
    """
    _rhc_tree(
        tmp_path,
        monkeypatch,
        [{"tool": "remote_host_read_file", "call": True, "output": False,
          "reason": "the result is the file"}],
    )
    planted = "invented-env-file-value-with-no-recognisable-shape"
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_r", name=_RHC,
                             args={"path": "/srv/app/.env"})],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_r", content=planted)],
    )
    result = _tick(conn, monkeypatch)

    assert result["orphan_results"] == 0, "the row paired — this is not a pairing miss"
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] is None
    assert posted["result_dropped"] is True, (
        "the drop has to be RECORDED — `result_dropped` already exists for "
        "exactly this and a parallel signal would need its own server half"
    )
    assert planted not in str(calls.ingest), "the planted value reached the ingest payload"
    # The CALL axis is untouched: the path argument was never the problem, and
    # an output opt-out that also emptied the arguments would make the two
    # booleans pointless.
    assert posted["args_text"] is not None and "/srv/app/.env" in posted["args_text"]
    assert result["policy_denied_results"] == 1
    assert result["policy_denied_calls"] == 0


def test_a_declared_output_true_keeps_the_result(calls, monkeypatch, tmp_path):
    """The other half of the signal. If `output: true` emptied results too,
    the slice would be a blanket ban with extra steps."""
    _rhc_tree(
        tmp_path,
        monkeypatch,
        [{"tool": "remote_host_read_file", "call": True, "output": True}],
    )
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_r", name=_RHC)],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_r", content="file contents")],
    )
    result = _tick(conn, monkeypatch)
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] == "file contents"
    assert posted["result_dropped"] is None
    assert result["policy_denied_results"] == 0


def test_a_never_declared_tools_result_still_lands(calls, monkeypatch, tmp_path):
    """The blacklist on the output axis: `add_kanban_comment` is attributed to
    an installed app that declares nothing, and its result must still project.
    The opposite default would empty ~1 GB of legitimately searchable text,
    which is how a security layer gets switched off.

    **This test used to read the real `/opt/aw-workspace/apps` and that is
    why slice 4's release went red.** `notion`/`aw-kanban` is a real
    installed app here, so the assertion held for anyone running the suite
    inside this workspace — and in CI, where the directory does not exist,
    `from_workspace()` degrades, every routed name fails closed, and the
    result is correctly denied. The test was asserting a property of the
    machine it ran on. The real tree is now unreachable by default
    (`tests/conftest.py`) and the app is declared here instead; the key is
    still the REAL one (`aw-kanban` -> slug `notion`, a key != slug join),
    because that join is the thing worth pinning.
    """
    _policy_tree(
        tmp_path,
        monkeypatch,
        {"notion": {"servers": ["notion", "aw-kanban"], "indexing": None}},
    )
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[
            _pg_call(
                "run-1",
                tool_use_id="toolu_k",
                name="mcp__aw-gateway__aw__aw_kanban__add_kanban_comment",
            )
        ],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_k", content="comment posted")],
    )
    result = _tick(conn, monkeypatch)
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] == "comment posted"
    assert posted["result_dropped"] is None
    assert result["policy_denied_results"] == 0
    assert result["policy_unresolved_results"] == 0


def test_an_unresolvable_name_loses_its_result_as_well_as_its_arguments(
    calls, monkeypatch, tmp_path
):
    """§3.5 on both axes. An unresolvable name is the one case where the
    output axis could plausibly have been left permissive, and it must not
    be: the output is the more dangerous of the two channels, so "nothing
    could attribute it, therefore index the result" would make the output
    axis strictly weaker than the call axis for no reason.

    Needs a tree that was READ SUCCESSFULLY and simply does not contain the
    upstream — a scan FAILURE reaches the same verdict by a different route
    (`policy_scan_error`), so leaving this on the real tree would have let
    the right assertion pass for the wrong reason in CI.
    """
    _policy_tree(
        tmp_path,
        monkeypatch,
        {"notion": {"servers": ["notion", "aw-kanban"], "indexing": None}},
    )
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[
            _pg_call("run-1", tool_use_id="toolu_g",
                     name="mcp__aw-gateway__aw__uninstalled_app__some_tool")
        ],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_g", content="whatever came back")],
    )
    result = _tick(conn, monkeypatch)
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] is None
    assert posted["args_text"] == exec_policy.UNRESOLVED_DROP_MARKER
    assert posted["name"] == "mcp__aw-gateway__aw__uninstalled_app__some_tool", (
        "name and timestamp are KEPT — the floor's own treatment"
    )
    assert posted["ts"] is not None
    # Counted apart from the policy denials, which is what keeps
    # `policy_denied_results` a readable signal.
    assert result["policy_unresolved_results"] == 1
    assert result["policy_denied_results"] == 0


def test_a_standing_unresolvable_upstream_does_not_drown_the_new_counter(
    calls, monkeypatch, tmp_path
):
    """The slice-2 measurement, as a tick. `fredericowu_browser` is a
    linked-workspace BYOD federation with no `apps/<slug>/mcp.json`: it fails
    closed permanently and no manifest anyone can write will ever attribute
    it. Its denials must not land in `policy_denied_results`, or the first
    real `output: false` opt-out to fire is indistinguishable from
    background — the same defect class as a doctor line that is always red.

    It is still denied, still counted, and still in `policy_unresolved`: the
    registry changes counting, never behaviour, and the acknowledge path on
    the doctor is slice 3's.

    **A SUCCESSFULLY-READ tree is load-bearing here, not incidental.** The
    registry's claim is "no manifest could ever attribute this", which is a
    statement about a tree that was read; under `policy_scan_error` nothing
    is classified as known (see `unresolved_split`). On the real tree in CI
    this test passed for the wrong reason — the scan had failed and the
    assertion happened to still hold before that rule existed.
    """
    name = "mcp__aw-gateway__aw__fredericowu_browser__fredericowu__playwright__browser_snapshot"
    _policy_tree(
        tmp_path,
        monkeypatch,
        {"notion": {"servers": ["notion", "aw-kanban"], "indexing": None}},
    )
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_f", name=name)],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_f", content="page snapshot")],
    )
    result = _tick(conn, monkeypatch)
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] is None, "still denied — this is not an exemption"
    assert result["policy_denied_results"] == 0
    assert result["policy_unresolved_results"] == 1
    assert result["policy_unresolved_actionable"] == 0, (
        "nothing here is actionable — no manifest can ever attribute it"
    )
    assert result["policy_unresolved_known"] == [name]
    assert name in result["policy_unresolved"], (
        "slice 2's shipped key is unchanged, known names included"
    )


def test_the_floor_and_policy_are_independent_gates_on_the_result(calls, monkeypatch, tmp_path):
    """A secret tool's result is dropped even when policy would have allowed
    it — a manifest that LIES. §2's invariant must hold at each gate
    independently, so that removing either one cannot remove the guarantee of
    the other."""
    root = tmp_path / "apps"
    (root / "secrets").mkdir(parents=True)
    (root / "secrets" / "mcp.json").write_text(
        json.dumps({"mcpServers": {"aw-secrets": {"type": "http"}}}), encoding="utf-8"
    )
    (root / "secrets" / "aw-app.json").write_text(
        json.dumps({"id": "secrets", "contributes": {"mcp": {"indexing": [
            {"tool": "read_secret", "call": True, "output": True}
        ]}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(exec_policy, "_apps_root", lambda: str(root))
    planted = "invented-vault-value-a-lying-manifest-must-not-release"
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_s",
                             name="mcp__aw-gateway__aw__aw_secrets__read_secret")],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_s", content=planted)],
    )
    result = _tick(conn, monkeypatch)
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] is None
    assert posted["result_dropped"] is True
    assert planted not in str(calls.ingest)
    # The floor's drops are NOT this layer's counter: it exists to say what
    # policy changed, and the floor predates it.
    assert result["policy_denied_results"] == 0


def test_an_output_denial_on_a_call_that_returned_nothing_claims_no_drop(
    calls, monkeypatch, tmp_path
):
    """Same honesty rule the call axis keeps for an argument-less call: a
    paired result with no content stays absent rather than being reported as
    dropped. `result_dropped` must mean something, and a row that claims a
    drop that never happened is how it stops meaning anything."""
    _rhc_tree(
        tmp_path,
        monkeypatch,
        [{"tool": "remote_host_read_file", "call": True, "output": False, "reason": "x"}],
    )
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_r", name=_RHC)],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_r", content=None)],
    )
    result = _tick(conn, monkeypatch)
    posted = calls.ingest[0]["tool_calls"][0]
    assert posted["result_text"] is None
    assert posted["result_dropped"] is None, "nothing was dropped, so nothing claims to be"
    # The DECISION is still counted — the row was governed by policy even
    # though there was nothing to remove. Same as `policy_denied_calls`.
    assert result["policy_denied_results"] == 1


def test_the_output_counters_ride_the_sentinel(calls, monkeypatch, tmp_path):
    """The `event_kinds_unreviewed` mechanism (§8), for the slice-4 numbers.
    The tick is the only thing that sees them, so a counter it does not post
    onto the sentinel is a counter no status or doctor surface can read."""
    _rhc_tree(
        tmp_path,
        monkeypatch,
        [{"tool": "remote_host_read_file", "call": True, "output": False, "reason": "x"}],
    )
    conn = _FakeConn(
        runs=[_pg_row("run-1")],
        tool_calls=[_pg_call("run-1", tool_use_id="toolu_r", name=_RHC)],
        tool_results=[_pg_result("run-1", tool_use_id="toolu_r", content="secretish")],
    )
    _tick(conn, monkeypatch)
    posted = calls.sync_state[-1]["counts"]
    assert posted["policy_denied_results"] == 1
    assert posted["policy_unresolved_results"] == 0
    assert posted["policy_unresolved_actionable"] == 0
    assert posted["policy_unresolved_known"] == []
