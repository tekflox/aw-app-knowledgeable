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

from knowledgeable_app import exec_scrub, exec_sync, vault_coverage  # noqa: E402
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
    def __init__(self, runs=(), tool_calls=(), counts=(), events=(), event_kinds=None):
        self.runs = list(runs)
        self.tool_calls = list(tool_calls)
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
