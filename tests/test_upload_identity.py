"""The cross-uploader upload-identity contract (card `3fa5bf3b-9510-812c-
a439-d7fcc773f2bd`): **two sequential uploads of the same unchanged file,
through EITHER of this connector's two uploaders, in EITHER order, must
leave exactly one document carrying one stable id.**

Its own file, deliberately, rather than extra cases in
``test_bulk_ingest.py`` or ``test_fs_sync.py``: the contract is about the
two uploaders AGREEING, and a per-uploader test cannot express that. This
module's whole point is that one body of assertions runs against both
`bulk_ingest.run_tick` and `fs_sync.run_tick` from one table
(:data:`UPLOADERS`) and over every ordered pair of them — the failure that
produced this card was a per-uploader test staying green all day while the
sibling call site was broken (memory
`fs-sync-writes-need-an-explicit-bucket`, the same module, the same
"one call site fixed, the other stayed broken" shape).

:class:`_FakeGraph` is what gives those assertions teeth. It models BOTH of
aw-knowledgeable's document-identity MERGE keys as the server really
implements them (docs/design/aw-knowledgeable-fs-sync.md §1.1):

* ``POST /api/documents`` → `create_document`, MERGE on
  ``(bucket, content_hash)``, and it never writes `workspace_slug`;
* ``PUT /api/documents/by-path`` → `upsert_document_by_path`, MERGE on
  ``(bucket, workspace_slug, source_path)``.

So an uploader that drifts back to the content-hash route for a path the
other uploader already owns mints a SECOND, disjoint node here exactly as
it did in production — the assertion fails instead of passing on a fake
that only ever had one notion of identity.
``test_fake_graph_models_the_disjoint_content_hash_route`` pins that
property down so these tests can never go vacuous.

Style follows the two sibling test modules: a plain ``_run`` helper driving
coroutines, ``client``'s async functions monkeypatched rather than
``httpx``, and the ``_FakeDb``/``_FakeLease`` doubles duplicated rather
than shared (this repo's existing convention — see
``test_bulk_ingest.py``'s module docstring).
"""
from __future__ import annotations

import asyncio
import hashlib
import itertools
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest, fs_sync  # noqa: E402
from knowledgeable_app.mcp import client  # noqa: E402

SLUG = "aw"
RELPATH = "memory/preferences.md"
CONTENT = "I like grape\n"


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeDb:
    """Double for ``ctx.db`` — see ``test_bulk_ingest.py``'s identical class
    for the full rationale (named ``:param`` binding, ``{table}``
    substitution, the Postgres-style upsert sqlite also accepts)."""

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


class _FakeLease:
    def __init__(self) -> None:
        self._held = False

    def claim(self, name: str) -> bool:
        if self._held:
            return False
        self._held = True
        return True

    def release(self, name: str) -> None:
        self._held = False


class _FakeGraph:
    """The server's two identity regimes, side by side — see the module
    docstring for why both are modelled rather than just the one under
    test."""

    def __init__(self) -> None:
        self.nodes: dict[str, dict] = {}
        self._minted = 0

    def _mint(self) -> str:
        self._minted += 1
        return f"doc-{self._minted:012d}"

    def create_document(self, *, bucket: str, content_hash: str, source_path: str | None) -> dict:
        """§1.1's legacy route: MERGE ``(bucket, content_hash)``. Writes
        `source_path` when the caller sends one, and NEVER `workspace_slug`
        — which is precisely why the by-path MERGE below can't see it."""
        for node in self.nodes.values():
            if node["bucket"] == bucket and node["content_hash"] == content_hash:
                return {**node, "deduplicated": True}
        node = {
            "id": self._mint(), "bucket": bucket, "content_hash": content_hash,
            "source_path": source_path, "workspace_slug": None, "rev": None,
        }
        self.nodes[node["id"]] = node
        return {**node, "deduplicated": False}

    def upsert_by_path(
        self, *, bucket: str, workspace_slug: str, source_path: str, content_hash: str
    ) -> dict:
        """§1.1's synced route: MERGE ``(bucket, workspace_slug,
        source_path)``, `content_hash` demoted from key to attribute, `rev`
        as the version. A node missing `workspace_slug` can never match —
        Neo4j MERGE requires an exact match on every pattern property, and
        absent is not a value."""
        for node in self.nodes.values():
            if (
                node["bucket"] == bucket
                and node["workspace_slug"] == workspace_slug
                and node["source_path"] == source_path
            ):
                if node["content_hash"] == content_hash:
                    return {"id": node["id"], "rev": node["rev"],
                            "content_hash": content_hash, "outcome": "unchanged"}
                node["content_hash"] = content_hash
                node["rev"] = (node["rev"] or 0) + 1
                return {"id": node["id"], "rev": node["rev"],
                        "content_hash": content_hash, "outcome": "updated"}
        node = {
            "id": self._mint(), "bucket": bucket, "content_hash": content_hash,
            "source_path": source_path, "workspace_slug": workspace_slug, "rev": 1,
        }
        self.nodes[node["id"]] = node
        return {"id": node["id"], "rev": 1, "content_hash": content_hash, "outcome": "created"}

    def for_path(self, source_path: str) -> list[dict]:
        return [n for n in self.nodes.values() if n["source_path"] == source_path]


@pytest.fixture(autouse=True)
def _workspace_home(tmp_path, monkeypatch):
    monkeypatch.setenv("AW_WORKSPACE_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture(autouse=True)
def _fresh_table_flag(monkeypatch):
    monkeypatch.setattr(bulk_ingest, "_table_ensured", False)


@pytest.fixture()
def graph(monkeypatch) -> _FakeGraph:
    """Wires BOTH server routes at once, so a test never has to know which
    one the uploader under test actually reaches — that is the thing being
    asserted, not something the harness should assume."""
    fake = _FakeGraph()

    async def fake_put_document_by_path(
        filename, raw, *, bucket, source_path, workspace_slug, base_hash=None, title=None
    ):
        return fake.upsert_by_path(
            bucket=bucket, workspace_slug=workspace_slug, source_path=source_path,
            content_hash=hashlib.sha256(raw).hexdigest(),
        ), None

    async def fake_upload_bytes(filename, raw, *, bucket, source_path=None, title=None):
        return fake.create_document(
            bucket=bucket, content_hash=hashlib.sha256(raw).hexdigest(), source_path=source_path,
        ), None

    async def fake_create_bucket(name):
        return {"bucket": name, "already_existed": True}, None

    async def fake_get_ingest_status(bucket=None):
        return {"extraction": {"claiming": False}, "processing": {"pending": 0}}, None

    async def fake_get_sync_paths(workspace_slug, after=None, limit=500, bucket=None):
        return {"paths": [], "cursor": None}, None

    async def fake_get_document_changes(since=None, limit=200):
        return {"changes": [], "cursor": since}, None

    monkeypatch.setattr(client, "put_document_by_path", fake_put_document_by_path)
    monkeypatch.setattr(client, "upload_bytes", fake_upload_bytes)
    monkeypatch.setattr(client, "create_bucket", fake_create_bucket)
    monkeypatch.setattr(client, "get_ingest_status", fake_get_ingest_status)
    monkeypatch.setattr(client, "get_sync_paths", fake_get_sync_paths)
    monkeypatch.setattr(client, "get_document_changes", fake_get_document_changes)
    return fake


@pytest.fixture()
def db() -> _FakeDb:
    return _FakeDb()


@pytest.fixture()
def lease() -> _FakeLease:
    return _FakeLease()


@pytest.fixture()
def kb(tmp_path) -> Path:
    """One 13-byte file at the card's own repro path, never edited."""
    root = tmp_path / "knowledge_base"
    path = root / RELPATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(CONTENT, encoding="utf-8")
    return root


# ---------------------------------------------------------------------------
# The table: one "upload everything that is ready" pass per uploader, each
# responsible for its own preconditions. `fs_sync` holds a newly observed
# hash for its quiet window (§20.2), so its pass runs a tick, back-dates the
# fs-sync-owned quiet clock, and runs a second tick — the engine's real
# behaviour, not a faked-away one.
# ---------------------------------------------------------------------------


async def _bulk_pass(db, lease) -> dict:
    return await bulk_ingest.run_tick(db, lease, max_uploads=50, workspace_slug=SLUG)


async def _fs_pass(db, lease) -> dict:
    result = await fs_sync.run_tick(db, lease, max_uploads=50, workspace_slug=SLUG)
    if result.get("created") or result.get("updated") or result.get("unchanged"):
        return result
    past = time.time() - 10_000
    db.execute(
        fs_sync.TABLE,
        "UPDATE {table} SET pending_since = :past, dirty_since = :past "
        "WHERE pending_hash IS NOT NULL",
        {"past": past},
    )
    return await fs_sync.run_tick(db, lease, max_uploads=50, workspace_slug=SLUG)


UPLOADERS = [("bulk_ingest", _bulk_pass), ("fs_sync", _fs_pass)]
_IDS = [name for name, _ in UPLOADERS]


def _journal_row(db: _FakeDb, relpath: str = RELPATH) -> dict:
    rows = db.execute(
        fs_sync.TABLE, "SELECT * FROM {table} WHERE relpath = :relpath", {"relpath": relpath}
    )
    assert rows, f"no journal row for {relpath}"
    return dict(rows[0])


@pytest.mark.parametrize("name,uploader", UPLOADERS, ids=_IDS)
def test_second_upload_of_an_unchanged_file_is_a_no_op_with_the_same_id(
    name, uploader, db, lease, graph, kb
):
    """The card's own reproduction, per uploader: `memory/preferences.md`,
    13 bytes, uploaded twice with no edit in between."""
    _run(uploader(db, lease))
    docs = graph.for_path(RELPATH)
    assert len(docs) == 1, f"{name}: first pass created {len(docs)} documents"
    first_id = docs[0]["id"]
    assert docs[0]["workspace_slug"] == SLUG, (
        f"{name}: uploaded through a route that writes no workspace_slug — "
        "the other uploader's by-path MERGE can never find this node"
    )
    assert _journal_row(db)["external_id"] == first_id

    _run(uploader(db, lease))
    docs = graph.for_path(RELPATH)
    assert len(docs) == 1, f"{name}: second pass of unchanged content created a duplicate"
    assert docs[0]["id"] == first_id, f"{name}: id changed on a re-upload"
    assert _journal_row(db)["external_id"] == first_id


@pytest.mark.parametrize(
    "first,second",
    list(itertools.product(UPLOADERS, UPLOADERS)),
    ids=[f"{a}_then_{b}" for a, b in itertools.product(_IDS, _IDS)],
)
def test_both_uploaders_converge_on_one_document_id(first, second, db, lease, graph, kb):
    """THE regression. Each ordered pair of uploaders over the same
    unchanged file: the second one must find the first one's document, not
    mint its own. `bulk_ingest` → `fs_sync` is the pair that stranded 544
    documents in production."""
    first_name, first_pass = first
    second_name, second_pass = second

    _run(first_pass(db, lease))
    docs = graph.for_path(RELPATH)
    assert len(docs) == 1
    first_id = docs[0]["id"]

    _run(second_pass(db, lease))
    docs = graph.for_path(RELPATH)
    assert len(docs) == 1, (
        f"{first_name} then {second_name}: {len(docs)} documents for one unchanged file — "
        f"ids {[d['id'] for d in docs]}. The second uploader minted a disjoint node, "
        "and the journal's single external_id column orphans whichever one it stops naming."
    )
    assert docs[0]["id"] == first_id
    # The journal is the ONLY place delete detection reads identity from, so
    # a stable graph is not enough — the row has to still name the survivor.
    row = _journal_row(db)
    assert row["external_id"] == first_id
    # Populated by BOTH uploaders now: it is what stops fs-sync seeing a
    # bulk-ingested row as never-synced and re-uploading it (which is how
    # the duplicate fired on essentially every shared file).
    assert row["last_synced_hash"] == hashlib.sha256(CONTENT.encode()).hexdigest()


@pytest.mark.parametrize(
    "first,second",
    list(itertools.product(UPLOADERS, UPLOADERS)),
    ids=[f"{a}_then_{b}" for a, b in itertools.product(_IDS, _IDS)],
)
def test_an_edit_after_an_upload_updates_that_document_instead_of_adding_one(
    first, second, db, lease, graph, kb
):
    """The same table, with a real edit between the two passes — the case
    that makes the ROUTE (not just the journal bookkeeping) load-bearing: a
    changed file is a second write for the same path, so it must find the
    existing document and bump it.

    `bulk_ingest` is not the uploader that picks edits up — `scan()`'s
    frozen-row rule leaves an already-`uploaded` row alone, deliberately
    (design §13 risk 4 is answered by `fs_sync` re-hashing the whole tree
    against `last_synced_hash`, which keeps edits inside §20's quiet
    window). So a `*_then_bulk_ingest` pair legitimately uploads nothing
    here; what must hold for every pair is that the path never ends up with
    two documents."""
    _, first_pass = first
    second_name, second_pass = second

    _run(first_pass(db, lease))
    first_id = graph.for_path(RELPATH)[0]["id"]

    (kb / RELPATH).write_text("I like grape, and also pears\n", encoding="utf-8")
    _run(second_pass(db, lease))

    docs = graph.for_path(RELPATH)
    assert len(docs) == 1, (
        f"an edit picked up by {second_name} created a second document — "
        f"ids {[d['id'] for d in docs]}"
    )
    assert docs[0]["id"] == first_id
    assert _journal_row(db)["external_id"] == first_id
    if second_name == "fs_sync":
        # fs-sync is the engine that owns edits: the content really did
        # change on the node, under the same id and a bumped rev.
        assert docs[0]["content_hash"] == hashlib.sha256(
            "I like grape, and also pears\n".encode()
        ).hexdigest()
        assert docs[0]["rev"] == 2


def test_fake_graph_reproduces_the_one_way_identity_gap():
    """Keeps the two tests above honest — and records that the production
    defect was ASYMMETRIC, which is why it fired on the pair it did.

    `create_document`'s MERGE key is `(tenant, bucket, content_hash)` and
    carries no path or slug, so it matches ANY node with that hash —
    including one the by-path route created. The reverse is not true: the
    by-path MERGE pattern names `workspace_slug`, and a node the legacy
    route created has no such property, so it can never match and a second
    node is minted. Legacy-then-by-path (which is bulk-ingest-then-fs-sync)
    is therefore the pair that stranded 544 documents; by-path-then-legacy
    would have silently deduplicated.

    If this ever stops holding, the two tests above are vacuous."""
    fake = _FakeGraph()
    legacy = fake.create_document(bucket="main", content_hash="h1", source_path=RELPATH)
    by_path = fake.upsert_by_path(
        bucket="main", workspace_slug=SLUG, source_path=RELPATH, content_hash="h1"
    )
    assert by_path["id"] != legacy["id"]
    assert by_path["outcome"] == "created"
    assert len(fake.for_path(RELPATH)) == 2, "the by-path MERGE must not adopt a slug-less node"

    other_way = _FakeGraph()
    first = other_way.upsert_by_path(
        bucket="main", workspace_slug=SLUG, source_path=RELPATH, content_hash="h1"
    )
    dedup_hit = other_way.create_document(bucket="main", content_hash="h1", source_path=RELPATH)
    assert dedup_hit["id"] == first["id"]
    assert dedup_hit["deduplicated"] is True
    assert len(other_way.for_path(RELPATH)) == 1


def test_bulk_ingest_refuses_to_run_without_a_workspace_slug(db, lease, graph, kb):
    """§6 parity with `fs_sync.run_tick`: the slug is a quarter of the
    identity this driver now uploads under, so an unset `AW_WORKSPACE` is a
    refusal with a declared reason, never a defaulted slug. Also asserts the
    lease is still free afterwards — a refusal must not hold the tick lock."""
    result = _run(bulk_ingest.run_tick(db, lease, max_uploads=50, workspace_slug=""))
    assert result["blocked"] is True
    assert "workspace_slug" in result["reason"]
    assert graph.nodes == {}
    assert lease.claim(bulk_ingest.LEASE_NAME) is True
