"""HTTP client for aw-knowledgeable's 6 M6 endpoints — this connector's tool
bodies.

Style ported from ``repos/aw-app-whiteboard/mcp_server/server.py``
(docs/design/aw-knowledgeable-infra.md §10, D2): a plain client of a remote
API, authenticating with a shared secret read **fresh on every call**
(never cached at import time, never read at module-import time) so a
rotated secret takes effect without restarting this Tier-1 process (D2 risk
5 — a Tier-1 restart is expensive, see memory
``tier1-inprocess-apps-have-no-restart-path``).

This module is a 1:1 HTTP mapping onto aw-knowledgeable's API — no Cypher,
no Neo4j driver, no import of aw-knowledgeable's own code (D2 §10: "the
connector is an HTTP client... no knowledge of core/graph.py"). Every
outbound body is built field-by-field from the tool's own known arguments,
never a pass-through of the whole ``arguments`` dict — so an unrelated key
the gateway may inject (e.g. ``_gateway_caller_run_id``, D2 risk 4) is
dropped here rather than depending on the far side tolerating it.
"""
from __future__ import annotations

import base64
import re
from typing import Callable

import httpx

DEFAULT_BASE_URL = "http://aw-knowledgeable:8090"

_base_url_resolver: Callable[[], str] = lambda: DEFAULT_BASE_URL
_secret_resolver: Callable[[], str] = lambda: ""


def configure(base_url_resolver: Callable[[], str], secret_resolver: Callable[[], str]) -> None:
    """Installed once from ``plugin.activate``; every function below reads
    through these two callables on every call — see module docstring."""
    global _base_url_resolver, _secret_resolver
    _base_url_resolver = base_url_resolver
    _secret_resolver = secret_resolver


def base_url() -> str:
    return (_base_url_resolver() or DEFAULT_BASE_URL).rstrip("/")


def _secret() -> str:
    try:
        return (_secret_resolver() or "").strip()
    except Exception:
        return ""


def configured() -> bool:
    return bool(_secret())


def _headers() -> dict:
    secret = _secret()
    return {"X-Internal-Secret": secret} if secret else {}


def _describe_error(resp: httpx.Response) -> str:
    try:
        data = resp.json()
    except ValueError:
        return f"HTTP {resp.status_code}"
    detail = data.get("detail") if isinstance(data, dict) else None
    return f"HTTP {resp.status_code}: {detail}" if detail else f"HTTP {resp.status_code}"


async def _get(path: str, params: dict | None = None) -> tuple[dict | None, str | None]:
    async with httpx.AsyncClient(timeout=30) as http_client:
        try:
            resp = await http_client.get(f"{base_url()}{path}", params=params, headers=_headers())
        except httpx.HTTPError as exc:
            return None, f"could not reach aw-knowledgeable at {base_url()}: {exc}"
    if resp.status_code >= 400:
        return None, _describe_error(resp)
    return resp.json(), None


async def _post_json(
    path: str, body: dict, *, params: dict | None = None, timeout: float = 30
) -> tuple[dict | None, str | None]:
    async with httpx.AsyncClient(timeout=timeout) as http_client:
        try:
            resp = await http_client.post(
                f"{base_url()}{path}", json=body, params=params, headers=_headers()
            )
        except httpx.HTTPError as exc:
            return None, f"could not reach aw-knowledgeable at {base_url()}: {exc}"
    if resp.status_code >= 400:
        return None, _describe_error(resp)
    return resp.json(), None


async def upload_document(args: dict) -> tuple[str, bool]:
    """POST /api/documents (multipart).

    Bytes travel through the gateway as a base64 argument — the agent
    container, the workspace container and aw-knowledgeable are three
    different filesystems, so a shared file path cannot be assumed (D2 risk
    3, same trap as memory
    ``remote-host-download-file-mcp-misses-the-shared-tree``). This is the
    one place base64 is decoded and re-encoded as real multipart bytes
    before forwarding.
    """
    filename = (args.get("filename") or "").strip()
    if not filename:
        return "filename is required", True
    content_b64 = args.get("content_base64")
    if not content_b64:
        return "content_base64 is required (base64-encoded file bytes)", True
    try:
        raw = base64.b64decode(content_b64, validate=True)
    except Exception as exc:
        return f"content_base64 is not valid base64: {exc}", True

    # A synchronous multipart POST, not a background job — the tunnel edge
    # cuts requests at 30s (D2 risk 3, memory
    # ``tunnel-edge-cuts-requests-at-30s``). aw-knowledgeable's own upload
    # handler is itself synchronous (write bytes + one Neo4j MERGE), so this
    # is fine up to its own 10MB cap; a slower ingest is out of this card's
    # scope to fix.
    async with httpx.AsyncClient(timeout=45) as http_client:
        try:
            resp = await http_client.post(
                f"{base_url()}/api/documents",
                files={"file": (filename, raw)},
                headers=_headers(),
            )
        except httpx.HTTPError as exc:
            return f"could not reach aw-knowledgeable at {base_url()}: {exc}", True
    if resp.status_code >= 400:
        return f"upload_document failed: {_describe_error(resp)}", True
    return _as_text(resp.json()), False


async def create_bucket(name: str) -> tuple[dict | None, str | None]:
    """POST /api/buckets — §13.1's precondition for the bulk-ingest driver's
    four buckets: each must exist before the first upload into it. Treated
    as idempotent from THIS caller's point of view even though the route
    itself answers a real 409 on a name collision (`api/buckets.py`'s own
    docstring: "already exists" is a real conflict, not a no-op, because an
    empty bucket is meaningful the moment it's created) — the driver calls
    this on every tick, so "already exists" has to read as steady state,
    not a failure."""
    data, err = await _post_json("/api/buckets", {"name": name})
    if err and err.startswith("HTTP 409"):
        return {"bucket": name, "already_existed": True}, None
    return data, err


async def get_ingest_status(bucket: str | None = None) -> tuple[dict | None, str | None]:
    """GET /api/ingest/status — the bulk-ingest driver's (§13.4-§13.6) two
    uses of this one route: the ignition guard (`extraction.claiming`,
    global regardless of `bucket`) and the per-bucket upload-backlog
    backpressure signal (`processing`, genuinely scoped to `bucket`)."""
    params = {"bucket": bucket} if bucket else None
    return await _get("/api/ingest/status", params)


async def upload_bytes(
    filename: str,
    raw: bytes,
    *,
    bucket: str,
    source_path: str | None = None,
    title: str | None = None,
) -> tuple[dict | None, str | None]:
    """POST /api/documents with real bytes, not base64 — the bulk-ingest
    driver's own upload path (§13.4/§13.6 item 4), distinct from the
    `upload_document` MCP tool below. The driver reads files straight off
    this process's own filesystem (the KB tree), so the base64 round trip
    that tool needs for an agent's wire format would only double memory for
    every file in a 181MB corpus, for no reason this caller has."""
    params: dict = {"bucket": bucket}
    data: dict = {}
    if source_path:
        data["source_path"] = source_path
    if title:
        data["title"] = title
    async with httpx.AsyncClient(timeout=45) as http_client:
        try:
            resp = await http_client.post(
                f"{base_url()}/api/documents",
                params=params,
                data=data,
                files={"file": (filename, raw)},
                headers=_headers(),
            )
        except httpx.HTTPError as exc:
            return None, f"could not reach aw-knowledgeable at {base_url()}: {exc}"
    if resp.status_code >= 400:
        return None, _describe_error(resp)
    return resp.json(), None


async def put_document_by_path(
    filename: str,
    raw: bytes,
    *,
    bucket: str,
    source_path: str,
    workspace_slug: str,
    base_hash: str | None = None,
    title: str | None = None,
) -> tuple[dict | None, str | None]:
    """PUT /api/documents/by-path (docs/design/aw-knowledgeable-fs-sync.md
    §1.2) — the fs-sync engine's path-keyed sibling of ``upload_bytes``
    above. The response (``{id, rev, content_hash, outcome}``) is the
    load-bearing contract the caller journals from directly, never from a
    second read — same real-bytes-not-base64 reasoning as ``upload_bytes``,
    since `fs_sync.py` also reads files off this process's own KB tree.

    `bucket` travels as a query param, same as ``upload_bytes`` above —
    the server's ``require_bucket_write`` reads it off the query string, not
    the multipart body. Without it the route falls back to "the tenant's
    sole writable bucket", which silently breaks the moment a second
    writable bucket exists."""
    params: dict = {"bucket": bucket}
    data: dict = {"source_path": source_path, "workspace_slug": workspace_slug}
    if base_hash:
        data["base_hash"] = base_hash
    if title:
        data["title"] = title
    async with httpx.AsyncClient(timeout=45) as http_client:
        try:
            resp = await http_client.put(
                f"{base_url()}/api/documents/by-path",
                params=params,
                data=data,
                files={"file": (filename, raw)},
                headers=_headers(),
            )
        except httpx.HTTPError as exc:
            return None, f"could not reach aw-knowledgeable at {base_url()}: {exc}"
    if resp.status_code >= 400:
        return None, _describe_error(resp)
    return resp.json(), None


async def mark_delete_by_path(
    source_path: str, workspace_slug: str, *, bucket: str, reason: str | None = None
) -> tuple[dict | None, str | None]:
    """POST /api/documents/mark-delete-by-path (docs/design/aw-knowledgeable-
    fs-sync.md §17.2) — marks EVERY document claiming
    ``{tenant, bucket, source_path}`` with ``workspace_slug = $slug OR
    workspace_slug IS NULL``. Replaces the old by-id ``mark_delete`` (card
    C removed it — dead since G2 switched fs-sync's delete-marking seam to
    this one, no other caller ever existed) as fs-sync's delete-marking
    seam: a journal row's `external_id` can only ever name one of several
    documents sharing a path (§17.1's 35 structurally-unreachable
    `cli_reference/` documents), so this engine stops dereferencing it for
    deletes entirely. Service-only, two-phase, no `force` variant.
    Response is ``{source_path, marked: [ids], count}``.

    `bucket` travels as a query param, same as ``put_document_by_path`` and
    ``upload_bytes`` above — this route is gated by ``require_bucket_write``
    too, which reads `bucket` off the query string and 400s as ambiguous
    the moment a second writable bucket exists. This was the same omission
    `put_document_by_path` had until it was fixed; this call site had been
    missed."""
    params: dict = {"bucket": bucket}
    body: dict = {"source_path": source_path, "workspace_slug": workspace_slug}
    if reason:
        body["reason"] = reason
    return await _post_json("/api/documents/mark-delete-by-path", body, params=params)


async def restore_by_path(
    source_path: str, workspace_slug: str, *, bucket: str
) -> tuple[dict | None, str | None]:
    """POST /api/documents/restore-by-path (docs/design/aw-knowledgeable-
    fs-sync.md §22) — the undo of ``mark_delete_by_path`` above, and a
    fan-out for the same §17.1 reason: it clears the pending-delete mark on
    EVERY document claiming ``{tenant, bucket, source_path}`` with
    ``workspace_slug = $slug OR workspace_slug IS NULL``, so a path whose
    file came back on disk leaves no §17.1 loser still marked. Idempotent
    server-side — a claimant that is not `pending_delete` is skipped, so a
    re-run restores 0 and causes no feed churn. Service-only, no `force`
    variant. Response is ``{source_path, restored: [ids], count}``.

    fs-sync's found-loop is the one caller: a human declining in the
    approval UI still goes through the by-id ``POST /{id}/restore``.

    `bucket` travels as a query param, same as every other write in this
    module — this route is ``require_bucket_write``-gated too, and an
    omitted bucket 400s as ambiguous the moment a second writable bucket
    exists (memory `fs-sync-writes-need-an-explicit-bucket`: that exact
    omission already broke `put_document_by_path` and
    `mark_delete_by_path` in production)."""
    params: dict = {"bucket": bucket}
    body: dict = {"source_path": source_path, "workspace_slug": workspace_slug}
    return await _post_json("/api/documents/restore-by-path", body, params=params)


async def mark_delete(
    node_id: str, *, bucket: str, reason: str | None = None
) -> tuple[dict | None, str | None]:
    """POST /api/documents/{node_id}/mark-delete — the BY-ID half of §7's
    two-phase delete, deliberately without ``force``: it sets
    ``sync_status: pending_delete`` and nothing else, so the document stays
    searchable until a human (or `approve_pending_deletions`) approves it.

    fs-sync's per-tick delete marking does NOT use this — it uses
    ``mark_delete_by_path`` above, because a journal row's `external_id` can
    only ever name one of several documents sharing a path (§17.1). This
    one exists for the opposite case: `orphan_reconcile.py` has to mark ONE
    specific document out of such a group and must leave its live sibling
    alone, which a by-path mark cannot express.

    `bucket` travels as a query param — same `require_bucket_write`
    reasoning as every other write in this module (memory
    `fs-sync-writes-need-an-explicit-bucket`: an omitted bucket 400s as
    ambiguous the moment a second writable bucket exists, and it has
    already broken two call sites in this file)."""
    body: dict = {}
    if reason:
        body["reason"] = reason
    return await _post_json(
        f"/api/documents/{node_id}/mark-delete", body, params={"bucket": bucket}
    )


async def get_documents(bucket: str | None = None) -> tuple[dict | None, str | None]:
    """GET /api/documents — the full document listing for a bucket, as JSON
    (the sibling of the `list_documents` MCP tool below, which returns the
    same payload shaped as agent-facing text). `orphan_reconcile.py` joins
    this against the journal, so it needs the structured form.

    `bucket` is optional for the same reason `get_documents_pending_delete`
    leaves it optional: this route resolves an omitted bucket through
    `resolve_search_scope` (every bucket the token can read), not through
    `require_bucket_read`'s single-bucket rule — so there is no ambiguity
    400 to avoid here. Pass it anyway when you mean one bucket."""
    params: dict = {}
    if bucket:
        params["bucket"] = bucket
    return await _get("/api/documents", params or None)


async def get_sync_paths(
    workspace_slug: str, *, after: str | None = None, limit: int = 500, bucket: str | None = None
) -> tuple[dict | None, str | None]:
    """GET /api/documents/sync-paths (§17.2) — paged, distinct
    `source_path` claims under the same slug-or-NULL filter as
    ``mark_delete_by_path``, including a path whose only claimant is
    already `pending_delete` (the §17.4 catch-up and this engine's own
    per-tick adoption both depend on seeing it). Response is
    ``{paths: [{source_path, claimant_count}], cursor}``.

    `bucket` is the same explicit-not-implicit rule every other call in
    this module already follows: the route sits behind
    `require_bucket_read`, which 400s as ambiguous the moment this
    tenant's service identity can see more than one bucket — silently
    (caught as `err` by this call's one caller, `fs_sync._adopt_paths`),
    which previously made adoption, GC and the restore-reconcile catch-up
    all quietly no-op every tick (they all gate on `sync_paths_seen`).
    Pass it; don't rely on the implicit single-bucket resolution a second
    registered bucket breaks."""
    params: dict = {"workspace_slug": workspace_slug, "limit": limit}
    if after:
        params["after"] = after
    if bucket:
        params["bucket"] = bucket
    return await _get("/api/documents/sync-paths", params)


async def get_document_changes(
    *, since: str | None = None, limit: int = 200
) -> tuple[dict | None, str | None]:
    """GET /api/documents/changes (docs/design/aw-knowledgeable-fs-sync.md
    §3.3 step 2) — the downsync's poll feed. `since` is the opaque cursor a
    prior call's `cursor` field returned; omitted on the very first poll.
    Response is ``{changes: [{id, source_path, rev, content_hash,
    sync_status, updated_at}], cursor}``."""
    params: dict = {"limit": limit}
    if since:
        params["since"] = since
    return await _get("/api/documents/changes", params)


async def get_documents_pending_delete(bucket: str | None = None) -> tuple[dict | None, str | None]:
    """GET /api/documents?pending_delete=1 (docs/design/aw-knowledgeable-
    fs-sync.md §18.4) — the server's currently-marked set, fetched once by
    the one-time `restore_reconcile_catchup_v1` pass to tell "restored
    through a channel the feed poll never saw" apart from "genuinely still
    awaiting approval". `bucket` is optional — omitted, the server's own
    `resolve_search_scope` spans every bucket this service token can read,
    matching the documents.py route's own contract; passed, it narrows to
    just that bucket (the pending-deletion tools' own listing knob). Response
    is ``{documents: [{source_path, ...}]}``."""
    params: dict = {"pending_delete": True}
    if bucket:
        params["bucket"] = bucket
    return await _get("/api/documents", params)


async def delete_document(node_id: str, *, bucket: str) -> tuple[bool, str | None]:
    """DELETE /api/documents/{node_id} (204, no body) — §7's approve half of
    the two-phase delete: permanent, no undo. `bucket` travels as an explicit
    query param, same `require_bucket_write` reasoning as every other
    bucket-requiring call in this module (fs-sync-delete-mark-bucket-missing)
    — omitted, the server resolves it to "the tenant's sole writable bucket",
    which 400s the moment a second one exists."""
    async with httpx.AsyncClient(timeout=30) as http_client:
        try:
            resp = await http_client.delete(
                f"{base_url()}/api/documents/{node_id}", params={"bucket": bucket}, headers=_headers()
            )
        except httpx.HTTPError as exc:
            return False, f"could not reach aw-knowledgeable at {base_url()}: {exc}"
    if resp.status_code >= 400:
        return False, _describe_error(resp)
    return True, None


async def restore_document(node_id: str, *, bucket: str) -> tuple[dict | None, str | None]:
    """POST /api/documents/{node_id}/restore — §7's decline half of the
    two-phase delete: clears a pending-delete mark, reversible. Same explicit
    `bucket` query param as ``delete_document`` above — this route is gated
    by `require_bucket_write` too."""
    return await _post_json(f"/api/documents/{node_id}/restore", {}, params={"bucket": bucket})


async def get_document_content(external_id: str) -> tuple[bytes | None, str | None]:
    """GET /api/documents/{id}/content — the downsync's byte-fetch. The
    `changes` feed above carries only metadata (no file bytes), so writing
    a pulled `authored/` file needs this second call. Returns raw bytes,
    not JSON, hence its own small request rather than going through
    ``_get``."""
    async with httpx.AsyncClient(timeout=45) as http_client:
        try:
            resp = await http_client.get(
                f"{base_url()}/api/documents/{external_id}/content", headers=_headers()
            )
        except httpx.HTTPError as exc:
            return None, f"could not reach aw-knowledgeable at {base_url()}: {exc}"
    if resp.status_code >= 400:
        return None, _describe_error(resp)
    return resp.content, None


async def push_playground_key(value: str) -> tuple[bool, str | None]:
    """POST /api/playground/key — hand production the Playground's ap-mt
    ApiKey, over the same ``X-Internal-Secret`` channel every other call in
    this module already uses.

    Not one of ``TOOLS_SCHEMA`` below: this is not an agent-facing MCP tool,
    it is the connector's own re-assertion path (``playground_key_push.py``),
    called from a background loop rather than from a tool call. Returns
    ``(True, None)`` on success, ``(False, <description>)`` otherwise — the
    caller logs the value never echoed here and by ``_describe_error``.
    """
    data, err = await _post_json("/api/playground/key", {"api_key": value})
    if err:
        return False, err
    return bool(data and data.get("ok")), None


async def push_ingest_key(value: str) -> tuple[bool, str | None]:
    """POST /api/ingest/key — hand production the extractor's ap-mt ApiKey,
    over the same ``X-Internal-Secret`` channel every other call in this
    module already uses.

    The twin of ``push_playground_key`` (card 3ec5bf3b): same shape, same
    channel, different endpoint and different credential — the extractor and
    the Playground reach different ap-mt agents, so one may be revoked,
    rotated or scoped differently without touching the other. Not one of
    ``TOOLS_SCHEMA`` below, for the same reason ``push_playground_key`` isn't:
    this is the connector's own re-assertion path
    (``ingest_key_push.py``), called from a background loop rather than from
    a tool call. Returns ``(True, None)`` on success, ``(False,
    <description>)`` otherwise — the caller logs the value never echoed here
    and by ``_describe_error``.
    """
    data, err = await _post_json("/api/ingest/key", {"api_key": value})
    if err:
        return False, err
    return bool(data and data.get("ok")), None


async def push_label_key(value: str) -> tuple[bool, str | None]:
    """POST /api/topics/key — hand production the topic labeller's ap-mt
    ApiKey, over the same ``X-Internal-Secret`` channel every other call in
    this module already uses.

    The third of the three pushed credentials (2026-10-03, closing the
    LLM-key veto): same shape as ``push_playground_key`` and
    ``push_ingest_key``, different endpoint and different credential — the
    labeller, the extractor and the Playground each reach a different ap-mt
    agent, so any one may be revoked, rotated or scoped differently without
    touching the others. Not one of ``TOOLS_SCHEMA`` below, for the same
    reason the other two pushes aren't: this is the connector's own
    re-assertion path (``label_key_push.py``), called from a background loop
    rather than from a tool call. Returns ``(True, None)`` on success,
    ``(False, <description>)`` otherwise — the caller logs the value never
    echoed here and by ``_describe_error``.
    """
    data, err = await _post_json("/api/topics/key", {"api_key": value})
    if err:
        return False, err
    return bool(data and data.get("ok")), None


# ---------------------------------------------------------------------------
# The exec-history projection (docs/design/execution-history-in-production-
# neo4j.md §4). Not in ``TOOLS_SCHEMA`` below — these are the sync engine's
# own calls (``exec_sync.py``), driven by a scheduled task and a CLI, not by
# an agent tool call. §9 step 4 adds agent-facing MCP tools over the search
# routes once the flag has been on and the reconcile is green.
#
# Same ``X-Internal-Secret`` channel every other call here uses, which is
# also the only credential involved: the exec routes are service-caller-only
# in v1, and this side never holds the Neo4j password.
# ---------------------------------------------------------------------------


async def get_exec_status() -> tuple[dict | None, str | None]:
    """GET /api/exec/status — the freshness sentinel plus live graph counts.

    This is where a tick reads its WATERMARK from: the server owns it, on the
    sentinel node, rather than the connector keeping a local copy. A local
    copy would survive a graph wipe (§9's rollback) and the next tick would
    then skip every run before the stale watermark — a rollback that silently
    half-restores.
    """
    return await _get("/api/exec/status")


async def post_exec_ingest(
    *,
    runs: list[dict],
    tool_calls: list[dict],
    called: list[dict],
    events: list[dict] | None = None,
) -> tuple[bool, str | None]:
    """POST /api/exec/ingest — one byte-bounded batch.

    `timeout=120`, not the module default of 30: an ingest batch is the one
    call here that writes thousands of nodes, and production is single-worker.
    The 30 s tunnel-edge cut does not apply — this call is container-to-
    container on aw-stack-net, by name, never through the edge (§4).

    `events` is §13 slice 1's timeline, and it is keyword-OPTIONAL rather
    than required for a reason that bites in exactly one direction: this
    connector releases independently of aw-knowledgeable, so a connector
    that already sends events can meet a service that does not yet accept
    them (the field is simply ignored there), and the reverse — a service
    ready, connector not yet updated — posts no key at all.
    """
    data, err = await _post_json(
        "/api/exec/ingest",
        {
            "runs": runs,
            "tool_calls": tool_calls,
            "called": called,
            "events": events or [],
        },
        timeout=120,
    )
    if err:
        return False, err
    return bool(data and data.get("ok")), None


async def put_exec_sync_state(
    *,
    source_tenant: str,
    watermark: str | None,
    counts: dict | None = None,
    last_error: str | None = None,
) -> tuple[bool, str | None]:
    """POST /api/exec/sync-state — stamp the sentinel, once, at the end of a
    pass (§8).

    `source_tenant` is the AP-MT tenant the pass READ, which is a different
    namespace from the graph tenant it was written to; the route requires it
    rather than defaulting it, so a mis-mapped sync is visible on the
    sentinel instead of silent.
    """
    data, err = await _post_json(
        "/api/exec/sync-state",
        {
            "source_tenant": source_tenant,
            "watermark": watermark,
            "counts": counts or {},
            "last_error": last_error,
        },
    )
    if err:
        return False, err
    return bool(data and data.get("ok")), None


async def post_exec_retention(days: int | None = None) -> tuple[dict | None, str | None]:
    """POST /api/exec/retention — §8's 180-day text valve. A monthly task; a
    no-op when nothing is old enough.

    Three arms since §13 slice 3 (§13.7): `ExecToolCall.args_text`,
    `ExecToolCall.result_text` and `ExecEvent.text`. The response's `trimmed`
    is a per-property map rather than one integer, so a caller can say which
    property actually shrank; `total` is the sum, for a caller that only
    wants the headline."""
    return await _post_json("/api/exec/retention", {"days": days} if days is not None else {})


async def post_exec_purge_secret_args() -> tuple[dict | None, str | None]:
    """POST /api/exec/purge-secret-args — §13.2's purge. Nulls `args_text` on
    every already-landed secret-tool call, matched on the name's terminal
    segment. Idempotent; all zeroes once it has run. Takes no body: the
    predicate is the server's own `SECRET_TOOL_NAMES`."""
    return await _post_json("/api/exec/purge-secret-args", {})


async def post_exec_purge_tool_text(
    *, name: str, args: bool = False, result: bool = False
) -> tuple[dict | None, str | None]:
    """POST /api/exec/purge-tool-text — exec-index-per-tool-policy.md §9
    slice 4's retro-purge, for ONE bare tool name.

    What it is for: a new opt-out (an app shipped `output: false`, or a
    human set an override) stops the next tick's rows and leaves the rows
    already in the graph untouched. This is what changes them, per row, from
    the policy UI.

    Deliberately NOT a list parameter and deliberately not defaulting either
    axis to true. The name-at-a-time shape is the guard — this repo's own
    scar is a script that wiped its target, and a purge taking a pattern is
    one typo from being that. The server validates the name is a single bare
    segment and 400s otherwise rather than matching nothing and reporting a
    cheerful zero."""
    return await _post_json(
        "/api/exec/purge-tool-text", {"name": name, "args": args, "result": result}
    )


# ---------------------------------------------------------------------------
# §9 step 4 — the two agent-facing MCP tools named in the design doc
# (`search_execution_graph`, `exec_run_detail`), over the search/detail
# routes the sync engine's own calls above already prove reachable. Both ARE
# in `TOOLS_SCHEMA` below — this is where the exec subgraph stops being
# something only the sync engine can see.
# ---------------------------------------------------------------------------

#: Lucene query-syntax metacharacters (standard analyser) — duplicated from
#: aw-knowledgeable's own `core/graph.py::_lucene_escape` rather than
#: imported: this connector is a plain HTTP client with no import of that
#: repo's code (module docstring, D2 §10).
_LUCENE_SPECIAL_RE = re.compile(r'([+\-&|!(){}\[\]^"~*?:\\/])')


def _lucene_escape(text: str) -> str:
    return _LUCENE_SPECIAL_RE.sub(r"\\\1", text)


def _lucene_wildcard_terms(text: str) -> str:
    """OR of per-word leading+trailing wildcards over the tool-call fulltext
    index — the fix for the measured defect named in the §9 step 4 card:
    Lucene's standard analyser does not split tokens on underscores, so a
    qualified tool-call name like
    `mcp__aw-gateway__aw__aw_knowledgeable__approve_pending_deletions` is ONE
    indexed token, and a bare term query for a substring of it (e.g.
    `approve_pending_deletions`) matches nothing even though the tool was
    called. Verified empirically against a throwaway Neo4j 5 + fulltext
    index: 0 hits on the bare term, 2 hits (the current and legacy
    MCP-gateway prefix spellings) on `*approve_pending_deletions*`, no
    parser error either way.

    Splits only on whitespace, so a multi-word free-text query ORs each
    word's wildcard rather than requiring one token to contain the whole
    phrase. The index DOES still split on hyphens — verified the same way, a
    wildcard pattern containing a literal hyphen matches nothing, because no
    indexed token contains that character anymore — so this cannot recover a
    query that spans a hyphen boundary (e.g. `aw-gateway`); search by the
    hyphen-free segment of a tool name instead (`approve_pending_deletions`,
    not the full qualified name).

    Unqualified — matches either `name` or `args_text`, whichever the
    `exec_toolcall_text` index finds it in first. This is `search_in='both'`
    (default)'s query, unchanged from before `search_in` existed (card
    3fe5bf3b-9510-8181-8ac4-fc5b334c5286 acceptance #3) — see
    `_lucene_wildcard_terms_scoped` for the field-restricted form that backs
    `search_in='name'`/`'args'`.
    """
    words = [w for w in text.split() if w]
    if not words:
        return ""
    return " OR ".join(f"*{_lucene_escape(w)}*" for w in words)


def _lucene_wildcard_terms_scoped(text: str, field: str) -> str:
    """Same per-word wildcard as `_lucene_wildcard_terms`, qualified to one
    Lucene field. `exec_toolcall_text` is one fulltext index over TWO
    properties (`tc.name`, `tc.args_text`) — Neo4j stores each as its own
    field in the underlying Lucene document, so a field-qualified clause
    (`name:*foo*`) restricts a hit to that one property instead of either.

    This is the actual fix for Defect 2 (card 3fe5bf3b-9510-8181-8ac4-
    fc5b334c5286): the measured failure was that a bare/unqualified query
    cannot tell "the tool was called" from "the tool's name string appears
    inside some OTHER call's arguments" — `search_in='name'` asks the index
    to only consider the `name` field, so a result literally cannot be an
    argument-blob false positive any more.
    """
    words = [w for w in text.split() if w]
    if not words:
        return ""
    return " OR ".join(f"{field}:*{_lucene_escape(w)}*" for w in words)


def _matched_field(result: dict, words: list[str]) -> str:
    """Which field actually carries the match, computed from the row's own
    already-returned `name` (never re-queried — no extra round trip). Used
    to label every tool-call result regardless of `search_in`, and to rank
    name hits above args hits in `search_in='both'` (default): a row whose
    only hit is inside `args_text` did NOT call the named tool, it only
    mentions the string inside its own arguments (e.g. a card brief, a
    wakeup prompt) — exactly the overcounting failure Defect 2 measured.
    """
    name = (result.get("name") or "").lower()
    if any(w.lower() in name for w in words):
        return "name"
    return "args"


#: Match-centred snippet window for Defect 1 (card 3fe5bf3b-9510-8181-8ac4-
#: fc5b334c5286): `args_text` measured at 5,831 bytes in a single result,
#: 83,609 characters across 20 results — the agent's tool-result budget
#: never saw it, it was spilled to a file instead. `_ARGS_SNIPPET_RADIUS` is
#: how much context survives on each side of the matched term;
#: `_ARGS_SNIPPET_MAX_CHARS` is a hard ceiling regardless of word length.
_ARGS_SNIPPET_RADIUS = 160
_ARGS_SNIPPET_MAX_CHARS = 400


def _text_snippet(args_text: str | None, words: list[str]) -> tuple[str | None, int]:
    """Return `(snippet, full_length)`. The snippet is centred on the first
    case-insensitive occurrence of any query word — deliberately NOT a
    head-truncation: the measured match is usually deep inside a long
    prompt, so truncating from the start would discard precisely what the
    caller searched for. Falls back to a leading window only when no query
    word appears in `args_text` at all (the result matched on `name`
    instead) — there is nothing to centre on in that case.

    Shared by the tool-call arm (`args_text`) and §13 slice 1's event arm
    (`ExecEvent.text`) rather than copied: §10 point 4's rule — search
    surfaces return match-centred snippets, full stored text only through
    `exec_run_detail` — is the same rule for both, and two implementations
    of it would drift into two different windows.
    """
    if not args_text:
        return args_text, 0
    length = len(args_text)
    lowered = args_text.lower()
    match_at = None
    match_len = 0
    for w in words:
        idx = lowered.find(w.lower())
        if idx != -1:
            match_at, match_len = idx, len(w)
            break
    if match_at is None:
        snippet = args_text[:_ARGS_SNIPPET_MAX_CHARS]
        return (snippet + "…" if length > _ARGS_SNIPPET_MAX_CHARS else snippet), length
    start = max(0, match_at - _ARGS_SNIPPET_RADIUS)
    end = min(length, match_at + match_len + _ARGS_SNIPPET_RADIUS)
    if end - start > _ARGS_SNIPPET_MAX_CHARS:
        end = start + _ARGS_SNIPPET_MAX_CHARS
    snippet = args_text[start:end]
    return (
        ("…" if start > 0 else "") + snippet + ("…" if end < length else ""),
        length,
    )


#: Attached to every tool-call search result. The measurement
#: (.tmp/exec-retrieval-test/RESULTS.md) found at least four non-equivalent
#: encodings of "run X called tool Y" that disagree with each other; this
#: tool sees exactly one of them (named MCP tool-call events synced from
#: AP-MT `run_events`), and must say so rather than imply completeness.
EXEC_TOOLCALL_SCOPE_CAVEAT = (
    "UNDERCOUNTING — what this never sees: a curl to an app's own /mcp route "
    "inside a Bash call (often guarded by a shell conditional whose exit "
    "status was never recorded), or a denormalised boolean column on `runs` "
    "(e.g. marked_flow_done) — both live only in Postgres and are not "
    "projected here. Covers only named MCP tool-call events synced from "
    "AP-MT run_events (kind='tool_call'). "
    "OVERCOUNTING — the more dangerous direction, stated just as plainly: in "
    "search_in='both' (the default), a result whose args_text merely "
    "mentions the queried string (e.g. inside a card brief or a wakeup "
    "prompt) is returned alongside genuine named calls with the SAME shape; "
    "check `matched_field` on each row — 'args' means that run did NOT call "
    "the tool, it only talked about it. Use search_in='name' to rule out "
    "this false-positive class entirely rather than trusting ranking alone. "
    "This arm searches the tool NAME and its ARGUMENTS only. A tool's "
    "RESULT has its own index: ask for search_in='result' (§13 slice 3) — "
    "the two cannot be queried in one call, and a name/args search finding "
    "nothing says nothing about what the tool returned. "
    "A tool may also appear under more than one spelling "
    "(a current vs. a legacy MCP-gateway prefix, or a malformed variant) — "
    "each distinct `name` string below is counted separately; sum the ones "
    "that mean the same tool yourself. 'How many runs called X in total' has "
    "no single honest number from this data — see name_counts_among_returned_"
    "results and possibly_truncated instead of trusting a bare count."
)


#: Attached to every `search_in='result'` result set (§13 slice 3). A
#: DIFFERENT caveat from the name/args one above, not a copy, because the
#: two arms are honest about different things: that one's hazard is
#: overcounting ("the name appears in another call's arguments"), which
#: cannot happen here — a hit in `result_text` IS that call's output. What
#: this arm owes instead is the list of results that are deliberately
#: absent, because every one of them makes a zero ambiguous.
EXEC_TOOLCALL_RESULT_SCOPE_CAVEAT = (
    "WHAT IS DELIBERATELY ABSENT FROM THIS INDEX, each making an empty "
    "result ambiguous rather than conclusive. (1) SECRET-HANDLING TOOLS' "
    "results are dropped wholesale and never projected — read_secret, "
    "write_secret, delete_secret, collect_secret, matched on the tool "
    "name's terminal segment so every gateway prefix is covered. Their "
    "node carries result_dropped=true instead; `exec_run_detail` shows it. "
    "The return value of a read_secret is not retrievable from this graph "
    "at all, by design. (2) RESULTS OLDER THAN 180 DAYS may have been "
    "nulled by the retention valve (result_trimmed on the call says so); "
    "Postgres still has them. (3) A CALL THAT NEVER RETURNED has no result "
    "— ~500 of 308k measured, plus anything still in flight. (4) Only "
    "named MCP tool-call events synced from AP-MT run_events are here at "
    "all: a curl to an app's /mcp route inside a Bash call is invisible to "
    "this whole layer, as its args_text sibling already states. "
    "An event's result is scrubbed then capped at 16,384 chars — result_len "
    "is the TRUE pre-cap length and result_truncated says whether the cap "
    "fired, which it genuinely does here (measured maximum 20.7 KB), so "
    "call exec_run_detail for the full stored text rather than assuming the "
    "snippet is all of it. "
    "SCRUBBING IS BEST-EFFORT AND THIS IS THE CHANNEL IT MATTERS MOST ON: "
    "results are how third-party credentials enter a run (a `cat .env`, a "
    "`docker inspect`, a git remote with an embedded token), and a "
    "credential that was never in this workspace's own secret store is "
    "covered by pattern matching alone. Treat anything that looks like a "
    "live credential here as one, and report it rather than quoting it. "
    "possibly_truncated is true when the result count hit `limit`: a count "
    "is only trustworthy when this is false."
)


#: Attached to every event search result set (§13 slice 1). The honesty this
#: one owes is different from the tool-call arm's: the timeline is being
#: projected in slices, so "no hit" is ambiguous in a way it is not for runs
#: or tool calls, and a caller who does not know which kinds are text-bearing
#: would read an empty result as "it never happened".
EXEC_EVENT_SCOPE_CAVEAT = (
    "PARTIAL BY CONSTRUCTION, in two separate ways. (1) Only text-BEARING "
    "event kinds are searchable here at all, because only they have a text "
    "property: `error` (a run's error string), `log` (a workflow log line), "
    "`thinking` (the model's own reasoning) and `node_end` (a workflow "
    "node's final output text). The other eleven projected kinds — "
    "node_start, finalizing, cli.first_byte, system.init, done, cli.error, "
    "auto_compact.blind, auto_compact.ineffective, connector.timing, "
    "container.started, cli.timeout — are STRUCTURED: they carry typed "
    "properties and no text, so they are read as a run's timeline through "
    "exec_run_detail, never found by this search. An empty result here does "
    "NOT mean the run emitted no events. (2) `tool_result` is NOT here: a "
    "tool's output rides the tool CALL, so search it with "
    "target='tool_calls', search_in='result'. `llm_token` is never "
    "projected anywhere: it is excluded from the timeline's own upstream "
    "definition, and its deltas reassemble into text already projected as a "
    "run's output_text. "
    "`thinking` is SPARSE — about 27% of runs emit any at all — so a run "
    "with no reasoning in its timeline is the normal case, not a gap. "
    "An event's `text` is scrubbed and capped at 16,384 chars; `text_len` is "
    "the TRUE pre-cap length and `text_truncated` says whether the cap fired "
    "— call exec_run_detail for the full stored text. "
    "possibly_truncated is true when the result count hit `limit`: a count "
    "is only trustworthy when this is false."
)


async def search_execution_graph(args: dict) -> tuple[str, bool]:
    """GET /api/exec/search/runs, /tool-calls and/or /events.

    `target` picks which layer of the exec subgraph's fulltext surface to
    hit: 'runs' (free text over Run input/output text), 'tool_calls'
    (tool-call name + arguments), 'events' (§13 slice 1 — `ExecEvent.text`),
    or 'both' (default).

    **'both' deliberately still means runs + tool_calls, not all three.**
    §13.3's rule for this surface is extend, never repurpose: a caller that
    never opts in must see byte-identical behaviour to the release before
    the event layer existed, and silently widening the default would change
    every existing caller's result shape. `target='events'` is explicit
    opt-in, like `search_in` was.

    Run-text queries are sent as literal Lucene terms, unwildcarded — the
    measurement's text-bearing pass (RESULTS.md v2) scored 10/10 on semantic
    recall, paraphrase recall and open synthesis with plain fulltext over
    natural-language run prose; no wildcarding was needed or wanted there.
    Tool-call queries are wildcard-wrapped per word (see
    `_lucene_wildcard_terms`) to fix the measured underscore-tokenization
    miss — without it, searching for a tool by its bare name finds nothing
    even when it was called.

    For `target in ('tool_calls', 'both')`, `search_in` (card
    3fe5bf3b-9510-8181-8ac4-fc5b334c5286) decides which Lucene field the
    query is scoped to: 'name' queries only `ExecToolCall.name` (genuine
    named calls, no argument-blob false positives — the fix for Defect 2),
    'args' queries only `args_text`, 'both' (default) sends the SAME
    unqualified query as always, unchanged. Every tool-call result carries
    `matched_field` ('name'/'args', computed from the row's own `name` —
    no extra round trip) regardless of mode, and in `search_in='both'`
    results are sorted name-hits-first.

    Every tool-call result also carries `args_text_snippet` (a window
    centred on the matched term, capped — never the full blob, which
    measured 5,831 bytes in a single result) and `args_text_len` (the true
    length); call `exec_run_detail` for the untruncated text (Defect 1).

    Every tool-call result set carries `scope_caveat` and
    `name_counts_among_returned_results` rather than a single trusted count:
    the measurement found at least four non-equivalent encodings of "which
    runs called tool X", and a tool returning one confident integer here
    would be lying. `possibly_truncated` is true when the result count hit
    `limit` — raise it (max 100) or narrow the query before treating a count
    as complete.
    """
    q = (args.get("q") or "").strip()
    if not q:
        return "q is required", True
    target = (args.get("target") or "both").strip().lower()
    if target not in ("runs", "tool_calls", "events", "both"):
        return "target must be one of: runs, tool_calls, events, both", True
    search_in = (args.get("search_in") or "both").strip().lower()
    if search_in not in ("name", "args", "result", "both"):
        return "search_in must be one of: name, args, result, both", True
    try:
        limit = int(args.get("limit") or 10)
    except (TypeError, ValueError):
        limit = 10
    limit = max(1, min(limit, 100))

    payload: dict = {"query": q, "target": target, "search_in": search_in}

    if target in ("runs", "both"):
        data, err = await _get(
            "/api/exec/search/runs", {"q": _lucene_escape(q), "limit": limit}
        )
        if err:
            return f"search_execution_graph failed (runs): {err}", True
        results = data.get("results", [])
        payload["runs"] = {
            "results": results,
            "count": len(results),
            "possibly_truncated": len(results) >= limit,
        }

    if target in ("tool_calls", "both") and search_in == "result":
        # §13 slice 3 — `search_in='result'` is its own ROUTE over its own
        # index (`exec_toolcall_result`), not a field-qualified query against
        # `exec_toolcall_text`: that index does not contain `result_text` at
        # all (§13.3 kept it a new index so the live name/args surface was
        # untouched by a rebuild over 288k populated nodes), so a
        # `result_text:*foo*` clause there would match nothing and read as
        # "no run ever got that output".
        #
        # Unwildcarded literal terms, like `runs` and `events` and unlike
        # `name`: a tool RESULT is output prose — a stdout blob, a JSON
        # document, an error message — not an underscore-joined identifier,
        # so the tokenization miss that forces wildcarding on tool NAMES
        # does not apply, and wildcarding natural-language words would only
        # widen every term for no recall gain.
        data, err = await _get(
            "/api/exec/search/tool-call-results",
            {"q": _lucene_escape(q), "limit": limit},
        )
        if err:
            return f"search_execution_graph failed (tool_call results): {err}", True
        results = data.get("results", [])
        words = [w for w in q.split() if w]
        for r in results:
            # One value, no ambiguity to resolve: this index covers exactly
            # one property, so unlike the name/args arm there is nothing to
            # compute. Stated on every row anyway so a caller can treat all
            # the arms uniformly.
            r["matched_field"] = "result_text"
            snippet, stored_len = _text_snippet(r.pop("result_text", None), words)
            r["result_text_snippet"] = snippet
            # The backend's `result_len` is the TRUE pre-cap length (§13.4) —
            # and this cap really fires here (measured max 20.7 KB against a
            # 16,384 ceiling), so falling back to the stored length when it
            # is present would understate it.
            if r.get("result_len") is None:
                r["result_len"] = stored_len
        name_counts: dict = {}
        for r in results:
            name = r.get("name") or "<unknown>"
            name_counts[name] = name_counts.get(name, 0) + 1
        payload["tool_calls"] = {
            "results": results,
            "count": len(results),
            "possibly_truncated": len(results) >= limit,
            "name_counts_among_returned_results": name_counts,
            "scope_caveat": EXEC_TOOLCALL_RESULT_SCOPE_CAVEAT,
        }
    elif target in ("tool_calls", "both"):
        if search_in == "name":
            tc_query = _lucene_wildcard_terms_scoped(q, "name")
        elif search_in == "args":
            tc_query = _lucene_wildcard_terms_scoped(q, "args_text")
        else:
            tc_query = _lucene_wildcard_terms(q)
        data, err = await _get(
            "/api/exec/search/tool-calls",
            {"q": tc_query, "limit": limit},
        )
        if err:
            return f"search_execution_graph failed (tool_calls): {err}", True
        results = data.get("results", [])
        words = [w for w in q.split() if w]
        for r in results:
            r["matched_field"] = _matched_field(r, words)
            snippet, full_len = _text_snippet(r.pop("args_text", None), words)
            r["args_text_snippet"] = snippet
            r["args_text_len"] = full_len
        # Stable sort: name hits first, original (score DESC) order
        # preserved as the tiebreak within each group.
        results.sort(key=lambda r: 0 if r["matched_field"] == "name" else 1)
        name_counts: dict = {}
        for r in results:
            name = r.get("name") or "<unknown>"
            name_counts[name] = name_counts.get(name, 0) + 1
        payload["tool_calls"] = {
            "results": results,
            "count": len(results),
            "possibly_truncated": len(results) >= limit,
            "name_counts_among_returned_results": name_counts,
            "scope_caveat": EXEC_TOOLCALL_SCOPE_CAVEAT,
        }

    if target == "events":
        # Literal Lucene terms, unwildcarded — the same choice `runs` makes
        # and for the same measured reason: event text is natural-language
        # prose (an error string, a workflow log line), not an
        # underscore-joined identifier, so the tokenization miss that forced
        # wildcarding on tool-call NAMES does not apply here.
        data, err = await _get(
            "/api/exec/search/events", {"q": _lucene_escape(q), "limit": limit}
        )
        if err:
            return f"search_execution_graph failed (events): {err}", True
        results = data.get("results", [])
        words = [w for w in q.split() if w]
        for r in results:
            # One `matched_field` value, not a guess between several: an
            # `ExecEvent` has exactly one free-text property, so unlike the
            # tool-call arm there is no name-vs-args ambiguity to resolve —
            # and the field is still stated on every row so a caller can
            # treat all three arms uniformly.
            r["matched_field"] = "event_text"
            snippet, stored_len = _text_snippet(r.pop("text", None), words)
            r["text_snippet"] = snippet
            # The backend's own `text_len` is the TRUE pre-cap length (§13.4);
            # only fall back to the stored length when it is absent.
            if r.get("text_len") is None:
                r["text_len"] = stored_len
        payload["events"] = {
            "results": results,
            "count": len(results),
            "possibly_truncated": len(results) >= limit,
            "scope_caveat": EXEC_EVENT_SCOPE_CAVEAT,
        }

    return _as_text(payload), False


async def exec_run_detail(args: dict) -> tuple[str, bool]:
    """GET /api/exec/runs/{run_id} — one run's metadata and scrubbed text,
    its tool calls, its event timeline (§13 slice 1) and its structural
    joins: direct CHILD_OF children, plus parent/qa/session/card pointers on
    the run itself.

    Passed through verbatim rather than re-formatted, which matters for one
    field in particular: the backend's own `caveat` — `CHILD_OF understates
    lineage across wakeups` (measured: 4 descendants reported where the real
    causal chain had 11) — stays in the response body exactly as the backend
    worded it, so there is one copy of that warning, not two that could
    drift apart.
    """
    run_id = (args.get("run_id") or "").strip()
    if not run_id:
        return "run_id is required", True
    params: dict = {}
    for key in ("toolcall_limit", "children_limit", "event_limit"):
        if args.get(key) is not None:
            try:
                params[key] = int(args[key])
            except (TypeError, ValueError):
                pass
    data, err = await _get(f"/api/exec/runs/{run_id}", params or None)
    if err:
        return f"exec_run_detail failed: {err}", True
    return _as_text(data), False


async def create_node(args: dict) -> tuple[str, bool]:
    label = (args.get("label") or "").strip()
    node_type = (args.get("type") or "").strip()
    if not label:
        return "label is required", True
    if not node_type:
        return "type is required", True
    data, err = await _post_json("/api/nodes", {"label": label, "type": node_type})
    if err:
        return f"create_node failed: {err}", True
    return _as_text(data), False


async def create_link(args: dict) -> tuple[str, bool]:
    from_id = (args.get("from_id") or "").strip()
    to_id = (args.get("to_id") or "").strip()
    link_type = (args.get("type") or "").strip()
    if not from_id or not to_id:
        return "from_id and to_id are required", True
    if not link_type:
        return "type is required", True
    data, err = await _post_json("/api/links", {"from_id": from_id, "to_id": to_id, "type": link_type})
    if err:
        return f"create_link failed: {err}", True
    return _as_text(data), False


async def get_graph(args: dict) -> tuple[str, bool]:
    focus = (args.get("focus") or "").strip()
    if not focus:
        return "focus is required — there is no whole-graph endpoint (docs/design/aw-knowledgeable-infra.md §5)", True
    depth = args.get("depth", 1)
    try:
        depth = int(depth)
    except (TypeError, ValueError):
        depth = 1
    # bug:knowledgeable-get-graph-search-nodes-bucket-default — mirrors
    # list_documents' `bucket` knob: omitted spans every bucket this token
    # can read (GET /api/graph's own `resolve_search_scope` dependency),
    # passed narrows to that one bucket.
    params = {"focus": focus, "depth": depth}
    bucket = (args.get("bucket") or "").strip()
    if bucket:
        params["bucket"] = bucket
    data, err = await _get("/api/graph", params)
    if err:
        return f"get_graph failed: {err}", True
    return _as_text(data), False


async def list_documents(args: dict) -> tuple[str, bool]:
    # bug:knowledgeable-list-documents-pinned-to-default-bucket — mirrors
    # search_graph's `bucket` knob: omitted spans every bucket this token can
    # read (GET /api/documents' own `resolve_search_scope` dependency),
    # passed narrows to that one bucket.
    bucket = (args.get("bucket") or "").strip()
    params = {"bucket": bucket} if bucket else None
    data, err = await _get("/api/documents", params)
    if err:
        return f"list_documents failed: {err}", True
    return _as_text(data), False


async def search_nodes(args: dict) -> tuple[str, bool]:
    q = args.get("q") or ""
    params: dict = {"q": q}
    if args.get("exclude"):
        params["exclude"] = args["exclude"]
    if args.get("limit") is not None:
        try:
            params["limit"] = int(args["limit"])
        except (TypeError, ValueError):
            pass
    # bug:knowledgeable-get-graph-search-nodes-bucket-default — mirrors
    # list_documents' `bucket` knob: omitted spans every bucket this token
    # can read, passed narrows to that one bucket.
    bucket = (args.get("bucket") or "").strip()
    if bucket:
        params["bucket"] = bucket
    data, err = await _get("/api/search", params)
    if err:
        return f"search_nodes failed: {err}", True
    return _as_text(data), False


async def search_graph(args: dict) -> tuple[str, bool]:
    """GET /api/search with the §12 (docs/design/aw-knowledgeable-v2-retrieval.md)
    retrieval knobs — lexical/semantic/tree, beam_width, min_score,
    related_vias, bucket. Distinct from ``search_nodes``, which stays
    lexical-only for the link picker (§6.4) and never grows these knobs.

    Types are coerced field-by-field here; knob×mode semantics are the
    backend's own validation matrix (§12) — a 400 from there reaches the
    caller verbatim via ``_describe_error``, never re-validated here, so
    there is exactly one source of truth for what is a valid combination.
    """
    q = args.get("q") or ""
    params: dict = {"q": q, "mode": args.get("mode") or "tree"}
    if args.get("limit") is not None:
        try:
            params["limit"] = int(args["limit"])
        except (TypeError, ValueError):
            pass
    if args.get("beam_width") is not None:
        try:
            params["beam_width"] = int(args["beam_width"])
        except (TypeError, ValueError):
            pass
    if args.get("min_score") is not None:
        try:
            params["min_score"] = float(args["min_score"])
        except (TypeError, ValueError):
            pass
    related_vias = args.get("related_vias")
    if related_vias:
        if isinstance(related_vias, str):
            params["related_vias"] = related_vias
        else:
            params["related_vias"] = ",".join(str(v) for v in related_vias)
    if args.get("bucket"):
        params["bucket"] = args["bucket"]
    if args.get("include_traversal") is not None:
        params["include_traversal"] = bool(args["include_traversal"])
    # §15.6 — coerce-and-forward, same pattern as every other knob above:
    # the backend's own validation matrix is the single source of truth,
    # never re-validated here.
    if args.get("collection"):
        params["collection"] = args["collection"]
    if args.get("anchor"):
        params["anchor"] = args["anchor"]
    if args.get("anchor_depth") is not None:
        try:
            params["anchor_depth"] = int(args["anchor_depth"])
        except (TypeError, ValueError):
            pass
    data, err = await _get("/api/search", params)
    if err:
        return f"search_graph failed: {err}", True
    return _as_text(data), False


async def knowledge_search_data(args: dict) -> tuple[dict | None, str | None]:
    """Same request shaping/timeout as `knowledge_search` below, returning
    the raw parsed JSON instead of pre-formatted text — the forwarding
    composition wrapper (`forwarding.compose_knowledge_search`,
    feature:knowledgeable-connector-forwards-to-codegraph) needs the dict to
    assemble the `{knowledge, code}` response; `knowledge_search` itself is
    the other caller, unchanged below.
    """
    query = (args.get("query") or "").strip()
    if not query:
        return None, "query is required"

    body: dict = {"query": query}
    for field in ("mode", "radius", "limit", "subqueries", "rounds", "budget_ms", "synthesize"):
        if args.get(field) is not None:
            body[field] = args[field]
    if args.get("filters"):
        body["filters"] = args["filters"]

    params: dict = {}
    if args.get("bucket"):
        params["bucket"] = args["bucket"]
    if args.get("collection"):
        params["collection"] = args["collection"]

    # Generous headroom over the server's own `budget_ms` (itself clamped
    # at 25000) — this connector's own timeout must never be the thing that
    # cuts the request before the server's own declared degradation gets a
    # chance to run.
    budget_ms = body.get("budget_ms") or 20000
    timeout = min(45.0, max(30.0, budget_ms / 1000.0 + 10.0))

    return await _post_json("/api/knowledge/search", body, params=params, timeout=timeout)


async def knowledge_search(args: dict) -> tuple[str, bool]:
    """POST /api/knowledge/search (feature:knowledge-search-tunable-
    endpoint) — the bounded plan -> fan-out -> fuse -> synthesize pipeline,
    distinct from `search_graph`'s single-arm retrieval: this tool runs
    MULTIPLE sub-queries across MULTIPLE arms in parallel and fuses them
    with Reciprocal Rank Fusion before optionally synthesizing an answer.

    Every knob below is advisory — the backend clamps, never 400s, on an
    out-of-floor value, and reports every override in the response's own
    `effective`/`clamped` fields. This tool forwards every knob field-by-
    field (never the raw `args` dict) and never re-validates a range
    itself, same rule `search_graph` already follows: the backend's own
    floors are the single source of truth.
    """
    query = (args.get("query") or "").strip()
    if not query:
        return "query is required", True

    data, err = await knowledge_search_data(args)
    if err:
        return f"knowledge_search failed: {err}", True
    return _as_text(data), False


def _as_text(data) -> str:
    import json

    return json.dumps(data, indent=2, ensure_ascii=False)


TOOLS_SCHEMA = [
    {
        "name": "upload_document",
        "description": (
            "Upload a document into aw-knowledgeable's knowledge graph. Accepts "
            "PDF, DOCX, TXT or MD, up to 10MB. Bytes go to aw-knowledgeable's own "
            "storage; Neo4j only gets a Document node plus a path reference."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "filename": {"type": "string", "description": "Original filename — its extension decides the accepted type."},
                "content_base64": {"type": "string", "description": "File bytes, base64-encoded."},
            },
            "required": ["filename", "content_base64"],
        },
    },
    {
        "name": "create_node",
        "description": "Create a standalone entity node (person/agent/system/concept/...) in the graph.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "label": {"type": "string", "description": "Human-readable label for the node."},
                "type": {"type": "string", "description": "Node type, e.g. 'person', 'concept', 'system'."},
            },
            "required": ["label", "type"],
        },
    },
    {
        "name": "create_link",
        "description": (
            "Create a relationship between two existing nodes (documents or entities). "
            "Both ids must already exist and belong to this connector's own tenant — "
            "a missing or cross-tenant id answers 404, never 403."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "from_id": {"type": "string", "description": "Source node id."},
                "to_id": {"type": "string", "description": "Target node id."},
                "type": {"type": "string", "description": "Relationship type, e.g. 'references', 'mentions'."},
            },
            "required": ["from_id", "to_id", "type"],
        },
    },
    {
        "name": "get_graph",
        "description": (
            "Get the neighbourhood graph (nodes + edges) around one focus node, 1 or 2 "
            "hops out, across every bucket your token can read. Pass bucket to narrow "
            "to just one. An unscoped neighbourhood is capped — the response declares "
            "neighbors_truncated/neighbors_returned/neighbors_available so a caller "
            "never has to guess. `focus` is required — there is no endpoint that "
            "returns the whole graph."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "focus": {"type": "string", "description": "Id of the node to center the view on."},
                "depth": {"type": "integer", "description": "1 or 2 hops. Default 1."},
                "bucket": {
                    "type": "string",
                    "description": "Knowledge bucket to narrow this view to. Omit to search every bucket your token can read.",
                },
            },
            "required": ["focus"],
        },
    },
    {
        "name": "list_documents",
        "description": (
            "List every document node in this connector's tenant, across every "
            "bucket your token can read. Pass bucket to narrow to just one."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "bucket": {
                    "type": "string",
                    "description": "Knowledge bucket to narrow this listing to. Omit to list every bucket your token can read.",
                },
            },
        },
    },
    {
        "name": "search_nodes",
        "description": (
            "Lexical (case-insensitive substring) search over node labels, across "
            "every bucket your token can read. Pass bucket to narrow to just one. "
            "Bounded by `limit` — this is deliberately not a whole-graph fanout."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "Substring to search for in node labels."},
                "exclude": {"type": "string", "description": "Optional node id to exclude from results (e.g. the node you're linking from)."},
                "limit": {"type": "integer", "description": "Max results. Default 20."},
                "bucket": {
                    "type": "string",
                    "description": "Knowledge bucket to narrow this search to. Omit to search every bucket your token can read.",
                },
            },
            "required": ["q"],
        },
    },
    {
        "name": "search_graph",
        "description": (
            "Retrieval search over the knowledge graph — lexical, semantic (vector "
            "similarity), or tree (beam-descend the bucket's topic tree). Unlike "
            "search_nodes (name matching for the link picker), this tool defaults "
            "to mode=tree because it is for knowledge retrieval, not label lookup. "
            "Two more scoping knobs, independent concepts: `collection` = stay inside "
            "this source folder; `anchor` = start the traversal from this node. "
            "The response envelope echoes the params that actually ran, plus "
            "strategy/not_applied/dropped_below_min_score — a knob the chosen mode "
            "can't honour is either a 400 (the knob does not exist for this mode "
            "or is out of range) or declared in the envelope (the request was valid "
            "but the world couldn't honour it, e.g. a bucket with no tree falling "
            "back to flat search). Always read the envelope; never assume the knob "
            "you set is the knob that ran."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {
                    "type": "string",
                    "description": "Query text. Required for every mode — semantic and tree embed it; lexical substring-matches it against node labels.",
                },
                "mode": {
                    "type": "string",
                    "enum": ["lexical", "semantic", "tree"],
                    "description": (
                        "Retrieval algorithm. 'lexical': case-insensitive substring match on node "
                        "labels. 'semantic': embed q and vector-search with a tenant/bucket filter. "
                        "'tree': beam-descend the bucket's topic tree, reporting topic_path per "
                        "result — requires an explicit bucket. Falls back to flat semantic search "
                        "if that bucket has no tree yet, or if bucket is omitted entirely (an "
                        "unscoped query spans every readable bucket, so there is no single tree to "
                        "descend); either way the fallback is declared as strategy='flat' in the "
                        "response, not silent. Default 'tree'."
                    ),
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results. Range 1-100 (backend-validated). Default 20.",
                },
                "beam_width": {
                    "type": "integer",
                    "description": (
                        "mode=tree only — rejected with 400 on any other mode. How many branches "
                        "survive each descent level. Range 1-10. Omit to use the server's configured "
                        "default (TOPIC_SEARCH_BEAM_WIDTH, 3)."
                    ),
                },
                "min_score": {
                    "type": "number",
                    "description": (
                        "mode=semantic or mode=tree only — rejected with 400 on mode=lexical. Drop "
                        "results below this cosine score, applied AFTER the top-`limit` results are "
                        "already chosen. Range 0.0-1.0. The response declares how many were dropped "
                        "as dropped_below_min_score."
                    ),
                },
                "related_vias": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["topic", "embedding", "entity"]},
                    "description": (
                        "mode=semantic or mode=tree only — rejected with 400 on mode=lexical. For "
                        "each result's document, attach its RELATED_TO neighbours restricted to "
                        "these edge kinds, returned as `related` in the response."
                    ),
                },
                "bucket": {
                    "type": "string",
                    "description": "Knowledge bucket to scope this search to. Omit to search every bucket your token can read.",
                },
                "collection": {
                    "type": "string",
                    "description": (
                        "mode=semantic or mode=tree only, and requires an explicit bucket — "
                        "rejected with 400 otherwise. A source-path folder prefix (e.g. "
                        "'notion/kanban/done/') to scope this search to — stay inside this "
                        "folder, full depth. On mode=tree this SKIPS the topic-tree descent "
                        "entirely (the human already supplied the scope the descent exists "
                        "to find): the response declares strategy='flat' and carries no "
                        "topic_path. Composes with `anchor` — the anchor's traversal still "
                        "walks freely across folders, this filter only narrows what is kept."
                    ),
                },
                "anchor": {
                    "type": "string",
                    "description": (
                        "mode=semantic or mode=tree only, requires an explicit bucket and a "
                        "non-empty q — rejected with 400 otherwise. The id of a Document, "
                        "Entity or Collection to START a traversal from (never a Topic id — "
                        "topics are rebuilt wholesale, so a saved anchor on one would die on "
                        "the next rebuild). `path:<folder prefix>` is sugar for anchoring on "
                        "that Collection. A 404 means the id does not resolve in your tenant. "
                        "The response declares strategy='anchored', an `anchor` block "
                        "(id/kind/depth/vias/expanded_documents/truncated), and an "
                        "`anchor_hops` field per result — ranking is by query score alone, "
                        "hops are informational, never blended into the score."
                    ),
                },
                "anchor_depth": {
                    "type": "integer",
                    "description": (
                        "Only valid with `anchor` — rejected with 400 without it. How many "
                        "LINKS_TO/RELATED_TO expansion rounds past the anchor's own base set "
                        "(the anchor itself for a Document, its direct members for a "
                        "Collection, its mentioning documents for an Entity). Range 0-2, "
                        "default 1. `related_vias` selects which RELATED_TO edges the "
                        "expansion follows; LINKS_TO is always followed."
                    ),
                },
                "include_traversal": {
                    "type": "boolean",
                    "description": (
                        "mode=semantic or mode=tree only — rejected with 400 on mode=lexical. "
                        "Attach `traversal` to the response: on mode=tree with a real tree, "
                        "kind='beam_descent' with the levels the beam actually walked (which "
                        "topics survived each descent, which were pruned and why, plus the "
                        "final chunk re-score); otherwise kind='flat' with a reason. Emitted by "
                        "the backend that actually walked the graph, never reconstructed. "
                        "Default false — this costs tokens, so ask only when you need to explain "
                        "how a result was found, not just what it is."
                    ),
                },
            },
            "required": ["q"],
        },
    },
    {
        "name": "knowledge_search",
        "description": (
            "Reasoning, multi-query retrieval: decomposes `query` into up to "
            "`subqueries` sub-questions (one cheap planner call), retrieves each "
            "in parallel across the modes below, fuses every arm with Reciprocal "
            "Rank Fusion (on ranks, never on raw scores — vector/lexical/graph "
            "scores are incomparable scales), and optionally synthesizes one "
            "closed-book answer with citations. Bounded and NOT agentic — one "
            "planning call, one fan-out, one fuse, one synthesis call (plus up to "
            "`rounds` more fan-out+fuse cycles); it never loops on its own. Use "
            "this instead of repeated search_graph calls when you want the model "
            "to decompose the question and fuse multiple retrieval strategies "
            "for you in one call. "
            "EVERY KNOB BELOW IS ADVISORY — the server clamps out-of-floor values "
            "rather than rejecting them, and the response's `effective` field is "
            "what ACTUALLY ran; `clamped` lists every override, in plain English. "
            "In particular: `limit` has a floor of 20 — a lower value is clamped "
            "up because this corpus measurably returns a WORSE result SET below "
            "that floor, not a prefix of a larger one. Always read `effective`/"
            "`clamped`, never assume the value you sent is the value that ran. "
            "The response also carries `chunks` (fused, ranked, with per-arm "
            "provenance), `citations`, `answer` (null if `synthesize=false` or "
            "if the synthesis door is degraded — retrieval still runs either way), "
            "and `plan` (which sub-queries actually ran, and whether the planner "
            "or a deterministic fallback produced them). "
            "`include_code` (default false) additionally forwards a code-reality "
            "question to the workspace code graph, when installed/available "
            "(docs/design/knowledgeable-connector-tool-forwarder.md) — the "
            "response then wraps as `{knowledge, code}` instead of the plain "
            "retrieval payload: `knowledge` is this tool's unchanged payload, "
            "`code` is a SEPARATE section (`status: 'ok'|'unavailable'`, "
            "`result`/`reason`, `elapsed_ms`) that is NEVER fused into `chunks` "
            "or `citations` — the knowledge graph and the code graph are "
            "disjoint key spaces, so a code result standing in for KG evidence "
            "would be fabricated comparability. Omitting `include_code` keeps "
            "the response byte-identical to today. "
            "`code_only` (default false, only meaningful with `include_code`) "
            "skips the knowledge-graph leg entirely — `knowledge` comes back "
            "null and only the code graph is queried. Use this when you want "
            "just the code perspective: it avoids a redundant fused KG search "
            "for an answer you are going to discard."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The question to search for. Required."},
                "include_code": {
                    "type": "boolean",
                    "description": (
                        "Default false. true also asks the workspace code graph "
                        "(codegraph), when installed and reachable, and returns its "
                        "answer in a separate top-level `code` section beside "
                        "`knowledge`. Absent entirely, the response is unchanged."
                    ),
                },
                "code_query": {
                    "type": "string",
                    "description": (
                        "Only used when `include_code` is true. The code-side "
                        "question, when it should differ from `query` (e.g. a "
                        "symbol name vs. a natural-language ask). Defaults to `query`."
                    ),
                },
                "code_only": {
                    "type": "boolean",
                    "description": (
                        "Default false. Only meaningful when `include_code` is "
                        "true — skips the knowledge-graph leg entirely, returning "
                        "`knowledge: null` and just the `code` section. Ignored "
                        "(and the response stays byte-identical to legacy) when "
                        "`include_code` is absent or false."
                    ),
                },
                "mode": {
                    "type": "string",
                    "enum": ["vector", "lexical", "graph", "fused"],
                    "description": (
                        "Which retrieval arm(s) to fan every sub-query across. 'vector': "
                        "embedding similarity only. 'lexical': BM25 full-text over chunk "
                        "text (distinct from search_nodes' label-substring match). "
                        "'graph': seed a few candidate documents by vector similarity, "
                        "then widen via LINKS_TO/RELATED_TO out to `radius` hops and score "
                        "the expanded set exactly — finds a differently-worded answer "
                        "reachable only by document structure, not by content similarity "
                        "alone. 'fused' (default): all three, RRF-merged."
                    ),
                },
                "radius": {
                    "type": "integer",
                    "description": (
                        "mode=graph or mode=fused only. LINKS_TO/RELATED_TO expansion hops "
                        "from the graph arm's own seed documents. Floor 0, ceiling 2 — "
                        "values outside that range are clamped, not rejected. Default 1."
                    ),
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "Per-arm result count AND the final fused count fed to synthesis. "
                        "FLOOR 20, NOT NEGOTIABLE — measured on this corpus, a lower limit "
                        "returns a worse result SET, not a prefix of this one. A value "
                        "below 20 is clamped up and declared in `clamped`, never honoured "
                        "as sent. Default 20."
                    ),
                },
                "subqueries": {
                    "type": "integer",
                    "description": (
                        "Max sub-queries the planner may produce from `query`. Floor 1, "
                        "HARD ceiling 5 (never raised above 5 regardless of value sent). "
                        "Default 3."
                    ),
                },
                "rounds": {
                    "type": "integer",
                    "description": (
                        "Extra refine fan-out+fuse cycles beyond the first, each excluding "
                        "sub-queries already tried. Floor 0, ceiling 2. Default 0 — raising "
                        "this roughly multiplies latency/cost per round, so it should be "
                        "requested deliberately, not as a default habit."
                    ),
                },
                "filters": {
                    "type": "object",
                    "description": (
                        "Optional post-retrieval narrowing, applied before fusion so ranks "
                        "reflect the filtered set. Keys: `document_ids` (array of string — "
                        "keep only these documents), `labels` (array of string — keep only "
                        "documents whose label is in this set), `date_from`/`date_to` (ISO "
                        "date strings — keep only documents created in this range). Any "
                        "other key is REJECTED with an error, never silently ignored."
                    ),
                    "properties": {
                        "document_ids": {"type": "array", "items": {"type": "string"}},
                        "labels": {"type": "array", "items": {"type": "string"}},
                        "date_from": {"type": "string"},
                        "date_to": {"type": "string"},
                    },
                },
                "budget_ms": {
                    "type": "integer",
                    "description": (
                        "Wall-clock budget for the whole pipeline. Default 20000, server-"
                        "capped at 25000 — stay under this, the production tunnel edge "
                        "cuts requests around 30s regardless. Past budget, synthesis is "
                        "skipped and the fused retrieval is still returned, declared in "
                        "`clamped` — never a timeout, never a 500."
                    ),
                },
                "synthesize": {
                    "type": "boolean",
                    "description": (
                        "Default true. false skips the closed-book synthesis call entirely "
                        "and returns just the fused chunks/citations — the cheap path for a "
                        "caller that wants to do its own reading."
                    ),
                },
                "bucket": {
                    "type": "string",
                    "description": "Knowledge bucket to scope this search to. Omit to search every bucket your token can read.",
                },
                "collection": {
                    "type": "string",
                    "description": "Source-path folder prefix to stay inside, full depth, e.g. 'notion/kanban/done/'.",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_execution_graph",
        "description": (
            "Full-text search over this workspace's own agent-run execution history "
            "(docs/design/execution-history-in-production-neo4j.md) — a separate, "
            "tenant-isolated subgraph next to the knowledge corpus, service-caller-only. "
            "Searches run input/output text and/or tool-call names+arguments. "
            "target='runs' searches Run.input_text/output_text (natural-language "
            "prose — plain fulltext, no wildcarding). target='tool_calls' searches "
            "ExecToolCall.name/args_text, wildcard-wrapped per word so a tool name "
            "like 'approve_pending_deletions' is found even though the full qualified "
            "name (e.g. 'mcp__aw-gateway__aw__aw_knowledgeable__approve_pending_deletions') "
            "is one indexed token that a bare term query would miss entirely — this "
            "cannot recover a query spanning a HYPHEN boundary, so search by the "
            "hyphen-free segment of a tool name, not the full prefixed name. "
            "target='events' searches ExecEvent.text — the Event timeline's "
            "free-text kinds: `error` strings, workflow `log` lines, "
            "`thinking` (the model's own reasoning, SPARSE — ~27% of runs) "
            "and `node_end` (a workflow node's final output text). Prose, so "
            "plain fulltext like 'runs'. The other eleven projected kinds "
            "are STRUCTURED (typed properties, no text) and are read as a "
            "run's chronological timeline through exec_run_detail's "
            "`events`, never found by this search — an empty result does not "
            "mean the run emitted no events. Each row "
            "carries kind, matched_field='event_text', text_snippet (a "
            "match-centred window) and text_len/text_truncated, plus a "
            "scope_caveat stating exactly which kinds are and are not "
            "reachable. A tool's RESULT is NOT an event — it rides the tool "
            "call; use target='tool_calls' with search_in='result'. "
            "target='both' (default) runs runs+tool_calls — NOT events: the "
            "default is byte-identical to the release before the event layer "
            "existed, so ask for target='events' explicitly. "
            "For tool_calls, `search_in` picks WHICH field counts as a hit: "
            "'name' returns ONLY genuine named calls of that tool — use this to "
            "answer 'did run X actually call tool Y'. 'args' returns ONLY matches "
            "inside a call's own arguments (the string is merely mentioned — e.g. "
            "in a prompt or card brief — that run did NOT call the tool). 'both' "
            "(default) is unchanged from before this parameter existed: every "
            "match either field, but now every result also carries `matched_field` "
            "('name'/'args') and name hits sort before args hits. The overcounting "
            "failure this fixes: at the default, a result with matched_field='args' "
            "looks identical to a real call unless you check that field — prefer "
            "search_in='name' over trusting ranking alone when the question is "
            "specifically 'who called X'. "
            "'result' searches what the tool RETURNED (ExecToolCall.result_text) "
            "— a separate index, so it is never mixed into the other three and a "
            "name/args search finding nothing says nothing about outputs. Use it "
            "for 'which run saw this error / got this output / read this file'. "
            "Rows carry matched_field='result_text', result_text_snippet and "
            "result_len/result_truncated (the cap really fires here — results run "
            "to 20 KB). SECRET-HANDLING TOOLS' RESULTS ARE NEVER PROJECTED (the "
            "return value of a read_secret/collect_secret is unreachable from "
            "this graph by design, `result_dropped` on the node says so), and "
            "results older than 180 days may have been nulled by the retention "
            "valve — so an empty result here is ambiguous; read the scope_caveat "
            "before reporting one as 'it never happened'. "
            "Every tool_calls result also carries args_text_snippet (a window "
            "centred on the matched term, capped — the full argument blob can run "
            "to kilobytes and would overflow an agent's own result budget) and "
            "args_text_len (the true full length); call exec_run_detail for the "
            "untruncated text. "
            "Every tool_calls result set carries scope_caveat (states BOTH the "
            "undercounting gaps — NOT a curl-to-/mcp-route inside Bash, NOT a "
            "denormalised boolean column on `runs` — AND the overcounting one "
            "above; multiple spellings of the same tool are counted separately) "
            "and name_counts_among_returned_results — there is no single honest "
            "total for 'how many runs called tool X', only a documented breakdown "
            "of what was actually found. possibly_truncated is true when the "
            "result count hit `limit`: a count is only trustworthy when this is "
            "false."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "Query text. For tool_calls, use the bare tool name (no gateway prefix, no hyphens) for the most reliable match."},
                "target": {
                    "type": "string",
                    "enum": ["runs", "tool_calls", "events", "both"],
                    "description": (
                        "Which layer of the exec subgraph to search. Default "
                        "'both' = runs + tool_calls (unchanged); 'events' is "
                        "explicit opt-in and is NOT included in 'both'."
                    ),
                },
                "search_in": {
                    "type": "string",
                    "enum": ["name", "args", "result", "both"],
                    "description": (
                        "Only applies to target='tool_calls'/'both'. 'name' = only "
                        "genuine named tool-call events, no argument-mention false "
                        "positives. 'args' = only matches inside a call's own "
                        "arguments (the tool was NOT called). 'result' = only "
                        "matches in what the tool RETURNED, over its own index; "
                        "secret-handling tools' results are never projected and "
                        "results over 180 days old may have been trimmed. 'both' "
                        "(default) = name + args, each row tagged matched_field, "
                        "name hits ranked first — it does NOT include results."
                    ),
                },
                "limit": {"type": "integer", "description": "Max results per target. Range 1-100. Default 10."},
            },
            "required": ["q"],
        },
    },
    {
        "name": "exec_run_detail",
        "description": (
            "One agent run from the execution-history subgraph: metadata, scrubbed "
            "input/output text, its tool calls, its EVENT TIMELINE, and its "
            "structural joins — direct "
            "CHILD_OF children plus parent_run_id/qa_of_run_id/session_id/card_page_id "
            "on the run itself. 404 if run_id does not exist in this tenant. "
            "`events` is the run's own timeline, chronological (ORDER BY ts), one "
            "row per projected run_events row with that kind's typed properties "
            "(node_start -> agent/label/provider/model_slug; finalizing -> "
            "tokens_in/tokens_out/cost_usd; system.init -> model/cwd/"
            "permission_mode/session_id/tools_count; cli.error -> subtype/is_error; "
            "error, log, thinking and node_end -> scrubbed `text`; and so on). "
            "Interleave it with `tool_calls` — each call at its own `ts` and its "
            "result at `result_ts` — to reconstruct the full Event timeline. "
            "COMPLETE as of §13 slice 3, with these standing exceptions, which "
            "you should state rather than letting a gap read as 'nothing "
            "happened': `tool_result` is on the tool CALL as `result_text`, not "
            "in `events`; `thinking` is sparse (~27% of runs emit any); and "
            "`llm_token` is never projected (excluded by the timeline's own "
            "definition — its deltas reassemble into the run's own output_text). "
            "A tool call's `result_text` can be absent for three DIFFERENT "
            "reasons the row distinguishes: result_dropped=true (a "
            "secret-handling tool — its return value is never projected), "
            "result_trimmed=true (the 180-day retention valve nulled it; "
            "Postgres still has it), or neither (the call never returned). "
            "The response always carries `caveat`: CHILD_OF understates lineage "
            "across wakeups (a wakeup-resumed run's causal parent is recorded as "
            "prose inside its input, not a foreign key) — measured: a chain whose "
            "real depth was 11 traversed as 4. Do not report a descendant count or a "
            "root run from this data as exhaustive without repeating that caveat."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "The run id to look up."},
                "toolcall_limit": {"type": "integer", "description": "Max tool calls to return. Range 0-2000. Default 200."},
                "children_limit": {"type": "integer", "description": "Max direct children to return. Range 0-1000. Default 100."},
                "event_limit": {"type": "integer", "description": "Max timeline events to return. Range 0-5000. Default 500."},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "list_pending_deletions",
        "description": (
            "List documents this connector's fs-sync engine has marked pending-delete "
            "(docs/design/aw-knowledgeable-fs-sync.md §7's two-phase delete — a sync tick "
            "only ever MARKS a document; nothing is removed until approve_pending_deletions "
            "is called). Returns each document's id and source_path, plus a "
            "prefix_breakdown grouping them by immediate parent directory (the same "
            "grouping the approval UI uses) so you can see the shape of the queue before "
            "scoping an approve or decline call."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "bucket": {
                    "type": "string",
                    "description": "Knowledge bucket to narrow this listing to. Omit to list every bucket your token can read.",
                },
            },
        },
    },
    {
        "name": "approve_pending_deletions",
        "description": (
            "Permanently delete a SCOPED, COUNTED subset of the pending-deletion queue — "
            "the approval half of §7's two-phase delete. Requires `bucket` (require_bucket_write "
            "has no cross-bucket mode) and at least one of `path_prefix`/`node_ids` to define "
            "the scope — there is deliberately no 'approve everything pending'. `expected_count` "
            "is REQUIRED: this tool lists the current pending-delete queue itself, computes "
            "which documents match your scope, and refuses to delete anything — returning an "
            "error instead, with nothing touched — if that count doesn't match `expected_count` "
            "exactly. Call list_pending_deletions first, count what you mean to approve, then "
            "pass that count here; it is the one safety net standing behind an irreversible "
            "action. Every call, including a rejected one, is written to this connector's own "
            "audit log (actor, timestamp, requested scope, resulting count) — there is no path "
            "to clearing this queue that leaves no record of who did it."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string", "description": "Knowledge bucket to act in. Required."},
                "path_prefix": {
                    "type": "string",
                    "description": (
                        "Only documents whose source_path starts with this prefix. Composable "
                        "with node_ids — when both are given a document must match both."
                    ),
                },
                "node_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Only these specific document ids. Composable with path_prefix — when "
                        "both are given a document must match both."
                    ),
                },
                "expected_count": {
                    "type": "integer",
                    "description": (
                        "Exactly how many documents your scope should match right now. "
                        "Required — a mismatch rejects the whole call and deletes nothing."
                    ),
                },
            },
            "required": ["bucket", "expected_count"],
        },
    },
    {
        "name": "decline_pending_deletions",
        "description": (
            "Restore a subset of the pending-deletion queue to normal — the reversible half "
            "of §7's two-phase delete: a document stays in the graph and its pending-delete "
            "mark clears. Requires `bucket`. Unlike approve, a scope is optional: omitting "
            "both `path_prefix` and `node_ids` declines EVERY document pending-delete in this "
            "bucket — safe because decline only restores, it never deletes. Pass "
            "`expected_count` if you want the same mismatch check approve_pending_deletions "
            "enforces (optional here, not required). Every call is written to this "
            "connector's own audit log, same as approve."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string", "description": "Knowledge bucket to act in. Required."},
                "path_prefix": {
                    "type": "string",
                    "description": (
                        "Only documents whose source_path starts with this prefix. Omit this "
                        "AND node_ids to decline every pending document in this bucket."
                    ),
                },
                "node_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Only these specific document ids. Omit this AND path_prefix to "
                        "decline every pending document in this bucket."
                    ),
                },
                "expected_count": {
                    "type": "integer",
                    "description": (
                        "Optional safety check — if given, the matched count must equal this "
                        "or the call is rejected and nothing is restored."
                    ),
                },
            },
            "required": ["bucket"],
        },
    },
]
