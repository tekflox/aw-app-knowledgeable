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


async def _post_json(path: str, body: dict) -> tuple[dict | None, str | None]:
    async with httpx.AsyncClient(timeout=30) as http_client:
        try:
            resp = await http_client.post(f"{base_url()}{path}", json=body, headers=_headers())
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
    data, err = await _get("/api/graph", {"focus": focus, "depth": depth})
    if err:
        return f"get_graph failed: {err}", True
    return _as_text(data), False


async def list_documents(args: dict) -> tuple[str, bool]:
    data, err = await _get("/api/documents")
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
    data, err = await _get("/api/search", params)
    if err:
        return f"search_graph failed: {err}", True
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
            "hops out. `focus` is required — there is no endpoint that returns the "
            "whole graph."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "focus": {"type": "string", "description": "Id of the node to center the view on."},
                "depth": {"type": "integer", "description": "1 or 2 hops. Default 1."},
            },
            "required": ["focus"],
        },
    },
    {
        "name": "list_documents",
        "description": "List every document node in this connector's tenant.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "search_nodes",
        "description": (
            "Lexical (case-insensitive substring) search over node labels. Bounded by "
            "`limit` — this is deliberately not a whole-graph fanout."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "Substring to search for in node labels."},
                "exclude": {"type": "string", "description": "Optional node id to exclude from results (e.g. the node you're linking from)."},
                "limit": {"type": "integer", "description": "Max results. Default 20."},
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
                        "result (falls back to flat semantic search if the bucket has no tree yet — "
                        "declared as strategy='flat' in the response, not silent). Default 'tree'."
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
                    "description": "Knowledge bucket to scope this search to. Omit to use the connector's default bucket.",
                },
            },
            "required": ["q"],
        },
    },
]
