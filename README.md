# aw-app-knowledgeable

Ingestion MCP connector for [fredericowu/aw-knowledgeable](https://github.com/fredericowu/aw-knowledgeable),
the standalone Neo4j knowledge-graph service. 6 tools, gateway-prefixed
`aw__aw_knowledgeable__*`, mapped 1:1 onto aw-knowledgeable's own API:

| Tool | aw-knowledgeable endpoint |
|---|---|
| `upload_document` | `POST /api/documents` (multipart) |
| `create_node` | `POST /api/nodes` |
| `create_link` | `POST /api/links` |
| `get_graph` | `GET /api/graph?focus=&depth=` |
| `list_documents` | `GET /api/documents` |
| `search_nodes` | `GET /api/search?q=&exclude=&limit=` |

This app has **no Neo4j driver and no Cypher** — it is a thin HTTP client. See
`docs/design/aw-knowledgeable-infra.md` §10 (decision D2) in `aw-workspace` for
the full design: why this is Tier-1 rather than Tier-2 or a stdio-bridge, why
reachability works by container **name** (never an IP), and why the tenant a
call lands in comes from this app's own credential, never from the calling
agent.

## Install

```bash
aw-workspace-cli marketplace install knowledgeable
```

Then open **Knowledgeable Connector** in the Apps grid:

1. Set **Base URL** in the app's Settings gear if aw-knowledgeable isn't at
   the default `http://aw-knowledgeable:8090`.
2. Save the **service secret** in the app window — it must equal
   aw-knowledgeable's own `KNOWLEDGEABLE_SERVICE_SECRET`.
3. Click **Test the connection**.

## The secret, not a caller identity

There is no per-agent identity to authenticate on this seam. aw-knowledgeable's
`resolve_tenant_id` maps this connector's shared secret to exactly one declared
tenant (`KNOWLEDGEABLE_SERVICE_TENANT_ID`) — never a caller-supplied header
like `X-Aw-Caller-Run-Id`, which is self-asserted and unauthenticated. Rotating
the secret takes effect on the next tool call with no restart on this side;
the matching value on aw-knowledgeable's side needs its own `.env` update +
restart there.

## Reachability

aw-knowledgeable must be reachable **by container name** from inside the
`aw-app-mcp-gateway` container — an IP is deliberately never used here (D2 §Q2:
"an IP would re-create the brittleness the socat workaround had to accept").
If tool calls fail with a DNS-looking error, check that aw-knowledgeable's own
compose service declares an explicit `container_name`/alias matching the
configured Base URL.
