# aw-app-knowledgeable

Ingestion + retrieval MCP connector for [fredericowu/aw-knowledgeable](https://github.com/fredericowu/aw-knowledgeable),
the standalone Neo4j knowledge-graph service. 8 tools, gateway-prefixed
`aw__aw_knowledgeable__*`, mapped 1:1 onto aw-knowledgeable's own API:

| Tool | aw-knowledgeable endpoint |
|---|---|
| `upload_document` | `POST /api/documents` (multipart) |
| `create_node` | `POST /api/nodes` |
| `create_link` | `POST /api/links` |
| `get_graph` | `GET /api/graph?focus=&depth=` |
| `list_documents` | `GET /api/documents` |
| `search_nodes` | `GET /api/search?q=&exclude=&limit=` |
| `search_graph` | `GET /api/search?q=&mode=&limit=&beam_width=&min_score=&related_vias=&collection=&anchor=&anchor_depth=` |
| `knowledge_search` | `POST /api/knowledge/search` — multi-query, multi-arm, RRF-fused retrieval + optional synthesis |

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

## Execution-history projection (flag-off)

`docs/design/execution-history-in-production-neo4j.md` in `aw-workspace`.
This connector also owns the **execution-history sync**: it reads agent-run
metadata and text from agents-platform-multitenant's Postgres, **scrubs
credentials out of it here**, and posts batches to aw-knowledgeable's
service-authed `/api/exec/*` routes, which land them in an `Exec*`-labelled
subgraph that is structurally disjoint from the knowledge corpus.

Two properties to keep in mind before touching any of it:

- **The scrubbing happens on this side, and only on this side.** This process
  is where the workspace `SecretStore` lives, so exact-match redaction against
  every known-live secret value is possible here and nowhere else
  (`knowledgeable_app/exec_scrub.py`). aw-knowledgeable never holds the
  workspace secret store.
- **This connector never holds the Neo4j password**, and it never speaks Bolt.
  That is why the loading is connector-side at all. The old
  `scripts/load_exec_retrieval_neo4j.py` harness in `aw-workspace` is dead as
  a production path — it wipes its target and bypasses aw-knowledgeable's
  template catalogue — and now refuses to run against any graph holding a
  `(:Document)` node.

Ships **off**. To turn it on:

1. Save the **AP-MT Postgres DSN** (`apmt_database_url`) in the app window —
   it goes to the secret store, not plain config, because a DSN carries a
   password.
2. Set **`exec_sync_source_tenant`** to the AP-MT `tenant_id` to read. There is
   deliberately no default: AP-MT's tenant namespace and aw-knowledgeable's
   graph `tenant` namespace are different things, and a wrong value here reads
   zero rows and reports a clean tick.
3. Set **`exec_sync_enabled`** true, then enable the `KB exec-history sync —
   tick` (10 min) and `KB exec-history — weekly reconcile` scheduled tasks.

```bash
aw-workspace-cli knowledgeable-exec-sync run [--max-runs N]   # one bounded tick
aw-workspace-cli knowledgeable-exec-sync status               # sentinel + staleness
aw-workspace-cli knowledgeable-exec-sync reconcile            # SQL vs graph counts
aw-workspace-cli knowledgeable-exec-sync retention [--days N] # the args_text valve
```

`status` exits non-zero when the sync is enabled and stale or unreachable —
silence is degraded, not healthy. Rollback is `POST /api/exec/wipe` on
aw-knowledgeable (one batched, label-scoped delete); Postgres is the system of
record, so the projection is always rebuildable and never the thing to repair
by hand.
