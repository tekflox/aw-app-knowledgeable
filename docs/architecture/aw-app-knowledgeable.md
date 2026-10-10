---
repo: architecture
path: docs/architecture/aw-app-knowledgeable.md
source: generated
edited: false
checksum: sha256:f4822851736eb99d84eb0846b1199d7108283254b6fff873c6ffdc125a974588
---
# Knowledgeable Connector

- **repo**: aw-app-knowledgeable
- **layer**: app
- **technologies**: python
- **health** (derived): planned

Ingestion + retrieval MCP connector for aw-knowledgeable (fredericowu/aw-knowledgeable), the standalone Neo4j knowledge-graph service — 7 tools mapped onto its API: upload_document, create_node, create_link, get_graph, list_documents, search_nodes (lexical name matching, for the link picker), search_graph (lexical/semantic/tree retrieval with the §12 knobs — beam_width, min_score, related_vias, bucket). This app is a thin HTTP client; it has no Neo4j driver and no knowledge of aw-knowledgeable's own tenant-isolation seam. Per docs/design/aw-knowledgeable-infra.md §10 (decision D2): the tenant a call lands in comes from THIS app's own shared service secret, never from the calling agent's self-asserted headers.

## Connections
- `http` → **aw-workspace** — routes mounted at /api/apps/knowledgeable
- `stdio-mcp` → **mcp-gateway** — MCP surface aggregated by the gateway

## MCP tools
- `create_link`
- `create_node`
- `get_graph`
- `list_documents`
- `search_graph`
- `search_nodes`
- `upload_document`

## Requirements
_none documented_
