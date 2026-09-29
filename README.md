# NoSQL MCP Server

A multi-NoSQL data source [MCP (Model Context Protocol)](https://modelcontextprotocol.io/)
server written in Go, built on [FreePeak/cortex](https://github.com/FreePeak/cortex).
It exposes Elasticsearch and Redis through a unified MCP interface so any
MCP-aware AI client (Trae, Cursor, Claude Desktop, and similar tools) can
query documents, inspect cache data, and check cluster health using natural
language — without writing a client or logging into the data stores.

## What this project actually does for you

MCP turns an AI assistant into a secure, conversational operations console
for your NoSQL infrastructure. Instead of context-switching between Kibana,
`redis-cli`, SSH sessions, and ad-hoc scripts, you ask the AI and it calls
the right tool, returns the result, and explains it.

### For developers

- **Debug production issues conversationally.** "Find the last 50 ERROR
  logs from service `order-api` in the last hour" becomes a single question;
  the server runs the native Elasticsearch Query DSL for you.
- **Inspect cache contents without `redis-cli`.** Look up a key's value,
  type and TTL, scan keys by pattern, and read hashes, lists, sets, sorted
  sets and streams (HGETALL / LRANGE / SMEMBERS / ZRANGE / XRANGE).
- **Understand data shape.** List indices with health/doc counts/size and
  read index mappings, so you can learn an unfamiliar system by asking.
- **Verify fixes end to end.** After deploying, ask the AI to confirm a
  document exists or a cache key was updated — no throwaway scripts.
- **Write test data when needed.** Index/update documents or run whitelisted
  Redis write commands (SET/HSET/LPUSH...) against non-read-only sources.
- **One config, many data sources.** Point a single server at local, test
  and production instances; tools are generated per source (`es_search_<id>`,
  `redis_get_<id>`), so the AI never guesses which store you mean.

### For operations / SRE

- **Fast incident triage.** Query logs and inspect cache state in one
  conversation, then let the AI summarize findings and suggest next steps.
- **Cluster health at a glance.** Check Elasticsearch cluster status, node
  and shard counts, or pull Redis `INFO` sections (memory, replication,
  persistence) without opening a shell.
- **Safe by default for production.** Every source can be marked
  `read_only`; write operations are then rejected before they reach the
  store.
- **Deny-by-default command control.** Redis commands must appear in a
  classification table — unknown commands are rejected. Dangerous commands
  (FLUSHALL/FLUSHDB/CONFIG/KEYS/EVAL/SHUTDOWN/...) are blocked even on
  writable sources unless `--allow-dangerous` is explicitly passed.
- **Production-friendly connections.** Explicit single-instance vs. cluster
  deployment modes (multi-seed addresses with node discovery),
  authentication and TLS support, per-source result limits, and operation
  timeouts.
- **No credential leakage.** Passwords are read from environment variables
  or `.env` via `${VAR}` placeholders and are never returned to the AI
  client; `list_sources` exposes only IDs, kinds and read-only flags.
- **Local staging that mirrors production.** Docker Compose files provide
  both single-instance and clustered Elasticsearch/Redis stacks for testing
  failover and cluster behavior before touching real environments.

## Supported data sources

| Data source | Status | Notes |
| --- | --- | --- |
| Elasticsearch 7.17.x / 8.x | Implemented | Client is selected by version auto-detection at connect time |
| Redis 6.x / 7.x (single instance and cluster) | Implemented | Standalone and Redis Cluster deployments |
| Kafka | Planned | Configuration schema reserved; client factory is not implemented yet |

## Exposed tools

Tools are generated per configured data source, suffixed with the source ID.

| Tool | Purpose | Read-only |
| --- | --- | --- |
| `list_sources` | List all configured source IDs and kinds (no credentials) | Yes |
| `es_search_<id>` | Run native Query DSL, with from/size/sort/`_source` filters | Yes |
| `es_get_<id>` | Fetch a single document by index and document id | Yes |
| `es_indices_<id>` | List indices with health, doc count and storage size | Yes |
| `es_mapping_<id>` | Get index mappings (field definitions) | Yes |
| `es_cluster_<id>` | Cluster health, node/index counts and active shards | Yes |
| `es_index_<id>` | Write or update a document | No |
| `es_delete_<id>` | Delete a document | No |
| `redis_get_<id>` | Get a key's value, type and TTL | Yes |
| `redis_scan_<id>` | Scan keys by glob pattern (KEYS is never used) | Yes |
| `redis_type_<id>` | Get a key's type and TTL | Yes |
| `redis_data_<id>` | Read structured data via HGETALL/LRANGE/SMEMBERS/ZRANGE/XRANGE | Yes |
| `redis_set_<id>` | Write via whitelisted commands such as SET/HSET/LPUSH | No |
| `redis_info_<id>` | Server INFO output, optionally by section | Yes |

## Quick start

Prerequisites: Go 1.24+ and Docker for the local data stores.

1. Start local data sources (single-instance stack):

```powershell
docker compose -f docker-compose.yml up -d
```

2. Set the credential placeholders used by [config.json](config.json):

```powershell
$env:ES_PASSWORD="changeme"
$env:REDIS_PASSWORD="changeme"
```

3. Build and run the server:

```powershell
go build -o bin/server ./cmd/server
./bin/server --config config.json --transport stdio
```

For remote/shared clients, use SSE instead (default listen address `:9091`):

```powershell
./bin/server --config config.json --transport sse --lazy-loading
```

Then connect your MCP client. For Trae:

```json
{
  "mcpServers": {
    "nosql-mcp-server": {
      "url": "http://127.0.0.1:9091/sse"
    }
  }
}
```

## Transports and the Streamable HTTP roadmap

The following transports are currently available:

| Transport | Status | Notes |
| --- | --- | --- |
| `stdio` | Supported | Recommended for local AI clients; logs go to stderr only. |
| `sse` | Supported (legacy) | HTTP+SSE transport kept for remote/shared clients. |
| Streamable HTTP | Not yet available | Planned for the M5 milestone. |

Streamable HTTP is not implemented yet because the underlying framework this
project builds on, [FreePeak/cortex](https://github.com/FreePeak/cortex), does
not provide it. As of cortex `v1.1.0` the only HTTP transport is the legacy
SSE server (the `/sse` and `/message` endpoints).

The MCP specification deprecated the standalone HTTP+SSE transport in the
2025-03-26 revision in favor of the single-endpoint
[Streamable HTTP](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#streamable-http)
transport. Once cortex ships Streamable HTTP, this project will adopt it and
keep SSE only for backwards compatibility.

To move this forward, we should either open an upstream feature request with
the cortex maintainers or contribute the Streamable HTTP implementation to
cortex directly. Track the integration work under the M5 milestone in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Command-line flags

| Flag | Default | Description |
| --- | --- | --- |
| `--config` | `config.json` | Path to the data source configuration file |
| `--transport` | `stdio` | Transport mode: `stdio` or `sse` (Streamable HTTP is not supported yet) |
| `--address` | `:9091` | Listen address for the SSE transport |
| `--lazy-loading` | `false` | Connect to a source on first use instead of at startup |
| `--allow-dangerous` | `false` | Permit dangerous cluster-level operations |

## Configuration

Data sources are declared in `config.json`. Each source has a unique `id`,
an explicit `deployment_mode` (`single` accepts one endpoint; `cluster`
accepts multiple seed addresses), optional auth/TLS settings, per-source
result limits and timeouts, and an optional `read_only` flag. Sensitive
values use `${VAR}` placeholders resolved from the environment or `.env`.

See [config.json](config.json) for a full example.

## Local environments

Two Compose stacks are provided and must not run simultaneously (they share
host ports):

- `docker-compose.yml`: single-instance Elasticsearch, Redis and Kafka.
- `docker-compose.cluster.yml`: 3-node Elasticsearch plus a 6-node Redis
  Cluster.

```powershell
# Single-instance stack
docker compose -f docker-compose.yml up -d

# Cluster stack
docker compose -f docker-compose.cluster.yml up -d
```

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — architecture and design
- [docs/TESTING.md](docs/TESTING.md) — test plan
- [docs/MCP-CLIENT-CONFIG.md](docs/MCP-CLIENT-CONFIG.md) — configuring Trae,
  Cursor and other MCP clients

## License

See the repository for license information.
