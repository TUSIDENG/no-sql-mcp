# NoSQL MCP Server

A multi-NoSQL data source [MCP (Model Context Protocol)](https://modelcontextprotocol.io/)
server written in Go, built on [FreePeak/cortex](https://github.com/FreePeak/cortex).
It exposes Elasticsearch, Redis and Kafka through a unified MCP interface so
any MCP-aware AI client (Trae, Cursor, Claude Desktop, and similar tools) can
query documents, inspect cache data, produce and consume event streams, and
check cluster health using natural language — without writing a client or
logging into the data stores.

## Related projects

- [FreePeak/db-mcp-server](https://github.com/FreePeak/db-mcp-server) — an
  MCP server for SQL databases such as MySQL, PostgreSQL and Oracle,
  complementing this project's NoSQL (Elasticsearch, Redis, Kafka) support.

## What this project actually does for you

MCP turns an AI assistant into a secure, conversational operations console
for your NoSQL infrastructure. Instead of context-switching between Kibana,
`redis-cli`, SSH sessions, and ad-hoc scripts, you ask the AI and it calls
the right tool, returns the result, and explains it.

### Practical scenarios

**Common**

- **One server, many data sources.** Point it at local, test and production
  instances; tools are generated per source (`es_search_<id>`,
  `redis_get_<id>`, `kafka_consume_<id>`), so the AI never guesses which
  store you mean.
- **Triage and verify in one conversation.** Query logs, cache and event
  streams, let the AI summarize findings, then confirm a fix end to end
  without throwaway scripts.
- **Safe against production data.** Mark sources `read_only` to reject
  writes, authenticate with TLS, bound result sizes and timeouts, and keep
  passwords in environment variables or `.env` via `${VAR}` so they are
  never returned to the client (`list_sources` exposes only IDs, kinds and
  read-only flags).
- **Stage locally before production.** Docker Compose provides single and
  clustered Elasticsearch/Redis/Kafka stacks for testing failover.

**Redis**

- **Work without `redis-cli`.** Read a key's value, type and TTL, scan keys
  by pattern, read hashes/lists/sets/sorted sets/streams, and pull `INFO`
  plus cluster slot topology.
- **Write under guardrails.** Commands are deny-by-default: unknown commands
  are rejected and dangerous ones (FLUSHALL/CONFIG/KEYS/EVAL/...) are
  blocked unless `--allow-dangerous` is passed.

**Elasticsearch**

- **Search logs and learn data shape conversationally.** Run native Query
  DSL ("the last 50 ERROR logs from `order-api` in the last hour"), list
  indices with health/doc/size, and read mappings.
- **Check cluster health at a glance.** See status, node, index and shard
  counts; node sniffing is opt-in through `discover_nodes` for clients on
  the cluster network.

**Kafka**

- **Inspect streams without the CLI.** List topics and groups, inspect
  partitions, leaders, ISRs and offsets, and run bounded consumes.
- **Track and test streaming.** Watch consumer group lag and committed
  offsets, review brokers/controller and leader/ISR placement, and produce
  messages to non-read-only sources.

## Supported data sources

| Data source | Status | Notes |
| --- | --- | --- |
| Elasticsearch 7.17.x / 8.x | Implemented | Client is selected by version auto-detection at connect time |
| Redis 6.x / 7.x (single instance and cluster) | Implemented | Standalone and Redis Cluster deployments |
| Kafka 3.x (KRaft) | Implemented | Single broker and multi-broker cluster bootstrap; SASL (PLAIN/SCRAM) and TLS |

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
| `kafka_topics_<id>` | List topics with partition and replica counts | Yes |
| `kafka_topic_detail_<id>` | Partitions, leaders, ISRs and earliest/latest offsets | Yes |
| `kafka_groups_<id>` | List consumer groups with total lag | Yes |
| `kafka_consume_<id>` | Bounded consume by partition/offset or group (offset not committed unless `commit=true`) | Yes* |
| `kafka_produce_<id>` | Send a single message | No |
| `kafka_cluster_<id>` | Broker list and current controller | Yes |

> \*`kafka_consume_<id>` never modifies broker data, but a group consume can
> advance the committed offset. By default offsets are not committed; pass
> `commit=true` to commit, which is rejected on read-only sources.

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

For Elasticsearch, node sniffing is opt-in through `discover_nodes` (only
valid with `cluster`). Leave it unset when the client cannot reach the
nodes' published addresses (for example an MCP server on the host talking
to a containerized cluster, which advertises internal IPs); the client then
load-balances over the configured `addresses`. Enable it when the client
shares the cluster network or when using `cloud_id`.

See [config.json](config.json) for a full example.

## Local environments

Two Compose stacks are provided and must not run simultaneously (they share
host ports):

- `docker-compose.yml`: single-instance Elasticsearch, Redis and Kafka.
- `docker-compose.cluster.yml`: 3-node Elasticsearch, a 6-node Redis Cluster
  and a 3-broker Kafka cluster.

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
