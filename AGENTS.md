# AGENTS.md

## Language requirements

- Documentation files under the `docs/` directory are NOT restricted to
  Chinese or English; either language may be used.
- All other documentation files in this repository MUST be written in English.
- All code comments, commit messages, and identifiers MUST be in English.

## Data source connection requirements

- Elasticsearch, Redis, and Kafka MUST support both single-instance and
  cluster deployments.
- Single-instance and cluster connections are configured differently and MUST
  be handled as distinct modes:
  - Elasticsearch: a single endpoint versus multiple seed node addresses with
    cluster node discovery.
  - Redis: a single address versus cluster mode (and, where applicable,
    Sentinel), with the corresponding routing and failover behavior.
  - Kafka: a single bootstrap address versus multiple broker bootstrap
    addresses.
- Data source configuration MUST explicitly distinguish the deployment mode
  and accept one endpoint for single-instance mode or a list of endpoints for
  cluster mode.

## Local data sources (Docker Compose)

Two Compose files provide Elasticsearch, Redis, and Kafka for local
development and testing. They MUST NOT be started at the same time because
the single-instance and cluster files share host ports (9200 for
Elasticsearch, 9092 for Kafka).

- `docker-compose.yml`: single-instance stack (one Elasticsearch node, one
  Redis node, one KRaft Kafka broker).
- `docker-compose.cluster.yml`: cluster stack (3 Elasticsearch nodes, a
  6-node Redis Cluster created automatically by the `redis-cluster-init`
  one-shot job, and a 3-broker KRaft Kafka cluster).

Endpoints:

| Service       | Single-instance | Cluster                                                   |
| ------------- | --------------- | --------------------------------------------------------- |
| Elasticsearch | `localhost:9200` | `localhost:9200`, `localhost:9201`, `localhost:9202`    |
| Redis         | `localhost:6379` | `localhost:7000` through `localhost:7005`               |
| Kafka         | `localhost:9092` | `localhost:9092`, `localhost:9094`, `localhost:9096`    |

Start the full stacks:

```powershell
# Single-instance stack
docker compose -f docker-compose.yml up -d

# Cluster stack
docker compose -f docker-compose.cluster.yml up -d
```

Start Elasticsearch only:

```powershell
# Single Elasticsearch instance
docker compose -f docker-compose.yml up -d elasticsearch

# Elasticsearch cluster (all 3 nodes)
docker compose -f docker-compose.cluster.yml up -d es-1 es-2 es-3
```

Start Redis or Kafka only:

```powershell
# Single Redis / single Kafka
docker compose -f docker-compose.yml up -d redis
docker compose -f docker-compose.yml up -d kafka

# Redis Cluster (the init job waits for all nodes, then builds the cluster)
docker compose -f docker-compose.cluster.yml up -d redis-1 redis-2 redis-3 redis-4 redis-5 redis-6 redis-cluster-init

# Kafka cluster
docker compose -f docker-compose.cluster.yml up -d kafka-1 kafka-2 kafka-3
```

Stop and remove the stacks (add `-v` to also delete named data volumes):

```powershell
docker compose -f docker-compose.yml down
docker compose -f docker-compose.cluster.yml down -v
```

Credentials default to `changeme` and can be overridden with the
`ES_PASSWORD` and `REDIS_PASSWORD` environment variables. On Linux hosts,
set `vm.max_map_count=262144` before starting Elasticsearch, and if the
Redis Cluster is not reachable from the host, override the announced address
with `REDIS_CLUSTER_ANNOUNCE_IP=<host-ip>`.

## Go toolchain location

Go is not on the default system PATH. It is configured through the PowerShell
profile (`$PROFILE`) as follows:

- `GOROOT`: `%USERPROFILE%\.g\go` (actual binary: `%USERPROFILE%\.g\go\bin\go.exe`)
- Extra PATH entries: `%USERPROFILE%\.g\bin` and `%USERPROFILE%\.g\go\bin`
- `GOPATH` / module cache: `D:\code\gobin` (mod cache at `D:\code\gobin\pkg\mod`)

Run Go commands after setting the environment in PowerShell:

```powershell
$env:GOROOT="$HOME\.g\go"
$env:Path="$HOME\.g\bin;$env:GOROOT\bin;$env:Path"
```

## Project overview

Multi-NoSQL data source MCP (Model Context Protocol) server written in Go,
based on [FreePeak/cortex](https://github.com/FreePeak/cortex). See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the architecture design and
[docs/TESTING.md](docs/TESTING.md) for the test plan.
