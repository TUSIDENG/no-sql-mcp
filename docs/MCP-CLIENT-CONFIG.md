# NoSQL MCP 客户端配置文档（Trae / Cursor）

本文档说明如何启动 NoSQL MCP Server，并在 Trae、Cursor 等 MCP 客户端中完成配置与验证。

## 1. 前置条件

- 已安装 Go（本仓库按 [AGENTS.md](../AGENTS.md) 中 "Go toolchain location" 一节配置环境）。
- 数据源已通过 Docker Compose 启动，例如单机 Elasticsearch：

```powershell
docker compose -f docker-compose.yml up -d elasticsearch
```

- 配置文件 [config.json](../config.json) 中使用 `${ES_PASSWORD}`、`${REDIS_PASSWORD}` 占位符引用密码，启动服务前必须设置对应的环境变量，否则配置加载会失败（fail-closed）。

## 2. 启动 MCP Server

### 2.1 构建

```powershell
$env:GOROOT="$HOME\.g\go"
$env:Path="$HOME\.g\bin;$env:GOROOT\bin;$env:Path"
go build -o bin/server.exe ./cmd/server
```

### 2.2 SSE 模式（推荐，远程客户端共用）

```powershell
$env:ES_PASSWORD="changeme"
$env:REDIS_PASSWORD="changeme"
.\bin\server.exe --config config.json --transport sse --lazy-loading
```

参数说明：

| 参数 | 说明 |
| --- | --- |
| `--config` | 数据源配置文件路径，默认 `config.json` |
| `--transport` | 传输模式：`sse` 或 `stdio` |
| `--address` | SSE 模式监听地址，默认 `:9091`（避免与 db-mcp-server 的 9092 冲突） |
| `--lazy-loading` | 首次使用时才连接数据源；未启动的数据源不会导致启动失败 |
| `--allow-dangerous` | 允许危险的集群级操作，默认关闭 |

启动成功后输出 `serving SSE on :9091`，SSE 端点为 `http://127.0.0.1:9091/sse`。

### 2.3 stdio 模式

```powershell
$env:ES_PASSWORD="changeme"
$env:REDIS_PASSWORD="changeme"
.\bin\server.exe --config config.json --transport stdio
```

stdio 模式下 stdout 仅用于 JSON-RPC，所有日志输出到 stderr。

## 3. Trae 配置

### 3.1 界面配置

1. 打开 Trae 设置 → MCP（插件市场中的 MCP Servers）。
2. 选择「添加」→「手动配置」，切换到「原始配置 (JSON)」。
3. 粘贴以下内容并确认：

```json
{
  "mcpServers": {
    "nosql-mcp-server": {
      "url": "http://127.0.0.1:9091/sse"
    }
  }
}
```

4. 在 MCP 列表中启用该服务，状态变为可用即表示连接成功。

### 3.2 配置文件位置

- 全局配置：`%APPDATA%\Trae CN\User\mcp.json`
- 项目级配置：项目根目录下的 `.trae/mcp.json`

stdio 模式可在配置文件中使用 `command` + `args`：

```json
{
  "mcpServers": {
    "nosql-mcp-server": {
      "command": "d:\\code\\no-sql-mcp\\bin\\server.exe",
      "args": ["--config", "d:\\code\\no-sql-mcp\\config.json"],
      "env": {
        "ES_PASSWORD": "changeme",
        "REDIS_PASSWORD": "changeme"
      }
    }
  }
}
```

## 4. Cursor 配置

### 4.1 SSE 模式

- 全局配置文件：`%USERPROFILE%\.cursor\mcp.json`
- 项目级配置文件：项目根目录下的 `.cursor/mcp.json`

```json
{
  "mcpServers": {
    "nosql-mcp-server": {
      "url": "http://127.0.0.1:9091/sse"
    }
  }
}
```

也可以在 Cursor 的 Settings → MCP → Add new server 中选择 URL 方式填入 `http://127.0.0.1:9091/sse`。

### 4.2 stdio 模式

```json
{
  "mcpServers": {
    "nosql-mcp-server": {
      "command": "d:\\code\\no-sql-mcp\\bin\\server.exe",
      "args": ["--config", "d:\\code\\no-sql-mcp\\config.json"],
      "env": {
        "ES_PASSWORD": "changeme",
        "REDIS_PASSWORD": "changeme"
      }
    }
  }
}
```

> macOS / Linux 下将 `command` 替换为对应的可执行文件路径（如 `/path/to/bin/server`），路径分隔符改为 `/`。

## 5. 已注册的工具

当前版本注册以下工具（Elasticsearch 工具按每个 ES 数据源生成，工具名后缀为数据源 ID）：

| 工具 | 说明 |
| --- | --- |
| `list_sources` | 列出全部已配置数据源（不返回凭证） |
| `es_search_<source_id>` | 使用原生 Query DSL 检索文档 |
| `es_get_<source_id>` | 按 index + 文档 ID 获取单条文档 |
| `es_indices_<source_id>` | 列出索引（健康状态、文档数、存储大小） |
| `es_mapping_<source_id>` | 查看索引 mapping |
| `es_cluster_<source_id>` | 查看集群健康状态 |
| `es_index_<source_id>` | 写入/更新单条文档（只读数据源会被拒绝） |
| `es_delete_<source_id>` | 删除单条文档（只读数据源会被拒绝） |

当前实现状态：Elasticsearch、Redis 客户端工厂已接入（Redis 工具尚未在注册表暴露）；Kafka 工厂尚未实现。

## 6. 验证

### 6.1 SSE 握手

```powershell
curl.exe -s -N --max-time 3 http://127.0.0.1:9091/sse
```

预期返回（exit code 28 为超时断开，属正常现象）：

```text
event: connected
data: {"sessionId": "<uuid>"}

event: endpoint
data: /message?sessionId=<uuid>
```

### 6.2 JSON-RPC 调用验证

通过 SSE 拿到 sessionId 后，向 `/message?sessionId=<sessionId>` 发送 POST 请求：

- `initialize`：返回 `serverInfo`（NoSQL MCP Server 0.1.0）。
- `tools/list`：返回全部已注册工具（当前 8 个）。
- `tools/call` 调用 `list_sources`：返回数据源清单，例如：

```text
Found 3 data source(s):
- cache_redis_local (redis, read_only=false)
- events_kafka_local (kafka, read_only=true)
- logs_es_local (elasticsearch, read_only=true)
```

- `tools/call` 调用 `es_indices_logs_es_local`：返回 ES 真实索引列表，证明端到端链路正常。

### 6.3 客户端内验证

在 Trae / Cursor 的对话中直接提问，例如：

- "列出可用的数据源"
- "列出 logs_es_local 的所有索引"
- "查看 t1_products 索引的 mapping"

客户端能正确调用对应工具并返回结果即配置完成。

## 7. 常见问题

| 现象 | 排查 |
| --- | --- |
| 启动报 `environment variable "ES_PASSWORD" ... is not set` | 先设置 `$env:ES_PASSWORD` / `$env:REDIS_PASSWORD` |
| 启动报 `connect data sources` 失败 | 未启动的数据源会阻断启动，加 `--lazy-loading` 或先启动对应容器 |
| 客户端连接失败 | 确认服务已启动、端口未被占用、URL 路径为 `/sse` |
| 写操作被拒绝 | 数据源配置了 `read_only: true`，写入/删除工具会返回错误 |
| 单机/集群连接异常 | 参考 [AGENTS.md](../AGENTS.md)，单机与集群 Compose 文件不能同时启动（端口冲突） |
