# NoSQL MCP Server 系统架构设计文档

## 一、项目简介

NoSQL MCP Server 是一个基于 [FreePeak/cortex](https://github.com/FreePeak/cortex) 框架、使用 Go 实现的**多 NoSQL 数据源 MCP（Model Context Protocol）服务器**。它让 AI 助手通过统一接口同时访问多种非关系型数据系统，执行检索、读写、集群探查、消息收发等操作。

一期支持的数据源：

| 数据源 | 类型定位 | Go 客户端（建议） |
| --- | --- | --- |
| Elasticsearch（**7.17.x / 8.x 双版本**） | 搜索引擎 / 文档库 | `go-elasticsearch/v7` + `go-elasticsearch/v8`，启动时按探测到的版本选择 |
| Redis（6.x/7.x） | KV 缓存 / 数据结构存储 | `github.com/redis/go-redis/v9` |
| Kafka（3.x，Broker 协议 2.x+） | 分布式消息流 | `github.com/segmentio/kafka-go` 或 `github.com/IBM/sarama` |

设计原则：**与 db-mcp-server 保持一致的整洁架构与工具命名/路由约定，但领域抽象不复用 SQL 模型**（无 `*sql.Rows`、无 ACID 事务、无表/列/Schema 概念）。

---

## 二、与关系型 db-mcp-server 的关键差异

| 维度 | db-mcp-server（SQL） | no-sql-mcp（本项目） |
| --- | --- | --- |
| 底层协议 | `database/sql` + 各驱动 | HTTP/JSON（ES）、RESP（Redis）、Kafka 二进制协议 |
| 请求载体 | SQL 文本 | Query DSL JSON / 命令名+参数 / Topic+消息体 |
| 事务 | 真实 ACID Tx 工具 | **不提供事务工具**；Redis 用 Lua/pipeline，Kafka 无事务概念（可选 producer 事务） |
| Schema 探查 | 表/列/索引/约束/外键 | ES index mappings；Redis key 扫描；Kafka topic/partition |
| 只读护栏 | SQL 写分类 + 引擎会话只读 | **操作白名单 + 命令/端点分类器**（见第八节） |
| 结果模型 | 行列结果集 | ES 命中文档；Redis 任意类型值；Kafka 消息记录 |

---

## 三、整体架构

沿用整洁架构（Clean Architecture），依赖方向从外到内：

| 层级 | 目录 | 职责 |
| --- | --- | --- |
| 入口层 | `cmd/server` | 解析命令行参数、装配各层、启动传输（stdio / sse / streamable HTTP） |
| 配置层 | `internal/config` | 读取 `config.json`、环境变量、`.env` |
| 交付层（MCP） | `internal/delivery/mcp` | 工具注册、请求处理与校验、鉴权、SSE / HTTP Handler |
| 用例层 | `internal/usecase` | 业务逻辑：ES 查询/索引、Redis 命令、Kafka 收发、只读护栏、限流、审计 |
| 领域层 | `internal/domain` | 核心接口与统一实体：`DataSource`、`DataSourceRepository`、文档/键值/消息实体 |
| 适配层 | `internal/adapter` | 将三类客户端适配为 `domain.DataSource` |
| 底层客户端包 | `pkg/clients/{es,redis,kafka}` | 连接管理、客户端封装、原生操作 |

### 调用链路

```
MCP Client
   │
   ▼
ToolRegistry（handler 闭包，绑定 sourceID + kind）
   │
   ▼
ToolSpec.Handle（参数解析与校验）
   │
   ▼
UseCase（只读护栏 / 超时 / 限流 / 审计 / 结果截断 / 脱敏）
   │
   ▼
Repository.GetDataSource(sourceID)
   │
   ▼
clients.Manager.Get(sourceID) ──► 具体客户端（es.Client / redis.Client / kafka 连接）
```

依赖倒置：delivery / adapter / usecase 依赖 domain 定义的接口；domain 不依赖任何外层与具体客户端。

---

## 四、目录结构

```
no-sql-mcp/
├── cmd/server/main.go                 程序入口，装配与启动
├── config.json                        数据源配置（示例）
├── internal/
│   ├── config/config.go               配置加载（JSON / env / .env）
│   ├── domain/
│   │   └── datasource.go              DataSource/Repository 接口与统一实体
│   ├── adapter/
│   │   ├── es_adapter.go              ES 适配器公共逻辑（版本无关）
│   │   ├── es_v7_adapter.go           v7 客户端 → domain.Searchable
│   │   ├── es_v8_adapter.go           v8 客户端 → domain.Searchable
│   │   ├── redis_adapter.go
│   │   └── kafka_adapter.go
│   ├── repository/
│   │   └── datasource_repository.go   仓储实现
│   ├── usecase/
│   │   ├── datasource_usecase.go      门面：按 sourceID 分发
│   │   ├── es_usecase.go
│   │   ├── redis_usecase.go
│   │   ├── kafka_usecase.go
│   │   ├── guard.go                   只读 / 命令白名单护栏
│   │   ├── audit.go                   JSONL 审计
│   │   ├── truncate.go                结果条数截断
│   │   └── masking.go                 字段脱敏
│   └── delivery/mcp/
│       ├── tool_registry.go           工具注册核心
│       ├── tool_specs.go              ToolSpec 接口与通用逻辑
│       ├── es_tools.go                ES 工具定义
│       ├── redis_tools.go             Redis 工具定义
│       ├── kafka_tools.go             Kafka 工具定义
│       ├── list_tool.go               全局 list_sources 工具
│       ├── auth.go                    API Key 鉴权
│       └── streamable.go              streamable HTTP transport
├── pkg/clients/
│   ├── manager.go                     按 sourceID 索引的多类型连接管理器
│   ├── es/                            ES 连接（v7/v8 双客户端）与原生操作封装
│   ├── redis/                         Redis 连接与命令封装
│   └── kafka/                         Kafka reader/writer/admin 封装
├── test/
│   ├── integration/                   集成测试（docker-compose 依赖）
│   └── testdata/                      测试夹具（DSL、消息、快照）
├── docker-compose.yml                 ES + Redis + Kafka 本地测试环境
├── Dockerfile
├── Makefile
└── go.mod
```

---

## 五、领域模型设计（核心）

SQL 项目里统一的"行列结果集"在此不适用。设计一个**最小公共接口 + 操作语义枚举**的方案，避免为三种差异巨大的系统强行造统一抽象。

```go
// internal/domain/datasource.go
package domain

import "context"

// Kind 标识数据源种类
type Kind string

const (
	KindES    Kind = "elasticsearch"
	KindRedis Kind = "redis"
	KindKafka Kind = "kafka"
)

// DataSource 是所有数据源的最小公共抽象。
// 具体能力由各 Kind 的专用接口表达，调用方按 Kind 断言。
type DataSource interface {
	ID() string
	Kind() Kind
	Connect() error
	Close() error
	Ping(ctx context.Context) error
	IsReadOnly() bool
}

// --- ES 能力接口（由 es adapter 实现）---
type Searchable interface {
	Search(ctx context.Context, in SearchInput) (*SearchResult, error)
	GetDoc(ctx context.Context, index, docID string) (map[string]any, error)
	IndexDoc(ctx context.Context, in IndexInput) error
	BulkIndex(ctx context.Context, in BulkInput) (BulkResult, error)
	DeleteDoc(ctx context.Context, index, docID string) error
	ListIndices(ctx context.Context, pattern string) ([]IndexInfo, error)
	GetMapping(ctx context.Context, index string) (map[string]any, error)
	ClusterHealth(ctx context.Context) (ClusterHealth, error)
}

// --- Redis 能力接口 ---
type KVStore interface {
	Get(ctx context.Context, key string) (RedisValue, error)
	Set(ctx context.Context, key string, value any, ttlSec int) error
	Del(ctx context.Context, keys []string) (int64, error)
	Exists(ctx context.Context, keys []string) (int64, error)
	Expire(ctx context.Context, key string, ttlSec int) error
	Keys(ctx context.Context, pattern string, limit int) ([]string, error)
	Type(ctx context.Context, key string) (string, error)
	// Generic 执行白名单内的 Redis 命令（结构化参数），
	// 用于覆盖 string/hash/list/set/zset/stream/pubsub 等结构。
	Generic(ctx context.Context, cmd string, args []any) (any, error)
	Info(ctx context.Context, section string) (string, error)
}

// --- Kafka 能力接口 ---
type Messaging interface {
	ListTopics(ctx context.Context) ([]TopicInfo, error)
	TopicDetail(ctx context.Context, topic string) (TopicDetail, error)
	ListConsumerGroups(ctx context.Context) ([]ConsumerGroupInfo, error)
	Produce(ctx context.Context, in ProduceInput) (ProduceResult, error)
	Consume(ctx context.Context, in ConsumeInput) ([]Message, error)
	GroupOffsets(ctx context.Context, group string) ([]PartitionOffset, error)
	ClusterInfo(ctx context.Context) (ClusterInfo, error)
}

// DataSourceRepository 仓储接口
type DataSourceRepository interface {
	Get(id string) (DataSource, error)
	List() []string
	GetKind(id string) (Kind, error)
	IsLazyLoading() bool
}
```

统一实体（节选）：

```go
type SearchInput struct {
	Indices []string
	QueryDSL map[string]any // 原生 ES Query DSL JSON
	From, Size int
	Sort []string
	SourceIncludes []string // _source 过滤，减小返回体
}
type SearchResult struct {
	Total    int64
	TookMs   int64
	Hits     []map[string]any // 命中文档（_source + _id/_index/_score）
	Truncated bool
}

type RedisValue struct {
	Type  string // string/hash/list/set/zset/stream/none
	Value any
	TTL   int64
}

type ProduceInput struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Partition int // -1 表示自动分区
}
type ConsumeInput struct {
	Topic, Group string
	Partition    int
	Offset       int64 // -10 latest / -2 earliest / 具体偏移
	MaxMessages  int
	TimeoutMs    int
}
type Message struct {
	Topic, Group string
	Partition    int
	Offset       int64
	Key, Value    []byte
	Headers       map[string]string
	Timestamp     int64
}
```

> 说明：Redis 的 `Generic` 与 Kafka 的 `Consume` 是"逃生舱"式设计——既提供高频结构化工具，又允许 AI 在护栏内覆盖长尾能力，避免为每个 Redis 数据结构都写一个工具。

---

## 六、MCP 工具设计

工具数量按数据源动态生成，提供两种模式（与 db-mcp-server 对齐）：

- **按源命名模式（默认）**：`tool_sourceID`，选工具即选源
- **统一工具模式（`--unified-tools`）**：工具带 `source` 参数，适合连接 5+ 数据源、对 token 敏感的客户端

### 6.1 Elasticsearch 工具（每实例 7 个 + 全局 1 个）

| 工具 | 作用 | 只读 |
| --- | --- | --- |
| `es_search_<id>` | 执行 Query DSL（JSON）检索文档，支持 from/size/sort/_source | 是 |
| `es_get_<id>` | 按 index + id 获取单文档 | 是 |
| `es_indices_<id>` | `_cat/indices` 风格列出索引（支持 pattern） | 是 |
| `es_mapping_<id>` | 获取索引 mapping（替代 describe/schema） | 是 |
| `es_cluster_<id>` | 集群健康、节点/索引概况（替代 health） | 是 |
| `es_index_<id>` | 写入/更新单文档（只读模式禁用） | 否 |
| `es_delete_<id>` | 删除文档（只读模式禁用） | 否 |

> `bulk` 批量写一期可并入 `es_index` 的数组入参，二期再拆独立工具。

### 6.2 Redis 工具（每实例 6 个）

| 工具 | 作用 | 只读 |
| --- | --- | --- |
| `redis_get_<id>` | 获取 key 的值与类型/TTL | 是 |
| `redis_scan_<id>` | SCAN 匹配 key（pattern + limit，禁用 KEYS 全量阻塞） | 是 |
| `redis_type_<id>` | 查看 key 类型与 TTL | 是 |
| `redis_data_<id>` | 读取结构化数据：hgetall/lrange/smembers/zrange/xrange 等 | 是 |
| `redis_set_<id>` | SET / HSET / LPUSH 等写操作（只读模式禁用） | 否 |
| `redis_info_<id>` | INFO 服务器信息（替代 health/performance） | 是 |

> 写操作也可走统一 `redis_exec_<id>`（结构化命令 + 白名单），一期建议直接提供 `redis_set` 覆盖高频写，长尾写命令通过白名单 Generic 暴露。

### 6.3 Kafka 工具（每实例 6 个）

| 工具 | 作用 | 只读 |
| --- | --- | --- |
| `kafka_topics_<id>` | 列出 topic 及分区/副本信息 | 是 |
| `kafka_topic_detail_<id>` | 单 topic 的分区、leader、ISR、起止 offset | 是 |
| `kafka_groups_<id>` | 列出消费组及 lag 概况 | 是 |
| `kafka_consume_<id>` | 按 topic/partition/offset 或 group 拉取消息（有界） | 是* |
| `kafka_produce_<id>` | 发送消息（只读模式禁用） | 否 |
| `kafka_cluster_<id>` | broker 列表、controller、集群概况 | 是 |

> *`kafka_consume` 不改变 broker 数据，但当传入 `group` 时会推进消费位点。默认实现走**不提交位点**的临时读取；只有显式 `commit=true` 才提交，且该选项在只读模式下禁用。

### 6.4 全局工具

| 工具 | 作用 |
| --- | --- |
| `list_sources` | 列出全部数据源 ID（不暴露密码等敏感信息），与 db-mcp-server 的 `list_databases` 对应 |

### 6.5 工具总数

- 默认模式：`7·ES + 6·Redis + 6·Kafka`（按实例数计）`+ 1` 全局
- 统一模式：各 Kind 工具合并并带 `source` 参数，总数与实例数无关

### 6.6 路由机制（复用 db-mcp-server 约定）

1. 默认模式：sourceID 在工具名后缀中，handler 闭包捕获；兜底用 `extractSourceIDFromName` 取最后一个 `_` 之后内容（注意 ES/Redis/Kafka 工具名已含固定前缀，解析时先剥离已知前缀）。
2. 统一模式：`source` 参数 + `extractAndValidateSource` 校验，非法时报错并列出全部可选源。
3. AI 获知 sourceID：工具描述中嵌入 `Available sources: ...`，或先调 `list_sources`。
4. 服务端取连接：sourceID 传至 `clients.Manager.Get(id)`，不猜测、不跨源混用。

---

## 七、配置设计

```json
{
  "sources": [
    {
      "id": "logs_es_prod",
      "type": "elasticsearch",
      "version": "auto",
      "deployment_mode": "cluster",
      "addresses": ["https://es1:9200", "https://es2:9200"],
      "username": "ai_reader",
      "password": "${ES_PASSWORD}",
      "api_key": "",
      "cloud_id": "",
      "default_index": "app-logs-*",
      "read_only": true,
      "max_docs": 200,
      "skip_tls_verify": false,
      "ca_cert": "/certs/es-ca.pem",
      "query_timeout": 30,
      "description": "生产应用日志检索",
      "masking_rules": [
        { "field": "email", "strategy": "partial", "keep_last": 0 },
        { "field": "phone", "strategy": "fixed_string", "value": "***" }
      ]
    },
    {
      "id": "cache_redis_test",
      "type": "redis",
      "addresses": ["redis://localhost:6379/0"],
      "username": "",
      "password": "${REDIS_PASSWORD}",
      "db": 0,
      "read_only": false,
      "max_keys": 500,
      "command_timeout": 5,
      "tls_enabled": false,
      "description": "测试环境缓存"
    },
    {
      "id": "events_kafka_prod",
      "type": "kafka",
      "brokers": ["kafka1:9092", "kafka2:9092"],
      "sasl_mechanism": "SCRAM-SHA-256",
      "username": "ai",
      "password": "${KAFKA_PASSWORD}",
      "tls_enabled": true,
      "ca_cert": "/certs/kafka-ca.pem",
      "read_only": true,
      "max_messages": 100,
      "default_topic": "order.events",
      "operation_timeout": 10,
      "description": "订单事件流"
    }
  ]
}
```

字段约定：

- 统一使用 `addresses`/`brokers` 数组，支持多节点；`id` 全局唯一、建议起得有语义（`<用途>_<类型>_<环境>`）
- 必须通过 `deployment_mode` 显式区分部署形态：`single`（单实例，仅接受一个地址，关闭节点嗅探）或 `cluster`（集群，接受多个种子地址，启用节点发现）；配置 `cloud_id` 时按集群处理
- ES 可选 `version` 字段：`auto`（默认，连接时探测）/`7`/`8`；见第 7.1 节
- `${VAR}` 占位符从环境变量/`.env` 解析；密码等敏感字段不进日志、不进 `list_sources` 返回
- 公共护栏字段：`read_only`、`max_docs/max_keys/max_messages`（各类型的结果上限）、`query_timeout`、`masking_rules`、`description`
- 环境变量对应：`CONFIG_PATH`、`TRANSPORT_MODE`、`SERVER_PORT`、`NOSQL_MCP_API_KEY`、`LAZY_LOADING` 等

### 7.1 Elasticsearch v7 / v8 双版本支持

同时支持 **7.17.x** 与 **8.x**，对外暴露完全相同的工具与 `domain.Searchable` 接口，版本差异全部收敛在 ES 客户端层与适配器层。

**客户端选择流程：**

```
Connect(es source)
   │  1. 用与目标版本无关的方式 GET / （或读取配置 version）
   ▼
解析 version.number 主版本号（7 或 8）
   │
   ├─ 7 → 构造 go-elasticsearch/v7 客户端 → esV7Adapter（实现 Searchable）
   └─ 8 → 构造 go-elasticsearch/v8 客户端 → esV8Adapter（实现 Searchable）
```

- `version: "auto"`：启动探测；探测失败时若无法判定主版本则报错（fail closed），不盲目选客户端
- `version: "7"/"8"`：跳过探测直接使用对应客户端（探测结果仍记录，与配置不一致时告警）
- 两个客户端库同时作为依赖；`pkg/clients/es` 提供统一工厂 `New(cfg) (domain.Searchable, error)`

**适配器结构：**

| 文件 | 职责 |
| --- | --- |
| `es_adapter.go` | 版本无关的公共逻辑：结果解析骨架、截断/脱敏挂载点、统一 SearchResult 组装 |
| `es_v7_adapter.go` | 嵌入 v7 客户端，处理 7.x 专有请求/响应差异 |
| `es_v8_adapter.go` | 嵌入 v8 客户端，处理 8.x 专有请求/响应差异 |

**需要按版本处理的差异点（白名单，开发时逐项核对）：**

| 差异点 | 7.17 | 8.x |
| --- | --- | --- |
| 安全默认 | 默认 HTTP、可无认证 | 默认 HTTPS + 认证开启 |
| 文档类型 | 请求中容忍 `_type`；mapping 可能含 type 层 | 无 type；禁止 `include_type_name` |
| `hits.total` | 对象 `{value, relation:"eq/gte"}`（7.x 默认已为对象） | 同对象结构 |
| 新增响应字段 | 无 | 可能多出 8.x 字段，解析时忽略未知字段 |
| 专属 API | 不调用 8.x 专属接口（如 `_health_report`） | 可使用，调用前按能力判断 |
| Content-Type | 显式带 `application/json` | 同 |

> 原则：只依赖 7/8 共有的稳定 API（search/get/index/delete/\_cat/\_mapping/\_cluster/health），版本专属特性通过接口能力判断降级，保证工具行为在两个版本上一致。Lazy 模式下同样在首次建连时完成探测与客户端选择。

---

## 八、生产级护栏设计

| 护栏 | 作用范围 | 实现 |
| --- | --- | --- |
| 只读模式 | 每源 | 见下方三层拦截 |
| 结果上限 | 每源 | ES `max_docs`、Redis `max_keys`、Kafka `max_messages`；超限截断并追加 `[Truncated]` |
| 字段脱敏 | 每源 | 按字段路径（ES `_source` 字段、Redis hash field）正则脱敏：fixed_string/null/partial |
| 超时 | 每源 | context timeout，在 usecase/adapter 层强制；Redis 单命令、Kafka 拉取、ES 查询分别配置 |
| 审计日志 | 进程 | 每条操作一条 JSONL（source、操作、耗时、是否拒绝），值内容默认不记录全文（可配置） |
| 限流 | 每源/进程 | 令牌桶，限制单位时间操作数与大结果拉取频率 |
| 危险操作拦截 | 全局 | ES `_shutdown`、Redis `FLUSHALL/CONFIG/KEYS`、Kafka 删除 topic 等直接拒绝 |

### 只读模式的分层实现

由于三类系统都没有 db-mcp-server 那种"会话只读事务"，只读依靠：

1. **应用层操作白名单**（核心，必做）：
   - ES：仅允许 GET/POST `_search`、`_doc/{id}`、`_cat`、`_mapping`、`_cluster/health`；拒绝 `_bulk`、index/delete、`_update_by_query`、`_delete_by_query`
   - Redis：命令分类表，仅放行 GET/STRLEN/TYPE/TTL/SCAN/HGETALL/HMGET/LRANGE/SMEMBERS/ZRANGE/ZCARD/XRANGE/INFO 等；拒绝 SET/DEL/EXPIRE/FLUSH*/EVAL/CONFIG 等
   - Kafka：仅允许元数据读取与不提交位点的 consume；拒绝 produce 与 `commit=true`
2. **账号最小权限**（运维约定）：ES 只读 role、Redis 只读用户（ACL）、Kafka 仅 describe+read ACL。纵深防御。
3. **危险命令黑名单**：即便非只读模式，也默认拒绝不可逆/集群级操作，需显式开关放开。

---

## 九、连接管理

`pkg/clients/Manager` 按 ID 索引三类客户端（参考 db-mcp-server 的 Manager，但 value 是统一接口 `domain.DataSource` 而非 `*sql.DB`）：

```go
type Manager struct {
	mu          sync.RWMutex
	sources     map[string]domain.DataSource
	configs     map[string]SourceConfig
	lazyLoading bool
}
```

- **Eager（默认）**：启动时建立全部连接并 Ping，失败给出明确错误（可配置单源失败不阻断启动）
- **Lazy（`--lazy-loading`）**：首次使用时建连，适合 10+ 数据源
- ES：多地址 + 连接池、API Key/Basic 认证、可选 TLS；建连时探测版本并选择 v7/v8 客户端（见 7.1）；`default_index` 作为未指定索引时的兜底
- Redis：单地址或哨兵/集群（二期）、DB 选择、连接池参数复用 go-redis 默认
- Kafka：broker 列表、SASL/TLS；Reader/Writer/Admin 三类客户端按需创建并复用

---

## 十、关键流程时序

### ES 检索（只读路径）

```
Client → es_search_logs: {indices, query_dsl, size}
  → ToolSpec 参数校验（DSL 合法 JSON、size ≤ max_docs）
  → Guard: 只读放行 _search
  → Usecase: 注入超时/审计
  → esAdapter.Search → go-elasticsearch REST
  → 解析 hits → 截断 → 字段脱敏 → 文本/JSON 封装
  → 返回 content[].text
```

### Redis 读写（受白名单约束）

```
Client → redis_set_cache: {key, value, ttl}
  → 校验（read_only? key 非空, ttl 合法）
  → Guard: SET 是否在白名单（只读模式拒绝）
  → redisAdapter.KVStore.Set → RESP
  → 审计（记录 key 与长度，不记明文值）
```

### Kafka 消费（有界、不提交位点）

```
Client → kafka_consume_events: {topic, partition, offset, max_messages}
  → 校验 max_messages ≤ 上限、offset 合法
  → Guard: consume 放行；commit 在只读下拒绝
  → kafkaAdapter.Consume（reader 设置 StartOffset，有界读取到 max/超时）
  → 消息列表截断/脱敏 → 返回
```

---

## 十一、错误处理与响应格式

- 成功：MCP 标准 `content: [{type:"text", text:"..."}]`；结构化结果同时提供紧凑文本（人类/AI 易读）与可选 JSON
- 失败：统一错误对象（`FormatResponse`），错误中包含 source、操作类别、可读原因；不回显密码/连接串
- 典型错误：source 不存在、只读拒绝、超时、结果超限（截断而非报错）、DSL/参数非法、认证失败、TLS 失败

---

## 十二、技术选型与非功能约束

- Go 1.23+；cortex v1.x（与 db-mcp-server 同版本，保证工具注册 API 一致）
- ES 同时依赖 `go-elasticsearch/v7` 与 `v8`，支持 7.17.x / 8.x（版本自动探测，见 7.1）
- 传输：stdio（本地 AI 客户端）、sse、streamable HTTP（远程 + API Key 鉴权）
- 日志：zap 结构化；stdio 模式只写 stderr/文件，不污染 stdout
- 可观测：`/health` 端点（源数量、各源连通状态、uptime）
- 构建产物：单一二进制 + Docker 镜像；提供 docker-compose 一键拉起 ES/Redis/Kafka 测试环境

---

## 十三、分阶段交付建议

| 阶段 | 内容 |
| --- | --- |
| M1 骨架 | 工程初始化、config、Manager、domain 接口、list_sources、stdio 跑通 |
| M2 Redis | 读工具 + set + 白名单护栏 + 单测/集成测 |
| M3 ES | search/get/indices/mapping/cluster + 只读白名单 + 集成测 |
| M4 Kafka | topics/groups/consume/produce + 有界消费 + 集成测 |
| M5 护栏完善 | 审计、脱敏、限流、截断、统一工具模式、`/health`、Docker 发布 |

> 建议按 M1→M2→M3→M4 顺序：Redis 协议最简单，可最先打通"注册→分发→适配→返回"整条链路并固化模式，再复制到 ES、Kafka。
