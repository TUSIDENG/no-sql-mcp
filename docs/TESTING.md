# NoSQL MCP Server 测试文档

> 配套文档：[ARCHITECTURE.md](./ARCHITECTURE.md)
> 本文档定义测试策略、环境、分层用例、测试夹具与验收标准，作为开发与 CI 的依据。

## 一、测试目标与范围

### 目标

1. 保证三类数据源（Elasticsearch / Redis / Kafka）的工具在**正常路径、边界条件、异常路径**下行为正确。
2. 保证**只读护栏、危险命令黑名单、脱敏、截断、超时、审计**等生产级机制不被绕过。
3. 保证工具**动态注册、路由解析、统一工具模式**与 db-mcp-server 约定一致。
4. 为 CI 提供可重复、可并行、失败可读的自动化测试。

### 范围

| 测试层 | 是否依赖真实中间件 | 占比目标 | 执行者 |
| --- | --- | --- | --- |
| 单元测试（Unit） | 否（接口 mock / 内存 fake） | ~60% | 开发 + CI 全量 |
| 集成测试（Integration） | 是（testcontainers 自管容器 / 既有实例） | ~30% | 开发可选 + CI |
| 端到端冒烟（E2E Smoke） | 是 + 真实 MCP 调用 | ~10% | CI 发布门禁 |
| 契约/快照测试 | 否 | 贯穿 | CI |

> 命名约定（沿用 db-mcp-server 习惯）：纯单测文件 `*_test.go` 同目录；需要真实中间件的用例文件以 `*_live_test.go` 结尾，并通过 build tag `//go:build live` 隔离，普通 `go test ./...` 不会因缺少中间件而失败。

---

## 二、测试环境

### 2.1 Testcontainers（集成测试，自管生命周期）

- 库：`github.com/testcontainers/testcontainers-go` 及各 module
- 每个集成测试包在 `TestMain` 中按需启动容器、读取随机映射端口、结束时自动销毁
- 需要指定既有实例时，通过环境变量 `NOSQL_TEST_TARGET` 指向目标地址，集成测试不再依赖外部部署脚本

### 2.2 测试配置

`test/testdata/config.test.json` 使用 `${VAR}` 占位与测试专用弱口令，禁止复用真实凭据；敏感断言只验证"不泄漏"，不写入真实 secret。

### 2.3 Makefile 目标

```makefile
test:            ## 运行全部单元测试（短模式）
	go test ./... -short -count=1

test-race:       ## 竞态检测
	go test ./... -race -count=1

test-live:       ## 集成测试（由 testcontainers 自管容器）
	go test -tags=live ./... -count=1

coverage:        ## 覆盖率报告
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

lint:
	golangci-lint run ./...

smoke:           ## 启动 server 并跑冒烟脚本
	bash scripts/smoke.sh
```

---

## 三、测试分层与通用规范

### 3.1 规范

1. 表驱动测试（table-driven）为主，用例名表达意图：`name: "reject write when read_only"`。
2. 断言使用 `github.com/stretchr/testify`（require/assert），与 db-mcp-server 保持一致。
3. 每个用例独立、可并行（`t.Parallel()`），不依赖执行顺序；集成测试用唯一 index/key 前缀/topic（如 `t1_<random>`）隔离。
4. 不 sleep 等待，轮询带超时（`require.Eventually`）。
5. Mock 边界在 **domain 接口**（Searchable/KVStore/Messaging/Repository），不 mock 具体客户端。
6. 子测试结构：`t.Run("unit/...")`、`t.Run("live/...")`，失败时能直接定位数据源与工具。

### 3.2 公共测试夹具

| 夹具 | 内容 |
| --- | --- |
| `test/testdata/es/*.json` | 合法/非法 Query DSL、mapping 定义、bulk 请求体、期望 hits 快照 |
| `test/testdata/redis/*.json` | 各类数据结构的写入/期望读取值、命令白/黑名单清单 |
| `test/testdata/kafka/*` | 消息样本（key/value/headers）、topic 定义、期望消费顺序 |
| `internal/delivery/mcp/mock_test.go` | `mockRepo`、各能力接口的 fake 实现（参考 db-mcp-server mock_test.go） |

---

## 四、单元测试用例

### 4.1 配置层（`internal/config`）

| 用例 | 预期 |
| --- | --- |
| 加载合法 config.json | 三类源全部解析，字段映射正确 |
| `${VAR}` 占位符 | 从环境变量解析；缺失变量时报明确错误（fail closed） |
| 缺省值 | 未填 timeout/max_* 时取默认值 |
| 未知 type | 返回 "unsupported type" 错误 |
| ES `version` 字段 | `auto/7/8` 正常解析；非法值报错 |
| 重复 id / 空 id | 启动校验失败 |
| 路径解析 | 相对 `ca_cert` 相对配置目录解析为绝对路径 |
| env 覆盖 | 环境变量优先级符合约定 |
| `.env` 不存在 | 不报错，回退环境变量 |

### 4.2 连接管理器（`pkg/clients`）

| 用例 | 预期 |
| --- | --- |
| Get 已注册源 | 返回对应 DataSource |
| Get 未注册 id | 返回 ErrNotFound |
| List | 返回全部 id，不含密码 |
| Lazy 模式 | 首次 Get/操作时才 Connect |
| Eager 单源失败 | 按策略：默认报错；配置容错时跳过并标记状态 |
| 并发 Get | `-race` 下无数据竞争 |
| Close | 全部客户端被关闭，重复 Close 安全 |

### 4.3 护栏（`internal/usecase/guard_test.go`）—— 重点

**只读模式（白名单）**

| 数据源 | 放行 | 拒绝 |
| --- | --- | --- |
| ES | search、get doc、_cat/indices、_mapping、_cluster/health | index、delete、bulk、update_by_query、delete_by_query、_shutdown |
| Redis | get、strlen、type、ttl、scan、hgetall、lrange、smembers、zrange、xrange、info | set、del、expire、flushall、flushdb、eval、config、rename、subscribe* |
| Kafka | topics、groups、topic_detail、consume(commit=false)、cluster | produce、consume(commit=true)、delete topic |

要求：每个分类用例枚举**完整命令/端点表**做数据驱动，防止新增命令时漏配（黑名单/白名单与文档清单双向一致性测试）。

**危险操作**：即使非只读模式，`FLUSHALL`、`CONFIG SET`、ES `_shutdown`、删除 topic 默认拒绝，需显式开关。

### 4.4 截断 / 脱敏 / 超时 / 审计

| 用例 | 预期 |
| --- | --- |
| 结果数 = 上限 | 全量返回，无截断标记 |
| 结果数 > 上限 | 截断到上限，追加 `[Truncated]` |
| max=0 | 不限制 |
| 脱敏 fixed_string | 命中字段替换为配置值 |
| 脱敏 null | 替换为 null |
| 脱敏 partial | 保留指定尾字符/部分掩码 |
| 脱敏字段不存在 | 结果不变，不报错 |
| 脱敏规则正则非法 | 启动即失败（fail closed） |
| 超时 | context 超时后操作被取消，返回超时错误，资源释放 |
| 审计 | 每次操作产生一条 JSONL，字段齐全；默认不含明文值/密码 |

### 4.5 工具注册与路由（`internal/delivery/mcp`）—— 重点

| 用例 | 预期 |
| --- | --- |
| 按源命名注册 | 每实例生成约定数量工具，命名为 `prefix_sourceID` |
| 工具数量公式 | `7·ES + 6·Redis + 6·Kafka + 1 全局`，断言精确数量 |
| handler 闭包绑定 | 调用 `es_search_a` 使用的是源 a 的客户端 |
| 后缀兜底解析 | 从工具名正确剥离前缀并解析 sourceID |
| 非法 sourceID | 报错并列出全部可用源 |
| 统一工具模式 | 工具带 `source` 参数，数量与实例数无关 |
| 工具描述 | 包含 `Available sources: ...` 与只读提示 |
| list_sources | 只返回 id；不包含 password/地址中的凭据 |
| 参数缺失/类型错误 | 返回结构化校验错误，不触发底层调用 |
| 无源时 | 行为优雅（明确提示 / mock 兜底策略，与 db-mcp-server 对齐） |

### 4.6 各 UseCase（mock domain 接口）

针对每个工具分别覆盖：成功返回、空结果、底层错误透传、只读拒绝、超时取消、参数非法。验证 usecase 调用了正确的 domain 方法与入参（用 mock 断言调用参数）。

---

## 五、集成测试用例（live）

> 文件 `*_live_test.go` + `//go:build live`。每个用例准备数据 → 调工具/usecase → 校验真实结果 → 清理。

### 5.1 Elasticsearch（v7 / v8 双版本矩阵）

> **所有 5.1 用例必须分别对 ES 7.17 与 ES 8.x 各跑一遍**（通过 `ES_BASE_URL` 切换 9200/9201，或 testcontainers 指定两个镜像），用例以 `t.Run("v7"/"v8")` 分组。前置：创建测试索引 `t1_products_<rand>` 并写入夹具文档。

#### 5.1.1 版本探测与客户端选择（双版本专属）

| 用例 | 步骤 / 预期 |
| --- | --- |
| auto 探测 v8 | 对 8.x 实例 Connect，选中 v8 客户端/适配器，记录版本 8.x |
| auto 探测 v7 | 对 7.17 实例 Connect，选中 v7 客户端/适配器，记录版本 7.17.x |
| 显式 version=8 | 跳过探测直接用 v8 客户端；探测版本不一致时告警 |
| 显式 version=7 | 同上，使用 v7 客户端 |
| 探测失败（auto） | 无法判定主版本时 Connect 失败（fail closed），不盲目选用 |
| 非法 version 配置 | 返回配置校验错误 |
| 适配器类型断言 | v7/v8 实例返回的对象分别为对应 adapter，且均实现 `domain.Searchable` |

#### 5.1.2 功能用例（每个版本都跑）

| 用例 | 步骤 / 预期 |
| --- | --- |
| search match_all | 命中全部种子文档，total 正确 |
| search term 查询 | 仅返回匹配文档 |
| search bool + sort + from/size | 排序与分页正确 |
| search _source 过滤 | 返回体只含指定字段 |
| get 存在/不存在文档 | 返回文档 / 明确 not found |
| indices pattern | 通配匹配到目标索引，含文档数 |
| mapping | 返回字段类型与写入一致 |
| cluster health | status 字段存在（green/yellow） |
| index 写入（非只读） | 写入后 get 可读取 |
| delete（非只读） | 删除后 get 返回 not found |
| 只读源做写入 | 应用层在发出 HTTP 前拒绝（且可断言 ES 中无数据变化） |
| 错误认证/TLS | 错误密码、错误 CA 分别返回可读错误 |
| 大结果截断 | 写入超过 max_docs 文档，返回截断标记 |
| 版本差异回归 | 7.x 带 type 层的 mapping 可正常解析；8.x 响应中新增字段被安全忽略；两版本 SearchResult 结构一致 |

### 5.2 Redis

| 用例 | 步骤 / 预期 |
| --- | --- |
| set + get string | 值一致，TTL 设置生效（-1 / 指定秒） |
| get 不存在 key | 返回类型 none，不报错 |
| hash：hset → redis_data(hgetall) | 字段与值一致 |
| list：lpush → lrange | 顺序与元素正确 |
| set / zset / stream | 各结构读写正确（xrange 含 id） |
| scan pattern + limit | 只返回匹配 key，不阻塞（断言未使用 KEYS） |
| type / expire | 类型正确，过期后 key 消失（Eventually） |
| info | 含版本、内存、连接数等字段 |
| 只读源写操作 | set/del/expire 被拒绝 |
| 危险命令 | flushall/config 在任意模式被拒绝 |
| 错误密码/DB | 连接/认证错误可读 |

### 5.3 Kafka

前置：创建 topic `t1_orders_<rand>`（多分区）。

| 用例 | 步骤 / 预期 |
| --- | --- |
| topics 列表 | 含目标 topic，分区/副本数正确 |
| topic_detail | 各分区 leader/ISR、起止 offset 正确 |
| produce → consume | 发 N 条，按 partition/earliest 拉取，key/value/headers 一致且有序 |
| consume latest/earliest/绝对 offset | 三种 offset 语义正确 |
| 有界消费 | 消息多于 max_messages 时只返回上限并标记截断 |
| consume 超时 | 无新消息时按 timeout 返回已得消息/空，不挂死 |
| groups + lag | 建立 group 提交位点后，group lag 数值正确 |
| consume 默认不提交位点 | 重复消费结果一致，group 位点不变 |
| commit=true（非只读） | 位点推进 |
| 只读源 produce / commit | 被拒绝 |
| 集群信息 | broker 列表与 controller 正确 |
| SASL/TLS（可选环境） | 正确凭据成功，错误凭据失败 |

---

## 六、端到端冒烟测试（E2E Smoke）

### 6.1 方式

构建二进制 → 由 testcontainers 准备依赖（或指向既有实例）→ 通过 MCP 审查工具与 JSON-RPC 真实调用服务，分别覆盖 stdio 与 streamable HTTP 两种传输。

### 6.2 通过 MCP Inspector 审查验证

[MCP Inspector](https://github.com/modelcontextprotocol/inspector) 是 MCP 官方的审查/调试工具（"MCP 版 Postman"），无需接入完整 AI 客户端即可浏览工具、查看请求/响应与 JSON-RPC 报文。运行需要 Node.js 22.19.0+，通过 `npx` 按需启动，无需安装。

**Web UI（交互式审查）**

```bash
# Pass the command/args that start our Go binary (stdio). The "--" is required
# because "--config" is also an Inspector flag; without it Inspector consumes it.
ES_PASSWORD=changeme REDIS_PASSWORD=changeme \
  npx @modelcontextprotocol/inspector ./bin/server.exe -- --config config.json --lazy-loading
```

```powershell
# PowerShell: pass env via -e before the target command, and add "--" before the
# server's own flags so Inspector does not interpret "--config" as its session flag.
npx @modelcontextprotocol/inspector `
  -e ES_PASSWORD=changeme -e REDIS_PASSWORD=changeme `
  .\bin\server.exe -- --config config.json --lazy-loading
```

启动后打开终端输出的本地地址，在界面中依次：点击 **Connect** → **Tools / List Tools** 查看工具清单 → 选择工具、填写参数并 **Run**，同时在监控侧栏核对每一次 JSON-RPC 报文。streamable HTTP 模式则在 UI 中选择对应 Transport 并填入服务 URL。

**CLI（可脚本化，用于 CI 与冒烟）**

```bash
# 仅握手探测：校验 serverInfo / protocolVersion / capabilities
npx @modelcontextprotocol/inspector --cli ./bin/server.exe --config config.json --lazy-loading \
  --method initialize

# 列出全部工具并退出
npx @modelcontextprotocol/inspector --cli ./bin/server.exe --config config.json --lazy-loading \
  --method tools/list

# 调用只读查询（参数以 JSON 原样传入）
npx @modelcontextprotocol/inspector --cli ./bin/server.exe --config config.json --lazy-loading \
  --method tools/call --tool-name es_search_logs_es_local \
  --tool-args-json '{"indices":["t1_products"],"query_dsl":{"match_all":{}}}'

# 在只读源上调用写入，应返回只读错误（配合退出码/输出断言）
npx @modelcontextprotocol/inspector --cli ./bin/server.exe --config config.json --lazy-loading \
  --method tools/call --tool-name es_index_logs_es_local \
  --tool-args-json '{"index":"t1_products","document":{"name":"x"}}'
```

PowerShell 等价写法。注意 CLI 模式与 Web 模式的 `--` 分隔方向相反：**服务命令及其参数放在 `--` 之前，Inspector 自己的选项（`--method`、`-e` 等）放在 `--` 之后**；用反引号 `` ` `` 续行，JSON 参数用单引号原样传递。

```powershell
# 仅握手探测：校验 serverInfo / protocolVersion / capabilities
npx @modelcontextprotocol/inspector --cli .\bin\server.exe --config config.json --lazy-loading `
  -- --method initialize -e ES_PASSWORD=changeme -e REDIS_PASSWORD=changeme

# 列出全部工具并退出
npx @modelcontextprotocol/inspector --cli .\bin\server.exe --config config.json --lazy-loading `
  -- --method tools/list -e ES_PASSWORD=changeme -e REDIS_PASSWORD=changeme

# 调用只读查询（参数以 JSON 原样传入）
npx @modelcontextprotocol/inspector --cli .\bin\server.exe --config config.json --lazy-loading `
  -- --method tools/call --tool-name es_search_logs_es_local `
     --tool-args-json '{"indices":["t1_products"],"query_dsl":{"match_all":{}}}' `
     -e ES_PASSWORD=changeme -e REDIS_PASSWORD=changeme

# 在只读源上调用写入，应返回只读错误（配合退出码/输出断言）
npx @modelcontextprotocol/inspector --cli .\bin\server.exe --config config.json --lazy-loading `
  -- --method tools/call --tool-name es_index_logs_es_local `
     --tool-args-json '{"index":"t1_products","document":{"name":"x"}}' `
     -e ES_PASSWORD=changeme -e REDIS_PASSWORD=changeme
```

> 兼容性说明（重要）：MCP Inspector CLI 在连接后默认还会发送一次 `logging/setLevel`，而本服务依赖的 cortex v1.0.5 未实现该方法，也没有暴露注册任意 JSON-RPC 方法的公开 API。该请求返回 `-32601 Method not found` 后 Inspector CLI 会整体中断并以非零码退出，因此**当前下列 CLI 命令仅作参数参考，无法拿到正常输出**。实际审查请使用上面的 **Web UI**（不强制该调用），或用直接的 JSON-RPC 请求验证；待后续升级/替换支持该方法的服务框架后，CLI 链路即可打通。

审查要点：`initialize` 握手成功；`tools/list` 工具名/数量/inputSchema 与配置源一致；工具调用返回统一的 `content[].text`；只读源写入被拒绝；错误响应与报文中不含密码或带凭据的连接串。

### 6.3 必测脚本场景（`scripts/smoke.sh`）

1. `tools/list`：断言返回的工具名、数量与配置源精确匹配（含命名前缀）。
2. `list_sources`：返回三个源 id，且响应中无密码。
3. ES：search 只读成功；在只读源上调用写入必须返回只读错误。
4. Redis：get/scan 成功；危险命令被拒绝。
5. Kafka：topics 成功；只读源 produce 被拒绝。
6. 鉴权：streamable 模式无 Bearer / 错误 Bearer 返回 401；正确 Bearer 成功。
7. `/health`：返回 200、源数量与各源状态正确。

### 6.4 发布门禁

- 单元 + race + lint 全绿
- 集成测试（三源；ES 覆盖 7.17 / 8.x 双版本）全绿
- 冒烟脚本与 MCP Inspector CLI 审查全绿；工具数量/名称快照无意外变化
- 覆盖率门槛：核心 usecase / guard / registry 行覆盖率 ≥ 85%，护栏分类逻辑要求分支覆盖

---

## 七、契约与快照测试

1. **工具契约快照**：固定一组源配置，对 `tools/list` 的名称、description、inputSchema 生成快照（`test/testdata/snapshots/`），任何 schema 变更需显式更新快照，防止 AI 侧工具描述被无意改动。
2. **命令分类一致性**：单一"命令清单"作为唯一事实源，护栏白/黑名单、工具文档、快照测试都从它派生/比对，避免文档与实现漂移。
3. **响应格式契约**：成功必为 `content[].text`；错误必为统一错误结构且不含敏感字段（用正则扫描输出，断言无密码/连接串泄漏）。

---

## 八、安全相关测试

| 用例 | 预期 |
| --- | --- |
| 输出敏感信息扫描 | 所有错误、审计、list_sources、日志中均不含密码/token/完整带凭据 URL |
| 注入 | Redis 命令只能结构化传参，不允许拼接命令字符串；ES DSL 以 JSON 结构提交，不允许把原始文本拼进 endpoint |
| 只读绕过 | 尝试通过 Generic/未知命令变体绕过白名单：未登记命令默认拒绝（默认拒绝原则） |
| 鉴权 | 无 key、错误 key、空 key 配置（关闭鉴权）三种情况行为正确 |
| TLS | skip_tls_verify 默认 false；错误证书拒绝连接 |

---

## 九、测试用例追踪矩阵（开发核对用）

| 模块 | 单测 | 集成 | 冒烟 | 关键护栏用例 |
| --- | --- | --- | --- | --- |
| config | ✓ | - | - | 占位符 fail closed |
| clients/manager | ✓ | ✓ connect | - | lazy、容错 |
| guard | ✓ 全命令枚举 | ✓ 真实拒绝 | ✓ | 只读/危险/默认拒绝 |
| truncate/masking/audit | ✓ | ✓ | - | 非法脱敏规则启动失败 |
| tool registry/route | ✓ | - | ✓ tools/list | 数量/命名/source 校验 |
| es tools | ✓ | ✓ v7+v8 双版本 | ✓ | 只读写入拒绝、版本探测 |
| redis tools | ✓ | ✓ | ✓ | 危险命令拒绝 |
| kafka tools | ✓ | ✓ | ✓ | produce/commit 拒绝 |
| auth/health | ✓ | - | ✓ | 401、敏感信息 |

---

## 十、CI 流水线建议（`.github/workflows`）

1. `lint + unit (-race)`：PR 必跑，无需 Docker。
2. `integration`：由 testcontainers 自管容器，跑 `-tags=live`；ES 用例对 **7.17 与 8.x 两个镜像并行 job** 各跑一遍，Redis/Kafka 各一个 job。
3. `smoke + build image`：合并主干后运行，通过 MCP Inspector Web UI 或直接 JSON-RPC 完成协议审查（待服务框架支持 `logging/setLevel` 后再切到 Inspector CLI），作为发布门禁。
4. 失败产物：上传 coverage.out、server 日志、容器日志、冒烟请求/响应，便于定位。

---

## 十一、完成定义（DoD）

- 新增/变更工具必须同步：表驱动单测、对应集成用例（ES 工具需 v7/v8 两版本都通过）、命令清单（若涉新命令）、快照更新。
- 护栏相关改动必须包含"绕过尝试"用例，且默认拒绝原则生效。
- 全部 `make test test-race test-live smoke` 通过，覆盖率不回退。
- 无真实凭据入库，输出敏感扫描通过。
