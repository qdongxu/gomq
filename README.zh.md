# gomq

兼容 AMQP 0-9-1 客户端的 Go 原生消息队列。
目标是用 Go 完整实现 RabbitMQ 服务端功能，
使用嵌入式 etcd 持久化，htmx 构建 Web 管理端。

## 系统要求

- **Go** 1.25 或更高版本
- **etcd** 3.5+（可选，用于集群持久化）
- **Make**（可选，用于便捷命令）

## 快速开始

```bash
# 克隆仓库
git clone https://github.com/qdongxu/gomq.git
cd gomq

# 编译（带版本注入）
make build

# 验证版本
./bin/gomqd -version

# 运行测试
make test

# 运行基准测试
make bench

# 格式化 + lint + 测试（CI 门禁）
make check

# 构建 Docker 镜像
make docker

# 交叉编译发行版
make release

# 使用默认配置运行
./bin/gomqd -config configs/gomq.default.toml
```

服务器默认监听 **5672**（AMQP）和 **15672**（Web 管理端）。

## 安装

### 从源码安装

```bash
go install github.com/qdongxu/gomq/cmd/gomqd@latest
```

### 二进制发行版

从 [Releases](https://github.com/qdongxu/gomq/releases) 页面下载预编译二进制文件。

## 构建与发布

### Make 目标

| 目标 | 用途 |
|------|------|
| `make build` | 编译 `bin/gomqd`，注入版本号 |
| `make test` | 运行单元测试 + 集成测试 |
| `make bench` | 运行基准测试套件（`tests/bench/`） |
| `make lint` | 使用项目配置运行 `golangci-lint` |
| `make fmt` | `gofmt` + `goimports` 自动格式化 |
| `make docker` | 构建多阶段 Docker 镜像（`< 20 MB`） |
| `make release` | 交叉编译 linux/darwin × amd64/arm64 |
| `make clean` | 清理 `bin/` 和 `dist/` |
| `make check` | CI 门禁：`fmt` + `lint` + `test` |

### Docker

```bash
make docker
# → qdongxu/gomqd:latest

docker run -p 5672:5672 -p 15672:15672 \
  -v $(pwd)/configs/gomq.default.toml:/etc/gomq/gomq.default.toml \
  qdongxu/gomqd:latest
```

镜像使用两阶段构建：
1. `golang:1.25-alpine` 编译静态二进制（`CGO_ENABLED=0`）
2. `gcr.io/distroless/static:nonroot` 运行最终镜像（无 shell，最小攻击面）

### 交叉编译

```bash
make release
# 生成：
#   dist/gomqd-linux-amd64
#   dist/gomqd-linux-arm64
#   dist/gomqd-darwin-amd64
#   dist/gomqd-darwin-arm64
```

### 版本注入

`make build` 通过 ldflags 注入 Git 标签和构建时间：

```bash
./bin/gomqd -version
# gomqd v0.1.0-54-g<hash> (built 2026-05-28T02:55:00Z)
```

## 配置

gomq 使用 **TOML** 配置文件。参见 `configs/gomq.default.toml` 获取完整示例。

### 环境变量覆盖

任何配置值都可通过环境变量覆盖，使用 `GOMQ_` 前缀和下划线分隔的路径：

```bash
GOMQ_NETWORK_HEARTBEAT=30
GOMQ_TLS_ENABLED=true
GOMQ_TLS_CERT_FILE=/etc/gomq/server.crt
```

### 最小配置

```toml
[network]
listeners = ["0.0.0.0:5672"]
heartbeat = 60

[log]
level = "info"
output = "stdout"
```

### TLS 配置

```toml
[tls]
enabled = true
cert_file = "/etc/gomq/server.crt"
key_file  = "/etc/gomq/server.key"

# 可选：双向 TLS（客户端证书验证）
ca_file       = "/etc/gomq/ca.crt"
verify_client = true
```

启用 TLS 后，gomq 在 **5671** 端口监听（或 `network.listeners` 的第二个地址）。

### 集群配置

```toml
[cluster]
node_id = "node-1"
discovery = "etcd"
etcd_endpoints = ["http://localhost:2379"]
```

静态发现（无需 etcd）

```toml
[cluster]
node_id = "node-1"
discovery = "static"
nodes = ["node-2@192.168.1.10:5672", "node-3@192.168.1.11:5672"]
```

### Quorum Queue & Raft 网络层

gomq 使用简化版 Raft 共识算法实现 Quorum Queue 的多节点复制：

- **Raft 状态机**：Leader 选举、日志复制、心跳机制
- **传输层**：支持内存传输（测试）和 HTTP/JSON 传输（生产）
- **故障转移**：Leader 故障后，剩余节点自动重新选举
- **集成测试**：3 节点本地集群验证 Leader 选举、日志复制和故障转移

相关实现位于 `internal/cluster/`：

| 文件 | 职责 |
|------|------|
| `raft.go` | Raft 核心状态机（Term、Log、CommitIndex） |
| `raft_transport.go` | Transport 接口 + 内存传输实现 |
| `raft_rpc.go` | HTTP/JSON 传输实现 |
| `raft_node.go` | 多节点扩展（Run 循环、选举、心跳） |
| `raft_integration_test.go` | 3 节点集成测试 |

### Prometheus 指标

```toml
[metrics]
enabled = true
listen = "0.0.0.0:15692"
```

指标以 Prometheus 格式暴露在 `http://<listen>/metrics`。

### ACL（访问控制）

```toml
[[acl.rules]]
user = "admin"
vhost = "/"
resource_type = "*"
resource_name = "*"
permission = "*"
allow = true

[[acl.rules]]
user = "guest"
vhost = "/"
resource_type = "exchange"
resource_name = "amq.*"
permission = "write"
allow = true

[[acl.rules]]
user = "*"
vhost = "*"
resource_type = "*"
resource_name = "*"
permission = "*"
allow = false
```

规则按**顺序**评估，首条匹配规则生效。无匹配规则时默认拒绝。

| 权限 | 对应 AMQP 操作 |
|------|---------------|
| `configure` | Exchange.Declare/Delete/Bind/Unbind, Queue.Declare/Bind/Delete/Purge/Unbind |
| `write` | Basic.Publish |
| `read` | Basic.Consume, Basic.Get |

### SASL 认证

gomq 在 AMQP 连接启动阶段支持三种 SASL 机制：

| 机制 | 说明 | 前置条件 |
|------|------|---------|
| `PLAIN` | 用户名/密码（默认） | 无 |
| `AMQPLAIN` | RabbitMQ 兼容的用户名/密码 | 无 |
| `EXTERNAL` | 以 TLS 客户端证书 CN 作为身份 | 双向 TLS |

服务端在 `Connection.Start` 帧中通告所有可用机制，客户端在
`Connection.Start-Ok` 中选择其一。

**EXTERNAL** 从对等 TLS 证书的 CommonName 提取用户名：

```toml
[tls]
enabled       = true
cert_file     = "/etc/gomq/server.crt"
key_file      = "/etc/gomq/server.key"
ca_file       = "/etc/gomq/ca.crt"
verify_client = true   # EXTERNAL 必需
```

客户端出示有效证书且 CN 为 `alice` 时，即认证为用户 `alice`，
无需发送密码。

### 内存优化

```toml
[memory]
# 触发 zlib 压缩的最小 payload 大小（字节）。
# 0 = 关闭。
compression_threshold = 1024

# 单个队列在内存中保留的最大消息数，超过后刷到磁盘。
# 0 = 关闭。
max_in_memory_messages = 10000

# 磁盘页文件存放目录。
page_dir = "/var/lib/gomq/pages"
```

**压缩**启用后，超过阈值的消息在入队时自动压缩、出队时解压。
**分页**启用后，当队列消息数超过内存上限时，旧消息被刷到磁盘页文件；可随时加载回内存。

### 速率限制与背压控制

```toml
[limits]
# 每秒最大新建连接数。0 = 无限制。
max_connections_per_second = 100

# 触发背压控制的内存使用百分比阈值。0 = 关闭。
memory_threshold_percent = 80

# 背压控制总开关。
backpressure_enabled = true
```

**令牌桶速率限制器**在突发流量耗尽时拒绝新连接。
**背压控制**监控堆内存，超过阈值后拒绝新连接，并可能向发布者发送 `Channel.Flow`（暂停）。

### Web 管理端

```toml
[web]
enabled = true
listen = "0.0.0.0:15672"
path_prefix = "/"
```

管理端基于 **htmx** 构建，提供连接、信道、交换机、队列、绑定、集群节点和管理控制的实时视图。支持多语言（英语、中文、日语），通过浏览器语言自动检测或手动切换。

### 管理端点（健康检查与就绪探针）

```toml
[management]
# 启用 /api/health 和 /api/ready 端点
health_enabled = true

# 启用 /debug/pprof/*（仅在 log.level = "debug" 时生效）
pprof_enabled = false

# 独立绑定地址；留空则复用 Web 管理端端口
bind_address = ""
```

| 端点 | 方法 | 用途 | 响应 |
|------|------|------|------|
| `/api/health` | GET | 节点健康状态（运行/宕机）、版本、运行时间、组件检查 | `200 OK` + JSON |
| `/api/ready` | GET | 就绪探针（监听器 + 存储状态） | `200 OK` 或 `503 Service Unavailable` + JSON |
| `/debug/pprof/` | GET | 运行时分析（堆内存、CPU、协程、互斥锁） | `200 OK`（仅调试模式） |

就绪端点在 AMQP 监听器未激活或持久化存储（etcd）不可达时返回 `503`。

### 热重载

```toml
[management]
# 启用配置文件监听以实现热重载
health_enabled = true
```

gomq 监听配置文件变更并自动应用可热重载的设置，无需重启进程。发送 `SIGHUP` 强制触发重载：

```bash
kill -HUP $(pgrep gomqd)
```

| 可热重载 | 不可热重载（需重启） |
|---------|---------------------|
| 日志级别 | 网络监听端口 |
| TLS 证书路径 | etcd 端点 |
| ACL 规则 | 集群节点 ID |
| 速率限制阈值 | Web UI / metrics 端口 |
| 背压阈值 | |
| 内存设置 | |

当不可热重载的配置项变更时，系统会打印警告并忽略该变更，直到下次重启。

## 项目结构

| 路径 | 说明 |
|------|------|
| `cmd/gomqd/` | 服务端入口 |
| `internal/server/` | AMQP 连接、信道、交换机、队列核心 |
| `internal/auth/` | ACL 规则引擎 **+ SASL 认证机制** |
| `internal/store/` | etcd 与内存持久化后端 |
| `internal/config/` | TOML 配置解析 |
| `internal/web/` | htmx 管理端 |
| `internal/cluster/` | 集群、节点发现、Raft 仲裁队列 |
| `internal/metrics/` | Prometheus 指标收集器 |
| `internal/queue/` | 队列实现（内存、仲裁） |
| `pkg/protocol/amqp091/` | AMQP 0-9-1 协议帧编解码 |
| `test/integration/` | 集成测试 |

## 功能状态

| 功能 | 状态 |
|---------|--------|
| AMQP 0-9-1 协议编解码 | ✅ |
| 连接握手与心跳 | ✅ |
| 信道多路复用 | ✅ |
| 交换机声明/删除（direct、fanout、topic、headers） | ✅ |
| 队列声明/删除/绑定/解绑/清空 | ✅ |
| Basic.Publish / Basic.Get / Basic.Consume | ✅ |
| Basic.Ack / Basic.Nack / Basic.Reject | ✅ |
| Basic.Qos（单信道与全局预取） | ✅ |
| Basic.Recover | ✅ |
| Connection.Close / Channel.Close | ✅ |
| Channel.Flow（客户端到服务端） | ✅ |
| Basic.Return（强制投递） | ✅ |
| 发布者确认（Confirm.Select + Basic.Ack/Nack） | ✅ |
| 事务支持（Tx.Select / Tx.Commit / Tx.Rollback） | ✅ |
| 死信交换机（DLX） | ✅ |
| 消息 TTL（单消息与队列级，后台扫描 + DLX 联动） | ✅ |
| **etcd 快照与恢复（定期备份 + 完整元数据恢复）** | ✅ |
| 优先级队列 | ✅ |
| 消息内存存储 | ✅ |
| 路由与投递 | ✅ |
| etcd 持久化（Store 接口 + 内存后端） | ✅ |
| etcd 持久化（etcd 后端） | ✅ |
| 启动时加载持久化状态 | ✅ |
| Web 管理端（htmx 框架） | ✅ |
| Web 管理端 — Overview 页面 | ✅ |
| Web 管理端 — Connections 页面 | ✅ |
| Web 管理端 — Channels 页面 | ✅ |
| Web 管理端 — Exchanges 页面 | ✅ |
| Web 管理端 — Queues 页面 | ✅ |
| **Web 管理端 — Messages 页面** | ✅ |
| Web 管理端 — Bindings 页面 | ✅ |
| Web 管理端 — Admin 页面 | ✅ |
| Web 管理端 — VHost 管理页面 | ✅ |
| Web 管理端 — 集群节点页面 | ✅ |
| Web 管理端 — 国际化（en/zh/ja） | ✅ |
| Quorum Queue（基于 Raft 的镜像复制） | ✅ |
| **Quorum Queue — 多节点 Raft 网络层** | ✅ |
| Exchange-to-Exchange 绑定 | ✅ |
| 集群节点发现（etcd） | ✅ |
| TLS 支持（AMQP over TLS + mTLS） | ✅ |
| Prometheus 指标导出 | ✅ |
| 内存池与批处理（性能优化） | ✅ |
| Channel.Recover 及边缘方法 | ✅ |
| ACL（访问控制列表）——虚拟主机级权限 | ✅ |
| **SASL 认证（PLAIN / AMQPLAIN / EXTERNAL）** | ✅ |
| **消息存储压缩与分页（zlib，磁盘溢出）** | ✅ |
| **速率限制与背压控制** | ✅ |
| **Connection.CloseOk / Channel.CloseOk 处理器** | ✅ |
| **Basic.Reject（重新入队 + 丢弃 + DLX）** | ✅ |
| **WebSocket 实时推送（管理端动态更新）** | ✅ |
| Web 管理端 — 会话与 CSRF 防护 | ✅ |
| **消息追踪与日志审计** | ✅ |
| **消费者组（负载均衡消费，round-robin / hash）** | ✅ |
| 镜像队列（HA Queue） | ✅ |
| 插件系统 | ✅ |
| Federation / Shovel | ✅ |
| **配置热重载** | ✅ |
| **健康检查与就绪探针** | ✅ |
| **pprof 运行时分析（调试模式）** | ✅ |
| **基准测试套件** | ✅ |
| **AMQP 兼容性测试套件** | ⚠️ | 基础设施就绪；因与 amqp091-go 客户端的协议握手问题暂被跳过 |

## 开发

```bash
# 格式化代码
make fmt

# 运行静态检查
make lint

# 清理构建产物
make clean
```

### 运行集成测试

```bash
# 先启动本地 etcd 实例，然后：
go test ./test/integration/...
```

## 贡献

1. Fork 本仓库
2. 创建功能分支：`git checkout -b feat/your-feature`
3. 提交更改（遵循 [Conventional Commits](https://www.conventionalcommits.org/)）
4. 推送到你的 fork 并创建 Pull Request

## 许可证

MIT
