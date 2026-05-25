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

# 编译
make build

# 使用默认配置运行
./bin/gomqd -config configs/gomq.default.toml

# 运行测试
make test
```

服务器默认监听 **5672**（AMQP）和 **15672**（Web 管理端）。

## 安装

### 从源码安装

```bash
go install github.com/qdongxu/gomq/cmd/gomqd@latest
```

### 二进制发行版

从 [Releases](https://github.com/qdongxu/gomq/releases) 页面下载预编译二进制文件。

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

静态发现（无需 etcd）：

```toml
[cluster]
node_id = "node-1"
discovery = "static"
nodes = ["node-2@192.168.1.10:5672", "node-3@192.168.1.11:5672"]
```

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

### Web 管理端

```toml
[web]
enabled = true
listen = "0.0.0.0:15672"
path_prefix = "/"
```

管理端基于 **htmx** 构建，提供连接、信道、交换机、队列、绑定和管理控制的实时视图。

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
| 消息 TTL（单消息与队列级） | ✅ |
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
| Web 管理端 — Bindings 页面 | ✅ |
| Web 管理端 — Admin 页面 | ✅ |
| Quorum Queue（基于 Raft 的镜像复制） | ✅ |
| Exchange-to-Exchange 绑定 | ✅ |
| 集群节点发现（etcd） | ✅ |
| TLS 支持（AMQP over TLS + mTLS） | ✅ |
| Prometheus 指标导出 | ✅ |
| 内存池与批处理（性能优化） | ✅ |
| Channel.Recover 及边缘方法 | ✅ |
| ACL（访问控制列表）——虚拟主机级权限 | ✅ |
| **SASL 认证（PLAIN / AMQPLAIN / EXTERNAL）** | ✅ |
| 镜像队列（HA Queue） | ✅ |
| 插件系统 | ✅ |
| Federation / Shovel | ✅ |

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
