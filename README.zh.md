# gomq

兼容 AMQP 0-9-1 客户端的 Go 原生消息队列。
目标是用 Go 完整实现 RabbitMQ 服务端功能，
使用嵌入式 etcd 持久化，htmx 构建 Web 管理端。

## 配置

gomq 使用 TOML 配置文件。参见 `configs/gomq.default.toml` 了解所有
可用选项。环境变量可通过 `GOMQ_` 前缀覆盖文件值
（例如 `GOMQ_NETWORK_HEARTBEAT=30`）。

## 快速开始

```bash
# 编译
make build

# 运行
./bin/gomqd -config configs/gomq.default.toml

# 测试
make test
```

## 项目结构

- `cmd/gomqd/` — 服务端入口
- `internal/server/` — AMQP 连接、信道、交换机、队列核心逻辑
- `internal/store/` — etcd 持久化封装
- `internal/config/` — TOML 配置解析
- `internal/web/` — htmx 管理端
- `internal/cluster/` — 集群与节点管理
- `pkg/protocol/amqp091/` — AMQP 0-9-1 协议帧编解码
- `test/integration/` — 集成测试

## 状态

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
| etcd 持久化（etcd 后端） | 开发中 |
| 启动时加载持久化状态 | ✅ |
| Web 管理端（htmx 框架） | ✅ |
| Web 管理端 — Overview 页面 | ✅ |
| Web 管理端 — Connections 页面 | ✅ |
| Web 管理端 — Channels 页面 | ✅ |
| Web 管理端 — Exchanges 页面 | ✅ |
| Web 管理端 — Queues 页面 | ✅ |
| Web 管理端 — Bindings 页面 | ✅ |
| Web 管理端 — Admin 页面 | ✅ |
| 集群 | ✅ |

开发中 — 文档和功能随代码实现持续更新。

## 许可证

MIT
