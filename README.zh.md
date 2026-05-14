# gomq

兼容 AMQP 0-9-1 客户端的 Go 原生消息队列。
目标是用 Go 完整实现 RabbitMQ 服务端功能，
使用嵌入式 etcd 持久化，htmx 构建 Web 管理端。

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

开发中 — 文档和功能随代码实现持续更新。

## 许可证

MIT
