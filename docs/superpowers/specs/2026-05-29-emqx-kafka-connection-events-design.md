# EMQX Kafka 插件连接事件设计

## 目标

在现有 `emqx_plugin_kafka` 插件上增加 MQTT 客户端连接生命周期事件转发能力，将 `connect` 和 `disconnect` 事件写入同一个独立 Kafka topic。

## 范围

本设计是对现有 Kafka 插件设计的增量扩展，面向当前仓库的 EMQX `4.4.19` 和已经存在的 `lib-extra/emqx_plugin_kafka` 实现。

包含：

- 新增 `client.connected` 和 `client.disconnected` hook 注册与注销。
- 新增连接事件专用配置段。
- 将连接和断开事件编码为 JSON，并发送到单独的 Kafka topic。
- 增加配置、payload 和 handler 的聚焦测试。
- 更新插件示例配置和 cuttlefish schema。

不包含：

- 改变现有 `message.publish` 到 Kafka 的规则式转发行为。
- 为连接事件引入第二套 Kafka client。
- Rule Engine、Dashboard UI 或动态热更新。

## 架构

现有 `message.publish` 路径保持不变，继续只处理 MQTT 消息转 Kafka。连接生命周期事件走独立路径：

- `message.publish` -> `emqx_plugin_kafka_producer:on_message_publish/1`
- `client.connected` -> `emqx_plugin_kafka_producer:on_client_connected/2`
- `client.disconnected` -> `emqx_plugin_kafka_producer:on_client_disconnected/3`

三个 hook 共用同一个 Kafka client 和 producer 基础配置，但连接事件不复用 `producer.rules`。这样可以把“MQTT 消息路由”和“客户端状态事件”明确隔离，避免把非消息事件伪装成 MQTT topic。

模块职责调整如下：

- `emqx_plugin_kafka`：新增连接事件 hook 的注册与注销。
- `emqx_plugin_kafka_config`：新增 `connection_events` 配置读取和规范化。
- `emqx_plugin_kafka_producer`：新增连接事件 handler 和发送 plan。
- `emqx_plugin_kafka_payload`：新增连接事件 JSON 编码函数。

## 配置

新增并列配置段：

```erlang
{connection_events, [
  {enabled, true},
  {topic, <<"mqtt_connection_events">>}
]}
```

对应 `.conf` 配置：

```ini
kafka.connection_events.enabled = true
kafka.connection_events.topic = mqtt_connection_events
```

对应 schema 路径：

- `kafka.connection_events.enabled`
- `kafka.connection_events.topic`

默认值建议为：

- `enabled = false`
- `topic = <<"mqtt_connection_events">>`

原因是连接事件属于新增行为，默认关闭更安全，避免升级插件后自动新增 Kafka 流量。

## 事件格式

`connect` 和 `disconnect` 共用同一个 Kafka topic，靠 JSON 字段 `action` 区分事件类型。

公共字段：

- `action`
- `clientid`
- `username`
- `node`
- `proto_name`
- `proto_ver`
- `peername`

连接事件：

```json
{
  "action": "connected",
  "clientid": "c1",
  "username": "u1",
  "node": "emqx@127.0.0.1",
  "proto_name": "MQTT",
  "proto_ver": 4,
  "peername": "10.0.0.8:53211",
  "connected_at": 1716970000000
}
```

断开事件：

```json
{
  "action": "disconnected",
  "clientid": "c1",
  "username": "u1",
  "node": "emqx@127.0.0.1",
  "proto_name": "MQTT",
  "proto_ver": 4,
  "peername": "10.0.0.8:53211",
  "reason": "normal",
  "disconnected_at": 1716970005000
}
```

字段约束：

- `action` 只允许 `connected` 或 `disconnected`。
- `clientid` 和 `username` 延续 EMQX hook 传入值；缺失时不强行造值。
- `peername` 编码成可读字符串，避免 Erlang tuple 直接进入 JSON。
- `reason` 仅出现在断开事件中，使用可序列化字符串表示。

Kafka message key 使用 `clientid`；如果缺失或为空，则退化为空 binary。

## 处理流程

连接事件：

1. `client.connected` hook 收到 `ClientInfo` 和 `ConnInfo`。
2. 读取缓存配置；若 `connection_events.enabled = false`，直接返回 `ok`。
3. 将事件编码为 JSON。
4. 调用 `brod:produce_cb/6` 写入配置的 `connection_events.topic`。
5. 无论 Kafka 是否发送成功，都向 EMQX 返回 `ok`。

断开事件：

1. `client.disconnected` hook 收到 `ClientInfo`、`Reason` 和 `ConnInfo`。
2. 读取缓存配置；若关闭则直接返回 `ok`。
3. 将事件编码为 JSON，并附带断开 `reason`。
4. 写入同一个 `connection_events.topic`。
5. 无论 Kafka 是否发送成功，都向 EMQX 返回 `ok`。

失败处理保持与现有 producer 一致：记录 warning 日志，不阻塞客户端连接和断开流程。

## 测试

单元测试覆盖：

- `connection_events` 默认值和配置规范化。
- `connected` payload 编码字段。
- `disconnected` payload 编码字段和 `reason`。
- handler 在配置关闭时返回 `ok`。
- handler 在配置开启时生成单条 Kafka publish plan。
- key 在 `clientid` 缺失时退化为空 binary。

本次不新增 Kafka 集成测试，保持本地开发和 CI 不依赖运行中的 Kafka broker。

## 风险

- `ClientInfo` / `ConnInfo` 的实际字段并不保证全部存在，编码逻辑必须容忍缺失字段。
- `Reason` 是 Erlang term，必须做稳定、可读且 JSON 安全的序列化。
- 连接事件量可能显著高于普通消息规则命中量，默认关闭可以降低误开启风险。
- 如果 consumer 端依赖强 schema，后续字段扩展需要保持向后兼容。
