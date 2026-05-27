# EMQX Kafka 插件设计

## 目标

为当前 EMQX 4.4 增加一个普通插件，用于集成 Kafka，不接入 Rule Engine，也不改 Dashboard。插件支持按配置的 MQTT topic filter 将消息 fan-out 转发到 Kafka topic，也支持从 Kafka topic 消费固定 JSON 格式的消息并发布回 EMQX。

## 范围

本设计面向当前仓库的 EMQX `4.4.19`。实现会参考公开项目 `ULTRAKID/emqx_plugin_kafka` 的 hook 式 MQTT 到 Kafka 转发逻辑，但需要适配当前仓库，并扩展多条 producer 规则和 Kafka consumer。

包含：

- 新增 `emqx_plugin_kafka` 普通 EMQX 插件应用。
- 通过 `brod` 启动 Kafka client、producer 和 consumer。
- 将 MQTT publish 事件转发到所有匹配规则对应的 Kafka topic。
- 消费配置的 Kafka topic，将合法 JSON 消息发布进 EMQX。
- 所有行为通过插件配置文件控制。
- 增加聚焦测试，覆盖配置解析、topic 规则匹配、fan-out、consumer payload 校验。

不包含：

- Rule Engine action/resource 集成。
- Dashboard UI 配置。
- 对齐 EMQX Enterprise 4.4 的完整功能。
- exactly-once 投递保证。
- 不重启插件的动态配置热更新。

## 架构

插件作为独立 OTP application 放在 `lib-extra/emqx_plugin_kafka`。应用模块带 `-emqx_plugin(?MODULE)` 标记，插件启动后启动自己的 supervisor，并在加载时注册 MQTT hook。

模块拆分如下：

- `emqx_plugin_kafka_app`：application callback 和 EMQX 插件标记。
- `emqx_plugin_kafka_sup`：管理 Kafka producer/client 和 consumer worker。
- `emqx_plugin_kafka`：插件 load/unload 入口和 MQTT hook 注册。
- `emqx_plugin_kafka_producer`：处理 MQTT publish、topic filter 匹配、JSON 编码和 `brod:produce_cb/6`。
- `emqx_plugin_kafka_consumer`：Kafka group subscriber callback，以及发布消息到 EMQX。
- `emqx_plugin_kafka_config`：读取并规范化 application env。
- `emqx_plugin_kafka_payload`：编码 MQTT 事件、解码 Kafka JSON 消息。

插件依赖 `brod` 处理 Kafka 协议，不依赖 EMQX Enterprise 模块。

## 配置

插件使用 `etc/emqx_plugin_kafka.config` 作为 application env 配置文件。后续可以再补 cuttlefish schema，但第一版先使用 Erlang config，足够满足本地编译和普通插件加载。

配置示例：

```erlang
[
  {emqx_plugin_kafka, [
    {kafka_hosts, [{"127.0.0.1", 9092}]},
    {client_id, emqx_plugin_kafka_client},
    {producer, [
      {enabled, true},
      {publish_base64, false},
      {rules, [
        {<<"sensor/+/up">>, <<"kafka_sensor_up">>},
        {<<"alarm/#">>, <<"kafka_alarm">>}
      ]}
    ]},
    {consumer, [
      {enabled, true},
      {group_id, <<"emqx_plugin_kafka">>},
      {topics, [<<"mqtt_downlink">>]},
      {begin_offset, earliest}
    ]},
    {brod_client_config, [
      {reconnect_cool_down_seconds, 10},
      {query_api_versions, true}
    ]},
    {producer_config, []},
    {consumer_config, []}
  ]}
].
```

Producer 规则按配置顺序保存，便于管理员阅读。但实际行为是 fan-out：所有匹配的规则都会产生一条 Kafka 消息。管理员需要在配置层避免不希望的 topic filter 重叠和重复转发。

## MQTT 到 Kafka

插件注册 `message.publish` hook。

每条 MQTT publish 的处理流程：

1. 如果 topic 以 `$SYS/` 开头，跳过。
2. 读取 MQTT topic、QoS、payload、timestamp、client ID、username 和当前节点。
3. 找出所有 MQTT topic filter 匹配当前消息 topic 的 producer 规则。
4. 对每条命中规则，将事件编码为 JSON，并写入对应 Kafka topic。
5. 无论 Kafka 写入结果如何，都向 EMQX 返回 `ok`。

Kafka key 优先使用 MQTT client ID；如果没有 client ID，则使用 MQTT topic。Kafka value 是 JSON。默认 `payload` 字段使用原始 MQTT payload；如果 `publish_base64 = true`，则先将 payload base64 编码，再写入 JSON，以支持任意字节流。

默认 producer JSON：

```json
{
  "action": "message_publish",
  "clientid": "client-a",
  "username": "user-a",
  "topic": "sensor/a/up",
  "qos": 1,
  "payload": "hello",
  "node": "emqx@127.0.0.1",
  "timestamp": 1710000000000
}
```

Producer 写 Kafka 失败时只记录 warning 日志，不拒绝、不延迟、不修改原始 MQTT publish。这个插件第一版定位是异步副作用桥接。

## Kafka 到 MQTT

只有当 `consumer.enabled = true` 时，插件才启动 Kafka consumer。

Consumer 接受固定 JSON 对象：

```json
{"topic":"xxx/xxx/xxx","qos":1,"payload":"xxxxxxxxxx"}
```

校验规则：

- `topic` 必须是非空 binary/string，且不能包含 MQTT 通配符。
- `qos` 必须是 `0`、`1` 或 `2`。缺省时使用 `0`。
- `payload` 必须是 binary/string。第一版不接受其他 JSON 类型作为 payload。

对于合法 Kafka 消息，插件生成并发布：

```erlang
#message{
  id = emqx_guid:gen(),
  qos = QoS,
  from = <<"emqx_plugin_kafka">>,
  flags = #{dup => false, retain => false},
  headers = #{},
  topic = Topic,
  payload = Payload,
  timestamp = erlang:system_time(millisecond)
}
```

发布使用 `emqx_broker:safe_publish/1`。

非法 Kafka 消息会被丢弃，并记录 warning 日志。Kafka 连接、重连和 group rebalance 交给 `brod`。

## 投递语义

插件提供 best-effort 桥接。

MQTT 到 Kafka：

- 原始 MQTT publish 不等待 Kafka 投递完成。
- Kafka produce callback 只负责记录失败日志。
- 第一版不引入本地磁盘缓冲。

Kafka 到 MQTT：

- 每条合法 Kafka 消息在被消费后发布进 EMQX。
- 如果发布进 EMQX 失败，第一版记录失败日志。
- offset commit 语义跟随最终选定的 `brod` group subscriber API；实现时需要在测试或注释里明确。

这套语义有意保持简单，不追求 EMQX Enterprise Data Integration 的完整能力。

## 编译和加载

插件作为 extra plugin 加入构建：

```bash
export EMQX_EXTRA_PLUGINS=emqx_plugin_kafka
make
```

release 中应包含：

```text
_build/emqx/rel/emqx/lib/emqx_plugin_kafka-<version>/
```

手动加载：

```bash
_build/emqx/rel/emqx/bin/emqx_ctl plugins load emqx_plugin_kafka
```

如果后续希望默认加载，可以再加入 `data/loaded_plugins.tmpl`。

## 测试

单元测试覆盖：

- 配置规范化和默认值。
- MQTT topic filter 匹配，包含 `+` 和 `#`。
- 多条规则同时匹配时的 fan-out 行为。
- `$SYS/` 消息跳过。
- producer payload JSON，包含 base64 开关两种情况。
- consumer JSON 校验。
- consumer JSON 转 `#message{}`。

Kafka 集成测试后续可以用 Docker Kafka 补充：

- 发布 MQTT 消息，断言 Kafka 收到预期 JSON。
- 写入 Kafka JSON，断言 MQTT subscriber 收到消息。

第一版应保持 Kafka 相关集成测试可选，避免普通本地编译依赖正在运行的 Kafka broker。

## 风险

- EMQX 4.4 的 hook callback 签名和旧 4.3 示例不同，插件必须以当前仓库的 `#message{}` 和 hook API 为准。
- `brod` 版本需要固定到兼容 OTP 24 和当前 EMQX 构建的版本。
- fan-out 会在管理员配置重叠 topic filter 时产生重复流量。
- producer 异步失败意味着 MQTT 客户端可能看到发布成功，但 Kafka 转发失败。
- consumer offset commit 策略需要避免意外丢消息；如果 `brod` API 允许，应优先在成功校验并发布后再 commit。
