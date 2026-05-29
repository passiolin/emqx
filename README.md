# EMQX 4.4 Kafka 插件版

这个分支基于 EMQX 4.4.x 社区版源码，新增了普通插件 `emqx_plugin_kafka`。插件不接入 Rule Engine，也不改 Dashboard，通过 EMQX 普通插件机制加载。

## 当前改动

新增插件目录：

```text
lib-extra/emqx_plugin_kafka
```

插件能力：

- MQTT 发布消息转发到 Kafka。
- MQTT 客户端 `connect` / `disconnect` 生命周期事件转发到独立 Kafka topic。
- 支持多条 MQTT topic filter 规则。
- 同一条 MQTT 消息命中多条规则时，会 fan-out 到多个 Kafka topic。
- 支持从 Kafka topic 消费固定 JSON，并发布回 EMQX。
- Kafka 到 MQTT 的消息 `from` 固定为 `<<"emqx_plugin_kafka">>`。
- 插件默认不自动加载，需要手动加载或通过 `EMQX_LOADED_PLUGINS` 指定。

构建相关改动：

- `lib-extra/plugins` 增加 `emqx_plugin_kafka`。
- `rebar.config.erl` 支持 `lib-extra/*` 作为 project app dir。
- `rebar.config.erl` 将测试依赖 `meck` 固定到 OTP 24 可用版本。
- 插件提供普通插件配置文件 `etc/emqx_plugin_kafka.conf`。
- 插件提供 cuttlefish schema `priv/emqx_plugin_kafka.schema`，release 会生成：

```text
_build/emqx/rel/emqx/etc/plugins/emqx_plugin_kafka.conf
_build/emqx/rel/emqx/lib/emqx_plugin_kafka-0.1.0/priv/emqx_plugin_kafka.schema
```

## 环境要求

deepin 25 上建议直接用官方 builder Docker 镜像编译，避免本机 Erlang/OTP、rebar3、gcc 版本不一致。

需要：

- Docker
- Git
- 当前源码目录可写

本文命令默认在仓库根目录执行。

## 编译

普通用户运行 Docker builder 时必须设置 `HOME` 和 `XDG_CACHE_HOME`，否则 rebar3 可能尝试写 `/.cache/rebar3/hex` 并失败。

```bash
mkdir -p .cache/rebar3

docker run --rm -it \
  -v "$PWD":/emqx \
  -w /emqx \
  --user "$(id -u):$(id -g)" \
  -e HOME=/emqx \
  -e XDG_CACHE_HOME=/emqx/.cache \
  -e EMQX_EXTRA_PLUGINS=emqx_plugin_kafka \
  ghcr.io/emqx/emqx-builder/4.4-20:24.3.4.2-1-debian11 \
  bash -lc 'make emqx'
```

编译成功后 release 位于：

```text
_build/emqx/rel/emqx
```

只想快速验证编译时，也可以执行：

```bash
docker run --rm \
  -v "$PWD":/emqx \
  -w /emqx \
  --user "$(id -u):$(id -g)" \
  -e HOME=/emqx \
  -e XDG_CACHE_HOME=/emqx/.cache \
  -e EMQX_EXTRA_PLUGINS=emqx_plugin_kafka \
  ghcr.io/emqx/emqx-builder/4.4-20:24.3.4.2-1-debian11 \
  bash -lc './rebar3 as emqx compile'
```

## 运行

启动 EMQX：

```bash
_build/emqx/rel/emqx/bin/emqx console
```

另开一个终端加载插件：

```bash
_build/emqx/rel/emqx/bin/emqx_ctl plugins load emqx_plugin_kafka
```

查看插件：

```bash
_build/emqx/rel/emqx/bin/emqx_ctl plugins list
```

停止：

```bash
_build/emqx/rel/emqx/bin/emqx stop
```

Dashboard 默认地址：

```text
http://127.0.0.1:18083
```

## 插件配置

release 中的普通配置文件：

```text
_build/emqx/rel/emqx/etc/plugins/emqx_plugin_kafka.conf
```

默认内容示例：

```conf
kafka.hosts = 127.0.0.1:9092
kafka.client_id = emqx_plugin_kafka_client

kafka.producer.enabled = true
kafka.producer.publish_base64 = false
kafka.producer.excluded_topics = internal/#, alarm/debug/#

kafka.producer.rule.1.mqtt_topic = sensor/+/up
kafka.producer.rule.1.kafka_topic = kafka_sensor_up
kafka.producer.rule.2.mqtt_topic = alarm/#
kafka.producer.rule.2.kafka_topic = kafka_alarm

kafka.consumer.enabled = false
kafka.consumer.group_id = emqx_plugin_kafka
kafka.consumer.topics = mqtt_downlink
kafka.consumer.begin_offset = earliest

kafka.connection_events.enabled = false
kafka.connection_events.topic = mqtt_connection_events
```

如果需要配置复杂的 `brod_client_config`、`producer_config` 或 `consumer_config`，可以手动创建 Erlang 配置文件：

```text
_build/emqx/rel/emqx/etc/plugins/emqx_plugin_kafka.config
```

该文件默认不存在，需要自己按 Erlang term 格式创建，内容形如：

```erlang
[{emqx_plugin_kafka,
  [{brod_client_config, []},
   {producer_config, []},
   {consumer_config, []}]}].
```

注意：如果 `.config` 存在，EMQX 插件加载逻辑会优先读取 `.config`，不会再用 `.conf + schema` 生成配置。

## MQTT 到 Kafka

插件注册 `message.publish` hook。

处理逻辑：

1. 跳过 `$SYS/` 系统 topic。
2. 使用 `kafka.producer.excluded_topics` 排除不需要转发到 Kafka 的 MQTT topic。
3. 使用 `emqx_topic:match/2` 匹配配置中的 MQTT topic filter。
4. 命中所有规则都会写 Kafka，即 fan-out。
5. Kafka 写入使用 `brod:produce_cb/6`。
6. Kafka 写入失败只记录日志，不拒绝原 MQTT publish。

`kafka.producer.excluded_topics` 使用逗号分隔，支持 MQTT topic filter 语法中的 `+` 和 `#`。排除列表优先级高于 producer rules；只要 MQTT topic 命中排除列表，即使同时命中 `kafka.producer.rule.*.mqtt_topic`，也不会写入 Kafka。

Kafka value JSON 示例：

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

如果 `kafka.producer.publish_base64 = true`，`payload` 会先 base64 编码，适合 MQTT payload 不是普通 UTF-8 文本的场景。

## MQTT 客户端连接事件到 Kafka

连接事件默认关闭：

```conf
kafka.connection_events.enabled = false
```

开启后，插件会额外挂载：

- `client.connected`
- `client.disconnected`

两类事件都会写入同一个 Kafka topic：

```conf
kafka.connection_events.topic = mqtt_connection_events
```

Kafka value JSON 中通过 `action` 区分事件类型，只使用两个值：

- `connected`
- `disconnected`

`connected` 示例：

```json
{
  "action": "connected",
  "clientid": "client-a",
  "username": "user-a",
  "node": "emqx@127.0.0.1",
  "proto_name": "MQTT",
  "proto_ver": 5,
  "peername": "10.0.0.8:53211",
  "connected_at": 1716970000000
}
```

`disconnected` 示例：

```json
{
  "action": "disconnected",
  "clientid": "client-a",
  "username": "user-a",
  "node": "emqx@127.0.0.1",
  "proto_name": "MQTT",
  "proto_ver": 5,
  "peername": "10.0.0.8:53211",
  "reason": "normal",
  "disconnected_at": 1716970005000
}
```

说明：

- Kafka message key 默认使用 MQTT `clientid`。
- 如果 `clientid` 缺失或为 `undefined`，Kafka key 会退化为空 binary，JSON 中也不会带 `clientid` 字段。
- IPv6 `peername` 会编码成带方括号的形式，例如 `[2001:db8::1]:1883`。
- Kafka 写入失败只记录日志，不阻塞客户端连接或断开流程。

## Kafka 到 MQTT

consumer 默认关闭：

```conf
kafka.consumer.enabled = false
```

开启后，插件会消费 `kafka.consumer.topics` 中配置的 Kafka topic。

Kafka 消息格式固定为：

```json
{"topic":"down/a","qos":1,"payload":"hello-from-kafka"}
```

校验规则：

- `topic` 必须是非空字符串，不能包含 MQTT 通配符 `+` 或 `#`。
- `qos` 必须存在，且只能是 `0`、`1`、`2`。
- `payload` 必须是字符串。

合法消息会通过 `emqx_broker:safe_publish/1` 发布进 EMQX。

## 测试

运行插件单元测试：

```bash
docker run --rm \
  -v "$PWD":/emqx \
  -w /emqx \
  --user "$(id -u):$(id -g)" \
  -e HOME=/emqx \
  -e XDG_CACHE_HOME=/emqx/.cache \
  -e EMQX_EXTRA_PLUGINS=emqx_plugin_kafka \
  ghcr.io/emqx/emqx-builder/4.4-20:24.3.4.2-1-debian11 \
  bash -lc './rebar3 eunit --dir lib-extra/emqx_plugin_kafka'
```

当前测试覆盖包括：

- 配置默认值和规范化。
- MQTT publish payload 编码。
- connect / disconnect 事件 payload 编码。
- producer topic 匹配与 connection event plan。
- 插件 hook 注册和注销。

如果本机直接执行 `make` / `rebar3`，需要先具备 `erl` 和 `escript`；否则会在进入测试前报：

```text
/usr/bin/env: ‘escript’: No such file or directory
```

完整 release 验证：

```bash
docker run --rm \
  -v "$PWD":/emqx \
  -w /emqx \
  --user "$(id -u):$(id -g)" \
  -e HOME=/emqx \
  -e XDG_CACHE_HOME=/emqx/.cache \
  -e EMQX_EXTRA_PLUGINS=emqx_plugin_kafka \
  ghcr.io/emqx/emqx-builder/4.4-20:24.3.4.2-1-debian11 \
  bash -lc './rebar3 as emqx release -n emqx'
```

## 打 Docker 镜像

`deploy/docker/Dockerfile` 已经改成默认编译 `emqx_plugin_kafka`。直接用仓库里的 Dockerfile 构建即可：

```bash
docker build -t emqx-kafka:4.4.19 -f deploy/docker/Dockerfile .
```

构建完成后可确认镜像里包含插件配置：

```bash
docker run --rm --entrypoint sh emqx-kafka:4.4.19 -lc \
  'test -f /opt/emqx/etc/plugins/emqx_plugin_kafka.conf && \
   test -f /opt/emqx/lib/emqx_plugin_kafka-0.1.0/priv/emqx_plugin_kafka.schema'
```

如果要覆盖 extra plugins，可以传 build arg：

```bash
docker build -t emqx-kafka:4.4.19 \
  --build-arg EMQX_EXTRA_PLUGINS=emqx_plugin_kafka \
  -f deploy/docker/Dockerfile .
```

运行镜像：

```bash
docker run -d --name emqx-kafka \
  -p 1883:1883 \
  -p 18083:18083 \
  -e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka" \
  emqx-kafka:4.4.19
```

如果 Kafka 在宿主机上，Linux Docker 里建议加：

```bash
--add-host=host.docker.internal:host-gateway
```

并设置：

```bash
-e EMQX_KAFKA__HOSTS=host.docker.internal:9092
```

完整示例：

```bash
docker run -d --name emqx-kafka \
  --add-host=host.docker.internal:host-gateway \
  -p 1883:1883 \
  -p 18083:18083 \
  -e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka" \
  -e EMQX_KAFKA__HOSTS=host.docker.internal:9092 \
  -e EMQX_KAFKA__PRODUCER__EXCLUDED_TOPICS="internal/#,alarm/debug/#" \
  -e EMQX_KAFKA__PRODUCER__RULE__1__MQTT_TOPIC="sensor/+/up" \
  -e EMQX_KAFKA__PRODUCER__RULE__1__KAFKA_TOPIC="kafka_sensor_up" \
  -e EMQX_KAFKA__CONSUMER__ENABLED=true \
  -e EMQX_KAFKA__CONSUMER__TOPICS=mqtt_downlink \
  emqx-kafka:4.4.19
```

查看日志：

```bash
docker logs -f emqx-kafka
```

进入容器：

```bash
docker exec -it emqx-kafka sh
```

查看插件：

```bash
docker exec -it emqx-kafka emqx_ctl plugins list
```

## 手动验收

准备 Kafka topic：

```text
kafka_sensor_up
kafka_alarm
mqtt_downlink
mqtt_connection_events
```

验证 MQTT 到 Kafka：

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -i client-a -t sensor/a/up -m hello -q 1
```

期望 Kafka topic `kafka_sensor_up` 收到类似 JSON：

```json
{"action":"message_publish","clientid":"client-a","topic":"sensor/a/up","qos":1,"payload":"hello"}
```

验证 Kafka 到 MQTT：

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -t down/a -q 1
```

向 Kafka topic `mqtt_downlink` 写入：

```json
{"topic":"down/a","qos":1,"payload":"hello-from-kafka"}
```

期望 MQTT subscriber 收到：

```text
hello-from-kafka
```

验证连接事件到 Kafka：

1. 在 Kafka 中消费 `mqtt_connection_events`
2. 建立一个 MQTT 连接，再主动断开

期望 Kafka 收到类似 JSON：

```json
{"action":"connected","clientid":"client-a","proto_name":"MQTT","proto_ver":5,"peername":"10.0.0.8:53211","connected_at":1716970000000}
```

以及：

```json
{"action":"disconnected","clientid":"client-a","proto_name":"MQTT","proto_ver":5,"peername":"10.0.0.8:53211","reason":"normal","disconnected_at":1716970005000}
```

## 常见问题

### `Could not write to "/.cache/rebar3/hex"`

Docker builder 中没有正确设置 HOME。使用本文命令里的：

```bash
-e HOME=/emqx
-e XDG_CACHE_HOME=/emqx/.cache
```

### `_build` 或 `.cache` 变成 root 权限

如果曾经用 root 运行过 builder，可以清理后重新编译：

```bash
docker run --rm \
  -v "$PWD":/emqx \
  -w /emqx \
  --user root \
  ghcr.io/emqx/emqx-builder/4.4-20:24.3.4.2-1-debian11 \
  bash -lc 'rm -rf _build .cache rebar.lock rebar.config.rendered'
```

### 镜像启动后插件没有加载

确认启动容器时包含：

```bash
-e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka"
```

或者进入容器手动执行：

```bash
emqx_ctl plugins load emqx_plugin_kafka
```

### Kafka 连接失败

检查：

- `kafka.hosts` 是否从 EMQX 容器内可访问。
- Kafka advertised listeners 是否对 EMQX 容器可达。
- producer 规则里的 Kafka topic 是否存在，或 Kafka 是否允许自动创建 topic。
- consumer 开启时，`kafka.consumer.topics` 是否存在。

## 当前限制

- MQTT 到 Kafka 是 best-effort，Kafka 写失败不会阻塞 MQTT publish。
- 没有本地磁盘缓存。
- 没有 Dashboard 配置界面。
- 没有 Rule Engine action/resource。
- 不支持运行时热更新配置；修改配置后需要重新加载插件或重启 EMQX。
