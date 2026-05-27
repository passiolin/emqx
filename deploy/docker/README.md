# EMQX Kafka 插件版 Docker 镜像

本目录用于构建和运行当前分支的 EMQX Docker 镜像。当前分支已经集成普通插件 `emqx_plugin_kafka`，`deploy/docker/Dockerfile` 默认会在构建阶段设置：

```text
EMQX_EXTRA_PLUGINS=emqx_plugin_kafka
```

因此用本目录 Dockerfile 打出的镜像会包含 Kafka 插件、插件配置文件和 schema。

## 快速构建

在仓库根目录执行：

```bash
docker build -t emqx-kafka:4.4.19 -f deploy/docker/Dockerfile .
```

如果需要显式覆盖 extra plugins：

```bash
docker build -t emqx-kafka:4.4.19 \
  --build-arg EMQX_EXTRA_PLUGINS=emqx_plugin_kafka \
  -f deploy/docker/Dockerfile .
```

构建完成后检查插件文件：

```bash
docker run --rm --entrypoint sh emqx-kafka:4.4.19 -lc \
  'test -f /opt/emqx/etc/plugins/emqx_plugin_kafka.conf && \
   test -f /opt/emqx/lib/emqx_plugin_kafka-0.1.0/priv/emqx_plugin_kafka.schema'
```

## 快速运行

启动 EMQX 并加载 Kafka 插件：

```bash
docker run -d --name emqx-kafka \
  -p 1883:1883 \
  -p 18083:18083 \
  -e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka" \
  emqx-kafka:4.4.19
```

查看插件状态：

```bash
docker exec -it emqx-kafka emqx_ctl plugins list
```

查看日志：

```bash
docker logs -f emqx-kafka
```

进入容器：

```bash
docker exec -it emqx-kafka sh
```

EMQX 在容器内使用 Linux 用户 `emqx` 运行。

## 连接宿主机 Kafka

如果 Kafka 跑在宿主机上，Linux Docker 里建议使用 `host.docker.internal`：

```bash
docker run -d --name emqx-kafka \
  --add-host=host.docker.internal:host-gateway \
  -p 1883:1883 \
  -p 18083:18083 \
  -e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka" \
  -e EMQX_KAFKA__HOSTS=host.docker.internal:9092 \
  emqx-kafka:4.4.19
```

如果 Kafka 在其他机器，把 `EMQX_KAFKA__HOSTS` 改成实际地址：

```bash
-e EMQX_KAFKA__HOSTS=10.10.10.244:9092
```

## Kafka 插件配置

镜像内默认配置文件：

```text
/opt/emqx/etc/plugins/emqx_plugin_kafka.conf
```

常用环境变量示例：

```bash
-e EMQX_KAFKA__HOSTS=10.10.10.244:9092
-e EMQX_KAFKA__CLIENT_ID=emqx_plugin_kafka
-e EMQX_KAFKA__PRODUCER__ENABLED=true
-e EMQX_KAFKA__PRODUCER__PUBLISH_BASE64=false
-e EMQX_KAFKA__PRODUCER__RULE__1__MQTT_TOPIC="sensor/+/up"
-e EMQX_KAFKA__PRODUCER__RULE__1__KAFKA_TOPIC="kafka_sensor_up"
-e EMQX_KAFKA__CONSUMER__ENABLED=true
-e EMQX_KAFKA__CONSUMER__GROUP_ID=emqx_plugin_kafka
-e EMQX_KAFKA__CONSUMER__TOPICS=mqtt_downlink
-e EMQX_KAFKA__CONSUMER__BEGIN_OFFSET=earliest
```

完整运行示例：

```bash
docker run -d --name emqx-kafka \
  --add-host=host.docker.internal:host-gateway \
  -p 1883:1883 \
  -p 18083:18083 \
  -e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka" \
  -e EMQX_KAFKA__HOSTS=host.docker.internal:9092 \
  -e EMQX_KAFKA__PRODUCER__RULE__1__MQTT_TOPIC="sensor/+/up" \
  -e EMQX_KAFKA__PRODUCER__RULE__1__KAFKA_TOPIC="kafka_sensor_up" \
  -e EMQX_KAFKA__CONSUMER__ENABLED=true \
  -e EMQX_KAFKA__CONSUMER__TOPICS=mqtt_downlink \
  emqx-kafka:4.4.19
```

## EMQX 环境变量映射规则

Docker 镜像会把 `EMQX_` 前缀的环境变量映射到配置项：

- 去掉前缀 `EMQX_`
- 大写转小写
- 双下划线 `__` 转成点 `.`

示例：

```text
EMQX_LISTENER__SSL__EXTERNAL__ACCEPTORS -> listener.ssl.external.acceptors
EMQX_MQTT__MAX_PACKET_SIZE              -> mqtt.max_packet_size
EMQX_KAFKA__HOSTS                       -> kafka.hosts
```

可以通过 `CUTTLEFISH_ENV_OVERRIDE_PREFIX` 修改前缀。例如：

```bash
docker run -d --name emqx \
  -e CUTTLEFISH_ENV_OVERRIDE_PREFIX=DEV_ \
  -e DEV_MQTT__MAX_PACKET_SIZE=1MB \
  emqx-kafka:4.4.19
```

以下变量不按配置项映射，它们用于节点名：

```text
EMQX_NAME
EMQX_HOST
```

如果设置了 `EMQX_NAME` 和 `EMQX_HOST`，且没有设置 `EMQX_NODE_NAME`，容器会使用：

```text
EMQX_NODE_NAME=$EMQX_NAME@$EMQX_HOST
```

## 加载插件

默认常用插件：

```text
emqx_recon
emqx_retainer
emqx_management
emqx_dashboard
```

当前 Kafka 镜像运行时需要额外加载：

```text
emqx_plugin_kafka
```

推荐：

```bash
-e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka"
```

也可以容器启动后手动加载：

```bash
docker exec -it emqx-kafka emqx_ctl plugins load emqx_plugin_kafka
```

## 暴露端口

常用端口：

| 端口 | 用途 |
| --- | --- |
| 1883 | MQTT TCP |
| 8081 | Management API |
| 8083 | WebSocket |
| 8084 | WSS/HTTPS |
| 8883 | MQTT SSL |
| 18083 | Dashboard |
| 4369 | epmd |
| 4370 | Erlang distribution |
| 5369 | gen_rpc |

最小本地测试通常只需要：

```bash
-p 1883:1883 -p 18083:18083
```

## 集群示例

创建 `docker-compose.yaml`：

```yaml
version: '3'

services:
  emqx1:
    image: emqx-kafka:4.4.19
    environment:
      - EMQX_NAME=emqx
      - EMQX_HOST=node1.emqx.io
      - EMQX_CLUSTER__DISCOVERY=static
      - EMQX_CLUSTER__STATIC__SEEDS=emqx@node1.emqx.io,emqx@node2.emqx.io
      - EMQX_LOADED_PLUGINS=emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka
    networks:
      emqx-bridge:
        aliases:
          - node1.emqx.io

  emqx2:
    image: emqx-kafka:4.4.19
    environment:
      - EMQX_NAME=emqx
      - EMQX_HOST=node2.emqx.io
      - EMQX_CLUSTER__DISCOVERY=static
      - EMQX_CLUSTER__STATIC__SEEDS=emqx@node1.emqx.io,emqx@node2.emqx.io
      - EMQX_LOADED_PLUGINS=emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka
    networks:
      emqx-bridge:
        aliases:
          - node2.emqx.io

networks:
  emqx-bridge:
    driver: bridge
```

启动：

```bash
docker compose -p my_emqx up -d
```

查看集群状态：

```bash
docker exec -it my_emqx-emqx1-1 sh -c "emqx_ctl cluster status"
```

## 持久化

需要持久化时，建议保留：

```text
/opt/emqx/data
/opt/emqx/etc
/opt/emqx/log
```

注意：部分数据会写在 `/opt/emqx/data/mnesia/${node_name}` 下。复用数据卷时，需要保持节点名一致，通常要固定：

```text
EMQX_NAME
EMQX_HOST
```

docker compose 示例：

```yaml
volumes:
  vol-emqx-data:
  vol-emqx-etc:
  vol-emqx-log:

services:
  emqx:
    image: emqx-kafka:4.4.19
    restart: always
    environment:
      EMQX_NAME: emqx
      EMQX_HOST: 127.0.0.1
      EMQX_LOADED_PLUGINS: emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka
    volumes:
      - vol-emqx-data:/opt/emqx/data
      - vol-emqx-etc:/opt/emqx/etc
      - vol-emqx-log:/opt/emqx/log
```

## 内核参数

Linux 宿主机上应优先在宿主机调优。也可以给 Docker 容器传入 `--sysctl`：

```bash
docker run -d --name emqx-kafka \
  -p 1883:1883 \
  -p 18083:18083 \
  --sysctl fs.file-max=2097152 \
  --sysctl fs.nr_open=2097152 \
  --sysctl net.core.somaxconn=32768 \
  --sysctl net.ipv4.tcp_max_syn_backlog=16384 \
  --sysctl net.core.netdev_max_backlog=16384 \
  --sysctl net.ipv4.ip_local_port_range="1000 65535" \
  --sysctl net.ipv4.tcp_fin_timeout=15 \
  -e EMQX_LOADED_PLUGINS="emqx_recon,emqx_retainer,emqx_management,emqx_dashboard,emqx_plugin_kafka" \
  emqx-kafka:4.4.19
```

不要用特权容器或挂载宿主机 `/proc` 的方式调内核参数。

## 常见问题

### 镜像里没有 Kafka 插件

确认使用的是当前分支的 Dockerfile：

```bash
docker build -t emqx-kafka:4.4.19 -f deploy/docker/Dockerfile .
```

并检查：

```bash
docker run --rm --entrypoint sh emqx-kafka:4.4.19 -lc \
  'ls /opt/emqx/lib | grep emqx_plugin_kafka && \
   ls /opt/emqx/etc/plugins/emqx_plugin_kafka.conf'
```

### 插件没有启动

确认 `EMQX_LOADED_PLUGINS` 包含 `emqx_plugin_kafka`，或手动加载：

```bash
docker exec -it emqx-kafka emqx_ctl plugins load emqx_plugin_kafka
```

### Kafka 连接失败

检查：

- `EMQX_KAFKA__HOSTS` 是否配置正确。
- Kafka `advertised.listeners` 是否能被 EMQX 容器访问。
- Kafka topic 是否存在，或 Kafka 是否允许自动创建 topic。
- 如果 Kafka 在宿主机上，是否添加了 `--add-host=host.docker.internal:host-gateway`。
