# EMQX 4.4 Topic 存储、路由与订阅同步源码阅读笔记

> 版本上下文：当前仓库源码，EMQX 4.4 系列。本文只基于源码阅读整理，用于技术参考。

## 1. Topic 存储与数据结构

EMQX 中“topic 存储”需要区分三类数据：

1. 本地订阅关系：当前节点上哪些 subscriber process 订阅了哪些 topic filter。
2. 集群路由表：某个 topic filter 应该路由到哪些节点或共享订阅组。
3. 通配 topic 索引：用于发布普通 topic name 时快速找出匹配的通配 topic filter。

### 1.1 本地订阅 ETS 表

源码：`src/emqx_broker.erl`

`emqx_broker:create_tabs/0` 创建三个本地 ETS 表：

```erlang
-define(SUBOPTION, emqx_suboption).
-define(SUBSCRIBER, emqx_subscriber).
-define(SUBSCRIPTION, emqx_subscription).
```

数据结构：

| 表 | 类型 | key/value 形态 | 作用 |
| --- | --- | --- | --- |
| `emqx_suboption` | `set` | `{{SubPid, Topic}, SubOpts}` | 保存某个 subscriber 对某个 topic 的订阅选项。 |
| `emqx_subscription` | `duplicate_bag` | `{SubPid, Topic}` | 从 subscriber 反查它订阅了哪些 topic。 |
| `emqx_subscriber` | `bag` | `{Topic, SubPid}` 或 `{{shard, Topic, I}, SubPid}` | 从 topic 查本地 subscriber。 |

这些表是每个节点本地内存表，不是集群复制表。它们只表示当前节点还活着的 subscriber process。

普通订阅写入流程：

```text
emqx_session:subscribe/4
  -> emqx_broker:subscribe/3
    -> emqx_broker:do_subscribe/3
      -> ETS emqx_subscription
      -> ETS emqx_subscriber
      -> ETS emqx_suboption
      -> broker worker 注册集群 route
```

### 1.2 订阅分片

源码：`src/emqx_broker_helper.erl`

`emqx_broker_helper:get_sub_shard/2` 用 `emqx_sequence` 统计某个 topic 的订阅数量。数量超过 `?SHARD = 1024` 后，会把 subscriber 放进分片：

```erlang
get_sub_shard(SubPid, Topic) ->
    case create_seq(Topic) of
        Seq when Seq =< ?SHARD -> 0;
        _ -> erlang:phash2(SubPid, shards_num()) + 1
    end.
```

对应 `emqx_subscriber` 表记录从 `{Topic, SubPid}` 变为：

```erlang
{{shard, Topic, I}, SubPid}
```

同时会在 `emqx_subscriber` 中保留 `{Topic, {shard, I}}` 这种入口，使 broker dispatch 能先路由到 shard，再展开 shard 内的 subscriber。

### 1.3 集群路由表 `emqx_route`

源码：`src/emqx_router.erl`

`emqx_router:mnesia/1` 创建 Mnesia 表：

```erlang
-define(ROUTE_TAB, emqx_route).
```

表属性：

```erlang
{type, bag},
{ram_copies, [node()]},
{record_name, route},
{attributes, record_info(fields, route)}
```

记录来自 `#route{topic = Topic, dest = Dest}`。`Dest` 有两类：

```erlang
node()
{Group, node()}
```

含义：

| dest | 含义 |
| --- | --- |
| `node()` | 普通订阅：该 topic filter 在某个节点有本地 subscriber。 |
| `{Group, node()}` | 共享订阅：该共享组在某个节点有 subscriber。 |

`emqx_route` 是 Mnesia `ram_copies`，用于集群路由，不持久落盘。路由表会复制到集群节点，使发布节点可以本地查表决定转发目标。

### 1.4 通配 topic Trie 表 `emqx_trie`

源码：`src/emqx_trie.erl`

`emqx_trie:mnesia/1` 创建 Mnesia 表：

```erlang
-define(TRIE, emqx_trie).
```

表属性：

```erlang
{type, ordered_set},
{ram_copies, [node()]},
{record_name, emqx_trie},
{attributes, [key, count]}
```

record：

```erlang
-record(emqx_trie, {
    key,
    count = 0
}).
```

key 分两类：

```erlang
{Topic, 1}   %% 完整通配 topic filter
{Prefix, 0}  %% 前缀节点
```

例如未压缩时，订阅 `a/b/+/c/#` 会生成完整 topic key 和多个 prefix key：

```text
{<<"a/b/+/c/#">>, 1}
{<<"a/b/+/c">>, 0}
{<<"a/b/+">>, 0}
{<<"a/b">>, 0}
{<<"a">>, 0}
```

`count` 是引用计数，多个通配 topic 共享同一个 prefix 时不会重复创建节点；删除 topic 时递减，归零后删除。

### 1.5 Trie 压缩

配置：`broker.perf.trie_compaction = true`

源码：`emqx_trie:compact/1`、`do_compact/1`

压缩逻辑会把连续普通层级合并到遇到通配符为止。例如源码注释：

```text
a/b/c/+/d/# => [a/b/c/+, d/#]
a/+/+/b     => [a/+, +, b]
```

它降低大量唯一前缀通配订阅的写入成本，但可能让发布匹配含很多层级的 topic 时变慢。配置说明也明确这是集群级配置，切换需要停止所有节点。

## 2. Topic 路由与消息投递

### 2.1 发布入口

源码：`src/emqx_broker.erl`

典型入口：

```erlang
emqx_broker:publish(Msg)
emqx_broker:safe_publish(Msg)
```

核心流程：

```text
publish(Msg)
  -> emqx_hooks:run_fold('message.publish', ...)
  -> emqx_router:match_routes(Topic)
  -> aggre(Routes)
  -> route(Routes, Delivery)
```

`safe_publish/1` 只是包了一层 `try/catch`，异常时记录日志并返回空结果。

### 2.2 route 匹配

源码：`src/emqx_router.erl`

```erlang
match_routes(Topic) ->
    case match_trie(Topic) of
        [] -> lookup_routes(Topic);
        Matched ->
            lists:append([lookup_routes(To) || To <- [Topic | Matched]])
    end.
```

含义：

1. 先用 `emqx_trie:match/1` 找出能匹配普通 topic name 的通配 topic filter。
2. 如果没有通配匹配，直接查精确 topic 的 `emqx_route`。
3. 如果有通配匹配，同时查精确 topic 和所有匹配到的通配 topic filter。

### 2.3 精确 topic 与通配 topic 的差异

订阅注册时：

```erlang
do_add_route(Topic, Dest) ->
    Route = #route{topic = Topic, dest = Dest},
    case emqx_topic:wildcard(Topic) of
        true  -> insert_trie_route(Route);
        false -> insert_direct_route(Route)
    end.
```

精确 topic：

```text
只写 emqx_route
```

通配 topic：

```text
写 emqx_route
额外写 emqx_trie，用于 publish 时反查通配匹配
```

### 2.4 路由结果聚合

源码：`emqx_broker:aggre/1`

普通订阅 route 以 `{To, Node}` 聚合。共享订阅 route 以 `{To, Group}` 聚合，使用 `lists:usort/1` 去重，确保同一个共享组只走一次共享订阅 dispatch。

### 2.5 本地投递与跨节点转发

源码：`src/emqx_broker.erl`

```erlang
do_route({To, Node}, Delivery) when Node =:= node() ->
    {Node, To, dispatch(To, Delivery)};

do_route({To, Node}, Delivery) when is_atom(Node) ->
    {Node, To, forward(Node, To, Delivery, emqx:get_env(rpc_mode, async))};

do_route({To, Group}, Delivery) ->
    {share, To, emqx_shared_sub:dispatch(Group, To, Delivery)}.
```

普通订阅：

| route 目标 | 行为 |
| --- | --- |
| 当前节点 | `dispatch/2` 查本地 ETS 并向 subscriber pid 发 `{deliver, Topic, Msg}`。 |
| 远端节点 | `emqx_rpc:cast/call(Node, emqx_broker, dispatch, ...)` 转发到远端节点。 |

`rpc_mode` 默认异步时，远端转发使用 `emqx_rpc:cast/5`。

### 2.6 dispatch 到 subscriber

源码：`emqx_broker:dispatch/2`

```text
dispatch(Topic, Delivery)
  -> subscribers(Topic)
  -> 对每个 SubPid 发送 {deliver, Topic, Msg}
```

本地 subscriber 查询来自 `emqx_subscriber`：

```erlang
subscribers(Topic) ->
    lookup_value(emqx_subscriber, Topic, []).
```

如果查不到 subscriber，触发 `message.dropped` hook，原因是 `no_subscribers`。

### 2.7 共享订阅

源码：`src/emqx_shared_sub.erl`

共享订阅不走普通 `{Topic, SubPid}` 直接 fanout，而是：

```text
emqx_broker:do_subscribe(Group, Topic, SubPid, SubOpts)
  -> emqx_shared_sub:subscribe(Group, Topic, SubPid)
```

共享订阅 Mnesia 表：

```erlang
-define(TAB, emqx_shared_subscription).
-record(emqx_shared_subscription, {group, topic, subpid}).
```

`emqx_shared_sub:init/1` 还会创建两个本地 ETS cache：

| 表 | 类型 | 作用 |
| --- | --- | --- |
| `emqx_shared_subscriber` | `protected, bag` | 缓存 `{{Group, Topic}, SubPid}`，用于本地快速 pick subscriber。 |
| `emqx_alive_shared_subscribers` | `protected, set` | 跟踪远端 shared subscriber pid 是否仍被认为存活。 |

共享订阅写入时会：

1. `mnesia:dirty_write/2` 写 `emqx_shared_subscription`。
2. 如果本节点该 `{Group, Topic}` 之前没有成员，则 `emqx_router:do_add_route(Topic, {Group, node()})`。
3. 写本地 `emqx_shared_subscriber` cache。
4. monitor subscriber pid。

共享 subscriber down 时，`emqx_shared_sub:cleanup_down/1` 会删除 Mnesia 记录、本地 cache，并在该 `{Group, Topic}` 没有本地成员时删除 route。

dispatch 时按策略选择一个 subscriber：

```erlang
random | round_robin | sticky | local | hash | hash_clientid | hash_topic
```

远端 subscriber 发送依然通过 `emqx_rpc:cast(Topic, Node, erlang, send, [Pid, Msg])`。

配置 `broker.shared_dispatch_ack_enabled = true` 时，QoS 1/2 的共享订阅投递会等待被选 subscriber ack；如果该 subscriber 离线、队列满或超时，源码会尝试重新选择组内其它 subscriber。

### 2.8 并发模型

源码：

* `src/emqx_broker_sup.erl`
* `src/emqx_router_sup.erl`

broker pool：

```erlang
PoolSize = emqx_vm:schedulers() * 2,
emqx_pool_sup:spec([broker_pool, hash, PoolSize, {emqx_broker, start_link, []}])
```

router pool：

```erlang
emqx_pool_sup:spec([router_pool, hash, {emqx_router, start_link, []}])
```

`emqx_broker:pick/1` 和 `emqx_router:pick/1` 都按 topic hash 选择 worker，避免单个 gen_server 承担全部 topic 的订阅/路由更新。

通配 topic 更新可配置锁策略：

```text
broker.perf.route_lock_type = key | tab | global
```

源码：`emqx_router:maybe_trans/2`

| 策略 | 行为 | 适用说明 |
| --- | --- | --- |
| `key` | Mnesia transaction，按 key 锁 | 配置注释推荐单节点。 |
| `tab` | 写锁 `emqx_trie` 表 | 配置注释推荐多节点。 |
| `global` | `global` lock + `mnesia:sync_dirty` | 配置注释推荐大集群。 |

## 3. Broker 宕机、大量连接重置与订阅同步

这里要区分三种场景：

1. 单个连接进程退出。
2. 同 clientid 新连接 takeover 旧连接。
3. 整个 EMQX 节点或 broker 节点宕机。

### 3.1 单连接退出：本地订阅清理

源码：`src/emqx_broker_helper.erl`

每个 subscriber 第一次订阅时会注册到 broker helper：

```erlang
emqx_broker_helper:register_sub(SubPid, SubId)
```

helper 维护两个 ETS 表：

| 表 | 形态 | 作用 |
| --- | --- | --- |
| `emqx_subid` | `{SubId, SubPid}` | 通过 clientid/subid 找 subscriber pid。 |
| `emqx_submon` | `{SubPid, SubId}` | 通过 pid 反查 subid，并用于进程监控。 |

`emqx_broker_helper` 用 `emqx_pmon` 监控 subscriber pid。收到 `DOWN` 后：

```text
handle_info({'DOWN', ...})
  -> drain_down(100000)
  -> emqx_pool:async_submit(clean_down)
  -> emqx_broker:subscriber_down(SubPid)
```

`?BATCH_SIZE = 100000`，说明源码对大量连接同时退出做了批量 drain，避免逐条 DOWN 同步处理。

`emqx_broker:subscriber_down/1` 会：

1. 通过 `emqx_subscription` 找该 pid 的全部 topic。
2. 删除 `emqx_suboption`。
3. 删除 `emqx_subscriber` 中的 `{Topic, SubPid}` 或 shard 记录。
4. 如果本节点该 topic 已无 subscriber，cast 给 broker worker 删除集群 route。

### 3.2 同 clientid takeover：先摘旧订阅，再挂新订阅

源码：

* `src/emqx_cm.erl`
* `src/emqx_channel.erl`
* `src/emqx_session.erl`

非 clean start 新连接打开 session：

```text
emqx_cm:open_session(false, ClientInfo, ConnInfo)
  -> emqx_cm_locker:trans(ClientId, ResumeStart)
  -> takeover_session(ClientId)
  -> emqx_session:resume(ClientInfo, Session)
  -> request_stepdown({takeover, 'end'}, OldChanPid)
  -> register_channel(NewChanPid)
```

旧连接处理 `{takeover, 'begin'}` 时会返回旧 session。新连接拿到 session 后调用：

```erlang
emqx_session:resume(ClientInfo, Session)
```

`resume/2` 的核心：

```erlang
lists:foreach(fun({TopicFilter, SubOpts}) ->
    ok = emqx_broker:subscribe(TopicFilter, ClientId, SubOpts)
end, maps:to_list(Subs)).
```

也就是 session record 内保存的 `subscriptions` map 是恢复订阅的源数据。恢复时逐条调用 `emqx_broker:subscribe/3`，重新写本地 ETS 和集群 route。

旧连接结束 takeover 时：

```erlang
emqx_session:takeover(Session)
```

核心：

```erlang
takeover(#session{subscriptions = Subs}) ->
    lists:foreach(fun emqx_broker:unsubscribe/1, maps:keys(Subs)).
```

这会把旧连接对应的本地订阅从 broker 摘掉，避免旧 pid 和新 pid 同时收到消息。

`emqx_channel` 注释里强调 takeover 顺序很重要：旧 channel 在 takeover 期间会排队 pending delivers，最终将 pending 转交给新 channel，减少切换窗口内消息丢失或乱序风险。

### 3.3 Channel 管理与集群查找

源码：

* `src/emqx_cm.erl`
* `src/emqx_cm_registry.erl`

本地 channel 管理 ETS：

| 表 | 类型 | 作用 |
| --- | --- | --- |
| `emqx_channel` | `bag` | `{ClientId, ChanPid}`，本地 channel 索引。 |
| `emqx_channel_conn` | `bag` | `{{ClientId, ChanPid}, ConnMod}`，用于 takeover 时调用旧连接模块。 |
| `emqx_channel_info` | `set, compressed` | channel info/stats。 |
| `emqx_channel_live` | `set` | live connection 统计。 |

全局 channel registry：

```erlang
-define(TAB, emqx_channel_registry).
-record(channel, {chid, pid}).
```

它是 Mnesia `bag + ram_copies`。如果 `enable_session_registry = true`，`lookup_channels/1` 会查全局 registry，否则只查本地 ETS。

节点 down 时，`emqx_cm_registry` 监听 `membership` 事件并删除 pid 所属节点等于 down node 的 registry 记录。

### 3.4 节点宕机：路由清理

源码：`src/emqx_router_helper.erl`

router helper 维护：

```erlang
-define(ROUTING_NODE, emqx_routing_node).
-record(routing_node, {name, const = unused}).
```

当 route 指向一个非当前集群成员节点时，`emqx_router_helper:monitor/1` 会记录并 monitor 该节点。

节点 down 或 Mnesia membership down：

```text
handle_info({nodedown, Node})
handle_info({membership, {mnesia, down, Node}})
  -> global:trans(...)
  -> mnesia:transaction(cleanup_routes(Node))
  -> mnesia:dirty_delete(emqx_routing_node, Node)
```

`cleanup_routes/1` 删除 `emqx_route` 中：

```erlang
#route{dest = Node}
#route{dest = {'_', Node}}
```

也就是普通订阅 route 和共享订阅 route 都会清理。

### 3.5 “数十万连接重置时如何保证订阅同步”

源码机制可以拆成两个方向。

#### 3.5.1 连接还在同一节点内正常退出或被重置

保证方式：

1. 每个 subscriber pid 被 `emqx_broker_helper` 监控。
2. `DOWN` 事件批量 drain，`?BATCH_SIZE = 100000`。
3. 清理异步提交到 pool，避免 helper 长时间阻塞。
4. 清理时先删本地 ETS，再在本节点 topic 无 subscriber 时删除集群 route。

这保证了断开的连接不会长期保留脏订阅路由。短窗口内可能存在 route 指向已死 pid，但 dispatch 会检查 `is_process_alive/1`，不可投递时计数为 0 并触发 drop。

#### 3.5.2 客户端重连并恢复 session

保证方式：

1. `emqx_cm_locker:trans(ClientId, ...)` 对同一 clientid 的 open/takeover 做串行化。
2. session record 内 `subscriptions` map 保存该 client 的订阅源数据。
3. resume 时逐条 `emqx_broker:subscribe/3` 重新注册订阅。
4. takeover 时旧 session 先被拿到，新 session 恢复订阅，旧连接再 stepdown 并 unsubscribe。

这保证同 clientid 的重连不会并发创建多个有效 session，也保证订阅从 session map 重新同步到 broker/router。

#### 3.5.3 整个节点宕机

源码边界要明确：

* 本地连接进程、channel ETS、本地订阅 ETS 都在宕机节点内存中，节点宕机后不可恢复。
* `emqx_tables:new/2` 创建的是 named ETS，没有 heir 迁移逻辑；它解决的是表已存在时不重复创建，不是跨进程或跨节点持久化。
* `emqx_route` / `emqx_trie` / `emqx_channel_registry` 是 Mnesia `ram_copies`，用于集群复制和运行时路由，不是持久会话存储。
* 其它节点会通过 membership/nodedown 清理指向宕机节点的 route 和 channel registry。
* 客户端必须重新连接到存活节点；新连接恢复订阅的前提是旧 session 数据仍可获得。

在当前 CE 4.4 代码路径里，session record 主要活在 channel 进程内；如果承载该 channel 的节点直接宕机，该内存 session 也随节点丢失。也就是说，源码能保证集群路由表清理和客户端重连后的重新注册，但不能从已经宕机节点的内存中恢复那些 session 订阅。要跨节点宕机保留持久 session，需要额外的持久会话/外部存储能力或架构层保证客户端重新订阅。

### 3.6 大量连接场景下的关键保护点

| 机制 | 源码 | 目的 |
| --- | --- | --- |
| `drain_down(100000)` | `emqx_broker_helper`, `emqx_cm` | 批量处理大量进程 DOWN。 |
| `emqx_pool:async_submit` | `emqx_broker_helper`, `emqx_cm` | 清理工作异步化，降低单 gen_server 堵塞。 |
| broker/router hash pool | `emqx_broker_sup`, `emqx_router_sup` | 分散 topic 订阅和路由更新压力。 |
| subscription shard | `emqx_broker_helper:get_sub_shard/2` | 单 topic 超大量订阅时降低单 key fanout 压力。 |
| route lock strategy | `emqx_router:maybe_trans/2` | 控制通配 route/trie 更新并发一致性。 |
| router helper cleanup | `emqx_router_helper` | 节点 down 后清理脏 route。 |
| channel registry cleanup | `emqx_cm_registry` | 节点 down 后清理脏 channel 索引。 |

## 4. 关键结论

1. EMQX 的本地订阅数据是 ETS，集群路由数据是 Mnesia `ram_copies`，通配 topic 另有 Mnesia Trie 索引。
2. 发布时不是广播全集群，而是本地查 `emqx_route`，只转发到存在匹配订阅的节点或共享订阅组。
3. 精确 topic 只查 route 表；通配订阅通过 `emqx_trie` 把普通发布 topic 反查到匹配的通配 topic filter，再查 route 表。
4. 单连接异常退出时，订阅清理由 pid monitor 驱动；大量退出通过 `drain_down(100000)` 批量处理。
5. 同 clientid takeover/reconnect 时，session 内的 `subscriptions` map 是恢复订阅的源数据，resume 会逐条重新 `emqx_broker:subscribe/3`。
6. 整个节点宕机时，集群能清理该节点 route/channel registry，但该节点内存中的 session 不会凭空恢复；客户端重连后是否自动恢复订阅取决于 session 数据是否还存在或客户端是否重新订阅。

## 5. 源码索引

| 课题 | 文件 | 关键函数 |
| --- | --- | --- |
| 本地订阅 ETS | `src/emqx_broker.erl` | `create_tabs/0`, `subscribe/3`, `do_subscribe/3`, `subscriber_down/1` |
| 集群 route | `src/emqx_router.erl` | `mnesia/1`, `do_add_route/2`, `match_routes/1`, `do_delete_route/2` |
| 通配 Trie | `src/emqx_trie.erl` | `mnesia/1`, `insert/1`, `delete/1`, `match/1`, `make_keys/1` |
| 消息投递 | `src/emqx_broker.erl` | `publish/1`, `route/2`, `do_route/2`, `dispatch/2`, `forward/4` |
| 订阅清理 | `src/emqx_broker_helper.erl` | `register_sub/2`, `handle_info({'DOWN', ...})`, `clean_down/1` |
| 路由节点清理 | `src/emqx_router_helper.erl` | `monitor/1`, `handle_info({nodedown, Node})`, `cleanup_routes/1` |
| session 订阅 | `src/emqx_session.erl` | `subscribe/4`, `unsubscribe/4`, `takeover/1`, `resume/2` |
| channel/session 管理 | `src/emqx_cm.erl` | `open_session/3`, `takeover_session/1`, `register_channel/3`, `clean_down/1` |
| 全局 channel registry | `src/emqx_cm_registry.erl` | `register_channel/1`, `lookup_channels/1`, `cleanup_channels/1` |
| 共享订阅 | `src/emqx_shared_sub.erl` | `subscribe/3`, `dispatch/3`, `pick/6`, `send/3` |
