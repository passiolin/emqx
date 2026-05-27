# EMQX Kafka 插件实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 EMQX `4.4.19` 中新增普通插件 `emqx_plugin_kafka`，支持 MQTT 到 Kafka 的多规则 fan-out 转发，以及 Kafka 固定 JSON 消息消费后发布回 EMQX。

**Architecture:** 插件放在 `lib-extra/emqx_plugin_kafka`，作为独立 OTP application 编译并通过 `EMQX_EXTRA_PLUGINS` 集成。Producer 方向注册 `message.publish` hook 并使用 `brod:produce_cb/6`；consumer 方向使用 `brod_group_subscriber_v2` 消费 Kafka topic，校验 JSON 后调用 `emqx_broker:safe_publish/1`。

**Tech Stack:** Erlang/OTP 24、EMQX 4.4 plugin API、rebar3、`brod`、`emqx_json`、`emqx_topic:match/2`、EUnit/Common Test。

---

## 文件结构

- Create `lib-extra/emqx_plugin_kafka/rebar.config`：插件 rebar 配置，声明 `brod` 依赖和测试配置。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.app.src`：OTP application 描述。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_app.erl`：application callback，带 `-emqx_plugin(?MODULE)`。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_sup.erl`：顶层 supervisor，启动 Kafka runtime worker 和 consumer supervisor。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_consumer_sup.erl`：`brod_group_subscriber_v2` worker supervisor。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl`：插件 load/unload，注册和注销 `message.publish` hook。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl`：读取、规范化配置并提供默认值。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl`：producer JSON 编码、consumer JSON 解码、`#message{}` 构造。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl`：topic 规则匹配和 producer 发送逻辑。
- Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_runtime.erl`：启动/停止 `brod` client、producer 和 consumer subscriber。
- Create `lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.config`：示例配置。
- Create `lib-extra/plugins` entry or modify existing `lib-extra/plugins`：声明 extra plugin 依赖。
- Test files under `lib-extra/emqx_plugin_kafka/test/`：覆盖配置、payload、producer 规则和 consumer 解析。

## Task 1: 插件骨架

**Files:**
- Create: `lib-extra/emqx_plugin_kafka/rebar.config`
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.app.src`
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_app.erl`
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_sup.erl`
- Create: `lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.config`

- [ ] **Step 1: 创建插件目录**

Run:

```bash
mkdir -p lib-extra/emqx_plugin_kafka/src lib-extra/emqx_plugin_kafka/etc lib-extra/emqx_plugin_kafka/test
```

Expected: directories exist.

- [ ] **Step 2: 写 `rebar.config`**

Create `lib-extra/emqx_plugin_kafka/rebar.config`:

```erlang
{erl_opts, [
    warn_unused_vars,
    warn_shadow_vars,
    warn_unused_import,
    warn_obsolete_guard,
    debug_info
]}.

{deps, [
    {brod, {git, "https://github.com/kafka4beam/brod.git", {tag, "3.16.3"}}}
]}.

{cover_enabled, true}.
{cover_opts, [verbose]}.
{cover_export_enabled, true}.

{shell, [
    {apps, [emqx, emqx_plugin_kafka]}
]}.
```

- [ ] **Step 3: 写 application 描述**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.app.src`:

```erlang
{application, emqx_plugin_kafka,
 [{description, "EMQX Kafka plugin"},
  {vsn, "0.1.0"},
  {registered, [emqx_plugin_kafka_sup]},
  {mod, {emqx_plugin_kafka_app, []}},
  {applications, [kernel, stdlib, emqx, brod]},
  {env, []},
  {modules, []},
  {licenses, ["Apache-2.0"]},
  {links, []}
 ]}.
```

- [ ] **Step 4: 写 application callback**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_app.erl`:

```erlang
-module(emqx_plugin_kafka_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_plugin_kafka_sup:start_link(),
    emqx_plugin_kafka:load([]),
    {ok, Sup}.

stop(_State) ->
    emqx_plugin_kafka:unload(),
    ok.
```

- [ ] **Step 5: 写空 supervisor**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_sup.erl`:

```erlang
-module(emqx_plugin_kafka_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    {ok, {SupFlags, []}}.
```

- [ ] **Step 6: 写示例配置**

Create `lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.config`:

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
      {enabled, false},
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

- [ ] **Step 7: 运行编译验证骨架失败点**

Run:

```bash
./rebar3 as test compile
```

Expected: compilation fails because `emqx_plugin_kafka` module is not created yet, or succeeds if module is already added by a later task. Do not proceed with runtime loading until Task 2 creates `emqx_plugin_kafka.erl`.

- [ ] **Step 8: Commit**

```bash
git add lib-extra/emqx_plugin_kafka
git commit -m "feat: add kafka plugin skeleton"
```

## Task 2: 配置规范化

**Files:**
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl`
- Create: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl`

- [ ] **Step 1: 写失败测试**

Create `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl`:

```erlang
-module(emqx_plugin_kafka_config_tests).

-include_lib("eunit/include/eunit.hrl").

defaults_test() ->
    application:unset_env(emqx_plugin_kafka, producer),
    application:unset_env(emqx_plugin_kafka, consumer),
    application:unset_env(emqx_plugin_kafka, kafka_hosts),
    Conf = emqx_plugin_kafka_config:get(),
    ?assertEqual([{"127.0.0.1", 9092}], maps:get(kafka_hosts, Conf)),
    ?assertEqual(emqx_plugin_kafka_client, maps:get(client_id, Conf)),
    ?assertMatch(#{enabled := true, publish_base64 := false, rules := []}, maps:get(producer, Conf)),
    ?assertMatch(#{enabled := false, group_id := <<"emqx_plugin_kafka">>, topics := []}, maps:get(consumer, Conf)).

normalizes_binary_rules_test() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {publish_base64, true},
        {rules, [
            {"a/+/c", "kafka_a"},
            {<<"b/#">>, <<"kafka_b">>}
        ]}
    ]),
    Conf = emqx_plugin_kafka_config:get(),
    Producer = maps:get(producer, Conf),
    ?assertEqual(true, maps:get(publish_base64, Producer)),
    ?assertEqual([{<<"a/+/c">>, <<"kafka_a">>}, {<<"b/#">>, <<"kafka_b">>}],
                 maps:get(rules, Producer)).
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_config_tests
```

Expected: FAIL with `emqx_plugin_kafka_config` undefined.

- [ ] **Step 3: 实现配置模块**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl`:

```erlang
-module(emqx_plugin_kafka_config).

-export([get/0]).

-define(APP, emqx_plugin_kafka).

get() ->
    #{
        kafka_hosts => application:get_env(?APP, kafka_hosts, [{"127.0.0.1", 9092}]),
        client_id => application:get_env(?APP, client_id, emqx_plugin_kafka_client),
        brod_client_config => application:get_env(?APP, brod_client_config, []),
        producer_config => application:get_env(?APP, producer_config, []),
        consumer_config => application:get_env(?APP, consumer_config, []),
        producer => producer(application:get_env(?APP, producer, [])),
        consumer => consumer(application:get_env(?APP, consumer, []))
    }.

producer(Opts) ->
    #{
        enabled => proplists:get_value(enabled, Opts, true),
        publish_base64 => proplists:get_value(publish_base64, Opts, false),
        rules => normalize_rules(proplists:get_value(rules, Opts, []))
    }.

consumer(Opts) ->
    #{
        enabled => proplists:get_value(enabled, Opts, false),
        group_id => to_bin(proplists:get_value(group_id, Opts, <<"emqx_plugin_kafka">>)),
        topics => [to_bin(T) || T <- proplists:get_value(topics, Opts, [])],
        begin_offset => proplists:get_value(begin_offset, Opts, earliest)
    }.

normalize_rules(Rules) ->
    [{to_bin(Filter), to_bin(KafkaTopic)} || {Filter, KafkaTopic} <- Rules].

to_bin(V) when is_binary(V) ->
    V;
to_bin(V) when is_atom(V) ->
    atom_to_binary(V, utf8);
to_bin(V) when is_list(V) ->
    unicode:characters_to_binary(V).
```

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_config_tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl
git commit -m "feat: normalize kafka plugin config"
```

## Task 3: Payload 编码和解码

**Files:**
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl`
- Create: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl`

- [ ] **Step 1: 写失败测试**

Create `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl`:

```erlang
-module(emqx_plugin_kafka_payload_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

encode_publish_plain_test() ->
    Msg = #message{
        id = emqx_guid:gen(),
        qos = 1,
        from = <<"client-a">>,
        flags = #{},
        headers = #{username => <<"user-a">>},
        topic = <<"sensor/a/up">>,
        payload = <<"hello">>,
        timestamp = 1710000000000
    },
    {Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, false),
    ?assertEqual(<<"client-a">>, Key),
    {ok, Decoded} = emqx_json:safe_decode(Json, [return_maps]),
    ?assertEqual(<<"message_publish">>, maps:get(<<"action">>, Decoded)),
    ?assertEqual(<<"sensor/a/up">>, maps:get(<<"topic">>, Decoded)),
    ?assertEqual(<<"hello">>, maps:get(<<"payload">>, Decoded)).

encode_publish_base64_test() ->
    Msg = #message{
        id = emqx_guid:gen(),
        qos = 0,
        from = <<"client-a">>,
        flags = #{},
        headers = #{},
        topic = <<"bin">>,
        payload = <<0, 1, 2>>,
        timestamp = 1
    },
    {_Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, true),
    {ok, Decoded} = emqx_json:safe_decode(Json, [return_maps]),
    ?assertEqual(base64:encode(<<0, 1, 2>>), maps:get(<<"payload">>, Decoded)).

decode_consumer_message_test() ->
    Json = <<"{\"topic\":\"down/a\",\"qos\":1,\"payload\":\"hello\"}">>,
    {ok, Msg} = emqx_plugin_kafka_payload:decode_consumer(Json),
    ?assertEqual(<<"down/a">>, Msg#message.topic),
    ?assertEqual(1, Msg#message.qos),
    ?assertEqual(<<"hello">>, Msg#message.payload),
    ?assertEqual(<<"emqx_plugin_kafka">>, Msg#message.from).

decode_consumer_rejects_wildcard_topic_test() ->
    Json = <<"{\"topic\":\"down/+\",\"qos\":1,\"payload\":\"hello\"}">>,
    ?assertMatch({error, _}, emqx_plugin_kafka_payload:decode_consumer(Json)).

decode_consumer_defaults_qos_test() ->
    Json = <<"{\"topic\":\"down/a\",\"payload\":\"hello\"}">>,
    {ok, Msg} = emqx_plugin_kafka_payload:decode_consumer(Json),
    ?assertEqual(0, Msg#message.qos).
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_payload_tests
```

Expected: FAIL with `emqx_plugin_kafka_payload` undefined.

- [ ] **Step 3: 实现 payload 模块**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl`:

```erlang
-module(emqx_plugin_kafka_payload).

-include_lib("emqx/include/emqx.hrl").

-export([encode_publish/2, decode_consumer/1]).

encode_publish(Msg = #message{}, PublishBase64) ->
    Key = key(Msg),
    Payload = payload(Msg#message.payload, PublishBase64),
    Username = maps:get(username, Msg#message.headers, undefined),
    Data0 = #{
        action => <<"message_publish">>,
        clientid => Msg#message.from,
        topic => Msg#message.topic,
        qos => Msg#message.qos,
        payload => Payload,
        node => atom_to_binary(node(), utf8),
        timestamp => Msg#message.timestamp
    },
    Data = case Username of
        undefined -> Data0;
        _ -> Data0#{username => Username}
    end,
    {ok, Json} = emqx_json:safe_encode(Data),
    {Key, iolist_to_binary(Json)}.

decode_consumer(Json) ->
    case emqx_json:safe_decode(Json, [return_maps]) of
        {ok, Map} when is_map(Map) ->
            decode_consumer_map(Map);
        {ok, _} ->
            {error, invalid_json_object};
        {error, Reason} ->
            {error, Reason}
    end.

decode_consumer_map(Map) ->
    with_topic(Map, fun(Topic) ->
        with_qos(Map, fun(QoS) ->
            with_payload(Map, fun(Payload) ->
                {ok, #message{
                    id = emqx_guid:gen(),
                    qos = QoS,
                    from = <<"emqx_plugin_kafka">>,
                    flags = #{dup => false, retain => false},
                    headers = #{},
                    topic = Topic,
                    payload = Payload,
                    timestamp = erlang:system_time(millisecond)
                }}
            end)
        end)
    end).

with_topic(Map, Fun) ->
    case maps:get(<<"topic">>, Map, undefined) of
        Topic0 when is_binary(Topic0); is_list(Topic0) ->
            Topic = to_bin(Topic0),
            case valid_topic(Topic) of
                true -> Fun(Topic);
                false -> {error, invalid_topic}
            end;
        _ ->
            {error, missing_topic}
    end.

with_qos(Map, Fun) ->
    case maps:get(<<"qos">>, Map, 0) of
        QoS when QoS =:= 0; QoS =:= 1; QoS =:= 2 ->
            Fun(QoS);
        _ ->
            {error, invalid_qos}
    end.

with_payload(Map, Fun) ->
    case maps:get(<<"payload">>, Map, undefined) of
        Payload when is_binary(Payload) ->
            Fun(Payload);
        Payload when is_list(Payload) ->
            Fun(to_bin(Payload));
        _ ->
            {error, invalid_payload}
    end.

key(#message{from = From, topic = Topic}) when is_binary(From) ->
    case From of
        <<>> -> Topic;
        _ -> From
    end;
key(#message{from = From}) when is_atom(From) ->
    atom_to_binary(From, utf8);
key(#message{topic = Topic}) ->
    Topic.

payload(Payload, true) ->
    base64:encode(Payload);
payload(Payload, false) ->
    Payload.

valid_topic(<<>>) ->
    false;
valid_topic(Topic) ->
    binary:match(Topic, [<<"+">>, <<"#">>]) =:= nomatch.

to_bin(V) when is_binary(V) ->
    V;
to_bin(V) when is_list(V) ->
    unicode:characters_to_binary(V).
```

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_payload_tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl
git commit -m "feat: encode kafka plugin payloads"
```

## Task 4: Producer 规则和 Hook

**Files:**
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl`
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl`
- Create: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl`

- [ ] **Step 1: 写 producer 失败测试**

Create `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl`:

```erlang
-module(emqx_plugin_kafka_producer_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

matching_topics_fanout_test() ->
    Rules = [
        {<<"sensor/+/up">>, <<"kafka_sensor">>},
        {<<"sensor/a/#">>, <<"kafka_sensor_a">>},
        {<<"alarm/#">>, <<"kafka_alarm">>}
    ],
    ?assertEqual([<<"kafka_sensor">>, <<"kafka_sensor_a">>],
                 emqx_plugin_kafka_producer:matching_kafka_topics(<<"sensor/a/up">>, Rules)).

sys_topic_skipped_test() ->
    Msg = #message{topic = <<"$SYS/brokers">>, payload = <<"x">>, qos = 0, headers = #{}, from = <<"c">>, timestamp = 1},
    Conf = #{producer => #{enabled => true, publish_base64 => false, rules => [{<<"$SYS/#">>, <<"sys">>}]}},
    ?assertEqual(skip, emqx_plugin_kafka_producer:publish_plan(Msg, Conf)).

publish_plan_contains_payloads_test() ->
    Msg = #message{topic = <<"sensor/a/up">>, payload = <<"x">>, qos = 0, headers = #{}, from = <<"c">>, timestamp = 1},
    Conf = #{producer => #{enabled => true, publish_base64 => false, rules => [{<<"sensor/#">>, <<"kafka_sensor">>}]}},
    {ok, [{<<"kafka_sensor">>, <<"c">>, Json}]} = emqx_plugin_kafka_producer:publish_plan(Msg, Conf),
    {ok, Decoded} = emqx_json:safe_decode(Json, [return_maps]),
    ?assertEqual(<<"sensor/a/up">>, maps:get(<<"topic">>, Decoded)).
```

- [ ] **Step 2: 运行 producer 测试确认失败**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_producer_tests
```

Expected: FAIL with `emqx_plugin_kafka_producer` undefined.

- [ ] **Step 3: 实现 producer 模块**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl`:

```erlang
-module(emqx_plugin_kafka_producer).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([on_message_publish/2, publish_plan/2, matching_kafka_topics/2]).

on_message_publish(Msg, _Env) ->
    Conf = emqx_plugin_kafka_config:get(),
    case publish_plan(Msg, Conf) of
        {ok, Plans} ->
            ClientId = maps:get(client_id, Conf),
            lists:foreach(fun({KafkaTopic, Key, Json}) ->
                brod:produce_cb(ClientId, KafkaTopic, hash, Key, Json,
                    fun(_Partition, Result) ->
                        case Result of
                            ok -> ok;
                            _ -> ?LOG(warning, "Kafka produce failed topic=~p reason=~p", [KafkaTopic, Result])
                        end
                    end)
            end, Plans),
            ok;
        skip ->
            ok
    end.

publish_plan(#message{topic = <<"$SYS/", _/binary>>}, _Conf) ->
    skip;
publish_plan(_Msg, #{producer := #{enabled := false}}) ->
    skip;
publish_plan(Msg = #message{topic = Topic}, #{producer := Producer}) ->
    Rules = maps:get(rules, Producer, []),
    PublishBase64 = maps:get(publish_base64, Producer, false),
    {Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, PublishBase64),
    Plans = [{KafkaTopic, Key, Json} || KafkaTopic <- matching_kafka_topics(Topic, Rules)],
    {ok, Plans}.

matching_kafka_topics(Topic, Rules) ->
    [KafkaTopic || {TopicFilter, KafkaTopic} <- Rules, emqx_topic:match(Topic, TopicFilter)].
```

- [ ] **Step 4: 实现插件 hook 入口**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl`:

```erlang
-module(emqx_plugin_kafka).

-export([load/1, unload/0]).

load(_Env) ->
    emqx:hook('message.publish', {emqx_plugin_kafka_producer, on_message_publish, []}).

unload() ->
    emqx:unhook('message.publish', {emqx_plugin_kafka_producer, on_message_publish}).
```

- [ ] **Step 5: 运行 producer 测试确认通过**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_producer_tests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl
git commit -m "feat: add kafka producer hook"
```

## Task 5: Kafka Runtime 和 Consumer

**Files:**
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_runtime.erl`
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_consumer.erl`
- Create: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_consumer_sup.erl`
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_sup.erl`
- Create: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_consumer_tests.erl`

- [ ] **Step 1: 写 consumer 失败测试**

Create `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_consumer_tests.erl`:

```erlang
-module(emqx_plugin_kafka_consumer_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("brod/include/brod.hrl").

handle_message_commits_valid_json_test() ->
    KafkaMsg = #kafka_message{
        offset = 1,
        key = <<"k">>,
        value = <<"{\"topic\":\"down/a\",\"qos\":1,\"payload\":\"hello\"}">>,
        ts = 0,
        ts_type = create,
        headers = []
    },
    State = #{publish_fun => fun(Msg) ->
        ?assertEqual(<<"down/a">>, Msg#message.topic),
        ?assertEqual(<<"hello">>, Msg#message.payload),
        ok
    end},
    ?assertEqual({ok, commit, State}, emqx_plugin_kafka_consumer:handle_message(KafkaMsg, State)).

handle_message_commits_invalid_json_test() ->
    KafkaMsg = #kafka_message{
        offset = 1,
        key = <<"k">>,
        value = <<"bad-json">>,
        ts = 0,
        ts_type = create,
        headers = []
    },
    State = #{publish_fun => fun(_Msg) -> error(should_not_publish) end},
    ?assertEqual({ok, commit, State}, emqx_plugin_kafka_consumer:handle_message(KafkaMsg, State)).
```

- [ ] **Step 2: 运行 consumer 测试确认失败**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_consumer_tests
```

Expected: FAIL with `emqx_plugin_kafka_consumer` undefined.

- [ ] **Step 3: 实现 consumer subscriber callback**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_consumer.erl`:

```erlang
-module(emqx_plugin_kafka_consumer).

-include_lib("emqx/include/logger.hrl").
-include_lib("brod/include/brod.hrl").

-export([init/2, handle_message/2]).

init(GroupData, State) ->
    {ok, State#{kafka_topic => maps:get(topic, GroupData, undefined)}}.

handle_message(#kafka_message{value = Value}, State) ->
    PublishFun = maps:get(publish_fun, State, fun emqx_broker:safe_publish/1),
    case emqx_plugin_kafka_payload:decode_consumer(Value) of
        {ok, Msg} ->
            case PublishFun(Msg) of
                _ -> ok
            end;
        {error, Reason} ->
            ?LOG(warning, "Drop invalid kafka message reason=~p", [Reason]),
            ok
    end,
    {ok, commit, State}.
```

- [ ] **Step 4: 实现 consumer supervisor**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_consumer_sup.erl`:

```erlang
-module(emqx_plugin_kafka_consumer_sup).

-behaviour(brod_supervisor3).

-export([start_link/0, start_child/2, ensure_child_deleted/1]).
-export([init/1]).

start_link() ->
    brod_supervisor3:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Id, GroupSubscriberConfig) ->
    ChildSpec = {
        Id,
        {brod_group_subscriber_v2, start_link, [GroupSubscriberConfig]},
        permanent,
        10000,
        worker,
        [brod_group_subscriber_v2]
    },
    case brod_supervisor3:start_child(?MODULE, ChildSpec) of
        {ok, Pid} -> {ok, Pid};
        {ok, Pid, _Info} -> {ok, Pid};
        {error, already_present} -> brod_supervisor3:restart_child(?MODULE, Id);
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.

ensure_child_deleted(Id) ->
    case brod_supervisor3:terminate_child(?MODULE, Id) of
        ok ->
            ok = brod_supervisor3:delete_child(?MODULE, Id),
            ok;
        {error, not_found} ->
            ok
    end.

init([]) ->
    {ok, {{one_for_one, 0, 1}, []}}.
```

- [ ] **Step 5: 实现 runtime worker**

Create `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_runtime.erl`:

```erlang
-module(emqx_plugin_kafka_runtime).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Conf = emqx_plugin_kafka_config:get(),
    ClientId = maps:get(client_id, Conf),
    Hosts = maps:get(kafka_hosts, Conf),
    ClientConfig = maps:get(brod_client_config, Conf),
    ok = application:ensure_all_started(brod),
    case brod:start_client(Hosts, ClientId, ClientConfig) of
        ok ->
            start_producers(ClientId, Conf),
            start_consumer(ClientId, Conf),
            {ok, #{client_id => ClientId}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{client_id := ClientId}) ->
    catch brod:stop_client(ClientId),
    ok;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_producers(ClientId, #{producer := #{enabled := true, rules := Rules}, producer_config := ProducerConfig}) ->
    KafkaTopics = lists:usort([KafkaTopic || {_Filter, KafkaTopic} <- Rules]),
    lists:foreach(fun(KafkaTopic) ->
        case brod:start_producer(ClientId, KafkaTopic, ProducerConfig) of
            ok -> ok;
            {error, already_started} -> ok;
            {error, Reason} -> ?LOG(warning, "Kafka producer start failed topic=~p reason=~p", [KafkaTopic, Reason])
        end
    end, KafkaTopics);
start_producers(_ClientId, _Conf) ->
    ok.

start_consumer(ClientId, #{consumer := #{enabled := true, group_id := GroupId, topics := Topics, begin_offset := BeginOffset},
                           consumer_config := ConsumerConfig}) when Topics =/= [] ->
    SubscriberId = <<"emqx_plugin_kafka_consumer">>,
    OffsetResetPolicy = case BeginOffset of
        earliest -> reset_to_earliest;
        latest -> reset_to_latest;
        _ -> reset_to_earliest
    end,
    GroupSubscriberConfig = #{
        client => ClientId,
        group_id => GroupId,
        topics => Topics,
        cb_module => emqx_plugin_kafka_consumer,
        init_data => #{},
        message_type => message,
        consumer_config => [{begin_offset, BeginOffset},
                            {offset_reset_policy, OffsetResetPolicy} | ConsumerConfig],
        group_config => []
    },
    case emqx_plugin_kafka_consumer_sup:start_child(SubscriberId, GroupSubscriberConfig) of
        {ok, _Pid} -> ok;
        {error, Reason} -> ?LOG(warning, "Kafka consumer start failed reason=~p", [Reason])
    end;
start_consumer(_ClientId, _Conf) ->
    ok.
```

- [ ] **Step 6: 修改顶层 supervisor**

Replace `init/1` in `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_sup.erl` with:

```erlang
init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Children = [
        #{
            id => emqx_plugin_kafka_consumer_sup,
            start => {emqx_plugin_kafka_consumer_sup, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => supervisor,
            modules => [emqx_plugin_kafka_consumer_sup]
        },
        #{
            id => emqx_plugin_kafka_runtime,
            start => {emqx_plugin_kafka_runtime, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_plugin_kafka_runtime]
        }
    ],
    {ok, {SupFlags, Children}}.
```

- [ ] **Step 7: 运行 consumer 测试确认通过**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_consumer_tests
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_runtime.erl lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_consumer.erl lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_consumer_sup.erl lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_sup.erl lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_consumer_tests.erl
git commit -m "feat: add kafka consumer runtime"
```

## Task 6: 集成到 EMQX extra plugins 构建

**Files:**
- Modify: `lib-extra/plugins`
- Optional Modify: `data/loaded_plugins.tmpl`

- [ ] **Step 1: 查看 `lib-extra/plugins` 当前格式**

Run:

```bash
sed -n '1,120p' lib-extra/plugins
```

Expected: 文件存在。如果文件为空或不存在，按下一步创建完整内容。

- [ ] **Step 2: 添加本地插件依赖**

Ensure `lib-extra/plugins` contains:

```erlang
{erlang_plugins,
  [ {emqx_plugin_kafka, {path, "lib-extra/emqx_plugin_kafka"}}
  ]
}.
```

If the file already has `erlang_plugins`, append `{emqx_plugin_kafka, {path, "lib-extra/emqx_plugin_kafka"}}` to the existing list instead of replacing other plugins.

- [ ] **Step 3: 不默认启用插件**

Do not modify `data/loaded_plugins.tmpl` in the first implementation. The plugin should be loaded manually with:

```bash
_build/emqx/rel/emqx/bin/emqx_ctl plugins load emqx_plugin_kafka
```

- [ ] **Step 4: 获取依赖并编译插件目录**

Run:

```bash
./rebar3 get-deps
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka
```

Expected: PASS for plugin unit tests.

- [ ] **Step 5: 编译完整 release**

Run with the existing Docker build pattern from this workspace:

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
  bash -lc 'make'
```

Expected: release builds and contains `_build/emqx/rel/emqx/lib/emqx_plugin_kafka-0.1.0/`.

- [ ] **Step 6: Commit**

```bash
git add lib-extra/plugins
git commit -m "build: include kafka extra plugin"
```

## Task 7: 手动验收

**Files:**
- No code changes unless verification exposes defects.

- [ ] **Step 1: 准备 Kafka**

Run a local Kafka broker or point `lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.config` to an existing broker. Use topics:

```text
kafka_sensor_up
kafka_alarm
mqtt_downlink
```

Expected: Kafka broker reachable from EMQX runtime.

- [ ] **Step 2: 启动 EMQX**

Run:

```bash
_build/emqx/rel/emqx/bin/emqx console
```

Expected: EMQX starts.

- [ ] **Step 3: 加载插件**

In another shell:

```bash
_build/emqx/rel/emqx/bin/emqx_ctl plugins load emqx_plugin_kafka
```

Expected: command returns success and logs show Kafka client/producers started.

- [ ] **Step 4: 验证 MQTT 到 Kafka**

Publish:

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -i client-a -t sensor/a/up -m hello -q 1
```

Expected: Kafka topic `kafka_sensor_up` receives JSON containing:

```json
{"action":"message_publish","clientid":"client-a","topic":"sensor/a/up","qos":1,"payload":"hello"}
```

- [ ] **Step 5: 验证 Kafka 到 MQTT**

Subscribe:

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -t down/a -q 1
```

Produce Kafka message to configured `mqtt_downlink`:

```json
{"topic":"down/a","qos":1,"payload":"hello-from-kafka"}
```

Expected: MQTT subscriber receives `hello-from-kafka`.

- [ ] **Step 6: Final verification**

Run:

```bash
git status --short
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka
```

Expected: only intentional files changed; plugin unit tests pass.

## Self-Review

- Spec coverage: plugin skeleton, config, producer fan-out, consumer JSON, build integration, and manual verification are all covered by tasks.
- 占位符检查：所有任务都给出了明确文件、命令和代码片段。
- Type consistency: config keys use `kafka_hosts`, `client_id`, `producer`, `consumer`, `brod_client_config`, `producer_config`, and `consumer_config` consistently across tasks.
