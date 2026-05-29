# EMQX Kafka Connection Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为现有 `emqx_plugin_kafka` 插件增加 `client.connected` / `client.disconnected` 事件转发能力，并将两类事件写入同一个独立 Kafka topic。

**Architecture:** 保持现有 `message.publish` 规则式转发不变，只在插件侧增加独立的 connection-events 配置、payload 编码和 hook handler。三个 hook 共用现有 Kafka client 与 produce 逻辑，但连接事件不复用 `producer.rules`，而是直接写入配置的单一 topic。

**Tech Stack:** Erlang/OTP 24、EMQX 4.4 hook API、`brod`、`emqx_json`、EUnit、cuttlefish schema。

---

## File Structure

- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl`
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl`
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl`
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl`
- Modify: `lib-extra/emqx_plugin_kafka/priv/emqx_plugin_kafka.schema`
- Modify: `lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.conf`
- Modify: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl`
- Modify: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl`
- Modify: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl`
- Create: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_tests.erl`

### Task 1: Add Connection Events Config

**Files:**
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl`
- Modify: `lib-extra/emqx_plugin_kafka/priv/emqx_plugin_kafka.schema`
- Modify: `lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.conf`
- Test: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl`

- [ ] **Step 1: Write the failing config tests**

Add these tests to `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl`:

```erlang
config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun defaults/0,
         fun normalizes_binary_rules/0,
         fun normalizes_excluded_topics/0,
         fun connection_events_defaults/0,
         fun connection_events_normalizes_topic/0
     ]}.

env_keys() ->
    [
        producer,
        consumer,
        connection_events,
        kafka_hosts,
        client_id,
        brod_client_config,
        producer_config,
        consumer_config
    ].

connection_events_defaults() ->
    application:unset_env(emqx_plugin_kafka, connection_events),
    Conf = emqx_plugin_kafka_config:get(),
    ?assertMatch(
        #{enabled := false, topic := <<"mqtt_connection_events">>},
        maps:get(connection_events, Conf)
    ).

connection_events_normalizes_topic() ->
    application:set_env(emqx_plugin_kafka, connection_events, [
        {enabled, true},
        {topic, "mqtt_conn_events"}
    ]),
    Conf = emqx_plugin_kafka_config:get(),
    ?assertEqual(
        #{enabled => true, topic => <<"mqtt_conn_events">>},
        maps:get(connection_events, Conf)
    ).
```

- [ ] **Step 2: Run the config tests to verify they fail**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_config_tests
```

Expected: FAIL because `connection_events` is not returned by `emqx_plugin_kafka_config:get/0`.

- [ ] **Step 3: Write the minimal config implementation**

Update `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl`:

```erlang
get() ->
    #{
        kafka_hosts => application:get_env(?APP, kafka_hosts, [{"127.0.0.1", 9092}]),
        client_id => application:get_env(?APP, client_id, emqx_plugin_kafka_client),
        brod_client_config => application:get_env(?APP, brod_client_config, []),
        producer_config => application:get_env(?APP, producer_config, []),
        consumer_config => application:get_env(?APP, consumer_config, []),
        producer => producer(application:get_env(?APP, producer, [])),
        consumer => consumer(application:get_env(?APP, consumer, [])),
        connection_events => connection_events(application:get_env(?APP, connection_events, []))
    }.

connection_events(Opts) ->
    #{
        enabled => proplists:get_value(enabled, Opts, false),
        topic => to_bin(proplists:get_value(topic, Opts, <<"mqtt_connection_events">>))
    }.
```

Update `lib-extra/emqx_plugin_kafka/priv/emqx_plugin_kafka.schema`:

```erlang
{mapping, "kafka.connection_events.enabled", "emqx_plugin_kafka.connection_events", [
  {default, false},
  {datatype, {enum, [true, false]}}
]}.

{mapping, "kafka.connection_events.topic", "emqx_plugin_kafka.connection_events", [
  {default, "mqtt_connection_events"},
  {datatype, string}
]}.

{translation, "emqx_plugin_kafka.connection_events", fun(Conf) ->
  [{enabled, cuttlefish:conf_get("kafka.connection_events.enabled", Conf)},
   {topic, unicode:characters_to_binary(cuttlefish:conf_get("kafka.connection_events.topic", Conf))}]
end}.
```

Update `lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.conf`:

```ini
## Connection lifecycle events to Kafka switch.
kafka.connection_events.enabled = false

## Kafka topic for both connect and disconnect events.
kafka.connection_events.topic = mqtt_connection_events
```

- [ ] **Step 4: Run the config tests to verify they pass**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_config_tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_config.erl \
        lib-extra/emqx_plugin_kafka/priv/emqx_plugin_kafka.schema \
        lib-extra/emqx_plugin_kafka/etc/emqx_plugin_kafka.conf \
        lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl
git commit -m "feat: add kafka connection events config"
```

### Task 2: Encode Connection Event Payloads

**Files:**
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl`
- Test: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl`

- [ ] **Step 1: Write the failing payload tests**

Add these tests to `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl`:

```erlang
encode_connected_event_test() ->
    ClientInfo = #{
        clientid => <<"client-a">>,
        username => <<"user-a">>
    },
    ConnInfo = #{
        proto_name => <<"MQTT">>,
        proto_ver => 4,
        connected_at => 123456789,
        peername => {{10,0,0,8}, 53211}
    },
    {Key, Json} = emqx_plugin_kafka_payload:encode_connection_event(connected, ClientInfo, ConnInfo),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"client-a">>, Key),
    ?assertEqual(<<"connected">>, maps:get(<<"action">>, Payload)),
    ?assertEqual(<<"client-a">>, maps:get(<<"clientid">>, Payload)),
    ?assertEqual(<<"user-a">>, maps:get(<<"username">>, Payload)),
    ?assertEqual(<<"MQTT">>, maps:get(<<"proto_name">>, Payload)),
    ?assertEqual(4, maps:get(<<"proto_ver">>, Payload)),
    ?assertEqual(<<"10.0.0.8:53211">>, maps:get(<<"peername">>, Payload)),
    ?assertEqual(123456789, maps:get(<<"connected_at">>, Payload)).

encode_disconnected_event_test() ->
    ClientInfo = #{
        clientid => <<"client-a">>,
        username => <<"user-a">>
    },
    ConnInfo = #{
        proto_name => <<"MQTT">>,
        proto_ver => 5,
        disconnected_at => 123456999,
        peername => {{10,0,0,8}, 53211}
    },
    {Key, Json} =
        emqx_plugin_kafka_payload:encode_connection_event(
            disconnected,
            ClientInfo,
            ConnInfo,
            normal
        ),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"client-a">>, Key),
    ?assertEqual(<<"disconnected">>, maps:get(<<"action">>, Payload)),
    ?assertEqual(<<"normal">>, maps:get(<<"reason">>, Payload)),
    ?assertEqual(123456999, maps:get(<<"disconnected_at">>, Payload)).

encode_connection_event_uses_empty_key_without_clientid_test() ->
    ClientInfo = #{username => <<"user-a">>},
    ConnInfo = #{connected_at => 1},
    {Key, Json} = emqx_plugin_kafka_payload:encode_connection_event(connected, ClientInfo, ConnInfo),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<>>, Key),
    ?assertNot(maps:is_key(<<"clientid">>, Payload)).
```

- [ ] **Step 2: Run the payload tests to verify they fail**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_payload_tests
```

Expected: FAIL because `encode_connection_event/3` and `encode_connection_event/4` do not exist yet.

- [ ] **Step 3: Write the minimal payload implementation**

Update the export list in `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl`:

```erlang
-export([encode_publish/2, encode_connection_event/3, encode_connection_event/4, decode_consumer/1]).
```

Add these functions to `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl`:

```erlang
encode_connection_event(connected, ClientInfo, ConnInfo) ->
    encode_connection_event(connected, ClientInfo, ConnInfo, undefined);
encode_connection_event(disconnected, ClientInfo, ConnInfo) ->
    encode_connection_event(disconnected, ClientInfo, ConnInfo, undefined).

encode_connection_event(Action, ClientInfo, ConnInfo, Reason) ->
    Key = maps:get(clientid, ClientInfo, <<>>),
    Base = #{
        action => action_bin(Action),
        node => atom_to_binary(node(), utf8)
    },
    Payload1 = maybe_put_map_value(clientid, maps:get(clientid, ClientInfo, undefined), Base),
    Payload2 = maybe_put_map_value(username, maps:get(username, ClientInfo, undefined), Payload1),
    Payload3 = maybe_put_map_value(proto_name, maps:get(proto_name, ConnInfo, undefined), Payload2),
    Payload4 = maybe_put_map_value(proto_ver, maps:get(proto_ver, ConnInfo, undefined), Payload3),
    Payload5 = maybe_put_map_value(peername, format_peername(maps:get(peername, ConnInfo, undefined)), Payload4),
    Payload6 = maybe_put_event_timestamp(Action, ConnInfo, Payload5),
    Payload7 = maybe_put_disconnect_reason(Action, Reason, Payload6),
    {Key, emqx_json:encode(Payload7)}.

action_bin(connected) -> <<"connected">>;
action_bin(disconnected) -> <<"disconnected">>.

maybe_put_event_timestamp(connected, ConnInfo, Payload) ->
    maybe_put_map_value(connected_at, maps:get(connected_at, ConnInfo, undefined), Payload);
maybe_put_event_timestamp(disconnected, ConnInfo, Payload) ->
    maybe_put_map_value(disconnected_at, maps:get(disconnected_at, ConnInfo, undefined), Payload).

maybe_put_disconnect_reason(disconnected, Reason, Payload) ->
    maybe_put_map_value(reason, reason_bin(Reason), Payload);
maybe_put_disconnect_reason(_, _, Payload) ->
    Payload.

maybe_put_map_value(_Key, undefined, Payload) ->
    Payload;
maybe_put_map_value(Key, Value, Payload) ->
    Payload#{Key => Value}.

format_peername(undefined) ->
    undefined;
format_peername({{A,B,C,D}, Port}) ->
    iolist_to_binary(io_lib:format("~B.~B.~B.~B:~B", [A, B, C, D, Port]));
format_peername(Other) ->
    iolist_to_binary(io_lib:format("~0p", [Other])).

reason_bin(undefined) ->
    undefined;
reason_bin(Reason) when is_binary(Reason) ->
    Reason;
reason_bin(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
reason_bin(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).
```

- [ ] **Step 4: Run the payload tests to verify they pass**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_payload_tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_payload.erl \
        lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl
git commit -m "feat: encode kafka connection events"
```

### Task 3: Add Connection Event Publish Plans and Hook Handlers

**Files:**
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl`
- Test: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl`

- [ ] **Step 1: Write the failing producer tests**

Add these tests to `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl`:

```erlang
producer_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun matching_kafka_topics_returns_all_matches_in_rule_order/0,
         fun publish_plan_skips_sys_topics/0,
         fun publish_plan_skips_excluded_topics_before_rules/0,
         fun publish_plan_skips_when_no_rule_matches/0,
         fun publish_plan_returns_encoded_publish_when_excluded_topic_does_not_match/0,
         fun publish_plan_returns_encoded_publish_for_matching_rule/0,
         fun connection_event_plan_skips_when_disabled/0,
         fun connected_event_plan_returns_single_topic/0,
         fun disconnected_event_plan_includes_reason/0,
         fun on_message_publish_one_arity_returns_ok_when_disabled/0,
         fun on_message_publish_one_arity_returns_ok_when_no_rules_match/0,
         fun on_client_connected_returns_ok_when_disabled/0,
         fun on_client_disconnected_returns_ok_when_disabled/0,
         fun produce_success_accepts_ok_partition_result/0
     ]}.

setup() ->
    emqx_plugin_kafka_config:purge(),
    #{
        producer => application:get_env(emqx_plugin_kafka, producer),
        connection_events => application:get_env(emqx_plugin_kafka, connection_events)
    }.

cleanup(Env) ->
    restore_env(producer, maps:get(producer, Env)),
    restore_env(connection_events, maps:get(connection_events, Env)).

restore_env(Key, undefined) ->
    application:unset_env(emqx_plugin_kafka, Key);
restore_env(Key, {ok, Value}) ->
    application:set_env(emqx_plugin_kafka, Key, Value).

connection_event_plan_skips_when_disabled() ->
    application:set_env(emqx_plugin_kafka, connection_events, [{enabled, false}]),
    ?assertEqual(
        skip,
        emqx_plugin_kafka_producer:connection_event_plan(connected, clientinfo(), conninfo())
    ).

connected_event_plan_returns_single_topic() ->
    application:set_env(emqx_plugin_kafka, connection_events, [
        {enabled, true},
        {topic, <<"mqtt_connection_events">>}
    ]),
    {ok, [{KafkaTopic, Key, Json}]} =
        emqx_plugin_kafka_producer:connection_event_plan(connected, clientinfo(), conninfo()),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"mqtt_connection_events">>, KafkaTopic),
    ?assertEqual(<<"client-a">>, Key),
    ?assertEqual(<<"connected">>, maps:get(<<"action">>, Payload)).

disconnected_event_plan_includes_reason() ->
    application:set_env(emqx_plugin_kafka, connection_events, [
        {enabled, true},
        {topic, <<"mqtt_connection_events">>}
    ]),
    {ok, [{_KafkaTopic, _Key, Json}]} =
        emqx_plugin_kafka_producer:connection_event_plan(
            disconnected,
            clientinfo(),
            conninfo()#{disconnected_at => 123456999},
            normal
        ),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"disconnected">>, maps:get(<<"action">>, Payload)),
    ?assertEqual(<<"normal">>, maps:get(<<"reason">>, Payload)).

on_client_connected_returns_ok_when_disabled() ->
    application:set_env(emqx_plugin_kafka, connection_events, [{enabled, false}]),
    ?assertEqual(ok, emqx_plugin_kafka_producer:on_client_connected(clientinfo(), conninfo())).

on_client_disconnected_returns_ok_when_disabled() ->
    application:set_env(emqx_plugin_kafka, connection_events, [{enabled, false}]),
    ?assertEqual(
        ok,
        emqx_plugin_kafka_producer:on_client_disconnected(clientinfo(), normal, conninfo())
    ).

clientinfo() ->
    #{
        clientid => <<"client-a">>,
        username => <<"user-a">>
    }.

conninfo() ->
    #{
        proto_name => <<"MQTT">>,
        proto_ver => 4,
        connected_at => 123456789,
        peername => {{10,0,0,8}, 53211}
    }.
```

- [ ] **Step 2: Run the producer tests to verify they fail**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_producer_tests
```

Expected: FAIL because `connection_event_plan/3`, `connection_event_plan/4`, `on_client_connected/2`, and `on_client_disconnected/3` do not exist yet.

- [ ] **Step 3: Write the minimal producer implementation**

Update the export list in `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl`:

```erlang
-export([
    on_message_publish/1,
    on_client_connected/2,
    on_client_disconnected/3,
    publish_plan/2,
    connection_event_plan/3,
    connection_event_plan/4,
    matching_kafka_topics/2
]).
```

Add these functions to `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl`:

```erlang
on_client_connected(ClientInfo, ConnInfo) ->
    Conf = emqx_plugin_kafka_config:cached(),
    case connection_event_plan(ClientInfo, ConnInfo, Conf) of
        {ok, Plans} ->
            ClientId = maps:get(client_id, Conf),
            lists:foreach(fun(Plan) -> produce(ClientId, Plan) end, Plans),
            ok;
        skip ->
            ok
    end.

on_client_disconnected(ClientInfo, Reason, ConnInfo) ->
    Conf = emqx_plugin_kafka_config:cached(),
    case connection_event_plan(ClientInfo, Reason, ConnInfo, Conf) of
        {ok, Plans} ->
            ClientId = maps:get(client_id, Conf),
            lists:foreach(fun(Plan) -> produce(ClientId, Plan) end, Plans),
            ok;
        skip ->
            ok
    end.

connection_event_plan(ClientInfo, ConnInfo) ->
    connection_event_plan(ClientInfo, ConnInfo, emqx_plugin_kafka_config:get()).

connection_event_plan(ClientInfo, Reason, ConnInfo) ->
    connection_event_plan(ClientInfo, Reason, ConnInfo, emqx_plugin_kafka_config:get()).

connection_event_plan(_ClientInfo, _ConnInfo, #{connection_events := #{enabled := false}}) ->
    skip;
connection_event_plan(ClientInfo, ConnInfo, #{connection_events := ConnectionEvents}) ->
    KafkaTopic = maps:get(topic, ConnectionEvents),
    {Key, Json} = emqx_plugin_kafka_payload:encode_connection_event(connected, ClientInfo, ConnInfo),
    {ok, [{KafkaTopic, Key, Json}]}.

connection_event_plan(_ClientInfo, _Reason, _ConnInfo, #{connection_events := #{enabled := false}}) ->
    skip;
connection_event_plan(ClientInfo, Reason, ConnInfo, #{connection_events := ConnectionEvents}) ->
    KafkaTopic = maps:get(topic, ConnectionEvents),
    {Key, Json} =
        emqx_plugin_kafka_payload:encode_connection_event(disconnected, ClientInfo, ConnInfo, Reason),
    {ok, [{KafkaTopic, Key, Json}]}.
```

If needed for tests, add this test-only export:

```erlang
-ifdef(TEST).
-export([is_produce_success/1]).
-endif.
```

- [ ] **Step 4: Run the producer tests to verify they pass**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_producer_tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka_producer.erl \
        lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl
git commit -m "feat: publish kafka connection events"
```

### Task 4: Register New Hooks and Run Focused Verification

**Files:**
- Modify: `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl`
- Test: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_config_tests.erl`
- Test: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_payload_tests.erl`
- Test: `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_producer_tests.erl`

- [ ] **Step 1: Write the failing hook registration test**

Add this test file if needed as `lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_tests.erl`:

```erlang
-module(emqx_plugin_kafka_tests).

-include_lib("eunit/include/eunit.hrl").

load_registers_all_hooks_test() ->
    meck:new(emqx, [non_strict]),
    meck:expect(emqx, hook, fun(_, _) -> ok end),
    ?assertEqual(ok, emqx_plugin_kafka:load([])),
    ?assert(meck:called(emqx, hook, ['message.publish', {emqx_plugin_kafka_producer, on_message_publish, []}])),
    ?assert(meck:called(emqx, hook, ['client.connected', {emqx_plugin_kafka_producer, on_client_connected, []}])),
    ?assert(meck:called(emqx, hook, ['client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected, []}])),
    meck:unload(emqx).

unload_unregisters_all_hooks_test() ->
    meck:new(emqx, [non_strict]),
    meck:expect(emqx, unhook, fun(_, _) -> ok end),
    ?assertEqual(ok, emqx_plugin_kafka:unload()),
    ?assert(meck:called(emqx, unhook, ['message.publish', {emqx_plugin_kafka_producer, on_message_publish}])),
    ?assert(meck:called(emqx, unhook, ['client.connected', {emqx_plugin_kafka_producer, on_client_connected}])),
    ?assert(meck:called(emqx, unhook, ['client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected}])),
    meck:unload(emqx).
```

- [ ] **Step 2: Run the hook registration test to verify it fails**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_tests
```

Expected: FAIL because only `message.publish` is currently hooked.

- [ ] **Step 3: Write the minimal hook registration implementation**

Update `lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl`:

```erlang
load(_Env) ->
    ok = emqx:hook('message.publish', {emqx_plugin_kafka_producer, on_message_publish, []}),
    ok = emqx:hook('client.connected', {emqx_plugin_kafka_producer, on_client_connected, []}),
    emqx:hook('client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected, []}).

unload() ->
    ok = emqx:unhook('message.publish', {emqx_plugin_kafka_producer, on_message_publish}),
    ok = emqx:unhook('client.connected', {emqx_plugin_kafka_producer, on_client_connected}),
    emqx:unhook('client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected}).
```

- [ ] **Step 4: Run focused verification**

Run:

```bash
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_tests
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_config_tests
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_payload_tests
./rebar3 eunit --dir lib-extra/emqx_plugin_kafka --module emqx_plugin_kafka_producer_tests
```

Expected: PASS for all four modules.

- [ ] **Step 5: Commit**

```bash
git add lib-extra/emqx_plugin_kafka/src/emqx_plugin_kafka.erl \
        lib-extra/emqx_plugin_kafka/test/emqx_plugin_kafka_tests.erl
git commit -m "feat: register kafka connection event hooks"
```
