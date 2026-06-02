-module(emqx_plugin_kafka_producer_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

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
         fun on_message_publish_one_arity_returns_message_when_disabled/0,
         fun on_message_publish_one_arity_returns_message_when_no_rules_match/0,
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

cleanup(#{producer := ProducerEnv, connection_events := ConnectionEventsEnv}) ->
    restore_env(producer, ProducerEnv),
    restore_env(connection_events, ConnectionEventsEnv).

matching_kafka_topics_returns_all_matches_in_rule_order() ->
    Rules = [
        {<<"sensor/+/up">>, <<"kafka-sensor-up">>},
        {<<"alarm/#">>, <<"kafka-alarm">>},
        {<<"sensor/a/#">>, <<"kafka-sensor-a">>}
    ],
    ?assertEqual(
        [<<"kafka-sensor-up">>, <<"kafka-sensor-a">>],
        emqx_plugin_kafka_producer:matching_kafka_topics(<<"sensor/a/up">>, Rules)
    ).

publish_plan_skips_sys_topics() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {rules, [{<<"$SYS/#">>, <<"kafka-sys">>}]}
    ]),
    ?assertEqual(skip, emqx_plugin_kafka_producer:publish_plan(sys_message(), config())).

publish_plan_skips_excluded_topics_before_rules() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {excluded_topics, [<<"sensor/+/up">>]},
        {rules, [{<<"sensor/#">>, <<"kafka-sensor">>}]}
    ]),
    ?assertEqual(skip, emqx_plugin_kafka_producer:publish_plan(message(), config())).

publish_plan_skips_when_no_rule_matches() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {rules, [{<<"alarm/#">>, <<"kafka-alarm">>}]}
    ]),
    ?assertEqual(skip, emqx_plugin_kafka_producer:publish_plan(message(), config())).

publish_plan_returns_encoded_publish_when_excluded_topic_does_not_match() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {excluded_topics, [<<"alarm/#">>]},
        {rules, [{<<"sensor/#">>, <<"kafka-sensor">>}]}
    ]),
    {ok, [{KafkaTopic, _Key, _Json}]} =
        emqx_plugin_kafka_producer:publish_plan(message(), config()),
    ?assertEqual(<<"kafka-sensor">>, KafkaTopic).

publish_plan_returns_encoded_publish_for_matching_rule() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {publish_base64, false},
        {rules, [{<<"sensor/+/up">>, <<"kafka-sensor-up">>}]}
    ]),
    {ok, [{KafkaTopic, Key, Json}]} =
        emqx_plugin_kafka_producer:publish_plan(message(), config()),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"kafka-sensor-up">>, KafkaTopic),
    ?assertEqual(<<"client-a">>, Key),
    ?assertEqual(<<"sensor/a/up">>, maps:get(<<"topic">>, Payload)).

connection_event_plan_skips_when_disabled() ->
    application:set_env(emqx_plugin_kafka, connection_events, [{enabled, false}]),
    ?assertEqual(
        skip, emqx_plugin_kafka_producer:connection_event_plan(client_info(), conn_info(), config())
    ).

connected_event_plan_returns_single_topic() ->
    application:set_env(emqx_plugin_kafka, connection_events, [
        {enabled, true},
        {topic, <<"kafka-connection-events">>}
    ]),
    {ok, [{KafkaTopic, Key, Json}]} =
        emqx_plugin_kafka_producer:connection_event_plan(client_info(), conn_info(), config()),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"kafka-connection-events">>, KafkaTopic),
    ?assertEqual(<<"client-a">>, Key),
    ?assertEqual(<<"connected">>, maps:get(<<"action">>, Payload)),
    ?assertEqual(<<"client-a">>, maps:get(<<"clientid">>, Payload)).

disconnected_event_plan_includes_reason() ->
    application:set_env(emqx_plugin_kafka, connection_events, [
        {enabled, true},
        {topic, <<"kafka-connection-events">>}
    ]),
    {ok, [{KafkaTopic, Key, Json}]} =
        emqx_plugin_kafka_producer:connection_event_plan(
            client_info(), normal, disconnected_conn_info(), config()
        ),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"kafka-connection-events">>, KafkaTopic),
    ?assertEqual(<<"client-a">>, Key),
    ?assertEqual(<<"disconnected">>, maps:get(<<"action">>, Payload)),
    ?assertEqual(<<"normal">>, maps:get(<<"reason">>, Payload)).

on_message_publish_one_arity_returns_message_when_disabled() ->
    application:set_env(emqx_plugin_kafka, producer, [{enabled, false}]),
    ?assertEqual({module, emqx_plugin_kafka_producer}, code:ensure_loaded(emqx_plugin_kafka_producer)),
    ?assert(erlang:function_exported(emqx_plugin_kafka_producer, on_message_publish, 1)),
    Msg = message(),
    ?assertEqual({ok, Msg}, emqx_plugin_kafka_producer:on_message_publish(Msg)).

on_message_publish_one_arity_returns_message_when_no_rules_match() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {rules, [{<<"alarm/#">>, <<"kafka-alarm">>}]}
    ]),
    Msg = message(),
    ?assertEqual({ok, Msg}, emqx_plugin_kafka_producer:on_message_publish(Msg)).

on_client_connected_returns_ok_when_disabled() ->
    application:set_env(emqx_plugin_kafka, connection_events, [{enabled, false}]),
    ?assertEqual(ok, emqx_plugin_kafka_producer:on_client_connected(client_info(), conn_info())).

on_client_disconnected_returns_ok_when_disabled() ->
    application:set_env(emqx_plugin_kafka, connection_events, [{enabled, false}]),
    ?assertEqual(
        ok,
        emqx_plugin_kafka_producer:on_client_disconnected(
            client_info(), normal, disconnected_conn_info()
        )
    ).

produce_success_accepts_ok_partition_result() ->
    ?assert(emqx_plugin_kafka_producer:is_produce_success(ok)),
    ?assert(emqx_plugin_kafka_producer:is_produce_success({ok, 0})),
    ?assertNot(emqx_plugin_kafka_producer:is_produce_success({error, leader_not_available})).

config() ->
    emqx_plugin_kafka_config:get().

message() ->
    #message{
        id = <<"id-a">>,
        from = <<"client-a">>,
        qos = 1,
        flags = #{retain => false},
        headers = #{username => <<"user-a">>},
        topic = <<"sensor/a/up">>,
        payload = <<"hello">>,
        timestamp = 123456789
    }.

sys_message() ->
    (message())#message{topic = <<"$SYS/brokers">>}.

client_info() ->
    #{
        clientid => <<"client-a">>,
        username => <<"user-a">>
    }.

conn_info() ->
    #{
        peername => {{127, 0, 0, 1}, 1883},
        proto_name => <<"MQTT">>,
        proto_ver => 5,
        connected_at => 123456789
    }.

disconnected_conn_info() ->
    #{
        peername => {{127, 0, 0, 1}, 1883},
        proto_name => <<"MQTT">>,
        proto_ver => 5,
        disconnected_at => 123456999
    }.

restore_env(Key, undefined) ->
    application:unset_env(emqx_plugin_kafka, Key);
restore_env(Key, {ok, Value}) ->
    application:set_env(emqx_plugin_kafka, Key, Value).
