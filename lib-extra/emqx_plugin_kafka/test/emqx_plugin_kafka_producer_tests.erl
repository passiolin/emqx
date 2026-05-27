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
         fun publish_plan_returns_encoded_publish_for_matching_rule/0,
         fun on_message_publish_one_arity_returns_ok_when_disabled/0,
         fun on_message_publish_one_arity_returns_ok_when_no_rules_match/0,
         fun produce_success_accepts_ok_partition_result/0
     ]}.

setup() ->
    application:get_env(emqx_plugin_kafka, producer).

cleanup(undefined) ->
    application:unset_env(emqx_plugin_kafka, producer);
cleanup({ok, Producer}) ->
    application:set_env(emqx_plugin_kafka, producer, Producer).

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

on_message_publish_one_arity_returns_ok_when_disabled() ->
    application:set_env(emqx_plugin_kafka, producer, [{enabled, false}]),
    ?assertEqual({module, emqx_plugin_kafka_producer}, code:ensure_loaded(emqx_plugin_kafka_producer)),
    ?assert(erlang:function_exported(emqx_plugin_kafka_producer, on_message_publish, 1)),
    ?assertEqual(ok, emqx_plugin_kafka_producer:on_message_publish(message())).

on_message_publish_one_arity_returns_ok_when_no_rules_match() ->
    application:set_env(emqx_plugin_kafka, producer, [
        {enabled, true},
        {rules, [{<<"alarm/#">>, <<"kafka-alarm">>}]}
    ]),
    ?assertEqual(ok, emqx_plugin_kafka_producer:on_message_publish(message())).

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
