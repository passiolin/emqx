-module(emqx_plugin_kafka_config_tests).

-include_lib("eunit/include/eunit.hrl").

config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [fun defaults/0, fun normalizes_binary_rules/0]}.

setup() ->
    [{Key, application:get_env(emqx_plugin_kafka, Key)} || Key <- env_keys()].

cleanup(Env) ->
    lists:foreach(
        fun
            ({Key, undefined}) ->
                application:unset_env(emqx_plugin_kafka, Key);
            ({Key, {ok, Value}}) ->
                application:set_env(emqx_plugin_kafka, Key, Value)
        end,
        Env).

env_keys() ->
    [
        producer,
        consumer,
        kafka_hosts,
        client_id,
        brod_client_config,
        producer_config,
        consumer_config
    ].

defaults() ->
    application:unset_env(emqx_plugin_kafka, producer),
    application:unset_env(emqx_plugin_kafka, consumer),
    application:unset_env(emqx_plugin_kafka, kafka_hosts),
    application:unset_env(emqx_plugin_kafka, client_id),
    application:unset_env(emqx_plugin_kafka, brod_client_config),
    application:unset_env(emqx_plugin_kafka, producer_config),
    application:unset_env(emqx_plugin_kafka, consumer_config),
    Conf = emqx_plugin_kafka_config:get(),
    ?assertEqual([{"127.0.0.1", 9092}], maps:get(kafka_hosts, Conf)),
    ?assertEqual(emqx_plugin_kafka_client, maps:get(client_id, Conf)),
    ?assertEqual([], maps:get(brod_client_config, Conf)),
    ?assertEqual([], maps:get(producer_config, Conf)),
    ?assertEqual([], maps:get(consumer_config, Conf)),
    ?assertMatch(
        #{enabled := true, publish_base64 := false, rules := []},
        maps:get(producer, Conf)
    ),
    ?assertMatch(
        #{enabled := false,
          group_id := <<"emqx_plugin_kafka">>,
          topics := [],
          begin_offset := earliest},
        maps:get(consumer, Conf)
    ).

normalizes_binary_rules() ->
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

cached_reads_live_env_when_not_loaded_test() ->
    emqx_plugin_kafka_config:purge(),
    application:set_env(emqx_plugin_kafka, client_id, fresh_cached_id),
    ?assertEqual(fresh_cached_id,
                 maps:get(client_id, emqx_plugin_kafka_config:cached())),
    application:unset_env(emqx_plugin_kafka, client_id).

reload_snapshots_until_next_reload_test() ->
    application:set_env(emqx_plugin_kafka, client_id, id_v1),
    emqx_plugin_kafka_config:reload(),
    %% changing env after reload must NOT change cached() until the next reload
    application:set_env(emqx_plugin_kafka, client_id, id_v2),
    ?assertEqual(id_v1, maps:get(client_id, emqx_plugin_kafka_config:cached())),
    emqx_plugin_kafka_config:reload(),
    ?assertEqual(id_v2, maps:get(client_id, emqx_plugin_kafka_config:cached())),
    emqx_plugin_kafka_config:purge(),
    application:unset_env(emqx_plugin_kafka, client_id).
