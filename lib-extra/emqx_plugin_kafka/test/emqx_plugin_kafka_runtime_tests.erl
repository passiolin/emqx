-module(emqx_plugin_kafka_runtime_tests).

-include_lib("eunit/include/eunit.hrl").

init_returns_before_kafka_connect_test() ->
    meck:new(emqx_plugin_kafka_config, [passthrough]),
    meck:expect(emqx_plugin_kafka_config, get, fun() ->
        #{
            kafka_hosts => [{"127.0.0.1", 9092}],
            client_id => emqx_plugin_kafka,
            brod_client_config => [],
            producer_config => [],
            consumer_config => [],
            producer => #{enabled => false, rules => []},
            consumer => #{enabled => false, topics => [], group_id => <<"g">>, begin_offset => earliest}
        }
    end),
    ?assertMatch({ok, #{client_id := emqx_plugin_kafka, connected := false}},
                 emqx_plugin_kafka_runtime:init([])),
    receive
        kafka_connect ->
            ok
    after 100 ->
        ?assert(false)
    end,
    meck:unload(emqx_plugin_kafka_config).

brod_is_a_release_dependency_test() ->
    AppFile = filename:join(["lib-extra", "emqx_plugin_kafka", "src", "emqx_plugin_kafka.app.src"]),
    {ok, [{application, emqx_plugin_kafka, Props}]} = file:consult(AppFile),
    Applications = proplists:get_value(applications, Props, []),
    %% emqx is the host application (always running), so it is intentionally NOT a dep.
    ?assertNot(lists:member(emqx, Applications)),
    %% brod MUST be a dep so relx bundles brod (+ kafka_protocol/snappyer/crc32cer)
    %% into the release lib dir; otherwise the runtime cannot start brod.
    ?assert(lists:member(brod, Applications)),
    ?assert(lists:member(supervisor3, Applications)).

default_client_config_adds_timeouts_test() ->
    Config = emqx_plugin_kafka_runtime:client_config([]),
    ?assertEqual(3000, proplists:get_value(connect_timeout, Config)),
    ?assertEqual(3, proplists:get_value(get_metadata_timeout_seconds, Config)).

default_client_config_keeps_explicit_values_test() ->
    Config = [{connect_timeout, 9000}, {get_metadata_timeout_seconds, 9}],
    ?assertEqual(Config, emqx_plugin_kafka_runtime:client_config(Config)).

ensure_dependency_paths_is_idempotent_test() ->
    ?assertEqual(ok, emqx_plugin_kafka_runtime:ensure_dependency_paths()),
    ?assertEqual(ok, emqx_plugin_kafka_runtime:ensure_dependency_paths()).

call_with_timeout_returns_value_test() ->
    ?assertEqual(ok, emqx_plugin_kafka_runtime:call_with_timeout(fun() -> ok end, 1000)).

call_with_timeout_returns_timeout_test() ->
    ?assertEqual(
        {error, timeout},
        emqx_plugin_kafka_runtime:call_with_timeout(
            fun() ->
                timer:sleep(1000),
                ok
            end,
            10
        )
    ).

start_client_call_times_out_test() ->
    ?assertEqual(
        {error, timeout},
        emqx_plugin_kafka_runtime:start_client_call(
            fun() ->
                timer:sleep(1000),
                ok
            end,
            10
        )
    ).

producer_topics_include_connection_events_topic_test() ->
    Conf = #{
        producer => #{
            enabled => true,
            rules => [
                {<<"sensor/+/up">>, <<"kafka_sensor_up">>},
                {<<"alarm/#">>, <<"kafka_alarm">>}
            ]
        },
        connection_events => #{
            enabled => true,
            topic => <<"mqtt_connection_events">>
        }
    },
    ?assertEqual(
        [<<"kafka_alarm">>, <<"kafka_sensor_up">>, <<"mqtt_connection_events">>],
        emqx_plugin_kafka_runtime:producer_topics(Conf)
    ).

producer_topics_skip_disabled_connection_events_test() ->
    Conf = #{
        producer => #{
            enabled => true,
            rules => [
                {<<"sensor/+/up">>, <<"kafka_sensor_up">>}
            ]
        },
        connection_events => #{
            enabled => false,
            topic => <<"mqtt_connection_events">>
        }
    },
    ?assertEqual(
        [<<"kafka_sensor_up">>],
        emqx_plugin_kafka_runtime:producer_topics(Conf)
    ).
