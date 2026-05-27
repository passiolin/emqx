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
