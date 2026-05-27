-module(emqx_plugin_kafka_config).

-export([get/0, reload/0, cached/0, purge/0]).

-define(APP, emqx_plugin_kafka).
-define(PT_KEY, {?APP, config}).

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

%% Cache the computed config in persistent_term so the message.publish hot path
%% does not rebuild it (7 app env reads + rule normalization) on every message.
%% Plugin config is static between (un)load, so the cache is refreshed on app
%% start (reload/0) and cleared on app stop (purge/0).
reload() ->
    Conf = ?MODULE:get(),
    persistent_term:put(?PT_KEY, Conf),
    Conf.

cached() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined -> ?MODULE:get();
        Conf -> Conf
    end.

purge() ->
    _ = persistent_term:erase(?PT_KEY),
    ok.

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
