-module(emqx_plugin_kafka_runtime).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-export([start_link/0]).
-export([
    call_with_timeout/2,
    client_config/1,
    ensure_dependency_paths/0,
    maybe_load_hooks/1,
    producer_topics/1,
    start_client_call/2
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(START_TIMEOUT, 5000).
-define(RECONNECT_INTERVAL, 10000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Conf = emqx_plugin_kafka_config:get(),
    ClientId = maps:get(client_id, Conf),
    erlang:send_after(0, self(), kafka_connect),
    {ok, #{client_id => ClientId, conf => Conf, connected => false, hooks_loaded => false}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(kafka_connect, State = #{conf := Conf}) ->
    NewState = connect(Conf, State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{connected := true, client_id := ClientId}) ->
    catch brod:stop_client(ClientId),
    ok;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

client_config(Config) ->
    ensure_config(get_metadata_timeout_seconds, 3,
                  ensure_config(connect_timeout, 3000, Config)).

call_with_timeout(Fun, Timeout) when is_function(Fun, 0) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() ->
        Parent ! {Ref, (catch Fun())}
    end),
    receive
        {Ref, {'EXIT', Reason}} ->
            {error, Reason};
        {Ref, Result} ->
            Result
    after Timeout ->
        exit(Pid, kill),
        {error, timeout}
    end.

ensure_brod_started() ->
    ensure_dependency_paths(),
    case application:ensure_all_started(brod) of
        {ok, _Apps} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

ensure_dependency_paths() ->
    LibDir = code:lib_dir(),
    Apps = [brod, kafka_protocol, snappyer, crc32cer, supervisor3],
    lists:foreach(fun(App) -> add_dependency_path(LibDir, App) end, Apps),
    ok.

add_dependency_path({error, _Reason}, _App) ->
    ok;
add_dependency_path(LibDir, App) ->
    Pattern = filename:join([LibDir, atom_to_list(App) ++ "-*", "ebin"]),
    lists:foreach(fun load_dependency_ebin/1, filelib:wildcard(Pattern)).

load_dependency_ebin(Ebin) ->
    code:add_pathz(Ebin),
    BeamFiles = filelib:wildcard(filename:join(Ebin, "*.beam")),
    lists:foreach(fun load_dependency_beam/1, BeamFiles).

load_dependency_beam(BeamFile) ->
    Module = list_to_atom(filename:basename(BeamFile, ".beam")),
    case code:is_loaded(Module) of
        false ->
            case code:load_file(Module) of
                {module, _} ->
                    ok;
                {error, not_purged} ->
                    ok;
                {error, sticky_directory} ->
                    ok;
                {error, _Reason} ->
                    ok
            end;
        {_File, _Loaded} ->
            ok
    end.

connect(Conf, State) ->
    ClientId = maps:get(client_id, Conf),
    Hosts = maps:get(kafka_hosts, Conf),
    ClientConfig = client_config(maps:get(brod_client_config, Conf)),
    case ensure_brod_started() of
        ok ->
            start_client(Hosts, ClientId, ClientConfig, Conf, State);
        {error, Reason} ->
            ?LOG(warning, "Kafka dependency start failed reason=~p", [Reason]),
            schedule_reconnect(State)
    end.

start_client(Hosts, ClientId, ClientConfig, Conf, State) ->
    case start_client(Hosts, ClientId, ClientConfig, Conf) of
        {ok, RuntimeState} ->
            maybe_load_hooks(maps:merge(State, RuntimeState#{connected => true}));
        {stop, Reason} ->
            ?LOG(warning, "Kafka runtime start failed reason=~p", [Reason]),
            catch brod:stop_client(ClientId),
            schedule_reconnect(State#{connected => false})
    end.

start_client(Hosts, ClientId, ClientConfig, Conf) ->
    case start_client_call(fun() -> brod:start_client(Hosts, ClientId, ClientConfig) end,
                           ?START_TIMEOUT) of
        ok ->
            handle_runtime_children_start(ClientId, Conf);
        {error, {already_started, _Pid}} ->
            handle_runtime_children_start(ClientId, Conf);
        {error, already_started} ->
            handle_runtime_children_start(ClientId, Conf);
        {error, Reason} ->
            ?LOG(warning, "Kafka client start failed reason=~p", [Reason]),
            {stop, Reason}
    end.

start_client_call(Fun, Timeout) ->
    call_with_timeout(Fun, Timeout).

schedule_reconnect(State) ->
    erlang:send_after(?RECONNECT_INTERVAL, self(), kafka_connect),
    State.

handle_runtime_children_start(ClientId, Conf) ->
    case start_runtime_children(ClientId, Conf) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            catch brod:stop_client(ClientId),
            {stop, Reason}
    end.

start_runtime_children(ClientId, Conf) ->
    case start_producers(ClientId, Conf) of
        ok ->
            case start_consumer(ClientId, Conf) of
                ok ->
                    {ok, #{client_id => ClientId}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

start_producers(ClientId, #{producer_config := ProducerConfig} = Conf) ->
    start_producer_topics(ClientId, producer_topics(Conf), ProducerConfig);
start_producers(_ClientId, _Conf) ->
    ok.

producer_topics(Conf) ->
    lists:usort(producer_rule_topics(Conf) ++ connection_event_topics(Conf)).

producer_rule_topics(#{producer := #{enabled := true, rules := Rules}}) ->
    [KafkaTopic || {_Filter, KafkaTopic} <- Rules];
producer_rule_topics(_Conf) ->
    [].

connection_event_topics(#{connection_events := #{enabled := true, topic := KafkaTopic}}) ->
    [KafkaTopic];
connection_event_topics(_Conf) ->
    [].

maybe_load_hooks(#{hooks_loaded := true} = State) ->
    State;
maybe_load_hooks(State) ->
    ok = emqx_plugin_kafka:load([]),
    State#{hooks_loaded => true}.

start_producer_topics(_ClientId, [], _ProducerConfig) ->
    ok;
start_producer_topics(ClientId, [KafkaTopic | Rest], ProducerConfig) ->
    case call_with_timeout(fun() -> brod:start_producer(ClientId, KafkaTopic, ProducerConfig) end,
                           ?START_TIMEOUT) of
        ok ->
            start_producer_topics(ClientId, Rest, ProducerConfig);
        {error, already_started} ->
            start_producer_topics(ClientId, Rest, ProducerConfig);
        {error, {already_started, _Pid}} ->
            start_producer_topics(ClientId, Rest, ProducerConfig);
        {error, Reason} ->
            ?LOG(warning, "Kafka producer start failed topic=~p reason=~p", [
                KafkaTopic,
                Reason
            ]),
            {error, {producer_start_failed, KafkaTopic, Reason}}
    end.

start_consumer(
    ClientId,
    #{
        consumer := #{
            enabled := true,
            group_id := GroupId,
            topics := Topics,
            begin_offset := BeginOffset
        },
        consumer_config := ConsumerConfig
    }
) when Topics =/= [] ->
    SubscriberId = emqx_plugin_kafka_consumer,
    GroupSubscriberConfig = #{
        client => ClientId,
        group_id => GroupId,
        topics => Topics,
        cb_module => emqx_plugin_kafka_consumer,
        init_data => #{},
        message_type => message,
        consumer_config => consumer_config(BeginOffset, ConsumerConfig),
        group_config => []
    },
    case call_with_timeout(
        fun() -> emqx_plugin_kafka_consumer_sup:start_child(SubscriberId, GroupSubscriberConfig) end,
        ?START_TIMEOUT
    ) of
        {ok, _Pid} ->
            ok;
        {error, Reason} ->
            ?LOG(warning, "Kafka consumer start failed reason=~p", [Reason]),
            {error, {consumer_start_failed, Reason}}
    end;
start_consumer(_ClientId, _Conf) ->
    ok.

consumer_config(BeginOffset, ConsumerConfig) ->
    [
        {begin_offset, BeginOffset},
        {offset_reset_policy, offset_reset_policy(BeginOffset)}
        | ConsumerConfig
    ].

offset_reset_policy(earliest) ->
    reset_to_earliest;
offset_reset_policy(latest) ->
    reset_to_latest;
offset_reset_policy(_BeginOffset) ->
    reset_to_earliest.

ensure_config(Key, Default, Config) ->
    case proplists:is_defined(Key, Config) of
        true ->
            Config;
        false ->
            [{Key, Default} | Config]
    end.
