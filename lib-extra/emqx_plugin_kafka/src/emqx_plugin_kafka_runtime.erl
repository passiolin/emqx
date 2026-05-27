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
    case ensure_brod_started() of
        ok ->
            start_client(Hosts, ClientId, ClientConfig, Conf);
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

ensure_brod_started() ->
    case application:ensure_all_started(brod) of
        {ok, _Apps} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

start_client(Hosts, ClientId, ClientConfig, Conf) ->
    case brod:start_client(Hosts, ClientId, ClientConfig) of
        ok ->
            handle_runtime_children_start(ClientId, Conf);
        {error, {already_started, _Pid}} ->
            handle_runtime_children_start(ClientId, Conf);
        {error, already_started} ->
            handle_runtime_children_start(ClientId, Conf);
        {error, Reason} ->
            {stop, Reason}
    end.

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

start_producers(
    ClientId,
    #{producer := #{enabled := true, rules := Rules}, producer_config := ProducerConfig}
) ->
    KafkaTopics = lists:usort([KafkaTopic || {_Filter, KafkaTopic} <- Rules]),
    start_producer_topics(ClientId, KafkaTopics, ProducerConfig);
start_producers(_ClientId, _Conf) ->
    ok.

start_producer_topics(_ClientId, [], _ProducerConfig) ->
    ok;
start_producer_topics(ClientId, [KafkaTopic | Rest], ProducerConfig) ->
    case brod:start_producer(ClientId, KafkaTopic, ProducerConfig) of
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
    case emqx_plugin_kafka_consumer_sup:start_child(SubscriberId, GroupSubscriberConfig) of
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
