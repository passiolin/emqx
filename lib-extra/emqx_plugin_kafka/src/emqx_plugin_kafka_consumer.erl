-module(emqx_plugin_kafka_consumer).

-include_lib("emqx/include/logger.hrl").
-include_lib("brod/include/brod.hrl").

-export([init/2, handle_message/2]).

init(GroupData, State) ->
    case maps:find(topic, GroupData) of
        {ok, Topic} ->
            {ok, State#{kafka_topic => Topic}};
        error ->
            {ok, State}
    end.

handle_message(#kafka_message{value = Value}, State) ->
    case emqx_plugin_kafka_payload:decode_consumer(Value) of
        {ok, Msg} ->
            publish(Msg, State),
            ok;
        {error, Reason} ->
            ?LOG(warning, "Drop invalid kafka message reason=~p", [Reason]),
            ok
    end,
    {ok, commit, State}.

publish(Msg, State) ->
    PublishFun = maps:get(publish_fun, State, fun emqx_broker:safe_publish/1),
    try
        _ = PublishFun(Msg),
        ok
    catch
        Class:Reason:Stacktrace ->
            ?LOG(warning, "Kafka consumer publish failed class=~p reason=~p stacktrace=~p", [
                Class,
                Reason,
                Stacktrace
            ]),
            ok
    end.
