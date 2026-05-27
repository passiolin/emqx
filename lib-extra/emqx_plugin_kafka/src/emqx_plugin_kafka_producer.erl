-module(emqx_plugin_kafka_producer).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([on_message_publish/1, publish_plan/2, matching_kafka_topics/2]).

-ifdef(TEST).
-export([is_produce_success/1]).
-endif.

on_message_publish(Msg) ->
    Conf = emqx_plugin_kafka_config:cached(),
    case publish_plan(Msg, Conf) of
        {ok, Plans} ->
            ClientId = maps:get(client_id, Conf),
            lists:foreach(fun(Plan) -> produce(ClientId, Plan) end, Plans),
            ok;
        skip ->
            ok
    end.

publish_plan(#message{topic = <<"$SYS/", _/binary>>}, _Conf) ->
    skip;
publish_plan(_Msg, #{producer := #{enabled := false}}) ->
    skip;
publish_plan(Msg = #message{topic = Topic}, #{producer := Producer}) ->
    ExcludedTopics = maps:get(excluded_topics, Producer, []),
    case excluded_topic(Topic, ExcludedTopics) of
        true ->
            skip;
        false ->
            Rules = maps:get(rules, Producer, []),
            case matching_kafka_topics(Topic, Rules) of
                [] ->
                    skip;
                KafkaTopics ->
                    PublishBase64 = maps:get(publish_base64, Producer, false),
                    {Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, PublishBase64),
                    {ok, [{KafkaTopic, Key, Json} || KafkaTopic <- KafkaTopics]}
            end
    end.

matching_kafka_topics(Topic, Rules) ->
    [
        KafkaTopic
     || {TopicFilter, KafkaTopic} <- Rules,
        emqx_topic:match(Topic, TopicFilter)
    ].

excluded_topic(Topic, ExcludedTopics) ->
    lists:any(
        fun(TopicFilter) ->
            emqx_topic:match(Topic, TopicFilter)
        end,
        ExcludedTopics
    ).

produce(ClientId, {KafkaTopic, Key, Json}) ->
    case
        brod:produce_cb(
            ClientId,
            KafkaTopic,
            hash,
            Key,
            Json,
            fun(_Partition, Offset) when is_integer(Offset) ->
                ok;
               (_Partition, ok) ->
                ok;
               (_Partition, Result) ->
                log_produce_failure(KafkaTopic, Result)
            end
        )
    of
        Result ->
            handle_produce_result(KafkaTopic, Result)
    end.

handle_produce_result(KafkaTopic, Result) ->
    case is_produce_success(Result) of
        true ->
            ok;
        false ->
            log_produce_failure(KafkaTopic, Result)
    end.

is_produce_success(ok) ->
    true;
is_produce_success({ok, _Partition}) ->
    true;
is_produce_success(_Result) ->
    false.

log_produce_failure(KafkaTopic, Result) ->
    ?LOG(warning, "Kafka produce failed topic=~p reason=~p", [
        KafkaTopic,
        Result
    ]).
