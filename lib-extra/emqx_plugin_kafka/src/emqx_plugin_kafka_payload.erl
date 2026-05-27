-module(emqx_plugin_kafka_payload).

-include_lib("emqx/include/emqx.hrl").

-export([encode_publish/2, decode_consumer/1]).

-define(FROM, <<"emqx_plugin_kafka">>).

encode_publish(Msg = #message{}, PublishBase64) ->
    From = from_bin(Msg#message.from),
    Payload = #{
        action => <<"message_publish">>,
        clientid => From,
        topic => Msg#message.topic,
        qos => Msg#message.qos,
        payload => encode_payload(Msg#message.payload, PublishBase64),
        node => atom_to_binary(node(), utf8),
        timestamp => Msg#message.timestamp
    },
    Json = emqx_json:encode(maybe_put_username(Msg#message.headers, Payload)),
    {From, Json}.

decode_consumer(Json) ->
    case emqx_json:safe_decode(Json, [return_maps]) of
        {ok, Map} when is_map(Map) ->
            try
                {ok, decode_consumer_map(Map)}
            catch
                error:Reason ->
                    {error, Reason}
            end;
        {ok, Other} ->
            {error, {invalid_json, Other}};
        {error, Reason} ->
            {error, Reason}
    end.

decode_consumer_map(Map) ->
    Topic = validate_topic(maps:get(<<"topic">>, Map, undefined)),
    Qos = validate_qos(maps:get(<<"qos">>, Map, undefined)),
    Payload = validate_payload(maps:get(<<"payload">>, Map, undefined)),
    #message{
        id = emqx_guid:gen(),
        qos = Qos,
        from = ?FROM,
        flags = #{dup => false, retain => false},
        headers = #{},
        topic = Topic,
        payload = Payload,
        timestamp = erlang:system_time(millisecond)
    }.

encode_payload(Payload, true) ->
    base64:encode(Payload);
encode_payload(Payload, false) ->
    Payload.

%% #message.from is atom() | binary(); coerce to binary so it is valid as both
%% the Kafka message key (must be iodata) and a JSON value.
from_bin(From) when is_binary(From) ->
    From;
from_bin(From) when is_atom(From) ->
    atom_to_binary(From, utf8).

maybe_put_username(Headers, Payload) ->
    case maps:find(username, Headers) of
        {ok, Username} ->
            Payload#{username => Username};
        error ->
            Payload
    end.

validate_topic(Topic) when is_binary(Topic) ->
    case valid_topic(Topic) of
        true ->
            Topic;
        false ->
            error({invalid_topic, Topic})
    end;
validate_topic(Topic) ->
    error({invalid_topic, Topic}).

valid_topic(<<>>) ->
    false;
valid_topic(Topic) ->
    nomatch =:= binary:match(Topic, [<<"+">>, <<"#">>]).

validate_qos(Qos) when Qos =:= 0; Qos =:= 1; Qos =:= 2 ->
    Qos;
validate_qos(Qos) ->
    error({invalid_qos, Qos}).

validate_payload(Payload) when is_binary(Payload) ->
    Payload;
validate_payload(Payload) ->
    error({invalid_payload, Payload}).
