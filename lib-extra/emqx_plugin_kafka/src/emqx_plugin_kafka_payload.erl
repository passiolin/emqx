-module(emqx_plugin_kafka_payload).

-include_lib("emqx/include/emqx.hrl").

-export([
    encode_publish/2,
    encode_connection_event/3,
    encode_connection_event/4,
    decode_consumer/1,
    decode_consumer/2
]).

-define(FROM, <<"emqx_plugin_kafka">>).
-define(DEFAULT_CONSUMER_QOS, 1).

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

encode_connection_event(connected, ClientInfo, ConnInfo) ->
    encode_connection_event(connected, ClientInfo, ConnInfo, undefined);
encode_connection_event(disconnected, ClientInfo, ConnInfo) ->
    encode_connection_event(disconnected, ClientInfo, ConnInfo, undefined).

encode_connection_event(Action, ClientInfo, ConnInfo, Reason) ->
    Key = clientid_key(ClientInfo),
    Payload0 = #{
        action => action_bin(Action),
        node => atom_to_binary(node(), utf8),
        peername => format_peername(maps:get(peername, ConnInfo, undefined)),
        event_timestamp_key(Action) => maps:get(event_timestamp_key(Action), ConnInfo)
    },
    Payload1 = maybe_put(clientid, ClientInfo, Payload0),
    Payload2 = maybe_put(username, ClientInfo, Payload1),
    Payload3 = maybe_put(proto_name, ConnInfo, Payload2),
    Payload4 = maybe_put(proto_ver, ConnInfo, Payload3),
    Payload5 = maybe_put_reason(Action, Reason, Payload4),
    {Key, emqx_json:encode(maps:filter(fun(_K, V) -> V =/= undefined end, Payload5))}.

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

decode_consumer(Topic0, Payload0) ->
    try
        Topic = validate_topic(Topic0),
        Payload = validate_payload(Payload0),
        {ok, consumer_message(Topic, ?DEFAULT_CONSUMER_QOS, Payload)}
    catch
        error:Reason ->
            {error, Reason}
    end.

decode_consumer_map(Map) ->
    Topic = validate_topic(maps:get(<<"topic">>, Map, undefined)),
    Qos = validate_qos(maps:get(<<"qos">>, Map, undefined)),
    Payload = validate_payload(maps:get(<<"payload">>, Map, undefined)),
    consumer_message(Topic, Qos, Payload).

consumer_message(Topic, Qos, Payload) ->
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

maybe_put(Field, Source, Payload) ->
    case maps:find(Field, Source) of
        {ok, undefined} ->
            Payload;
        {ok, Value} ->
            Payload#{Field => Value};
        error ->
            Payload
    end.

clientid_key(ClientInfo) ->
    case maps:get(clientid, ClientInfo, <<>>) of
        undefined -> <<>>;
        ClientId -> ClientId
    end.

maybe_put_reason(disconnected, Reason, Payload) when Reason =/= undefined ->
    Payload#{reason => reason_bin(Reason)};
maybe_put_reason(_, _, Payload) ->
    Payload.

action_bin(connected) ->
    <<"connected">>;
action_bin(disconnected) ->
    <<"disconnected">>.

event_timestamp_key(connected) ->
    connected_at;
event_timestamp_key(disconnected) ->
    disconnected_at.

format_peername({IP, Port}) when tuple_size(IP) =:= 4 ->
    iolist_to_binary([inet:ntoa(IP), $:, integer_to_list(Port)]);
format_peername({IP, Port}) ->
    iolist_to_binary([$[, inet:ntoa(IP), $], $:, integer_to_list(Port)]);
format_peername(undefined) ->
    undefined.

reason_bin(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
reason_bin(Reason) when is_binary(Reason) ->
    Reason;
reason_bin(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).

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
