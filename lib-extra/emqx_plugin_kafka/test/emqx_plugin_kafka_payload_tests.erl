-module(emqx_plugin_kafka_payload_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

encode_publish_raw_payload_test() ->
    Msg = message(<<"hello">>),
    {Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, false),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"client-a">>, Key),
    ?assertEqual(<<"message_publish">>, maps:get(<<"action">>, Payload)),
    ?assertEqual(<<"client-a">>, maps:get(<<"clientid">>, Payload)),
    ?assertEqual(<<"sensors/a">>, maps:get(<<"topic">>, Payload)),
    ?assertEqual(1, maps:get(<<"qos">>, Payload)),
    ?assertEqual(<<"hello">>, maps:get(<<"payload">>, Payload)),
    ?assertEqual(atom_to_binary(node(), utf8), maps:get(<<"node">>, Payload)),
    ?assertEqual(123456789, maps:get(<<"timestamp">>, Payload)),
    ?assertEqual(<<"user-a">>, maps:get(<<"username">>, Payload)).

encode_publish_base64_payload_test() ->
    Msg = message(<<0, 1, 2, 255>>),
    {_Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, true),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(base64:encode(<<0, 1, 2, 255>>), maps:get(<<"payload">>, Payload)).

encode_publish_omits_missing_username_test() ->
    Msg = (message(<<"hello">>))#message{headers = #{}},
    {_Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, false),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertNot(maps:is_key(<<"username">>, Payload)).

encode_publish_coerces_atom_from_to_binary_test() ->
    Msg = (message(<<"hello">>))#message{from = sys_internal, headers = #{}},
    {Key, Json} = emqx_plugin_kafka_payload:encode_publish(Msg, false),
    ?assertEqual(<<"sys_internal">>, Key),
    Payload = emqx_json:decode(Json, [return_maps]),
    ?assertEqual(<<"sys_internal">>, maps:get(<<"clientid">>, Payload)).

decode_consumer_valid_payload_test() ->
    Json = <<"{\"topic\":\"down/a\",\"qos\":1,\"payload\":\"hello\"}">>,
    {ok, Msg} = emqx_plugin_kafka_payload:decode_consumer(Json),
    ?assertMatch(#message{}, Msg),
    ?assertEqual(<<"down/a">>, Msg#message.topic),
    ?assertEqual(1, Msg#message.qos),
    ?assertEqual(<<"hello">>, Msg#message.payload),
    ?assertEqual(<<"emqx_plugin_kafka">>, Msg#message.from),
    ?assertEqual(#{dup => false, retain => false}, Msg#message.flags),
    ?assertEqual(#{}, Msg#message.headers),
    ?assert(is_binary(Msg#message.id)),
    ?assert(is_integer(Msg#message.timestamp)).

decode_consumer_rejects_wildcard_topic_test() ->
    Json = <<"{\"topic\":\"down/+\",\"qos\":1,\"payload\":\"hello\"}">>,
    ?assertEqual({error, {invalid_topic, <<"down/+">>}},
                 emqx_plugin_kafka_payload:decode_consumer(Json)).

decode_consumer_rejects_array_topic_test() ->
    Json = <<"{\"topic\":[100,111,119,110],\"qos\":1,\"payload\":\"x\"}">>,
    ?assertEqual({error, {invalid_topic, [100, 111, 119, 110]}},
                 emqx_plugin_kafka_payload:decode_consumer(Json)).

decode_consumer_rejects_missing_qos_test() ->
    Json = <<"{\"topic\":\"down/a\",\"payload\":\"hello\"}">>,
    ?assertEqual({error, {invalid_qos, undefined}},
                 emqx_plugin_kafka_payload:decode_consumer(Json)).

decode_consumer_rejects_invalid_qos_test() ->
    Json = <<"{\"topic\":\"down/a\",\"qos\":3,\"payload\":\"hello\"}">>,
    ?assertEqual({error, {invalid_qos, 3}},
                 emqx_plugin_kafka_payload:decode_consumer(Json)).

decode_consumer_rejects_array_payload_test() ->
    Json = <<"{\"topic\":\"down/a\",\"qos\":1,\"payload\":[65,66]}">>,
    ?assertEqual({error, {invalid_payload, [65, 66]}},
                 emqx_plugin_kafka_payload:decode_consumer(Json)).

decode_consumer_rejects_non_object_json_test() ->
    ?assertEqual({error, {invalid_json, [1, 2]}},
                 emqx_plugin_kafka_payload:decode_consumer(<<"[1,2]">>)).

decode_consumer_returns_error_for_malformed_json_test() ->
    ?assertMatch({error, _}, emqx_plugin_kafka_payload:decode_consumer(<<"{">>)).

message(Payload) ->
    #message{
        id = <<"id-a">>,
        from = <<"client-a">>,
        qos = 1,
        flags = #{retain => false},
        headers = #{username => <<"user-a">>},
        topic = <<"sensors/a">>,
        payload = Payload,
        timestamp = 123456789
    }.
