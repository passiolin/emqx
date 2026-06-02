-module(emqx_plugin_kafka_consumer_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("brod/include/brod.hrl").

handle_message_key_value_publishes_with_default_qos_and_commits_test() ->
    Parent = self(),
    PublishFun = fun(Msg) ->
        Parent ! {published, Msg},
        ok
    end,
    State = #{publish_fun => PublishFun},
    KafkaMsg = #kafka_message{
        key = <<"down/a">>,
        value = <<"hello">>
    },
    ?assertEqual({ok, commit, State}, emqx_plugin_kafka_consumer:handle_message(KafkaMsg, State)),
    receive
        {published, Msg = #message{}} ->
            ?assertEqual(<<"down/a">>, Msg#message.topic),
            ?assertEqual(1, Msg#message.qos),
            ?assertEqual(<<"hello">>, Msg#message.payload)
    after 100 ->
        ?assert(false)
    end.

handle_message_invalid_key_drops_and_commits_test() ->
    Parent = self(),
    PublishFun = fun(Msg) ->
        Parent ! {published, Msg},
        ok
    end,
    State = #{publish_fun => PublishFun},
    KafkaMsg = #kafka_message{
        key = <<"down/+">>,
        value = <<"hello">>
    },
    ?assertEqual({ok, commit, State}, emqx_plugin_kafka_consumer:handle_message(KafkaMsg, State)),
    receive
        {published, Msg} ->
            ?assertEqual({unexpected_publish, Msg}, no_publish_expected)
    after 100 ->
        ok
    end.

handle_message_publish_failure_commits_test() ->
    PublishFun = fun(_Msg) ->
        error(publish_failed)
    end,
    State = #{publish_fun => PublishFun},
    KafkaMsg = #kafka_message{
        key = <<"down/a">>,
        value = <<"hello">>
    },
    ?assertEqual({ok, commit, State}, emqx_plugin_kafka_consumer:handle_message(KafkaMsg, State)).
