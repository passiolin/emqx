-module(emqx_plugin_kafka).

-export([load/1, unload/0]).

load(_Env) ->
    emqx:hook('message.publish', {emqx_plugin_kafka_producer, on_message_publish, []}).

unload() ->
    emqx:unhook('message.publish', {emqx_plugin_kafka_producer, on_message_publish}).
