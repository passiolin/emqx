-module(emqx_plugin_kafka).

-export([load/1, unload/0]).

load(_Env) ->
    emqx:hook('message.publish', {emqx_plugin_kafka_producer, on_message_publish, []}),
    emqx:hook('client.connected', {emqx_plugin_kafka_producer, on_client_connected, []}),
    emqx:hook('client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected, []}).

unload() ->
    emqx:unhook('message.publish', {emqx_plugin_kafka_producer, on_message_publish}),
    emqx:unhook('client.connected', {emqx_plugin_kafka_producer, on_client_connected}),
    emqx:unhook('client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected}).
