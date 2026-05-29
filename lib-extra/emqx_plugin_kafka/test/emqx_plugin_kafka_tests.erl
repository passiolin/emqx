-module(emqx_plugin_kafka_tests).

-include_lib("eunit/include/eunit.hrl").

load_registers_all_hooks_test() ->
    ok = meck:new(emqx, [non_strict, passthrough, no_history]),
    ok = meck:expect(emqx, hook, fun(_, _) -> ok end),
    try
        ?assertEqual(ok, emqx_plugin_kafka:load(#{})),
        ?assert(meck:called(
            emqx,
            hook,
            ['message.publish', {emqx_plugin_kafka_producer, on_message_publish, []}]
        )),
        ?assert(meck:called(
            emqx,
            hook,
            ['client.connected', {emqx_plugin_kafka_producer, on_client_connected, []}]
        )),
        ?assert(meck:called(
            emqx,
            hook,
            ['client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected, []}]
        ))
    after
        ok = meck:unload(emqx)
    end.

unload_unregisters_all_hooks_test() ->
    ok = meck:new(emqx, [non_strict, passthrough, no_history]),
    ok = meck:expect(emqx, unhook, fun(_, _) -> ok end),
    try
        ?assertEqual(ok, emqx_plugin_kafka:unload()),
        ?assert(meck:called(
            emqx,
            unhook,
            ['message.publish', {emqx_plugin_kafka_producer, on_message_publish}]
        )),
        ?assert(meck:called(
            emqx,
            unhook,
            ['client.connected', {emqx_plugin_kafka_producer, on_client_connected}]
        )),
        ?assert(meck:called(
            emqx,
            unhook,
            ['client.disconnected', {emqx_plugin_kafka_producer, on_client_disconnected}]
        ))
    after
        ok = meck:unload(emqx)
    end.
