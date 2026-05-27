-module(emqx_plugin_kafka_consumer_sup_tests).

-include_lib("eunit/include/eunit.hrl").

init_uses_nonzero_restart_intensity_test() ->
    {ok, {{Strategy, Intensity, Period}, Children}} =
        emqx_plugin_kafka_consumer_sup:init([]),
    ?assertEqual(one_for_one, Strategy),
    ?assert(Intensity >= 1),
    ?assert(Period >= 1),
    ?assertEqual([], Children).
