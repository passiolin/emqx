-module(emqx_plugin_kafka_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Children = [
        #{
            id => emqx_plugin_kafka_consumer_sup,
            start => {emqx_plugin_kafka_consumer_sup, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => supervisor,
            modules => [emqx_plugin_kafka_consumer_sup]
        },
        #{
            id => emqx_plugin_kafka_runtime,
            start => {emqx_plugin_kafka_runtime, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_plugin_kafka_runtime]
        }
    ],
    {ok, {SupFlags, Children}}.
