-module(emqx_plugin_kafka_consumer_sup).

-behaviour(supervisor3).

-export([start_link/0, start_child/2, ensure_child_deleted/1]).
-export([init/1, post_init/1]).

start_link() ->
    supervisor3:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Id, GroupSubscriberConfig) ->
    ChildSpec = {
        Id,
        {brod_group_subscriber_v2, start_link, [GroupSubscriberConfig]},
        permanent,
        10000,
        worker,
        [brod_group_subscriber_v2]
    },
    case supervisor3:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {ok, Pid, _Info} ->
            {ok, Pid};
        {error, already_present} ->
            normalize_start_result(supervisor3:restart_child(?MODULE, Id));
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

ensure_child_deleted(Id) ->
    case supervisor3:terminate_child(?MODULE, Id) of
        ok ->
            ok = supervisor3:delete_child(?MODULE, Id),
            ok;
        {error, not_found} ->
            ok
    end.

init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

post_init(_) ->
    ignore.

normalize_start_result({ok, Pid}) ->
    {ok, Pid};
normalize_start_result({ok, Pid, _Info}) ->
    {ok, Pid};
normalize_start_result({error, {already_started, Pid}}) ->
    {ok, Pid};
normalize_start_result({error, Reason}) ->
    {error, Reason}.
