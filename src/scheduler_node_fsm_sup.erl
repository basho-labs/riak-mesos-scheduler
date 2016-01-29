-module(scheduler_node_fsm_sup).

-behavior(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    NodeFsmSpec = {scheduler_node_fsm,
                   {scheduler_node_fsm, start_link, []},
                   transient, 5000, worker, [scheduler_node_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [NodeFsmSpec]}}.
