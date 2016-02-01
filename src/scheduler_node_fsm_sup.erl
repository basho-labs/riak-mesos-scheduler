-module(scheduler_node_fsm_sup).

-behavior(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, top_sup).

init(top_sup) ->
    ChildSupName = scheduler_node_fsm_child_sup,
    ChildSupSpec = {ChildSupName,
                    {supervisor, start_link, [{local, ChildSupName}, ?MODULE, child_sup]},
                    permanent, 5000, supervisor, [?MODULE]},

    ManagerSpec = {scheduler_node_fsm_mgr,
                   {scheduler_node_fsm_mgr, start_link, []},
                   permanent, 5000, worker, [scheduler_node_fsm_mgr]},

    {ok, {{one_for_all, 10, 10}, [ChildSupSpec, ManagerSpec]}};

init(child_sup) ->
    NodeFsmSpec = {scheduler_node_fsm,
                   {scheduler_node_fsm, start_link, []},
                   transient, 5000, worker, [scheduler_node_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [NodeFsmSpec]}}.
