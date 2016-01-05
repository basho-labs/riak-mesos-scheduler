-module(mesos_scheduler_data_test).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_ZK_SERVER, [{"localhost", 2181}]).

sched_data_test_() ->
    SetupFun = fun() ->
                       application:ensure_all_started(erlzk),
                       {ok, _} = mesos_metadata_manager:start_link(?TEST_ZK_SERVER,"schd-dat-test"),
                       {ok, _} = mesos_scheduler_data:start_link(),
                       ok = mesos_scheduler_data:reset_all_data()
               end,
    TeardownFun = fun(_) ->
                          mesos_scheduler_data:stop(),
                          mesos_metadata_manager:stop(),
                          application:stop(erlzk)
                  end,

    {foreach,
     SetupFun,
     TeardownFun,
     [
      fun add_delete_cluster/0,
      fun set_cluster_status/0,
      fun add_delete_node/0,
      fun join_node_to_cluster/0,
      fun test_persistence/0
     ]}.

-define(C1, <<"test-cluster1">>).
-define(N1, <<"test-node1">>).
-define(NODES, [a, b, c]).

add_delete_cluster() ->
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_cluster(?C1),

    {error, {not_found, ?C1}} = mesos_scheduler_data:delete_cluster(?C1),
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_cluster(?C1),

    ok = mesos_scheduler_data:add_cluster(?C1, requested, ?NODES),
    {ok, requested, ?NODES} = mesos_scheduler_data:get_cluster(?C1),

    ok = mesos_scheduler_data:delete_cluster(?C1),
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_cluster(?C1).

set_cluster_status() ->
    ok = mesos_scheduler_data:add_cluster(?C1, requested, ?NODES),
    ok = mesos_scheduler_data:set_cluster_status(?C1, starting),
    {ok, starting, ?NODES} = mesos_scheduler_data:get_cluster(?C1).

add_delete_node() ->
    {error, {not_found, ?N1}} = mesos_scheduler_data:delete_node(?N1),
    ok = mesos_scheduler_data:add_node(?N1, requested, "127.0.0.1"), %% Location format may change
    ok = mesos_scheduler_data:delete_node(?N1).

join_node_to_cluster() ->
    ok = mesos_scheduler_data:add_cluster(?C1, requested, []),
    {ok, requested, []} = mesos_scheduler_data:get_cluster(?C1),

    {error, {node_not_found, ?N1}} = mesos_scheduler_data:join_node_to_cluster(?C1, ?N1),
    {ok, requested, []} = mesos_scheduler_data:get_cluster(?C1),

    ok = mesos_scheduler_data:add_node(?N1, requested, "127.0.0.1"), %% Location format may change
    {ok, requested, []} = mesos_scheduler_data:get_cluster(?C1),

    {error, {node_not_active,?N1,requested}} = mesos_scheduler_data:join_node_to_cluster(?C1, ?N1),
    {ok, requested, []} = mesos_scheduler_data:get_cluster(?C1),

    ok = mesos_scheduler_data:set_node_status(?N1, active),
    {error,{cluster_not_active,?C1,requested}} = mesos_scheduler_data:join_node_to_cluster(?C1,?N1),
    {ok, requested, []} = mesos_scheduler_data:get_cluster(?C1),

    ok = mesos_scheduler_data:set_cluster_status(?C1, active),
    {ok, active, []} = mesos_scheduler_data:get_cluster(?C1),
    ok = mesos_scheduler_data:join_node_to_cluster(?C1, ?N1),
    {ok, active, [?N1]} = mesos_scheduler_data:get_cluster(?C1).

test_persistence() ->
    ok = mesos_scheduler_data:add_cluster(?C1, active, []),
    ok = mesos_scheduler_data:add_node(?N1, active, "127.0.0.1"), %% Location format may change
    ok = mesos_scheduler_data:join_node_to_cluster(?C1, ?N1),

    ok = mesos_scheduler_data:stop(),
    {ok, _Pid} = mesos_scheduler_data:start_link(),

    {ok, active, [?N1]} = mesos_scheduler_data:get_cluster(?C1).
