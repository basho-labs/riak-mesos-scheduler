-module(mesos_scheduler_data_test).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_ZK_SERVER, [{"localhost", 2181}]).

sched_data_test_() ->
    SetupFun = fun() ->
                       application:ensure_all_started(erlzk),
                       {ok, _} = mesos_metadata_manager:start_link(?TEST_ZK_SERVER,"md-mgr-test"),
                       {ok, _} = mesos_scheduler_data:start_link(),
                       ok = mesos_scheduler_data:reset_all_data()
               end,
    TeardownFun = fun(_) ->
                          mesos_scheduler_data:stop(),
                          mesos_metadata_manager:stop()
                  end,

    {foreach,
     SetupFun,
     TeardownFun,
     [
      fun add_cluster/0,
      fun set_cluster_status/0
     ]}.

-define(C1, <<"test-cluster1">>).
-define(NODES, [a, b, c]).

add_cluster() ->
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_cluster(?C1),
    ok = mesos_scheduler_data:add_cluster(?C1, requested, ?NODES),
    {ok, requested, ?NODES} = mesos_scheduler_data:get_cluster(?C1).

set_cluster_status() ->
    ok = mesos_scheduler_data:add_cluster(?C1, requested, ?NODES),
    ok = mesos_scheduler_data:set_cluster_status(?C1, starting),
    {ok, starting, ?NODES} = mesos_scheduler_data:get_cluster(?C1).
