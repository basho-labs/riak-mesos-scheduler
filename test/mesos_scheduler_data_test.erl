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
      fun add_cluster/0
     ]}.

add_cluster() ->
    C1 = <<"test-cluster1">>,
    Nodes = [a, b, c],

    {error, {not_found, C1}} = mesos_scheduler_data:get_cluster(C1),
    ok = mesos_scheduler_data:add_cluster(C1, requested, Nodes),
    {ok, requested, Nodes} = mesos_scheduler_data:get_cluster(C1),

    pass.
