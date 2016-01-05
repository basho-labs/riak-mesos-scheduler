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
      fun dummy/0
     ]}.

dummy() ->
    pass.
