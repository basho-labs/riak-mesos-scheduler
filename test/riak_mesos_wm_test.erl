-module(riak_mesos_wm_test).

-include_lib("eunit/include/eunit.hrl").

riak_mesos_wm_test_() ->
    SetupFun = fun() ->
                       application:ensure_all_started(riak_mesos_scheduler),
                       ok = mesos_scheduler_data:reset_all_data()
               end,
    TeardownFun = fun(_) ->
                          application:stop(riak_mesos_scheduler)
                  end,
    {foreach,
     SetupFun,
     TeardownFun,
     [
      fun add_delete_cluster/0
     ]}.

-define(C1, "wm-test-cluster1").

add_delete_cluster() ->
    GetRequest = {url("clusters/" ++ ?C1), []},
    {ok, Res1} = httpc:request(get, GetRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 404, "Object Not Found"}, _, _}, Res1),

    CreateRequest = {url("clusters/" ++ ?C1), [], "plain/text", ""},
    {ok, Res2} = httpc:request(put, CreateRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 200, "OK"}, _, _}, Res2),

    {ok, Res3} = httpc:request(get, GetRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 200, "OK"}, _, _}, Res3),
    {_, _, GetJSON} = Res3,
    Cluster = mochijson2:decode(GetJSON),
    ?assertMatch({struct, [{<<?C1>>, {struct, _}}]}, Cluster),

    {ok, Res4} = httpc:request(delete, GetRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 200, "OK"}, _, _}, Res4),

    {ok, Res5} = httpc:request(get, GetRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 404, "Object Not Found"}, _, _}, Res5).

url(Resource) ->
    "http://localhost:9090/api/v1/" ++ Resource.
