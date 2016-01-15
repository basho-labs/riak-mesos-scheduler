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
      fun add_delete_cluster/0,
      fun list_clusters/0,
      fun set_get_cluster_config/0
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

list_clusters() ->
    ListRequest = {url("clusters"), []},
    {ok, Res1} = httpc:request(get, ListRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 200, "OK"}, _, _}, Res1),
    {_, _, ListJSON1} = Res1,
    ClusterList1 = mochijson2:decode(ListJSON1),
    ?assertEqual({struct, [{<<"clusters">>, []}]}, ClusterList1),

    CreateRequest = {url("clusters/" ++ ?C1), [], "plain/text", ""},
    {ok, Res2} = httpc:request(put, CreateRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 200, "OK"}, _, _}, Res2),

    {ok, Res3} = httpc:request(get, ListRequest, [], []),
    ?assertMatch({{"HTTP/1.1", 200, "OK"}, _, _}, Res1),
    {_, _, ListJSON2} = Res3,
    ClusterList2 = mochijson2:decode(ListJSON2),
    ?assertEqual({struct, [{<<"clusters">>, [<<?C1>>]}]}, ClusterList2).

set_get_cluster_config() ->
    RiakConfUrl = url("/clusters/" ++ ?C1 ++ "/riak.conf"),
    AdvancedConfUrl = url("/clusters/" ++ ?C1 ++ "/advanced.config"),

    GetRiakConfRequest = {RiakConfUrl, []},
    GetAdvancedConfRequest = {AdvancedConfUrl, []},

    verify_http_request(get, GetRiakConfRequest, 404),
    verify_http_request(get, GetAdvancedConfRequest, 404),

    CreateRequest = {url("clusters/" ++ ?C1), [], "plain/text", ""},
    verify_http_request(put, CreateRequest, 200),

    %% Getting config should succeed now, but actual config contents
    %% are undefined (in practice will probably default to "").
    verify_http_request(get, GetRiakConfRequest, 200),
    verify_http_request(get, GetAdvancedConfRequest, 200),

    TestConfig = "fake-test-config",
    SetRiakConfRequest = {RiakConfUrl, [], "plain/text", TestConfig},
    SetAdvancedConfRequest = {AdvancedConfUrl, [], "plain/text", TestConfig},

    verify_http_request(put, SetRiakConfRequest, 200),
    verify_http_request(put, SetAdvancedConfRequest, 200),

    {_, _, CurrentRiakConfig} = verify_http_request(get, GetRiakConfRequest, 200),
    {_, _, CurrentAdvancedConfig} = verify_http_request(get, GetAdvancedConfRequest, 200),
    ?assertEqual(CurrentRiakConfig, TestConfig),
    ?assertEqual(CurrentAdvancedConfig, TestConfig).

verify_http_request(Method, Request, ExpectedCode) ->
    {ok, Res} = httpc:request(Method, Request, [], []),
    ?assertMatch({{"HTTP/1.1", ExpectedCode, _}, _, _}, Res),
    Res.

url(Resource) ->
    "http://localhost:9090/api/v1/" ++ Resource.
