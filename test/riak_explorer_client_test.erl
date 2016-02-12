-module(riak_explorer_client_test).

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
      fun set_get_cluster_config/0,
      fun create_node/0,
      fun list_nodes/0,
      fun get_node/0,
      fun test_new_node_fun/0,
      fun delete_node/0
     ]}.

-define(C1, "wm-test-cluster1").

add_delete_cluster() ->
    GetRequest = {url("clusters/" ++ ?C1), []},
    verify_http_request(get, GetRequest, 404),

    add_cluster(?C1),

    {_, _, GetJSON} = verify_http_request(get, GetRequest, 200),
    Cluster = mochijson2:decode(GetJSON),
    ?assertMatch({struct, [{<<?C1>>, {struct, _}}]}, Cluster),

    verify_http_request(delete, GetRequest, 200),

    verify_http_request(get, GetRequest, 404).

list_clusters() ->
    ListRequest = {url("clusters"), []},
    {_, _, ListJSON1} = verify_http_request(get, ListRequest, 200),
    ClusterList1 = mochijson2:decode(ListJSON1),
    ?assertEqual({struct, [{<<"clusters">>, []}]}, ClusterList1),

    add_cluster(?C1),

    {_, _, ListJSON2} = verify_http_request(get, ListRequest, 200),
    ClusterList2 = mochijson2:decode(ListJSON2),
    ?assertEqual({struct, [{<<"clusters">>, [<<?C1>>]}]}, ClusterList2).

set_get_cluster_config() ->
    RiakConfUrl = url("/clusters/" ++ ?C1 ++ "/riak.conf"),
    AdvancedConfUrl = url("/clusters/" ++ ?C1 ++ "/advanced.config"),

    GetRiakConfRequest = {RiakConfUrl, []},
    GetAdvancedConfRequest = {AdvancedConfUrl, []},

    verify_http_request(get, GetRiakConfRequest, 404),
    verify_http_request(get, GetAdvancedConfRequest, 404),

    add_cluster(?C1),

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

create_node() ->
    add_cluster(?C1),

    add_node(?C1),

    GetRequest = {url("clusters/" ++ ?C1), []},
    {_, _, ClusterDataBin} = verify_http_request(get, GetRequest, 200),
    ClusterData = mochijson2:decode(ClusterDataBin),
    {struct, [{<<?C1>>, {struct, Fields}}]} = ClusterData,
    {<<"nodes">>, Nodes} = lists:keyfind(<<"nodes">>, 1, Fields),
    ExpectedNode = <<?C1, "-1">>,
    ?assertEqual([ExpectedNode], Nodes).

list_nodes() ->
    CreateRequest = {url("clusters/" ++ ?C1), [], "plain/text", ""},
    verify_http_request(put, CreateRequest, 200),

    ListRequest = {url("clusters/" ++ ?C1 ++ "/nodes"), []},
    {_, _, Result} = verify_http_request(get, ListRequest, 200),
    ?assertEqual([], decode_node_list(Result)),

    %% Add three nodes
    [add_node(?C1) || _ <- [1, 2, 3]],

    {_, _, Result2} = verify_http_request(get, ListRequest, 200),
    Nodes = decode_node_list(Result2),
    SortedNodes = lists:sort(Nodes),

    ExpectedNodes = [?C1 ++ N || N <- ["-1", "-2", "-3"]],
    ?assertEqual(ExpectedNodes, SortedNodes).

delete_node() ->
    NodeName = ?C1 ++ "-1",

    GetRequest = {url("clusters/" ++ ?C1 ++ "/nodes/" ++ NodeName), []},
    verify_http_request(get, GetRequest, 404),

    add_cluster(?C1),
    add_node(?C1),

    verify_http_request(get, GetRequest, 200),

    verify_http_request(delete, GetRequest, 200),

    verify_http_request(get, GetRequest, 404).

get_node() ->
    ClusterKey = ?C1,
    NodeKey = ?C1 ++ "-1",
    NodeKeyBin = list_to_binary(NodeKey),
    add_cluster(ClusterKey),
    add_node(ClusterKey),

    GetRequest = {url("clusters/" ++ ClusterKey ++ "/nodes/" ++ NodeKey), []},
    {_, _, ResultJson} = verify_http_request(get, GetRequest, 200),

    Result = mochijson2:decode(ResultJson),
    ?assertMatch({struct, [{NodeKeyBin, {struct,
                                         [{<<"key">>, NodeKeyBin},
                                          {<<"status">>, <<"requested">>},
                                          {<<"location">>, {struct, _}},
                                          {<<"container_path">>, _},
                                          {<<"persistence_id">>, _}]}}]},
                 Result).

decode_node_list(Response) ->
    {struct, [{<<"nodes">>, NodeList}]} = mochijson2:decode(Response),
    [binary_to_list(N) || N <- NodeList].

add_cluster(Cluster) ->
    CreateRequest = {url("clusters/" ++ Cluster), [], "plain/text", ""},
    verify_http_request(put, CreateRequest, 200).

add_node(Cluster) ->
    NodeRequest = {url("/clusters/" ++ Cluster ++ "/nodes"), [], "plain/text", ""},
    verify_http_request(post, NodeRequest, 201).

test_new_node_fun() ->
    C1 = #rms_cluster{key = ?C1, nodes = []},
    N1 = ?C1 ++ "-1",
    ?assertMatch(#rms_cluster{key = ?C1, nodes = [N1]},
                 riak_mesos_wm_resource:update_cluster_with_new_node(C1)),

    C2 = C1#rms_cluster{nodes = [N1]},
    N2 = ?C1 ++ "-2",
    ?assertMatch(#rms_cluster{key = ?C1, nodes = [N2, N1]},
                 riak_mesos_wm_resource:update_cluster_with_new_node(C2)),

    N5 = ?C1 ++ "-5",
    N6 = ?C1 ++ "-6",
    NBad1 = "nomatch-test",
    NBad2 = "doesn't match-99",
    Nodes = [N1, NBad1, N5, NBad2, N2],
    C3 = C1#rms_cluster{nodes = Nodes},
    ?assertMatch(#rms_cluster{nodes = [N6 | Nodes]},
                 riak_mesos_wm_resource:update_cluster_with_new_node(C3)).

verify_http_request(Method, Request, ExpectedCode) ->
    {ok, Res} = httpc:request(Method, Request, [], []),
    ?assertMatch({{"HTTP/1.1", ExpectedCode, _}, _, _}, Res),
    Res.

url(Resource) ->
    "http://localhost:9090/api/v1/" ++ Resource.
