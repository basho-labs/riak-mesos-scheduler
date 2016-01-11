-module(mesos_scheduler_data_test).

-include_lib("eunit/include/eunit.hrl").

-include("mesos_scheduler_data.hrl").

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
      fun duplicate_adds/0,
      fun test_persistence/0
     ]}.

-define(C1, <<"test-cluster1">>).
-define(N1, <<"test-node1">>).
-define(NODES, [a, b, c]).

add_delete_cluster() ->
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_cluster(?C1),
    [] = mesos_scheduler_data:get_all_clusters(),

    {error, {not_found, ?C1}} = mesos_scheduler_data:delete_cluster(?C1),
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_cluster(?C1),
    [] = mesos_scheduler_data:get_all_clusters(),

    Cluster = #rms_cluster{key = ?C1, status = requested, nodes = ?NODES},
    ok = mesos_scheduler_data:add_cluster(Cluster),
    {ok, Cluster} = mesos_scheduler_data:get_cluster(?C1),
    [Cluster] = mesos_scheduler_data:get_all_clusters(),

    ok = mesos_scheduler_data:delete_cluster(?C1),
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_cluster(?C1),
    [] = mesos_scheduler_data:get_all_clusters().

set_cluster_status() ->
    Cluster = #rms_cluster{key = ?C1, status = requested, nodes = ?NODES},

    ok = mesos_scheduler_data:add_cluster(Cluster),
    ok = mesos_scheduler_data:set_cluster_status(?C1, starting),

    Expected = Cluster#rms_cluster{status = starting},
    {ok, Expected} = mesos_scheduler_data:get_cluster(?C1).

add_delete_node() ->
    Cluster = #rms_cluster{key = ?C1, status = requested, nodes = []},
    Node = #rms_node{key = ?N1, status = requested, cluster = ?C1},

    {error, {not_found, ?N1}} = mesos_scheduler_data:delete_node(?N1),
    [] = mesos_scheduler_data:get_all_nodes(),

    {error, {no_such_cluster, ?C1}} = mesos_scheduler_data:add_node(Node),
    {error, {not_found, ?N1}} = mesos_scheduler_data:delete_node(?N1),
    [] = mesos_scheduler_data:get_all_nodes(),

    ok = mesos_scheduler_data:add_cluster(Cluster),
    ok = mesos_scheduler_data:add_node(Node),
    {ok, #rms_node{status = requested}} = mesos_scheduler_data:get_node(?N1),
    {ok, #rms_cluster{nodes = [?N1]}} = mesos_scheduler_data:get_cluster(?C1),
    [#rms_node{status = requested}] = mesos_scheduler_data:get_all_nodes(),
    [#rms_cluster{nodes = [?N1]}] = mesos_scheduler_data:get_all_clusters(),

    ok = mesos_scheduler_data:delete_node(?N1),
    {error, {not_found, ?N1}} = mesos_scheduler_data:get_node(?N1),
    [] = mesos_scheduler_data:get_all_nodes(),

    {ok, #rms_cluster{nodes = []}} = mesos_scheduler_data:get_cluster(?C1),
    [#rms_cluster{nodes = []}] = mesos_scheduler_data:get_all_clusters().

duplicate_adds() ->
    Cluster = #rms_cluster{key = ?C1, status = requested, nodes = []},
    Node = #rms_node{key = ?N1, status = requested, cluster = ?C1},

    ok = mesos_scheduler_data:add_cluster(Cluster),
    ok = mesos_scheduler_data:add_node(Node),

    {error, {cluster_exists, ?C1}} = mesos_scheduler_data:add_cluster(Cluster),
    {error, {node_exists, ?N1}} = mesos_scheduler_data:add_node(Node).

test_persistence() ->
    Cluster = #rms_cluster{key = ?C1, status = requested, nodes = []},
    Node = #rms_node{key = ?N1, status = active, cluster = ?C1},

    %% Check that writes and updates are persisted
    ok = mesos_scheduler_data:add_cluster(Cluster),
    ok = mesos_scheduler_data:add_node(Node),
    ok = mesos_scheduler_data:set_cluster_status(?C1, active),

    ok = mesos_scheduler_data:stop(),
    {ok, _} = mesos_scheduler_data:start_link(),

    {ok, #rms_cluster{status = active, nodes = [?N1]}} = mesos_scheduler_data:get_cluster(?C1),

    %% Check that deleted records are correctly de-persisted
    ok = mesos_scheduler_data:delete_node(?N1),
    ok = mesos_scheduler_data:delete_cluster(?C1),

    ok = mesos_scheduler_data:stop(),
    {ok, _} = mesos_scheduler_data:start_link(),

    {error, {not_found, ?N1}} = mesos_scheduler_data:get_node(?N1),
    {error, {not_found, ?C1}} = mesos_scheduler_data:get_node(?C1).
