-module(rms_node_SUITE).

-export([all/0,
         suite/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         group/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2]).

%% test cases
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     {group, node_fsm}
    ].

suite() ->
    [{ct_hooks,[cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [
     {node_fsm, [], [
                     t_normal_start_stop
                    ]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

group(_Groupname) -> [].

init_per_group(_Groupname, Config) -> Config.  
end_per_group(_Groupname, _Config) -> ok.

init_per_testcase(_TestCase, Config) -> Config.
end_per_testcase(_TestCase, _Config) -> ok.

%t_normal_start_stop(_Config) ->
%    meck:new(rms_metadata),
%    meck:expect(rms_metadata, get_nodes, fun() -> [] end),
%    {ok, Sup} = rms_node_manager:start_link(),
%    meck:unload().

t_normal_start_stop(_Config) ->
    meck:new(rms_metadata),
    meck:expect(rms_metadata, update_node, fun(_, _) -> ok end),
    meck:expect(rms_metadata, get_node, fun("riak-default-1") ->
                                                {error, not_found} end),
    meck:expect(rms_metadata, add_node, fun(_) -> ok end),
    meck:expect(rms_metadata, delete_node, fun(_) -> ok end),
    meck:new(rms_cluster_manager),
    meck:expect(rms_cluster_manager, maybe_join, fun(_, _) -> ok end),
    meck:expect(rms_cluster_manager, node_started, fun(_, _) -> ok end),
    meck:expect(rms_cluster_manager, node_stopped, fun(_, _) -> ok end),
    meck:expect(rms_cluster_manager, leave, fun(_, _) -> ok end),
    NodeKey = "riak-default-1",
    ClusterKey = "default",
    {ok, Node} = rms_node:start_link(NodeKey, ClusterKey),
    {requested, _} = sys:get_state(Node),
    % set_reserve
    ok = rms_node:set_reserve(Node, "hostname", "agent_id_value", "persistence_id", []),
    {reserved, _} = sys:get_state(Node),
    % STAGING
    ok = rms_node:handle_status_update(Node, 'TASK_STAGING', ''),
    {starting, _} = sys:get_state(Node),
    % STARTING
    ok = rms_node:handle_status_update(Node, 'TASK_STARTING', ''),
    {starting, _} = sys:get_state(Node),
    % RUNNING
    ok = rms_node:handle_status_update(Node, 'TASK_RUNNING', ''),
    {started, _} = sys:get_state(Node),
    % destroy
    ok = rms_node:destroy(Node, false),
    {leaving, _} = sys:get_state(Node),
    % The FSM needs to figure out when the riak-node has left the cluster
    % FINISHED
    ok = rms_node:handle_status_update(Node, 'TASK_FINISHED', ''),
    
    meck:unload(rms_cluster_manager),
    meck:unload(rms_metadata).
