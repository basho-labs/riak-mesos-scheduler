%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_mesos_wm_node).
-export([routes/0, dispatch/0]).
-export([init/1]).
-export([service_available/2,
         allowed_methods/2,
         content_types_provided/2,
         content_types_accepted/2,
         resource_exists/2,
         provide_content/2,
         accept_content/2]).

-record(ctx, {
    cluster :: string() | undefined,
    node :: string | undefined,
    resource :: string() | undefined,
    method :: atom()
}).

-include_lib("webmachine/include/webmachine.hrl").

-define(noCluster(),
    #ctx{cluster=undefined}).
-define(nodes(Cluster),
    #ctx{method='GET', cluster=Cluster, node=undefined}).
-define(createNode(Cluster),
    #ctx{method='POST', cluster=Cluster, node=undefined}).
-define(restart(Cluster, Node),
    #ctx{cluster=Cluster, node=Node, resource="restart"}).
-define(node(Cluster, Node),
    #ctx{cluster=Cluster, node=Node}).

%%%===================================================================
%%% API
%%%===================================================================

routes() ->
    riak_mesos_wm_util:build_routes([
        ["clusters", cluster, "nodes"],
        ["clusters", cluster, "nodes", node],
        ["clusters", cluster, "nodes", node, "restart"]
    ]).

dispatch() -> lists:map(fun(Route) -> {Route, ?MODULE, []} end, routes()).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, #ctx{}}.

service_available(RD, Ctx) ->
    Cluster1 = wrq:path_info(cluster, RD),
    Cluster = case cluster_exists(Cluster1) of
        false -> undefined;
        _ -> Cluster1
    end,
    {true, RD, Ctx#ctx{
        cluster = Cluster,
        node = wrq:path_info(node, RD),
        resource = riak_mesos_wm_util:path_part(5, RD),
        method = wrq:method(RD)
    }}.

allowed_methods(RD, Ctx=?nodes(_)) -> {['GET'], RD, Ctx};
allowed_methods(RD, Ctx=?createNode(_)) -> {['POST'], RD, Ctx};
allowed_methods(RD, Ctx=?restart(_, _)) -> {['POST'], RD, Ctx};
allowed_methods(RD, Ctx=?node(_, _)) -> {['GET', 'DELETE'], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    {[{"application/json", provide_content}], RD, Ctx}.

content_types_accepted(RD, Ctx) ->
    {[
        {"application/octet-stream", accept_content}
     ], RD, Ctx}.

resource_exists(RD, Ctx=?noCluster()) ->
    {false, RD, Ctx};
resource_exists(RD, Ctx=?nodes(_Cluster)) ->
    {true, RD, Ctx};
resource_exists(RD, Ctx=?createNode(Cluster)) ->
    Response = create_node(Cluster),
    riak_mesos_wm_util:halt_json(200, Response, RD, Ctx);
resource_exists(RD, Ctx=?restart(Cluster, Node)) ->
    case node_exists(Cluster, Node) of
        true ->
            Response = restart_node(Cluster, Node),
            riak_mesos_wm_util:halt_json(200, Response, RD, Ctx);
        false ->
            {false, RD, Ctx}
    end;
resource_exists(RD, Ctx=?node(Cluster, Node)) ->
    {node_exists(Cluster, Node), RD, Ctx};
resource_exists(RD, Ctx) -> {false, RD, Ctx}.

provide_content(RD, Ctx=?nodes(Cluster)) ->
    Data = get_nodes(Cluster),
    {mochijson2:encode(Data), RD, Ctx};
provide_content(RD, Ctx=?node(Cluster, Node)) ->
    Data = get_node(Cluster, Node),
    {mochijson2:encode(Data), RD, Ctx}.

accept_content(RD, Ctx) -> {false, RD, Ctx}.

%% ====================================================================
%% Private
%% ====================================================================

get_nodes(Cluster) ->
    [{nodes, [
        list_to_binary(Cluster ++ "-1"),
        list_to_binary(Cluster ++ "-2"),
        list_to_binary(Cluster ++ "-3")
    ]}].

create_node(_Cluster) -> [{success, true}].

get_node(_, Node) ->
    [{list_to_binary(Node), [
        {key, list_to_binary(Node)},
        {status, active},
        {location, [
            {node_name, list_to_binary(Node ++ "@somehost")},
            {hostname, <<"somehost">>},
            {http_port, 1234},
            {pb_port, 1235},
            {disterl_port, 1236},
            {slave_id, <<"some_slave_id">>}
        ]},
        {container_path, <<"root">>},
        {persistence_id, <<"some_uuid">>}
    ]}].

restart_node(_Cluster, _Node) -> [{success, true}].

node_exists(_Cluster, _Node) -> true.

cluster_exists(_Cluster) -> true.
