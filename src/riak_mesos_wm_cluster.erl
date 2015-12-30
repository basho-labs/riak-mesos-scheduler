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

-module(riak_mesos_wm_cluster).
-export([routes/0, special_routes/0]).
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
    resource :: string() | undefined,
    method :: atom()
}).

-include_lib("webmachine/include/webmachine.hrl").

-define(clusters(),
    #ctx{cluster=undefined}).
-define(config(Cluster),
    #ctx{cluster=Cluster, resource="riak.conf"}).
-define(advanced(Cluster),
    #ctx{cluster=Cluster, resource="advanced.config"}).
-define(restart(Cluster),
    #ctx{cluster=Cluster, resource="restart"}).
-define(createCluster(Cluster),
    #ctx{method='PUT', cluster=Cluster}).
-define(cluster(Cluster),
    #ctx{cluster=Cluster}).

%%%===================================================================
%%% API
%%%===================================================================

special_routes() ->
    [
        {clusters,
            ['GET'],
            [{"application/json", provide_content}],
            ["clusters"]},
        {cluster,
            ['GET', 'PUT', 'DELETE'],
            [{"application/json", provide_content}],
            ["clusters", cluster]},
        {restart,
            ['POST'],
            [{"application/json", provide_content}],
            ["clusters", cluster, "restart"]},
        {'riak.conf',
            ['GET', 'PUT'],
            [{"plain/text", provide_content}],
            ["clusters", cluster, "riak.conf"]},
        {'advanced.config',
            ['GET', 'PUT'],
            [{"plain/text", provide_content}],
            ["clusters", cluster, "advanced.config"]}
    ].

routes() ->
    [
        ["clusters"],
        ["clusters", cluster],
        ["clusters", cluster, "restart"],
        ["clusters", cluster, "riak.conf"],
        ["clusters", cluster, "advanced.config"]
    ].

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, #ctx{}}.

service_available(RD, Ctx) ->
    lager:info("RD: ~p", [RD]),
    {true, RD, Ctx#ctx{
        cluster = wrq:path_info(cluster, RD),
        resource = riak_mesos_wm_util:path_part(3, RD),
        route = riak_mesos_wm_util:reverse_lookup_route(special_routes(), RD)
    }}.

allowed_methods(RD, Ctx=#ctx{route={_,Methods,_,_}}) -> {Methods, RD, Ctx}.

content_types_provided(RD, Ctx=#ctx{route={_,_,Types,_}}) -> {Types, RD, Ctx}.

content_types_accepted(RD, Ctx) ->
    {[
        {"application/octet-stream", accept_content},
        {"application/x-www-form-urlencoded", accept_content},
        {"plain/text", accept_content}
     ], RD, Ctx}.

resource_exists(RD, Ctx=?clusters()) ->
    {true, RD, Ctx};
resource_exists(RD, Ctx=?config(_)) ->
    {true, RD, Ctx};
resource_exists(RD, Ctx=?advanced(_)) ->
    {true, RD, Ctx};
resource_exists(RD, Ctx=?createCluster(Cluster)) ->
    case cluster_exists(Cluster) of
        true ->
            Response = [{error, <<"Cluster already exists">>}],
            riak_mesos_wm_util:halt_json(409, Response, RD, Ctx);
        false ->
            Response = create_cluster(Cluster),
            riak_mesos_wm_util:halt_json(200, Response, RD, Ctx)
    end,
    riak_mesos_wm_util:halt_json(200, Response, RD, Ctx);
resource_exists(RD, Ctx=?restart(Cluster)) ->
    case cluster_exists(Cluster) of
        true ->
            Response = restart_cluster(Cluster),
            riak_mesos_wm_util:halt_json(200, Response, RD, Ctx);
        false ->
            {false, RD, Ctx}
    end;
resource_exists(RD, Ctx=?cluster(Cluster)) ->
    {cluster_exists(Cluster), RD, Ctx};
resource_exists(RD, Ctx) -> {false, RD, Ctx}.

provide_content(RD, Ctx=?clusters()) ->
    Data = get_clusters(),
    {mochijson2:encode(Data), RD, Ctx};
provide_content(RD, Ctx=?config(Cluster)) ->
    Data = get_cluster(Cluster),
    ClusterInfo = proplists:get_value(list_to_binary(Cluster), Data),
    Config = proplists:get_value(riak_conf, ClusterInfo),
    {binary_to_list(Config), RD, Ctx};
provide_content(RD, Ctx=?advanced(Cluster)) ->
    Data = get_cluster(Cluster),
    ClusterInfo = proplists:get_value(list_to_binary(Cluster), Data),
    Config = proplists:get_value(advanced_config, ClusterInfo),
    {binary_to_list(Config), RD, Ctx};
provide_content(RD, Ctx=?cluster(Cluster)) ->
    Data = get_cluster(Cluster),
    {mochijson2:encode(Data), RD, Ctx}.

accept_content(RD, Ctx=?config(Cluster)) ->
    Response = set_config(Cluster, wrq:req_body(RD)),
    riak_mesos_wm_util:halt_json(200, Response, RD, Ctx);
accept_content(RD, Ctx=?advanced(Cluster)) ->
    Response = set_advanced(Cluster, wrq:req_body(RD)),
    riak_mesos_wm_util:halt_json(200, Response, RD, Ctx);
accept_content(RD, Ctx) -> {false, RD, Ctx}.

%% ====================================================================
%% Private
%% ====================================================================

get_clusters() -> [{clusters, [<<"default">>]}].

create_cluster(_Cluster) -> [{success, true}].

get_cluster(Cluster) ->
    [{list_to_binary(Cluster), [
        {key, <<"default">>},
        {status, active},
        {nodes, [
            <<"default-1">>,
            <<"default-2">>,
            <<"default-3">>
        ]},
        {node_cpus, 2.0},
        {node_mem, 2048.0},
        {node_disk, 20000.0},
        {node_ports, 3},
        {riak_conf, <<"riak configuration">>},
        {advanced_config, <<"advanced configuration">>}
    ]}].

restart_cluster(_Cluster) -> [{success, true}].

set_config(_Cluster, _Conf) -> [{success, true}].

set_advanced(_Cluster, _Conf) -> [{success, true}].

cluster_exists(_Cluster) -> true.
