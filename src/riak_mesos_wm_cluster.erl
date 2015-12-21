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
    resource :: string() | undefined,
    method :: atom()
}).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_mesos.hrl").

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

routes() ->
    riak_mesos_wm_util:build_routes([
        ["clusters"],
        ["clusters", cluster],
        ["clusters", cluster, "restart"],
        ["clusters", cluster, "riak.conf"],
        ["clusters", cluster, "advanced.config"]
    ]).

dispatch() -> lists:map(fun(Route) -> {Route, ?MODULE, []} end, routes()).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, #ctx{}}.

service_available(RD, Ctx) ->
    {true, RD, Ctx#ctx{
        cluster = wrq:path_info(cluster, RD),
        resource = riak_mesos_wm_util:path_part(3, RD),
        method = wrq:method(RD)
    }}.

allowed_methods(RD, Ctx=?clusters()) -> {['GET'], RD, Ctx};
allowed_methods(RD, Ctx=?config(_)) -> {['GET', 'PUT'], RD, Ctx};
allowed_methods(RD, Ctx=?advanced(_)) -> {['GET', 'PUT'], RD, Ctx};
allowed_methods(RD, Ctx=?restart(_)) -> {['POST'], RD, Ctx};
allowed_methods(RD, Ctx=?createCluster(_)) -> {['PUT'], RD, Ctx};
allowed_methods(RD, Ctx=?cluster(_)) -> {['GET', 'DELETE'], RD, Ctx}.

content_types_provided(RD, Ctx=?config(_)) ->
    {[{"plain/text", provide_content}], RD, Ctx};
content_types_provided(RD, Ctx=?advanced(_)) ->
    {[{"plain/text", provide_content}], RD, Ctx};
content_types_provided(RD, Ctx) ->
    {[{"application/json", provide_content}], RD, Ctx}.

content_types_accepted(RD, Ctx) ->
    {[
        {"application/octet-stream", accept_content},
        {"application/x-www-form-urlencoded", accept_content},
        {"plain/text", accept_content}
     ], RD, Ctx}.

resource_exists(RD, Ctx=?clusters()) ->
    {true, RD, Ctx};
resource_exists(RD, Ctx=?createCluster(Cluster)) ->
    Response = create_cluster(Cluster),
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
    Config = proplists:get_value(<<"riak.conf">>, Data),
    {binary_to_list(Config), RD, Ctx};
provide_content(RD, Ctx=?advanced(Cluster)) ->
    Data = get_cluster(Cluster),
    Config = proplists:get_value(<<"advanced.config">>, Data),
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

%% TODO: Populate from metadata store
get_clusters() -> [{clusters, [<<"default">>]}].

create_cluster(_Cluster) -> [{success, true}].

get_cluster(Cluster) ->
    [{list_to_binary(Cluster), [
        {<<"riak.conf">>, <<"riak configuration">>},
        {<<"advanced.config">>, <<"advanced configuration">>}
    ]}].

restart_cluster(_Cluster) -> [{success, true}].

set_config(_Cluster, _Conf) -> [{success, true}].

set_advanced(_Cluster, _Conf) -> [{success, true}].

cluster_exists("default") -> true;
cluster_exists(_Cluster) -> false.
