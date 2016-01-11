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

-module(riak_mesos_wm_resource).
-export([
  routes/0,
  dispatch/2
]).
-export([
  get_clusters/1,
  cluster_exists/1,
  create_cluster/1,
  delete_cluster/1,
  get_cluster/1,
  restart_cluster/1,
  riak_conf/1,
  set_riak_conf/1,
  advanced_config/1,
  set_advanced_config/1
]).
-export([
  get_nodes/1,
  node_exists/1,
  create_node_and_path/1,
  noop_create_node/1,
  delete_node/1,
  get_node/1,
  restart_node/1
]).
-export([
  healthcheck/1
]).
-export([init/1]).
-export([
  service_available/2,
  allowed_methods/2,
  content_types_provided/2,
  content_types_accepted/2,
  resource_exists/2,
  provide_content/2,
  delete_resource/2,
  process_post/2,
  provide_text_content/2,
  accept_content/2,
  post_is_create/2,
  create_path/2]).

-define(api_base, "api").
-define(api_version, "v1").
-define(api_route, [?api_base, ?api_version]).
-define(accept(T), {T, accept_content}).
-define(provide(T), {T, provide_content}).
-define(json_type, "application/json").
-define(text_type, "plain/text").
-define(octet_type, "application/octet-stream").
-define(form_type, "application/x-www-form-urlencoded").
-define(provide_text, [{?text_type, provide_text_content}]).
-define(accept_text, [?accept(?form_type),
                      ?accept(?octet_type),
                      ?accept(?text_type)]).

-record(route, {
    base = ?api_route :: [string()],
    path :: [string() | atom()],
    methods = ['GET'] :: [atom()],
    accepts = [] :: [{string(), atom()}],
    provides = [?provide(?json_type)] :: [{string(), atom()}],
    exists = true :: {module(), atom()} | boolean(),
    content = [{success, true}] :: {module(), atom()} | nonempty_list(),
    accept :: {module(), atom()} | undefined,
    delete :: {module(), atom()} | undefined,
    post_create = false :: boolean(),
    post_path :: {module(), atom()} | undefined
}).

-type route() :: #route{}.

-record(ctx, {
    route :: route()
}).

-include_lib("webmachine/include/webmachine.hrl").

-include("mesos_scheduler_data.hrl").

%%%===================================================================
%%% API
%%%===================================================================

routes() ->
    [
    % Clusters
    #route{path=["clusters"],
           content={?MODULE, get_clusters}},
    #route{path=["clusters", cluster],
           methods=['GET', 'PUT', 'DELETE'], exists={?MODULE, cluster_exists},
           content={?MODULE, get_cluster},
           accepts=?accept_text, accept={?MODULE, create_cluster},
           delete={?MODULE, delete_cluster}},
    #route{path=["clusters", cluster, "restart"],
           methods=['POST'], exists={?MODULE, cluster_exists},
           accepts=?accept_text, accept={?MODULE, restart_cluster}},
    #route{path=["clusters", cluster, "riak.conf"],
           methods=['GET', 'PUT'], exists={?MODULE, cluster_exists},
           provides=?provide_text, content={?MODULE, riak_conf},
           accepts=?accept_text,   accept={?MODULE, set_riak_conf}},
    #route{path=["clusters", cluster, "advanced.config"],
           methods=['GET', 'PUT'], exists={?MODULE, cluster_exists},
           provides=?provide_text, content={?MODULE, advanced_config},
           accepts=?accept_text,   accept={?MODULE, set_advanced_config}},
    % Nodes
    #route{path=["clusters", cluster, "nodes"],
           methods=['GET', 'POST'],
           post_create=true, post_path={?MODULE, create_node_and_path},
           accepts=?accept_text, accept={?MODULE, noop_create_node},
           content={?MODULE, get_nodes}},
    #route{path=["clusters", cluster, "nodes", node],
           methods=['GET', 'DELETE'], exists={?MODULE, node_exists},
           content={?MODULE, get_node},
           delete={?MODULE, delete_node}},
    #route{path=["clusters", cluster, "nodes", node, "restart"],
           methods=['POST'], exists={?MODULE, node_exists},
           accepts=?accept_text, accept={?MODULE, restart_node}},
    % Healthcheck
    #route{base=[], path=["healthcheck"],
           content={?MODULE, healthcheck}}
    ].

dispatch(Ip, Port) ->
    Resources = build_wm_routes(routes(), []),
    [
        {ip, Ip},
        {port, Port},
        {nodelay, true},
        {log_dir, "log"},
        {dispatch, lists:flatten(Resources)}
    ].

%% Clusters

get_clusters(RD) ->
    ClusterList = [C#rms_cluster.key || C <- mesos_scheduler_data:get_all_clusters()],
    {[{clusters, ClusterList}], RD}.

cluster_exists(RD) ->
    {true, RD}.

create_cluster(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

delete_cluster(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

get_cluster(RD) ->
    ClusterKey = list_to_binary(wrq:path_info(cluster, RD)),
    ClusterData = [{ClusterKey, [
        {key, ClusterKey},
        {status, active},
        {nodes, [
            list_to_binary(wrq:path_info(cluster, RD) ++ "-1"),
            list_to_binary(wrq:path_info(cluster, RD) ++ "-2"),
            list_to_binary(wrq:path_info(cluster, RD) ++ "-3")
        ]},
        {node_cpus, 2.0},
        {node_mem, 2048.0},
        {node_disk, 20000.0},
        {node_ports, 3},
        {riak_conf, <<"riak configuration">>},
        {advanced_config, <<"advanced configuration">>}
    ]}],
    {ClusterData, RD}.

restart_cluster(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

riak_conf(RD) ->
    ClusterKey = list_to_binary(wrq:path_info(cluster, RD)),
    {Cluster, RD1} = get_cluster(RD),
    ClusterInfo = proplists:get_value(ClusterKey, Cluster),
    {proplists:get_value(riak_conf, ClusterInfo), RD1}.

set_riak_conf(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

advanced_config(RD) ->
    ClusterKey = list_to_binary(wrq:path_info(cluster, RD)),
    {Cluster, RD1} = get_cluster(RD),
    ClusterInfo = proplists:get_value(ClusterKey, Cluster),
    {proplists:get_value(advanced_config, ClusterInfo), RD1}.

set_advanced_config(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

%% Nodes

get_nodes(RD) ->
    ClusterKeyStr = wrq:path_info(cluster, RD),
    Nodes = [{nodes, [
        list_to_binary(ClusterKeyStr ++ "-1"),
        list_to_binary(ClusterKeyStr ++ "-2"),
        list_to_binary(ClusterKeyStr ++ "-3")
    ]}],
    {Nodes, RD}.

node_exists(RD) ->
    {ClusterExists, RD1} = cluster_exists(RD),
    {ClusterExists and true, RD1}.

create_node_and_path(RD) ->
    ClusterKeyStr = wrq:path_info(cluster, RD),
    Path = ClusterKeyStr ++ "-1",
    Body = [{success, true}],
    {Path, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

noop_create_node(RD) ->
    {true, RD}.

delete_node(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

get_node(RD) ->
    NodeKey = list_to_binary(wrq:path_info(node, RD)),
    NodeData = [{NodeKey, [
        {key, NodeKey},
        {status, active},
        {location, [
            {node_name, list_to_binary(wrq:path_info(node, RD) ++ "@somehost")},
            {hostname, <<"somehost">>},
            {http_port, 1234},
            {pb_port, 1235},
            {disterl_port, 1236},
            {slave_id, <<"some_slave_id">>}
        ]},
        {container_path, <<"root">>},
        {persistence_id, <<"some_uuid">>}
    ]}],
    {NodeData, RD}.

restart_node(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

healthcheck(RD) ->
    {[{success, true}], RD}.

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, #ctx{}}.

service_available(RD, Ctx) -> {true, RD, Ctx#ctx{route=get_route(routes(), RD)}}.

allowed_methods(RD, Ctx=#ctx{route=Route}) ->
    {Route#route.methods, RD, Ctx}.

content_types_provided(RD, Ctx=#ctx{route=Route}) ->
    {Route#route.provides, RD, Ctx}.

content_types_accepted(RD, Ctx=#ctx{route=Route}) ->
    {Route#route.accepts, RD, Ctx}.

resource_exists(RD, Ctx=#ctx{route=#route{exists={M,F}}}) ->
    {Success, RD1} = M:F(RD),
    {Success, RD1, Ctx};
resource_exists(RD, Ctx=#ctx{route=#route{exists=Exists}}) when is_boolean(Exists) ->
    {Exists, RD, Ctx}.

delete_resource(RD, Ctx=#ctx{route=#route{delete={M,F}}}) ->
    {Success, RD1} = M:F(RD),
    {Success, RD1, Ctx}.

provide_content(RD, Ctx=#ctx{route=#route{content={M,F}}}) ->
    {Body, RD1} = M:F(RD),
    {mochijson2:encode(Body), RD1, Ctx}.

provide_text_content(RD, Ctx=#ctx{route=#route{content={M,F}}}) ->
    {Body, RD1} = M:F(RD),
    {binary_to_list(Body), RD1, Ctx}.

accept_content(RD, Ctx=#ctx{route=#route{accept={M,F}}}) ->
    {Success, RD1} = M:F(RD),
    {Success, RD1, Ctx};
accept_content(RD, Ctx=#ctx{route=#route{accept=undefined}}) ->
    {false, RD, Ctx}.

process_post(RD, Ctx=#ctx{route=#route{accept={M,F}}}) ->
    {Success, RD1} = M:F(RD),
    {Success, RD1, Ctx}.

post_is_create(RD, Ctx=#ctx{route=#route{post_create=PostCreate}}) ->
    {PostCreate, RD, Ctx}.

create_path(RD, Ctx=#ctx{route=#route{post_path={M,F}}}) ->
    {Path, RD1} = M:F(RD),
    {Path, RD1, Ctx}.

%% ====================================================================
%% WM Util
%% ====================================================================

get_route([], _RD) ->
    undefined;
get_route([Route|Rest], RD) ->
    BaseLength = length(Route#route.base),
    Tokens = string:tokens(wrq:path(RD), "/"),
    PathTokensLength = length(Tokens),
    case BaseLength =< PathTokensLength of
        true ->
            ReqPath = lists:nthtail(BaseLength, Tokens),
            case expand_path(Route#route.path, RD, []) of
                ReqPath -> Route;
                _ -> get_route(Rest, RD)
            end;
        _ -> get_route(Rest, RD)
    end.

expand_path([], _, Accum) ->
    lists:reverse(Accum);
expand_path([Part|Rest], RD, Accum) when is_list(Part) ->
    expand_path(Rest, RD, [Part|Accum]);
expand_path([Part|Rest], RD, Accum) when is_atom(Part) ->
    expand_path(Rest, RD, [wrq:path_info(Part, RD)|Accum]).

build_wm_routes([], Accum) ->
    [lists:reverse(Accum)];
build_wm_routes([#route{base=Base, path=Path}|Rest], Accum) ->
    build_wm_routes(Rest, [{Base ++ Path, ?MODULE, []}|Accum]).
