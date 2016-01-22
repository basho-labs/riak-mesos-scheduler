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
  update_cluster_with_new_node/1,
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
    ClusterList = [list_to_binary(C#rms_cluster.key) ||
                   C <- mesos_scheduler_data:get_all_clusters()],
    {[{clusters, ClusterList}], RD}.

cluster_exists(RD) ->
    Cluster = wrq:path_info(cluster, RD),
    Result = case mesos_scheduler_data:get_cluster(Cluster) of
                 {error, {not_found, _}} -> false;
                 {ok, _} -> true
             end,
    {Result, RD}.

create_cluster(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    Cluster = #rms_cluster{key = ClusterKey},
    Response = build_response(fun mesos_scheduler_data:add_cluster/1, [Cluster]),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), RD)}.

delete_cluster(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    ResponseBody = build_response(fun mesos_scheduler_data:delete_cluster/1, [ClusterKey]),
    {true, wrq:append_to_response_body(mochijson2:encode(ResponseBody), RD)}.

get_cluster(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    {ok, Cluster} = mesos_scheduler_data:get_cluster(ClusterKey),
    #rms_cluster{
       status = Status,
       riak_conf = RiakConf,
       advanced_config = AdvancedConfig,
       nodes = Nodes
    } = Cluster,
    NodeData = [list_to_binary(NodeKey) || NodeKey <- Nodes],
    ClusterData = [{ClusterKey, [
        {key, list_to_binary(ClusterKey)},
        {status, Status},
        {nodes, NodeData},
        %%{node_cpus, 2.0},
        %%{node_mem, 2048.0},
        %%{node_disk, 20000.0},
        %%{node_ports, 3},
        {riak_conf, list_to_binary(RiakConf)},
        {advanced_config, list_to_binary(AdvancedConfig)}
    ]}],
    {ClusterData, RD}.

restart_cluster(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

riak_conf(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    {ok, Cluster} = mesos_scheduler_data:get_cluster(ClusterKey),
    RiakConf = Cluster#rms_cluster.riak_conf,
    {RiakConf, RD}.

set_riak_conf(RD) ->
    Config = binary_to_list(wrq:req_body(RD)),
    UpdateFun = fun(Cluster) -> Cluster#rms_cluster{riak_conf = Config} end,
    update_cluster(RD, UpdateFun).

update_cluster(RD, UpdateFun) ->
    ClusterKey = wrq:path_info(cluster, RD),
    ReplyBody = build_response(fun mesos_scheduler_data:update_cluster/2, [ClusterKey, UpdateFun]),
    {true, wrq:append_to_response_body(mochijson2:encode(ReplyBody), RD)}.

advanced_config(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    {ok, Cluster} = mesos_scheduler_data:get_cluster(ClusterKey),
    AdvancedConfig = Cluster#rms_cluster.advanced_config,
    {AdvancedConfig, RD}.

set_advanced_config(RD) ->
    Config = binary_to_list(wrq:req_body(RD)),
    UpdateFun = fun(Cluster) -> Cluster#rms_cluster{advanced_config = Config} end,
    update_cluster(RD, UpdateFun).


%% Nodes

get_nodes(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    case mesos_scheduler_data:get_cluster(ClusterKey) of
        {ok, #rms_cluster{nodes = Nodes}} ->
            Result = [{nodes, [list_to_binary(N) || N <- Nodes]}],
            {Result, RD};
        {error, Error} ->
            ErrStr = iolist_to_binary(io_lib:format("~p", [Error])),
            [{success, false}, {error, ErrStr}]
    end.

node_exists(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    NodeKey = wrq:path_info(node, RD),
    case mesos_scheduler_data:get_cluster(ClusterKey) of
        {error, {not_found, _}} ->
            {false, RD};
        {ok, Cluster} ->
            case lists:member(NodeKey, Cluster#rms_cluster.nodes) of
                false ->
                    {false, RD};
                true ->
                    case mesos_scheduler_data:get_node(NodeKey) of
                        {error, {not_found, _}} ->
                            {false, RD};
                        {ok, _Node} ->
                            {true, RD}
                    end
            end
    end.

create_node_and_path(RD) ->
    ClusterKey = wrq:path_info(cluster, RD),
    case mesos_scheduler_data:update_cluster(ClusterKey, fun update_cluster_with_new_node/1) of
        {error, {not_found, _}} ->
            [{success, false}, {error, <<"cluster not found">>}];
        {ok, Cluster} ->
            %% After adding the new node, the node name should be at the
            %% head of the cluster's node list:
            #rms_cluster{nodes = [NewNodeName | _]} = Cluster,
            NewNode = #rms_node{
                         key = NewNodeName,
                         cluster = ClusterKey
                        },
            Result = build_response(fun mesos_scheduler_data:add_node/1, [NewNode]),
            {NewNodeName, wrq:append_to_response_body(mochijson2:encode(Result), RD)}
    end.

update_cluster_with_new_node(Cluster) ->
    #rms_cluster{
       key = Key,
       nodes = Nodes
      } = Cluster,
    %% First thing we need to do is figure out what to name our new node.
    %% We want to pick something of the form ClusterName-NodeNumber where
    %% NodeNumber is an ascending integer.
    Pattern = Key ++ "-(\\d+)",
    MatchResults = [re:run(Node, Pattern, [{capture, [1], list}]) || Node <- Nodes],
    ExistingNodeNumbers = [list_to_integer(NodeNum) || {match, [NodeNum]} <- MatchResults],
    MaxNodeNumber = lists:max([0 | ExistingNodeNumbers]), %% Prepend 0 in case we have no nodes yet
    NewNodeName = lists:append([Key, "-", integer_to_list(MaxNodeNumber + 1)]),

    NewClusterNodes = [NewNodeName | Nodes],
    Cluster#rms_cluster{nodes = NewClusterNodes}.

noop_create_node(RD) ->
    {true, RD}.

delete_node(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

get_node(RD) ->
    NodeKey = wrq:path_info(node, RD),
    %% Slight chance of crash here if a node is deleted after the call to node_exists
    %% but before we call get_node. Should be a small enough chance to be negligible
    %% in practice, though, and if we do crash it should harmlessly fail the HTTP request.
    {ok, Node} = mesos_scheduler_data:get_node(NodeKey),

    #rms_node{
       status = Status,
       node_name = NodeName,
       hostname = Hostname,
       http_port = HttpPort,
       pb_port = PbPort,
       disterl_port = DisterlPort,
       slave_id = SlaveId,
       container_path = ContainerPath,
       persistence_id = PersistenceId
      } = Node,

    NodeData = [{list_to_binary(NodeKey), [
        {key, list_to_binary(NodeKey)},
        {status, Status},
        {location, [
            {node_name, NodeName},
            {hostname, list_to_binary(Hostname)},
            {http_port, HttpPort},
            {pb_port, PbPort},
            {disterl_port, DisterlPort},
            {slave_id, list_to_binary(SlaveId)}
        ]},
        {container_path, list_to_binary(ContainerPath)},
        {persistence_id, list_to_binary(PersistenceId)}
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
    case is_binary(Body) of
        true ->
            {binary_to_list(Body), RD1, Ctx};
        false ->
            {Body, RD1, Ctx}
    end.

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

build_response(Fun, Args) ->
    case apply(Fun, Args) of
        ok ->
            [{success, true}];
        {ok, _} ->
            [{success, true}];
        {error, Error} ->
            ErrStr = iolist_to_binary(io_lib:format("~p", [Error])),
            [{success, false}, {error, ErrStr}]
    end.
