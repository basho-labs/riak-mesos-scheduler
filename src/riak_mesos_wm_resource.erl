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
  dispatch/2,
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
  accept_content/2]).

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
    delete :: {module(), atom()} | undefined
}).

-type route() :: #route{}.

-record(ctx, {
    route :: route()
}).

-include_lib("webmachine/include/webmachine.hrl").

%%%===================================================================
%%% API
%%%===================================================================

routes() ->
    [
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
           accepts=?accept_text,   accept={?MODULE, set_advanced_config}}
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

get_clusters(RD) -> {[{clusters, [<<"default">>]}], RD}.

cluster_exists(RD) -> {true, RD}.

create_cluster(RD) -> {true, RD}.

delete_cluster(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

get_cluster(RD) ->
    ClusterKey = list_to_binary(wrq:path_info(cluster, RD)),
    ClusterData = [{ClusterKey, [
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
    {Success, RD1, Ctx};
process_post(RD, Ctx=#ctx{route=#route{accept=undefined}}) ->
    {false, RD, Ctx}.

%% ====================================================================
%% WM Util
%% ====================================================================

get_route([], _RD) ->
    undefined;
get_route([Route|Rest], RD) ->
    BaseLength = length(Route#route.base),
    ReqPath = lists:nthtail(BaseLength, string:tokens(wrq:path(RD), "/")),
    case expand_path(Route#route.path, RD, []) of
        ReqPath -> Route;
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

% halt(Code, RD, Ctx) ->
%     {{halt, Code}, RD, Ctx}.
% halt(Code, Headers, RD, Ctx) ->
%     {{halt, Code}, wrq:set_resp_headers(Headers, RD), Ctx}.
% halt(Code, Headers, Data, RD, Ctx) ->
%     {{halt, Code}, wrq:set_resp_headers(Headers, wrq:set_resp_body(Data, RD)), Ctx}.
%
% halt_json(Code, Data, RD, Ctx) ->
%     halt(Code, [{<<"Content-Type">>, <<"application/json">>}],
%          mochijson2:encode(Data), RD, Ctx).
