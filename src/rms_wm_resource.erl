-module(rms_wm_resource).

-export([routes/0, dispatch/2]).

-export([clusters/1,
         cluster_exists/1,
         get_cluster/1,
         add_cluster/1,
         delete_cluster/1,
         restart_cluster/1,
         get_cluster_riak_config/1,
         set_cluster_riak_config/1,
         get_cluster_advanced_config/1,
         set_cluster_advanced_config/1]).

-export([nodes/1,
         node_exists/1,
         get_node/1,
         add_node/1,
         delete_node/1,
         restart_node/1]).

-export([healthcheck/1]).

-export([init/1,
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

-define(API_BASE, "api").
-define(API_VERSION, "v1").
-define(API_ROUTE, [?API_BASE, ?API_VERSION]).
-define(ACCEPT(T), {T, accept_content}).
-define(PROVIDE(T), {T, provide_content}).
-define(JSON_TYPE, "application/json").
-define(TEXT_TYPE, "plain/text").
-define(OCTET_TYPE, "application/octet-stream").
-define(FORM_TYPE, "application/x-www-form-urlencoded").
-define(PROVIDE_TEXT, [{?TEXT_TYPE, provide_text_content}]).
-define(ACCEPT_TEXT, [?ACCEPT(?FORM_TYPE),
                      ?ACCEPT(?OCTET_TYPE),
                      ?ACCEPT(?TEXT_TYPE)]).

-record(route, {base = ?API_ROUTE :: [string()],
                path :: [string() | atom()],
                methods = ['GET'] :: [atom()],
                accepts = [] :: [{string(), atom()}],
                provides = [?PROVIDE(?JSON_TYPE)] :: [{string(), atom()}],
                exists = true :: {module(), atom()} | boolean(),
                content = [{success, true}] :: {module(), atom()} |
                          nonempty_list(),
                accept :: {module(), atom()} | undefined,
                delete :: {module(), atom()} | undefined,
                post_create = false :: boolean(),
                post_path :: {module(), atom()} | undefined}).

-type route() :: #route{}.

-record(ctx, {route :: route()}).

-include_lib("webmachine/include/webmachine.hrl").

%% External functions.

routes() ->
    [%% Clusters.
     #route{path = ["clusters"],
            content = {?MODULE, clusters}},
     #route{path = ["clusters", key],
            methods = ['GET', 'PUT', 'DELETE'],
            exists = {?MODULE, cluster_exists},
            content = {?MODULE, get_cluster},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, add_cluster},
            delete = {?MODULE, delete_cluster}},
     #route{path = ["clusters", key, "restart"],
            methods = ['POST'],
            exists = {?MODULE, cluster_exists},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, restart_cluster}},
     #route{path = ["clusters", key, "config"],
            methods = ['GET', 'PUT'],
            exists = {?MODULE, cluster_exists},
            provides = ?PROVIDE_TEXT,
            content = {?MODULE, get_cluster_riak_config},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, set_cluster_riak_config}},
     #route{path = ["clusters", key, "advancedConfig"],
            methods = ['GET', 'PUT'],
            exists = {?MODULE, cluster_exists},
            provides = ?PROVIDE_TEXT,
            content = {?MODULE, get_cluster_advanced_config},
            accepts = ?ACCEPT_TEXT,
            accept={?MODULE, set_cluster_advanced_config}},
     %% Nodes.
     #route{path = ["clusters", key, "nodes"],
            methods = ['GET', 'POST'],
            exists = {?MODULE, cluster_exists},
            accepts = ?ACCEPT_TEXT,
            content = {?MODULE, nodes},
            accept = {?MODULE, add_node}},
     #route{path = ["clusters", key, "nodes", node_key],
            methods = ['GET', 'DELETE'],
            exists = {?MODULE, node_exists},
            content = {?MODULE, get_node},
            delete = {?MODULE, delete_node}},
     #route{path = ["clusters", key, "nodes", node_key, "restart"],
            methods = ['POST'],
            exists = {?MODULE, node_exists},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, restart_node}},
     % Healthcheck
     #route{base = [],
            path = ["healthcheck"],
            content = {?MODULE, healthcheck}}].

dispatch(Ip, Port) ->
    Resources = build_wm_routes(routes(), []),
    [{ip, Ip},
     {port, Port},
     {nodelay, true},
     {log_dir, "log"},
     {dispatch, lists:flatten(Resources)}].

%% Clusters.

clusters(ReqData) ->
    KeyList = [list_to_binary(Key) ||
               Key <- rms_cluster_manager:get_cluster_keys()],
    {[{clusters, KeyList}], ReqData}.

cluster_exists(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Result = case rms_cluster_manager:get_cluster(Key) of
                 {ok, _Cluster} ->
                     true;
                 {error, not_found} ->
                     false
             end,
    {Result, ReqData}.

get_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    {ok, Cluster} = rms_cluster_manager:get_cluster(Key),
    Status = proplists:get_value(status, Cluster),
    RiakConfig = proplists:get_value(riak_config, Cluster),
    AdvancedConfig = proplists:get_value(advanced_config, Cluster),
    NodeKeys = proplists:get_value(node_keys, Cluster),
    BinNodeKeys = [list_to_binary(NodeKey) || NodeKey <- NodeKeys],
    ClusterData = [{Key, [{key, list_to_binary(Key)},
                          {status, Status},
                          {advanced_config, list_to_binary(AdvancedConfig)},
                          {riak_config, list_to_binary(RiakConfig)},
                          {node_keys, BinNodeKeys}]}],
    {ClusterData, ReqData}.

add_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = build_response(rms_cluster_manager:add_cluster(Key)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

delete_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = build_response(rms_cluster_manager:delete_cluster(Key)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response),
     ReqData)}.

restart_cluster(ReqData) ->
    %% TODO: implement restart call through rms_cluster_manager.
    Response = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

get_cluster_riak_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    {ok, RiakConfig} = rms_cluster_manager:get_cluster_riak_config(Key),
    Response = [{key, list_to_binary(Key)},
                {riak_config, list_to_binary(RiakConfig)}],
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

set_cluster_riak_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    RiakConfig = binary_to_list(wrq:req_body(ReqData)),
    Result = rms_cluster_manager:set_cluster_riak_config(Key, RiakConfig),
    Response = build_response(Result),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

get_cluster_advanced_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    {ok, AdvancedConfig} = rms_cluster_manager:get_cluster_advanced_config(Key),
    Response = [{key, list_to_binary(Key)},
                {advanced_config, list_to_binary(AdvancedConfig)}],
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

set_cluster_advanced_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    AdvancedConfig = binary_to_list(wrq:req_body(ReqData)),
    Result = rms_cluster_manager:set_cluster_advanced_config(Key,
                                                             AdvancedConfig),
    Response = build_response(Result),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

%% Nodes.

nodes(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    NodeKeyList = [list_to_binary(NodeKey) ||
                   NodeKey <- rms_node_manager:get_node_keys(Key)],
    {[{nodes, NodeKeyList}], ReqData}.

node_exists(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    NodeKeys = rms_node_manager:get_node_keys(Key),
    NodeKey = wrq:path_info(node_key, ReqData),
    Result = lists:member(NodeKey, NodeKeys),
    {Result, ReqData}.

get_node(ReqData) ->
    NodeKey = wrq:path_info(node_key, ReqData),
    {ok, Node} = rms_node_manager:get_node(NodeKey),
    Status = proplists:get_value(status, Node),
    NodeName = proplists:get_value(node_name, Node),
    Hostname = proplists:get_value(hostname, Node),
    HttpPort = proplists:get_value(http_port, Node),
    PbPort = proplists:get_value(pb_port, Node),
    DisterlPort = proplists:get_value(disterl_port, Node),
    AgentId = proplists:get_value(agent_id, Node),
    ContainerPath = proplists:get_value(container_path, Node),
    PersistenceId = proplists:get_value(persistence_id, Node),
    Location = [{node_name, list_to_binary(NodeName)},
                {hostname, list_to_binary(Hostname)},
                {http_port, HttpPort},
                {pb_port, PbPort},
                {disterl_port, DisterlPort},
                {slave_id, list_to_binary(AgentId)}],
    NodeData = [{NodeKey, [{key, list_to_binary(NodeKey)},
                           {status, Status},
                           {location, Location},
                           {container_path, list_to_binary(ContainerPath)},
                           {persistence_id, list_to_binary(PersistenceId)}]}],
    {NodeData, ReqData}.

add_node(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = build_response(rms_cluster_manager:add_node(Key)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

delete_node(ReqData) ->
    NodeKey = wrq:path_info(node_key, ReqData),
    Response = build_response(rms_node_manager:delete_node(NodeKey)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

restart_node(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

healthcheck(ReqData) ->
    {[{success, true}], ReqData}.

%% wm callback functions.

init(_) ->
    {ok, #ctx{}}.

service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx#ctx{route = get_route(routes(), ReqData)}}.

allowed_methods(ReqData, Ctx = #ctx{route = Route}) ->
    {Route#route.methods, ReqData, Ctx}.

content_types_provided(ReqData, Ctx = #ctx{route = Route}) ->
    {Route#route.provides, ReqData, Ctx}.

content_types_accepted(ReqData, Ctx = #ctx{route = Route}) ->
    {Route#route.accepts, ReqData, Ctx}.

resource_exists(ReqData, Ctx = #ctx{route = #route{exists = {M, F}}}) ->
    {Success, ReqData1} = M:F(ReqData),
    {Success, ReqData1, Ctx};
resource_exists(ReqData, Ctx = #ctx{route = #route{exists = Exists}})
  when is_boolean(Exists) ->
    {Exists, ReqData, Ctx}.

delete_resource(ReqData, Ctx = #ctx{route = #route{delete = {M, F}}}) ->
    {Success, ReqData1} = M:F(ReqData),
    {Success, ReqData1, Ctx}.

provide_content(ReqData, Ctx = #ctx{route = #route{content = {M, F}}}) ->
    {Body, ReqData1} = M:F(ReqData),
    {mochijson2:encode(Body), ReqData1, Ctx}.

provide_text_content(ReqData, Ctx = #ctx{route = #route{content = {M, F}}}) ->
    {Body, ReqData1} = M:F(ReqData),
    case is_binary(Body) of
        true ->
            {binary_to_list(Body), ReqData1, Ctx};
        false ->
            {Body, ReqData1, Ctx}
    end.

accept_content(ReqData, Ctx = #ctx{route = #route{accept = {M, F}}}) ->
    {Success, ReqData1} = M:F(ReqData),
    {Success, ReqData1, Ctx};
accept_content(ReqData, Ctx = #ctx{route = #route{accept = undefined}}) ->
    {false, ReqData, Ctx}.

process_post(ReqData, Ctx = #ctx{route = #route{accept = {M, F}}}) ->
    {Success, ReqData1} = M:F(ReqData),
    {Success, ReqData1, Ctx}.

post_is_create(ReqData, Ctx = #ctx{route = #route{post_create = PostCreate}}) ->
    {PostCreate, ReqData, Ctx}.

create_path(ReqData, Ctx = #ctx{route = #route{post_path = {M, F}}}) ->
    {Path, ReqData1} = M:F(ReqData),
    {Path, ReqData1, Ctx}.

%% Internal functions.

get_route([], _ReqData) ->
    undefined;
get_route([Route | Rest], ReqData) ->
    BaseLength = length(Route#route.base),
    Tokens = string:tokens(wrq:path(ReqData), "/"),
    PathTokensLength = length(Tokens),
    case BaseLength =< PathTokensLength of
        true ->
            ReqPath = lists:nthtail(BaseLength, Tokens),
            case expand_path(Route#route.path, ReqData, []) of
                ReqPath ->
                    Route;
                _ ->
                    get_route(Rest, ReqData)
            end;
        false ->
            get_route(Rest, ReqData)
    end.

expand_path([], _ReqData, Acc) ->
    lists:reverse(Acc);
expand_path([Part|Rest], ReqData, Acc) when is_list(Part) ->
    expand_path(Rest, ReqData, [Part | Acc]);
expand_path([Part|Rest], ReqData, Acc) when is_atom(Part) ->
    expand_path(Rest, ReqData, [wrq:path_info(Part, ReqData) | Acc]).

build_wm_routes([], Acc) ->
    [lists:reverse(Acc)];
build_wm_routes([#route{base = Base, path = Path} | Rest], Acc) ->
    build_wm_routes(Rest, [{Base ++ Path, ?MODULE, []} | Acc]).

build_response(ok) ->
    [{success, true}];
build_response({error, Error}) ->
    ErrStr = iolist_to_binary(io_lib:format("~p", [Error])),
    [{success, false}, {error, ErrStr}].