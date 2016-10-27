-module(rms_wm_resource).

-export([routes/0, dispatch/0]).

-export([static_types/1,
         static_file_exists/1,
         static_last_modified/1,
         static_file/1]).

-export([riak_versions/1]).

-export([clusters/1,
         set_clusters/1,
         cluster_exists/1,
         get_cluster/1,
         add_cluster/1,
         set_cluster/1,
         destroy_cluster/1,
         restart_cluster/1,
         cluster_riak_config_exists/1,
         get_cluster_riak_config/1,
         set_cluster_riak_config/1,
         delete_cluster_riak_config/1,
         cluster_advanced_config_exists/1,
         get_cluster_advanced_config/1,
         set_cluster_advanced_config/1,
         delete_cluster_advanced_config/1]).

-export([nodes/1,
         node_exists/1,
         get_node/1,
         add_node/1,
         delete_node/1,
         restart_node/1,
         get_node_aae/1,
         get_node_status/1,
         get_node_ringready/1,
         get_node_transfers/1,
         get_node_bucket_types/1,
         get_node_bucket_type/1,
         set_node_bucket_type/1]).

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
         provide_static_content/2,
         accept_content/2,
         post_is_create/2,
         create_path/2,
         last_modified/2]).

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
                      ?ACCEPT(?TEXT_TYPE),
                      ?ACCEPT(?JSON_TYPE)]).

-define(CLUSTER_TO_JSON_OPTIONS,
        [{rename_keys, [{key, name}]},
         {replace_values, [{riak_config, <<>>, null},
                           {advanced_config, <<>>, null}]}]).

-define(CLUSTER_FROM_JSON_OPTIONS,
        [{rename_keys, [{name, key}]},
         {replace_values, [{riak_config, null, <<>>},
                           {advanced_config, null, <<>>}]}]).

-record(route, {base = ?API_ROUTE :: [string()],
                path :: [string() | atom()],
                methods = ['GET'] :: [atom()],
                accepts = [] :: [{string(), atom()}],
                provides = [?PROVIDE(?JSON_TYPE)] :: [{string(), atom()}] |
                           {module(), atom()},
                exists = true :: {module(), atom()} | boolean(),
                content = [{success, true}] :: {module(), atom()} |
                          nonempty_list(),
                accept :: {module(), atom()} | undefined,
                delete :: {module(), atom()} | undefined,
                post_create = false :: boolean(),
                post_path :: {module(), atom()} | undefined,
                last_modified :: {module(), atom()} | undefined}).

-type route() :: #route{}.

-record(ctx, {route :: route()}).

-include_lib("webmachine/include/webmachine.hrl").

%% External functions.

routes() ->
    [%% Static.
     #route{base = [],
            %% TODO "static" is magic
            path = ["static", static_resource],
            exists = {?MODULE, static_file_exists},
            provides = {?MODULE, static_types},
            content = {?MODULE, static_file},
            last_modified = {?MODULE, static_last_modified}},
     %% Riak versions.
     #route{path = ["riak", "versions"],
            methods = ['GET'],
            content = {?MODULE, riak_versions}},
     %% Clusters.
     #route{path = ["clusters"],
            methods = ['GET', 'PUT'],
            content = {?MODULE, clusters},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, set_clusters}},
     #route{path = ["clusters", key],
            methods = ['GET', 'PUT', 'DELETE'],
            exists = {?MODULE, cluster_exists},
            content = {?MODULE, get_cluster},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, set_cluster},
            delete = {?MODULE, destroy_cluster}},
     #route{path = ["clusters", key, "create"],
            methods = ['POST'],
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, add_cluster}},
     #route{path = ["clusters", key, "restart"],
            methods = ['POST'],
            exists = {?MODULE, cluster_exists},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, restart_cluster}},
     #route{path = ["clusters", key, "config"],
            methods = ['GET', 'PUT', 'DELETE'],
            exists = {?MODULE, cluster_riak_config_exists},
            provides = ?PROVIDE_TEXT,
            content = {?MODULE, get_cluster_riak_config},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, set_cluster_riak_config},
            delete = {?MODULE, delete_cluster_riak_config}},
     #route{path = ["clusters", key, "advancedConfig"],
            methods = ['GET', 'PUT', 'DELETE'],
            exists = {?MODULE, cluster_advanced_config_exists},
            provides = ?PROVIDE_TEXT,
            content = {?MODULE, get_cluster_advanced_config},
            accepts = ?ACCEPT_TEXT,
            accept={?MODULE, set_cluster_advanced_config},
            delete = {?MODULE, delete_cluster_advanced_config}},
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
     #route{path = ["clusters", key, "nodes", node_key, "aae"],
            provides = [{?JSON_TYPE, provide_static_content}],
            exists = {?MODULE, node_exists},
            content = {?MODULE, get_node_aae}},
     #route{path = ["clusters", key, "nodes", node_key, "status"],
            provides = [{?JSON_TYPE, provide_static_content}],
            exists = {?MODULE, node_exists},
            content = {?MODULE, get_node_status}},
     #route{path = ["clusters", key, "nodes", node_key, "ringready"],
            provides = [{?JSON_TYPE, provide_static_content}],
            exists = {?MODULE, node_exists},
            content = {?MODULE, get_node_ringready}},
     #route{path = ["clusters", key, "nodes", node_key, "transfers"],
            provides = [{?JSON_TYPE, provide_static_content}],
            exists = {?MODULE, node_exists},
            content = {?MODULE, get_node_transfers}},
     #route{path = ["clusters", key, "nodes", node_key, "types"],
            provides = [{?JSON_TYPE, provide_static_content}],
            exists = {?MODULE, node_exists},
            content = {?MODULE, get_node_bucket_types}},
     #route{path = ["clusters", key, "nodes", node_key, "types", bucket_type],
            methods = ['GET', 'POST'],
            provides = [{?JSON_TYPE, provide_static_content}],
            exists = {?MODULE, node_exists},
            content = {?MODULE, get_node_bucket_type},
            accepts = ?ACCEPT_TEXT,
            accept = {?MODULE, set_node_bucket_type}},
     %% Healthcheck.
     #route{base = [],
            path = ["healthcheck"],
            content = {?MODULE, healthcheck}}].

dispatch() ->
    Routes = lists:flatten(build_wm_routes(routes(), [])),
    Routes ++ rms_wm_explorer:dispatch().

%% Static.

static_types(ReqData) ->
    Resource = wrq:path_info(static_resource, ReqData),
    CT = webmachine_util:guess_mime(Resource),
    {[{CT, provide_static_content}], ReqData}.

static_file_exists(ReqData) ->
    Filename = static_filename(ReqData),
    lager:info("Attempting to fetch static file: ~p", [Filename]),
    Result = filelib:is_regular(Filename),
    {Result, ReqData}.

static_last_modified(ReqData) ->
    LM = filelib:last_modified(static_filename(ReqData)),
    {LM, ReqData}.

static_file(ReqData) ->
    Filename = static_filename(ReqData),
    {ok, Response} = file:read_file(Filename),
    ET = hash_body(Response),
    ReqData1 = wrq:set_resp_header("ETag", webmachine_util:quoted_string(ET), ReqData),
    {Response, ReqData1}.

%% Riak versions.

riak_versions(ReqData) ->
    JsonRiakUrls = rms_wm_helper:to_json(rms_wm_helper:riak_urls()),
    {[{riak_versions, JsonRiakUrls}], ReqData}.

%% Clusters.

clusters(ReqData) ->
    ClustersList = rms_wm_helper:get_clusters_list_with_nodes_list(),
    JsonClustersList = rms_wm_helper:to_json(ClustersList, ?CLUSTER_TO_JSON_OPTIONS),
    {[{clusters, JsonClustersList}], ReqData}.

set_clusters(ReqData) ->
    Json = mochijson2:decode(wrq:req_body(ReqData)),
    ClustersWithNodes = rms_wm_helper:from_json(Json,
                                                ?CLUSTER_FROM_JSON_OPTIONS),
    ClustersListsWithNodesLists = proplists:get_value(clusters,
                                                      ClustersWithNodes),
    ResultsList = rms_wm_helper:add_clusters_list_with_nodes_list(ClustersListsWithNodesLists),
    ToJsonOptions = [{rename_keys, [{key, name}]}],
    Body = mochijson2:encode(rms_wm_helper:to_json(ResultsList, ToJsonOptions)),
    {true, wrq:append_to_response_body(Body, ReqData)}.

cluster_exists(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    {rms_wm_helper:cluster_exists(Key), ReqData}.

get_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    {ok, ClusterWithNodesList} = rms_wm_helper:get_cluster_with_nodes_list(Key),
    JsonCluster = rms_wm_helper:to_json([{Key, ClusterWithNodesList}],
                                        ?CLUSTER_TO_JSON_OPTIONS),
    {JsonCluster, ReqData}.

add_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Json = mochijson2:decode(wrq:req_body(ReqData)),
    Cluster = [{key, Key} | rms_wm_helper:from_json(Json)],
    Response = build_response(rms_wm_helper:add_cluster(Cluster)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

set_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Json = mochijson2:decode(wrq:req_body(ReqData)),
    ClusterWithNodes = rms_wm_helper:from_json(Json,
                                               ?CLUSTER_FROM_JSON_OPTIONS),
    ClusterWithNodes1 = [{key, Key} | proplists:delete(key, ClusterWithNodes)],
    Response = build_response(rms_wm_helper:add_cluster_with_nodes_list(ClusterWithNodes1)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

destroy_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = build_response(rms_cluster_manager:destroy_cluster(Key)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response),
     ReqData)}.

restart_cluster(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = build_response(rms_cluster_manager:restart_cluster(Key)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

cluster_riak_config_exists(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Result = case rms_cluster_manager:get_cluster(Key) of
                 {ok, _} ->
                     case rms_cluster_manager:get_cluster_riak_config(Key) of
                         {ok, <<>>} -> false;
                         {ok, _} -> true
                     end;
                 {error, not_found} ->
                     false
             end,
    {Result, ReqData}.

get_cluster_riak_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    {ok, Response} = rms_cluster_manager:get_cluster_riak_config(Key),
    {Response, ReqData}.

set_cluster_riak_config(ReqData) ->
    case wrq:req_body(ReqData) of
        <<>> ->
            Response = build_response({error, empty}),
            {false, wrq:append_to_response_body(mochijson2:encode(Response),
             ReqData)};
        Body ->
            Key = wrq:path_info(key, ReqData),
            RiakConfig = binary_to_list(Body),
            Result =
                rms_cluster_manager:set_cluster_riak_config(Key, RiakConfig),
            Response = build_response(Result),
            {true, wrq:append_to_response_body(mochijson2:encode(Response),
             ReqData)}
    end.

delete_cluster_riak_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = rms_cluster_manager:set_cluster_riak_config(Key, <<>>),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

cluster_advanced_config_exists(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Result = case rms_cluster_manager:get_cluster(Key) of
                 {ok, _} ->
                     case rms_cluster_manager:get_cluster_advanced_config(Key) of
                         {ok, <<>>} -> false;
                         {ok, _} -> true
                     end;
                 {error, not_found} ->
                     false
             end,
    {Result, ReqData}.

get_cluster_advanced_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    {ok, Response} = rms_cluster_manager:get_cluster_advanced_config(Key),
    {Response, ReqData}.

set_cluster_advanced_config(ReqData) ->
    case wrq:req_body(ReqData) of
        <<>> ->
            Response = build_response({error, empty}),
            {false, wrq:append_to_response_body(mochijson2:encode(Response),
             ReqData)};
        Body ->
            Key = wrq:path_info(key, ReqData),
            AdvancedConfig = binary_to_list(Body),
            Result = rms_cluster_manager:set_cluster_advanced_config(Key,
                                                                     AdvancedConfig),
            Response = build_response(Result),
            {true, wrq:append_to_response_body(mochijson2:encode(Response),
             ReqData)}
    end.

delete_cluster_advanced_config(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = rms_cluster_manager:set_cluster_advanced_config(Key, <<>>),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

%% Nodes.

nodes(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    NodeKeyList = [list_to_binary(NodeKey) ||
                   NodeKey <- rms_node_manager:get_active_node_keys(Key)],
    {[{nodes, NodeKeyList}], ReqData}.

node_exists(ReqData) ->
    NodeKey = wrq:path_info(node_key, ReqData),
    {rms_wm_helper:node_exists(NodeKey), ReqData}.

get_node(ReqData) ->
    NodeKey = wrq:path_info(node_key, ReqData),
    {ok, Node} = rms_wm_helper:get_node_with_location(NodeKey),
    NodeJson = rms_wm_helper:to_json([{NodeKey, Node}]),
    {NodeJson, ReqData}.

add_node(ReqData) ->
    Key = wrq:path_info(key, ReqData),
    Response = build_response(rms_cluster_manager:add_node(Key, undefined)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

delete_node(ReqData) ->
    NodeKey = wrq:path_info(node_key, ReqData),
    Force = list_to_atom(wrq:get_qs_value("force", "false", ReqData)),
    Response = build_response(rms_node_manager:destroy_node(NodeKey, Force)),
    {true, wrq:append_to_response_body(mochijson2:encode(Response), ReqData)}.

restart_node(RD) ->
    Body = [{success, true}],
    {true, wrq:append_to_response_body(mochijson2:encode(Body), RD)}.

get_node_aae(ReqData) ->
    riak_explorer_command(ReqData, aae_status).

get_node_status(ReqData) ->
    riak_explorer_command(ReqData, status).

get_node_ringready(ReqData) ->
    riak_explorer_command(ReqData, ringready).

get_node_transfers(ReqData) ->
    riak_explorer_command(ReqData, transfers).

get_node_bucket_types(ReqData) ->
    riak_explorer_command(ReqData, bucket_types).

get_node_bucket_type(ReqData) ->
    Type = wrq:path_info(bucket_type, ReqData),
    riak_explorer_command(ReqData, bucket_type, [Type]).

set_node_bucket_type(ReqData) ->
    Props = wrq:req_body(ReqData),
    Type = wrq:path_info(bucket_type, ReqData),
    {Response, ReqData1} = riak_explorer_command(ReqData, create_bucket_type, [list_to_binary(Type), Props]),
    {true, wrq:append_to_response_body(Response, ReqData1)}.

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
    case Route#route.provides of
        {M, F} ->
            {CTs, ReqData1} = M:F(ReqData),
            {CTs, ReqData1, Ctx};
        Provides ->
            {Provides, ReqData, Ctx}
     end.

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

provide_static_content(ReqData, Ctx = #ctx{route = #route{content = {M, F}}}) ->
    {Body, ReqData1} = M:F(ReqData),
    {Body, ReqData1, Ctx}.

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

last_modified(ReqData, Ctx = #ctx{route = #route{last_modified = undefined}}) ->
    {undefined, ReqData, Ctx};
last_modified(ReqData, Ctx = #ctx{route = #route{last_modified = {M, F}}}) ->
    {LM, ReqData1} = M:F(ReqData),
    {LM, ReqData1, Ctx}.

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

static_filename(ReqData) ->
    Resource = wrq:path_info(static_resource, ReqData),
    filename:join([rms_config:static_root(), Resource]).

hash_body(Body) -> mochihex:to_hex(binary_to_list(crypto:hash(sha,Body))).

riak_explorer_command(ReqData, Command) ->
    riak_explorer_command(ReqData, Command, []).

riak_explorer_command(ReqData, Command, Args) ->
    NodeKey = wrq:path_info(node_key, ReqData),
    case riak_explorer_client:apply_command(NodeKey, Command, Args) of
        {error, not_found} ->
            {mochijson2:encode(
               [{error, <<"Unable to retrieve node key or node name">>}]
              ), ReqData};
        {error, Reason} ->
            {mochijson2:encode(
               [{error, list_to_binary(io_lib:format("~p", [Reason]))}]
              ), ReqData};
        {ok, Body} ->
            {mochijson2:encode(Body), ReqData}
    end.
