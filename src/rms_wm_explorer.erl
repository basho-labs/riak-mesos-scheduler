-module(rms_wm_explorer).

-export([dispatch/0]).

-export([proxy_request/3]).

-include_lib("webmachine/include/webmachine.hrl").

%% External functions.

dispatch() ->
    re_wm:dispatch([{proxy, {?MODULE, proxy_request}}]).

%% Local Riak Explorer Handler

-spec proxy_request(module(), atom(), #wm_reqdata{}) ->
                           {ok, term()} | {forward, term()}.
proxy_request(re_wm_static, _, _) ->
    {forward, local};
proxy_request(re_wm_proxy, proxy_available, ReqData) ->
    case rd_node_location(ReqData) of
        {error, not_found} ->
            {ok, {{halt, 404}, ReqData}};
        Location ->
            ClusterKey = wrq:path_info(cluster, ReqData),
            Path = "/" ++ wrq:disp_path(ReqData),
            NewLocation = "/explore/clusters/" ++ ClusterKey,
            {ok, re_wm_proxy:send_proxy_request(Location, Path, NewLocation, ReqData)}
    end;
proxy_request(re_wm_explore, clusters, ReqData) ->
    {ok, re_wm:rd_content(get_explorer_clusters(), ReqData)};
proxy_request(_, _, ReqData) ->
    case rd_node_location(ReqData) of
        {error, _} ->
            {ok, {{halt, 404}, ReqData}};
        Location ->
            ClusterKey = wrq:path_info(cluster, ReqData),
            Path = 
                case ClusterKey of
                    undefined ->
                        wrq:path(ReqData);
                    K ->
                        re:replace(wrq:path(ReqData), "/clusters/" ++ K, "/clusters/default", [{return, list}])
                end,
            NewPath = wrq:path(ReqData),
            {forward, {location, Location ++ "/admin", Path, NewPath}}
    end.

%% Internal functions.

cluster_from_props(ClusterKey, Props) ->
    DevMode = proplists:get_value(<<"development_mode">>, Props, false),
    RiakNode = proplists:get_value(<<"riak_node">>, Props, unavailable),
    RiakType = proplists:get_value(<<"riak_type">>, Props, unavailable),
    RiakVersion = proplists:get_value(<<"riak_version">>, Props, unavailable),
    Available = proplists:get_value(<<"available">>, Props, false),
    [{id, list_to_binary(ClusterKey)},
     {development_mode, DevMode}, 
     {riak_node, RiakNode},
     {riak_type, RiakType}, 
     {riak_version, RiakVersion},
     {available, Available}].

get_explorer_cluster(ClusterKey) ->
    case riak_explorer_client:node_key_from_cluster(ClusterKey) of
        {error, not_found} ->
            cluster_from_props(ClusterKey, []);
        NodeKey ->
            case rms_node_manager:get_node_http_url(NodeKey) of
                {ok, H} ->
                    case riak_explorer_client:clusters(list_to_binary(H)) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, [{<<"clusters">>, [Props]}]} ->
                            cluster_from_props(ClusterKey, Props)
                    end;
                _ ->
                    cluster_from_props(ClusterKey, [])
            end
    end.

get_explorer_clusters() ->
    ClusterKeys = rms_cluster_manager:get_cluster_keys(),
    [ get_explorer_cluster(ClusterKey) || ClusterKey <- ClusterKeys ].

rd_node_key(ReqData) ->
    NodeName = wrq:path_info(node, ReqData),
    case NodeName of
        undefined ->
            ClusterKey = wrq:path_info(cluster, ReqData),
            riak_explorer_client:node_key_from_cluster(ClusterKey);
        Name ->
            riak_explorer_client:node_name_to_key(Name)
    end.

rd_node_location(ReqData) ->
    case rd_node_key(ReqData) of
        {error, not_found} ->
            {error, not_found};
        NodeKey ->
            node_location(NodeKey)
    end.

node_location(NodeKey) ->
    case rms_node_manager:get_node_http_url(NodeKey) of
        {ok, H} ->
            "http://" ++ H;
        _ ->
            {error, not_found}
    end.
