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

-module(rms_wm_helper).

-export([get_clusters_list/0,
         get_nodes_list/1,
         get_cluster_with_nodes_list/1,
         get_clusters_list_with_nodes_list/0,
         get_clusters_list/1,
         get_nodes_list/2,
         get_cluster_with_nodes_list/3,
         get_clusters_list_with_nodes_list/2,
         add_clusters_list_with_nodes_list/1,
         add_cluster_with_nodes_list/1]).

-export([to_json/1, to_json/2, from_json/1, from_json/2]).

-define(CLUSTER_FIELDS, [key, riak_config, advanced_config, generation]).

-define(NODE_FIELDS, [key, status, container_path, persistence_id]).

%% External functions.

-spec get_clusters_list() -> {list, [rms_metadata:cluster_state()]}.
get_clusters_list() ->
    get_clusters_list(?CLUSTER_FIELDS).

-spec get_nodes_list(rms_cluster:key()) -> {list, [rms_metadata:node_state()]}.
get_nodes_list(ClusterKey) ->
    get_nodes_list(ClusterKey, ?NODE_FIELDS).

-spec get_cluster_with_nodes_list(rms_cluster:key()) ->
    {ok, [rms_metadata:cluster_state() |
          {nodes, {list, [rms_metadata:node_state()]}}]} | {error, term()}.
get_cluster_with_nodes_list(ClusterKey) ->
    get_cluster_with_nodes_list(ClusterKey, ?CLUSTER_FIELDS, ?NODE_FIELDS).

-spec get_clusters_list_with_nodes_list() ->
    {list, [rms_metadata:cluster_state() |
            {nodes, {list, [rms_metadata:node_state()]}}]}.
get_clusters_list_with_nodes_list() ->
    get_clusters_list_with_nodes_list(?CLUSTER_FIELDS, ?NODE_FIELDS).

-spec get_clusters_list([atom()]) -> {list, [rms_metadata:cluster_state()]}.
get_clusters_list(ClusterFields) ->
    ClusterKeys = rms_cluster_manager:get_cluster_keys(),
    get_clusters_list(ClusterKeys, ClusterFields, [], []).

-spec get_nodes_list(rms_cluster:key(), [atom()]) ->
    {list, [rms_metadata:node_state()]}.
get_nodes_list(ClusterKey, NodeFields) ->
    NodeKeys = rms_node_manager:get_node_keys(ClusterKey),
    get_nodes_list(NodeKeys, NodeFields, []).

-spec get_cluster_with_nodes_list(rms_cluster:key(), [atom()], [atom()]) ->
    {ok, [rms_metadata:cluster_state() |
          {nodes, {list, [rms_metadata:node_state()]}}]} | {error, term()}.
get_cluster_with_nodes_list(ClusterKey, ClusterFields, NodeFields) ->
    case rms_cluster:get(ClusterKey, ClusterFields) of
        {ok, Cluster} ->
            {ok, [{nodes, get_nodes_list(ClusterKey, NodeFields)} | Cluster]};
        {error, _Reason} = Error ->
            Error
    end.

-spec get_clusters_list_with_nodes_list([atom()], [atom()]) ->
    {list, [rms_metadata:cluster_state() |
            {nodes, {list, [rms_metadata:node_state()]}}]}.
get_clusters_list_with_nodes_list(ClusterFields, NodeFields) ->
    ClusterKeys = rms_cluster_manager:get_cluster_keys(),
    get_clusters_list(ClusterKeys, ClusterFields, NodeFields, []).

add_clusters_list_with_nodes_list({list, Clusters}) ->
    add_clusters_list_with_nodes_list(Clusters, []).

add_cluster_with_nodes_list(Cluster) ->
    {list, Nodes} = proplists:get_value(nodes, Cluster),
    rms_cluster_manager:add_cluster(Cluster, Nodes).

to_json(Value) ->
    to_json(Value, []).

%% @doc Option: {rename_keys, [{FromKey, ToKey}]}.
%%      Option: {replace_values, [{Key, FromValue, ToValue}]}.
to_json({list, List}, Options) ->
    to_json_array(List, [], Options);
to_json([{_Key, _Value} | _Fields] = Object, Options) ->
    to_json_object(Object, [], Options);
to_json(String, _Options) when is_list(String) ->
    list_to_binary(String);
to_json(Value, _Options) ->
    Value.

from_json(Value) ->
    from_json(Value, []).

%% @doc Option: {rename_keys, [{FromKey, ToKey}]}.
%%      Option: {replace_values, [{Key, FromValue, ToValue}]}.
from_json(List, Options) when is_list(List) ->
    from_json_array(List, [], Options);
from_json({struct, Object}, Options) ->
    from_json_object(Object, [], Options);
from_json(Binary, _Options) when is_binary(Binary) ->
    binary_to_list(Binary);
from_json(Value, _Options) ->
    Value.

%% Internal functions.

-spec get_clusters_list([rms_cluster:key()], [atom()], [atom()],
                        [rms_metadata:cluster_state() |
                         {nodes, {list, [rms_metadata:node_state()]}}]) ->
    {list, [rms_metadata:cluster_state() |
            {nodes, {list, [rms_metadata:node_state()]}}]}.
get_clusters_list([ClusterKey | ClusterKeys], ClusterFields, NodeFields,
                  Clusters) ->
    case rms_cluster_manager:get_cluster(ClusterKey, ClusterFields) of
        {ok, Cluster} when NodeFields =:= [] ->
            get_clusters_list(ClusterKeys, ClusterFields, NodeFields,
                              [Cluster | Clusters]);
        {ok, Cluster} ->
            Cluster1 = [{nodes, get_nodes_list(ClusterKey, NodeFields)} |
                        Cluster],
            get_clusters_list(ClusterKeys, ClusterFields, NodeFields,
                              [Cluster1 | Clusters]);
        {error, _Reason} ->
            get_clusters_list(ClusterKeys, ClusterFields, NodeFields, Clusters)
    end;
get_clusters_list([], _ClusterFields, _NodeFields, Clusters) ->
    {list, lists:reverse(Clusters)}.

-spec get_nodes_list([rms_node:key()], [atom()], [rms_metadata:node_state()]) ->
    {list, [rms_metadata:node_state()]}.
get_nodes_list([NodeKey | NodeKeys], NodeFields, Nodes) ->
    case rms_node_manager:get_node(NodeKey, NodeFields) of
        {ok, Node} ->
            get_nodes_list(NodeKeys, NodeFields, [Node | Nodes]);
        {error, _Reason} ->
            get_nodes_list(NodeKeys, NodeFields, Nodes)
    end;
get_nodes_list([], _NodeFields, Nodes) ->
    {list, lists:reverse(Nodes)}.

add_clusters_list_with_nodes_list([Cluster | Clusters], Results) ->
    {list, Nodes} = proplists:get_value(nodes, Cluster),
    Key = proplists:get_value(key, Cluster),
    Result = case rms_cluster_manager:add_cluster(Cluster, Nodes) of
                 ok ->
                     [{key, Key}, {success, true}];
                 {error, Reason} ->
                     [{key, Key},
                      {success, false},
                      {reason, io_lib:format("~p", [Reason])}]
             end,
    add_clusters_list_with_nodes_list(Clusters, [Result | Results]);
add_clusters_list_with_nodes_list([], Results) ->
    {list, lists:reverse(Results)}.

to_json_array([Value | Values], JsonArray, Options) ->
    JsonArray1 = [to_json(Value, Options) | JsonArray],
    to_json_array(Values, JsonArray1, Options);
to_json_array([], JsonArray, _Options) ->
    {array, lists:reverse(JsonArray)}.

to_json_object([{Key, Value} | Fields], JsonObject, Options) ->
    Key1 = json_object_key(Key, Options),
    Value1 = to_json(json_object_value(Key, Value, Options), Options),
    JsonObject1 = [{Key1, Value1} | JsonObject],
    to_json_object(Fields, JsonObject1, Options);
to_json_object([], JsonObject, _Options) ->
    {struct, lists:reverse(JsonObject)}.

json_object_key(Key, Options) ->
    RenameObjectKeys = proplists:get_value(rename_keys, Options, []),
    case lists:keyfind(Key, 1, RenameObjectKeys) of
        {_FromKey, ToKey} ->
            ToKey;
        false ->
            Key
    end.

json_object_value(Key, Value, Options) ->
    ReplaceValues = proplists:get_value(replace_values, Options, []),
    case lists:keyfind(Key, 1, ReplaceValues) of
        {Key, Value, ToValue} ->
            ToValue;
        {_Key, _FromValue, _ToValue} ->
            Value;
        false ->
            Value
    end.

from_json_array([Value | Values], Array, Options) ->
    Array1 = [from_json(Value, Options) | Array],
    from_json_array(Values, Array1, Options);
from_json_array([], Array, _Options) ->
    {list, lists:reverse(Array)}.

from_json_object([{Key, Value} | Fields], Object, Options) ->
    Key1 = binary_to_atom(Key, utf8),
    Key2 = json_object_key(Key1, Options),
    Value1 = json_object_value(Key1, from_json(Value, Options), Options),
    Object1 = [{Key2, Value1} | Object],
    from_json_object(Fields, Object1, Options);
from_json_object([], Object, _Options) ->
    lists:reverse(Object).
