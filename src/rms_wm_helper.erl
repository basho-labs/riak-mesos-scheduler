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
         get_clusters_list_with_nodes_list/2]).

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
