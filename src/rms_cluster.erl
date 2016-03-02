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

-module(rms_cluster).

-behaviour(gen_server).

-export([start_link/1]).

-export([get_riak_config/1,
         get_advanced_config/1,
         set_riak_config/2,
         set_advanced_config/2,
         delete/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(cluster, {key :: rms_cluster:key(),
                  status = requested :: status(),
                  riak_config = "" :: string(),
                  advanced_config = "" :: string(),
                  node_keys = [] :: [rms_node:key()],
                  generation = 1 :: pos_integer()}).

-type key() :: string().
-export_type([key/0]).

-type status() :: undefined | requested | shutting_down.
-export_type([status/0]).

-type cluster() :: #cluster{}.
-export_type([cluster/0]).

%% External functions.

-spec start_link(key()) ->
    {ok, pid()} | {error, term()}.
start_link(Key) ->
    gen_server:start_link(?MODULE, Key, []).

-spec get_riak_config(key()) -> {ok, string()} | {error, term()}.
get_riak_config(Key) ->
    case get_cluster(Key) of
        {ok, #cluster{riak_config = RiakConfig}} ->
            {ok, RiakConfig};
        {error, Reason} ->
            {stop, Reason}
    end.

-spec get_advanced_config(key()) -> {ok, string()} | {error, term()}.
get_advanced_config(Key) ->
    case get_cluster(Key) of
        {ok, #cluster{advanced_config = AdvancedConfig}} ->
            {ok, AdvancedConfig};
        {error, Reason} ->
            {stop, Reason}
    end.

-spec set_riak_config(pid(), string()) -> ok | {error, term()}.
set_riak_config(Pid, RiakConfig) ->
    gen_server:call(Pid, {set_riak_config, RiakConfig}).

-spec set_advanced_config(pid(), string()) -> ok | {error, term()}.
set_advanced_config(Pid, AdvancedConfig) ->
    gen_server:call(Pid, {set_advanced_config, AdvancedConfig}).

-spec delete(pid()) -> ok | {error, term()}.
delete(Pid) ->
    gen_server:call(Pid, delete).

%% gen_server callback functions.

init(Key) ->
    case get_cluster(Key) of
        {ok, Cluster} ->
            {ok, Cluster};
        {error, not_found} ->
            Cluster = #cluster{key = Key},
            case add_cluster(Cluster) of
                ok ->
                    {ok, Cluster};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({set_riak_config, RiakConfig}, _From, Cluster) ->
    Cluster1 = Cluster#cluster{riak_config = RiakConfig},
    update_cluster_state(Cluster, Cluster1);
handle_call({set_advanced_config, AdvancedConfig}, _From, Cluster) ->
    Cluster1 = Cluster#cluster{advanced_config = AdvancedConfig},
    update_cluster_state(Cluster, Cluster1);
handle_call(delete, _From, Cluster) ->
    Cluster1 = Cluster#cluster{status = shutting_down},
    update_cluster_state(Cluster, Cluster1);
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Internal functions.

-spec get_cluster(key()) -> {ok, cluster()} | {error, term()}.
get_cluster(Key) ->
    case rms_metadata:get_cluster(Key) of
        {ok, Cluster} ->
            {ok, from_list(Cluster)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec add_cluster(cluster()) -> ok | {error, term()}.
add_cluster(Cluster) ->
    rms_metadata:add_cluster(to_list(Cluster)).

-spec update_cluster_state(cluster(), cluster()) ->
    {reply, ok | {error, term()}, cluster()}.
update_cluster_state(#cluster{key = Key} = Cluster, NewCluster) ->
    case update_cluster(Key, NewCluster) of
        ok ->
            {reply, ok, NewCluster};
        {error, Reason} ->
            {reply, {error, Reason}, Cluster}
    end.

-spec update_cluster(key(), cluster()) -> ok | {error, term()}.
update_cluster(Key, Cluster) ->
    rms_metadata:update_cluster(Key, to_list(Cluster)).

-spec from_list(rms_metadata:cluster()) -> cluster().
from_list(ClusterList) ->
    #cluster{key = proplists:get_value(key, ClusterList),
             status = proplists:get_value(status, ClusterList),
             riak_config = proplists:get_value(riak_config, ClusterList),
             advanced_config = proplists:get_value(advanced_config,
                                                   ClusterList),
             node_keys = proplists:get_value(node_keys, ClusterList),
             generation = proplists:get_value(generation, ClusterList)}.

-spec to_list(cluster()) -> rms_metadata:cluster().
to_list(#cluster{key = Key,
                 status = Status,
                 riak_config = RiakConf,
                 advanced_config = AdvancedConfig,
                 node_keys = NodeKeys,
                 generation = Generation}) ->
    [{key, Key},
     {status, Status},
     {riak_config, RiakConf},
     {advanced_config, AdvancedConfig},
     {node_keys, NodeKeys},
     {generation, Generation}].
