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

-export([start_link/1]).

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
                  nodes = [] :: [rms_node:key()],
                  generation = 1 :: pos_integer()}).

-type key() :: string().
-export_type([key/0]).

-type status() :: requested.
-export_type([status/0]).

-type cluster() :: #cluster{}.
-export_type([cluster/0]).

%% External functions.

-spec start_link({add, key(), string(), string()} | {restore, key()}) ->
    {ok, pid()} | {error, term()}.
start_link(Init) ->
    gen_server:start_link(?MODULE, Init, []).

%% gen_server callback functions.

init({add, Key, RiakConfig, AdvancedConfig}) ->
    Cluster = #cluster{key = Key,
                       riak_config = RiakConfig,
                       advanced_config = AdvancedConfig},
    case add_cluster(Cluster) of
        ok ->
            {ok, Cluster};
        {error, Reason} ->
            {stop,Reason}
    end;
init({restore, Key}) ->
    case get_cluster(Key) of
        {ok, Cluster} ->
            {ok, Cluster};
        {error, Reason} ->
            {stop,Reason}
    end.

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

-spec from_list(rms_metadata:cluster()) -> cluster().
from_list(ClusterList) ->
    #cluster{key = proplists:get_value(key, ClusterList),
             status = proplists:get_value(status, ClusterList),
             riak_config = proplists:get_value(riak_conf, ClusterList),
             advanced_config = proplists:get_value(advanced_config,
                                                   ClusterList),
             nodes = proplists:get_value(nodes, ClusterList),
             generation = proplists:get_value(generation, ClusterList)}.

-spec to_list(cluster()) -> rms_metadata:cluster().
to_list(#cluster{key = Key,
                 status = Status,
                 riak_config = RiakConf,
                 advanced_config = AdvancedConfig,
                 nodes = Nodes,
                 generation = Generation}) ->
    [{key, Key},
     {status, Status},
     {riak_config, RiakConf},
     {advanced_config, AdvancedConfig},
     {nodes, Nodes},
     {generation, Generation}].
