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

-module(riak_mesos_cluster_manager).
-behaviour(gen_server).

-export([start_link/1]).
-export([get_status/0, add_cluster/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("riak_mesos.hrl").

-record(state, {clusters}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(_) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_status() ->
    gen_server:call(?MODULE, get_status).

add_cluster(ClusterId) ->
    gen_server:cast(?MODULE, {add_cluster, ClusterId}).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    process_flag(trap_exit, true),
    {ok, #state{clusters=[]}}.

handle_call(get_status, _From, State) ->
    {reply, State#state.clusters, State}.

handle_cast({add_cluster, ClusterId}, State=#state{clusters=Clusters}) ->
    Cluster = #riak_cluster{
        id= ClusterId,
        nodes= [],
        node_cpus= 1.0,
        node_mem= 2048.0,
        node_disk= 20000.0,
        node_ports= 2
    },
    {ok, Pid} = riak_mesos_cluster_fsm:start_link(Cluster),
    {noreply, State#state{clusters=[{ClusterId, Pid}|Clusters]}}.

handle_info({'EXIT', _Pid, _Reason}, State) ->
    {noreply, State};
handle_info(Message, State) ->
    lager:warning("Received unknown message: ~p", [Message]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, _State) ->
    lager:error("riak_mesos_event_coordinator terminated, reason: ~p", [Reason]),
    ok.

%%%===================================================================
%%% Private
%%%===================================================================
