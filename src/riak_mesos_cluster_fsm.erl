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

-module(riak_mesos_cluster_fsm).
-behaviour(gen_fsm).

-export([start_link/1]).
-export([get_status/1, add_node/1]).
-export([
  init/1, handle_event/3, handle_sync_event/4, handle_info/3, code_change/4,
  terminate/3]).
-export([
  ready/2
]).

-include("riak_mesos.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start_link(Cluster=#riak_cluster{id=ClusterId}) ->
    gen_fsm:start_link({local, list_to_atom(binary_to_list(ClusterId))}, ?MODULE, Cluster, []).

add_node(ClusterId) ->
    gen_fsm:send_event(list_to_atom(binary_to_list(ClusterId)), add_node).

get_status(ClusterId) ->
    gen_fsm:sync_send_all_state_event(list_to_atom(binary_to_list(ClusterId)), get_status).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(Cluster) ->
    {ok, ready, Cluster}.

ready(add_node, State=#riak_cluster{nodes=Nodes, id=ClusterId}) ->
    NodeNum = length(Nodes) + 1,
    NodeId = list_to_binary(binary_to_list(ClusterId) ++ "-" ++ integer_to_list(NodeNum)),
    Node = #riak_node{
        id= NodeId,
        cluster_id= ClusterId,
        container_path= <<"root">>
    },
    {ok, Pid} = riak_mesos_node_fsm:start_link(Node),
    {next_state, ready, State#riak_cluster{nodes=[{NodeId, Pid}|Nodes]}, 30000};
ready(timeout, State) ->
    { next_state, ready, State }.

handle_event(stop, _StateName, State) ->
	{ stop, normal, State }.

handle_sync_event(get_status, _From, StateName, State) ->
	{ reply, {StateName, State}, StateName, State }.

handle_info(_Info, _StateName, State) ->
    {next_state, ready, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(Reason, _StateName, _StateData) ->
    lager:error("riak_mesos_event_coordinator terminated, reason: ~p", [Reason]),
    ok.
