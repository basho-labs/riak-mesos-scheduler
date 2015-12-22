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

-module(riak_mesos_node_fsm).
-behaviour(gen_fsm).

-export([start_link/1]).
-export([
  get_status/1,
  set_persistence_id/2
]).
-export([
  init/1, handle_event/3, handle_sync_event/4, handle_info/3, code_change/4,
  terminate/3
]).
-export([
  ready/2,
  reserved/2,
  starting/2,
  started/2,
  shutdown/2,
  failed/2
]).

-include("riak_mesos.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start_link(Node=#riak_node{id=NodeId}) ->
    gen_fsm:start_link({local, list_to_atom(binary_to_list(NodeId))}, ?MODULE, Node, []).

get_status(NodeId) ->
    gen_fsm:sync_send_all_state_event(list_to_atom(binary_to_list(NodeId)), get_status).

set_persistence_id(NodeId, PersistenceID) ->
    gen_fsm:send_event(list_to_atom(binary_to_list(NodeId)), {set_persistence_id, PersistenceID}).

%%%===================================================================
%%% State Callbacks
%%%===================================================================

init(Node=#riak_node{}) ->
    {ok, ready, Node}.

ready(timeout, State) ->
    { next_state, ready, State };
ready({set_persistence_id, PersistenceID}, State) ->
    { next_state, reserved, State#riak_node{persistence_id=PersistenceID}}.

reserved(timeout, State) ->
    { next_state, ready, State }.

starting(timeout, State) ->
    { next_state, ready, State }.

started(timeout, State) ->
    { next_state, ready, State }.

shutdown(timeout, State) ->
    { next_state, ready, State }.

failed(timeout, State) ->
    { next_state, ready, State }.

%%%===================================================================
%%% Other Callbacks
%%%===================================================================

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
