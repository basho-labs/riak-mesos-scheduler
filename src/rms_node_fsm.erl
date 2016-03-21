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

-module(rms_node_fsm).

-behaviour(gen_fsm).

-export([start_link/2]).
-export([get/1,
         get_field_value/2,
         needs_to_be_reconciled/1,
         can_be_scheduled/1,
         has_reservation/1,
         set_unreserve/1,
         delete/1]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).
-export([
		 undefined/2,
		 undefined/3,
		 requested/2,
		 requested/3,
		 reserved/2,
		 reserved/3,
		 starting/2,
		 starting/3,
		 started/2,
		 started/3,
		 shutting_down/2,
		 shutting_down/3,
		 shutdown/2,
		 shutdown/3,
		 restarting/2,
		 restarting/3,
		 failed/2,
		 failed/3
		]).

-type status() :: undefined | %% Possible status for waiting reconciliation.
                  requested |
                  reserved |
                  starting |
                  started |
                  shutting_down |
                  shutdown |
                  failed |
                  restarting.
-export_type([status/0]).

-define(SERVER, ?MODULE).

-record(node, {key :: key(),
               cluster_key :: rms_cluster:key(),
               node_name = "" :: string(),
               hostname = "" :: string(),
               http_port :: pos_integer(),
               pb_port :: pos_integer(),
               disterl_port :: pos_integer(),
               agent_id_value = "" :: string(),
               container_path = "" :: string(),
               persistence_id = "" :: string()}).

-type key() :: string().
-export_type([key/0]).


-type node_state() :: {Status :: atom(), #node{}}.
-export_type([node_state/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Key, ClusterKey) ->
	gen_fsm:start_link(?MODULE, {Key, ClusterKey}, []).

%% TODO The following API functions operate only on the rms_metadata, but
%% it feels like we should be asking the running FSM/server for that node,
%% which should look in rms_metadata only if necessary.
%% TODO Define "if necessary"

-spec get(key()) -> {ok, rms_metadata:node_state()} | {error, term()}.
get(Key) ->
    rms_metadata:get_node(Key).

-spec get_field_value(atom(), key()) -> {ok, term()} | {error, term()}.
get_field_value(Field, Key) ->
    case rms_metadata:get_node(Key) of
        {ok, Node} ->
            case proplists:get_value(Field, Node, field_not_found) of
                field_not_found ->
                    {error, field_not_found};
                Value ->
                    {ok, Value}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec needs_to_be_reconciled(key()) -> {ok, boolean()} | {error, term()}.
needs_to_be_reconciled(Key) ->
    case get_node(Key) of
        {ok, {Status, _}} ->
            %% Basic simple solution.
            %% TODO: implement "needs to be reconciled" logic here.
            NeedsReconciliation = Status =:= undefined,
            {ok, NeedsReconciliation};
        {error, Reason} ->
            {error, Reason}
    end.

-spec can_be_scheduled(key()) -> {ok, boolean()} | {error, term()}.
can_be_scheduled(Key) ->
    case get_node(Key) of
        {ok, {Status, _}} ->
            %% Basic simple solution.
            %% TODO: implement "can be scheduled" logic here.
            CanBeScheduled = Status =:= requested,
            {ok, CanBeScheduled};
        {error, Reason} ->
            {error, Reason}
    end.

-spec has_reservation(key()) -> {ok, boolean()} | {error, term()}.
has_reservation(Key) ->
    case get_node(Key) of
        {ok, {_, #node{persistence_id = PersistenceId}}} ->
            HasReservation = PersistenceId =/= "",
            {ok, HasReservation};
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_reserve(pid(), string(), string(), string()) ->
    ok | {error, term()}.
set_reserve(Pid, Hostname, AgentIdValue, PersistenceId) ->
    gen_fsm:sync_send_all_state_event(
	  Pid, {set_reserve, Hostname, AgentIdValue, PersistenceId}).

-spec set_unreserve(pid()) -> ok | {error, term()}.
set_unreserve(_Pid) ->
    %% TODO: implement unreserve here via sync call to the node process.
    ok.

-spec delete(pid()) -> ok | {error, term()}.
delete(Pid) ->
	gen_fsm:sync_send_all_state_event(Pid, delete).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
init({Key, ClusterKey}) ->
	case get_node(Key) of
		{ok, Node} ->
			{ok, requested, Node};
		{error, not_found} ->
			Node = #node{key = Key,
						 cluster_key = ClusterKey},
			case add_node(Node) of
				ok ->
					{ok, requested, Node};
				{error, Reason} ->
					{stop, Reason}
			end
	end.

%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
undefined(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
requested(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
reserved(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
starting(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
started(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
shutting_down(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
shutdown(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
failed(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.
restarting(_Event, Node) ->
	{stop, {unhandled_event, _Event}, Node}.

%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
requested(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, requested, Node}.
undefined(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, undefined, Node}.
reserved(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, reserved, Node}.
starting(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, starting, Node}.
started(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, started, Node}.
shutting_down(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, shutting_down, Node}.
shutdown(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, shutdown, Node}.
failed(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, failed, Node}.
restarting(_Event, _From, Node) ->
	{reply, {error, unhandled_event}, restarting, Node}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
        {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event({set_reserve, Hostname, AgentIdValue, PersistenceId},
				  _From, Status, #node{key = Key} = Node) ->
	Node1 = Node#node{hostname = Hostname,
					  agent_id_value = AgentIdValue,
					  persistence_id = PersistenceId},
	case update_node(Key, {reserved, Node1}) of
		ok ->
			{reply, ok, reserved, Node1};
		{error, Reason} ->
			{reply, {error, Reason}, Status, Node}
	end;
handle_sync_event(delete, _From, Status, #node{key = Key} = Node) ->
	case update_node(Key, {shutting_down, Node}) of
		ok ->
			{reply, ok, shutting_down, Node};
		{error, Reason} ->
			{reply, {error, Reason}, Status, Node}
	end;
handle_sync_event(_Event, _From, StateName, State) ->
        Reply = ok,
        {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
        {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
        ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
        {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_node(key()) -> {ok, node_state()} | {error, term()}.
get_node(Key) ->
    case rms_metadata:get_node(Key) of
        {ok, Node} ->
            {ok, from_list(Node)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec add_node(node_state()) -> ok | {error, term()}.
add_node(Node) ->
    rms_metadata:add_node(to_list(Node)).

-spec update_node(key(), node_state()) -> ok | {error, term()}.
update_node(Key, Node) ->
    rms_metadata:update_node(Key, to_list(Node)).

-spec from_list(rms_metadata:node_state()) -> node_state().
from_list(NodeList) ->
	{proplists:get_value(status, NodeList),
    #node{key = proplists:get_value(key, NodeList),
          cluster_key = proplists:get_value(cluster_key, NodeList),
          node_name = proplists:get_value(node_name, NodeList),
          hostname = proplists:get_value(hostname, NodeList),
          http_port = proplists:get_value(http_port, NodeList),
          pb_port = proplists:get_value(pb_port, NodeList),
          disterl_port = proplists:get_value(disterl_port, NodeList),
          agent_id_value = proplists:get_value(agent_id_value, NodeList),
          container_path = proplists:get_value(container_path, NodeList),
          persistence_id = proplists:get_value(persistence_id, NodeList)}}.

-spec to_list(node_state()) -> rms_metadata:node_state().
to_list(
  {Status,
   #node{key = Key,
         cluster_key = ClusterKey,
         node_name = NodeName,
         hostname = Hostname,
         http_port = HttpPort,
         pb_port = PbPort,
         disterl_port = DisterlPort,
         agent_id_value = AgentIdValue,
         container_path = ContainerPath,
         persistence_id = PersistenceId}}) ->
    [{key, Key},
     {status, Status},
     {cluster_key, ClusterKey},
     {node_name, NodeName},
     {hostname, Hostname},
     {http_port, HttpPort},
     {pb_port, PbPort},
     {disterl_port, DisterlPort},
     {agent_id_value, AgentIdValue},
     {container_path, ContainerPath},
     {persistence_id, PersistenceId}].
