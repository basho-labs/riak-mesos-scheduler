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

%% Node transitions:
%% add_node: requested -> reserved -> starting -> started
%%                                             |> reserved -> ...
%%                                 |> reserved -> ...
%%                     |> requested
%% remove_node: * -> shutting_down -> shutdown
%%                |> shutdown
%% restart_node: * -> starting -> reserved -> ...
%%                 |> reserved -> ...

%% TODO FSM process never terminates in shutdown, just sits there.

-module(rms_node).

-behaviour(gen_fsm).

%%% API
-export([start_link/2]).
-export([get/1,
         get_field_value/2,
         needs_to_be_reconciled/1,
         can_be_scheduled/1,
         has_reservation/1,
         can_be_shutdown/1,
         set_reserve/5,
         set_unreserve/1,
         set_agent_info/9,
         destroy/1,
         destroy/2,
         restart/1,
         handle_status_update/3]).

%%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%%% States
-export([
         requested/2,
         requested/3,
         reserved/2,
         reserved/3,
         starting/2,
         starting/3,
         restarting/2,
         restarting/3,
         started/2,
         started/3,
         leaving/2,
         leaving/3,
         shutting_down/2,
         shutting_down/3,
         shutdown/2,
         shutdown/3
        ]).

%% TODO Maybe we could use a backoff instead of this
-define(LEAVING_TIMEOUT, 15000).

-record(node, {key :: key(),
               cluster_key :: rms_cluster:key(),
               node_name = "" :: string(),
               hostname = "" :: string(),
               http_port :: pos_integer(),
               pb_port :: pos_integer(),
               disterl_port :: pos_integer(),
               executor_id_value = "" :: string(),
               agent_id_value = "" :: string(),
               container_path = "" :: string(),
               persistence_id = "" :: string(),
               reconciled = false :: boolean(),
               attributes = [] :: rms_offer_helper:attributes()}).

-type key() :: string().
-export_type([key/0]).

-type node_state() :: #node{}.
-export_type([node_state/0]).

%%% API

-spec start_link(key(), rms_cluster:key()) ->
                        {ok, pid()} | {error, Error :: term()}.
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
        {ok, {_, #node{reconciled = Reconciled}}} ->
            NeedsReconciliation = Reconciled =:= false,
            {ok, NeedsReconciliation};
        {error, Reason} ->
            {error, Reason}
    end.

-spec can_be_scheduled(key()) -> {ok, boolean()} | {error, term()}.
can_be_scheduled(Pid) when is_pid(Pid) ->
    case gen_fsm:sync_send_event(Pid, can_be_scheduled) of
        {error, unhandled_event} -> {ok, false};
        {ok, Reply} -> {ok, Reply}
    end.

-spec can_be_shutdown(key() | pid()) -> {ok, boolean()} | {error, term()}.
can_be_shutdown(Pid) when is_pid(Pid) ->
    case gen_fsm:sync_send_event(Pid, can_be_shutdown) of
        {error, unhandled_event} -> {ok, false};
        {ok, Reply} -> {ok, Reply}
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

-spec handle_status_update(pid(), atom(), atom()) -> ok | {error, term()}.
handle_status_update(Pid, TaskStatus, Reason) ->
    case Reason of
        'REASON_RECONCILIATION' ->
            set_reconciled(Pid);
        _ ->
            ok
    end,
    case gen_fsm:sync_send_event(Pid, {status_update, TaskStatus, Reason}) of
        ok ->
            ok;
        {error, E} ->
            {error, E}
    end.

-spec set_reconciled(pid()) -> ok | {error, term()}.
set_reconciled(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, set_reconciled).

-spec set_reserve(pid(), string(), string(), string(), rms_offer_helper:attributes()) ->
                         ok | {error, term()}.
set_reserve(Pid, Hostname, AgentIdValue, PersistenceId, Attributes) ->
    gen_fsm:sync_send_event(
      Pid, {set_reserve, Hostname, AgentIdValue, PersistenceId, Attributes}).

-spec set_unreserve(pid()) -> ok | {error, term()}.
set_unreserve(Pid) ->
    gen_fsm:sync_send_event(Pid, set_unreserve).

-spec set_agent_info(pid(), string(), string(), pos_integer(), pos_integer(),
                     pos_integer(), string(), string(), string()) ->
    ok | {error, term()}.
set_agent_info(Pid, 
               NodeName,
               Hostname,
               HttpPort,
               PbPort,
               DisterlPort,
               AgentIdValue,
               ExecutorIdValue,
               ContainerPath) ->
    gen_fsm:sync_send_event(
      Pid, {set_agent_info,
            NodeName,
            Hostname,
            HttpPort,
            PbPort,
            DisterlPort,
            AgentIdValue,
            ExecutorIdValue,
            ContainerPath}).

-spec destroy(pid()) -> ok | {error, term()}.
destroy(Pid) ->
    destroy(Pid, false).

-spec destroy(pid(), boolean()) -> ok | {error, term()}.
destroy(Pid, Force) ->
    gen_fsm:sync_send_event(Pid, {destroy, Force}).

-spec restart(pid()) -> ok | {error, term()}.
restart(Pid) ->
    gen_fsm:sync_send_event(Pid, restart).

%%% gen_fsm callbacks
-spec init({key(), rms_cluster:key()}) ->
                  {ok, StateName :: atom(), node_state()}
                      | {stop, reason()}.
init({Key, ClusterKey}) ->
    case get_node(Key) of
        {ok, {State, Node}} ->
            Node1 = Node#node{reconciled = false},
            case rms_metadata:update_node(Key, to_list({State, Node1})) of
                ok ->
                    lager:info("Found existing node ~p with state ~p.", [Node1, State]),
                    {ok, State, Node1};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, not_found} ->
            Node = #node{key = Key,
                         cluster_key = ClusterKey,
                         reconciled = true},
            case add_node({requested, Node}) of
                ok ->
                    {ok, requested, Node};
                {error, Reason} ->
                    {stop, Reason}
            end
    end.

%%% Async per-state event handling
%%% Note that, as of now, there is none.
-type state_timeout() :: non_neg_integer() | infinity.
-type state() :: atom().
-type from() :: {pid(), Tag :: term()}.
-type event() :: term().
-type reply() :: term().
-type reason() :: term().
-type state_cb_return() ::
        {stop, reason(), New::node_state()}
      | {next_state, Next::state(), New::node_state()}
      | {next_state, Next::state(), New::node_state(), state_timeout()}.

-spec requested(event(), node_state()) -> state_cb_return().
requested(_Event, Node) ->
    delete_and_stop({requested, Node}, {error, {unhandled_event, _Event}}).

-spec reserved(event(), node_state()) -> state_cb_return().
reserved(_Event, Node) ->
    delete_and_stop({reserved, Node}, {error, {unhandled_event, _Event}}).

-spec starting(event(), node_state()) -> state_cb_return().
starting(_Event, Node) ->
    delete_and_stop({starting, Node}, {error, {unhandled_event, _Event}}).

-spec restarting(event(), node_state()) -> state_cb_return().
restarting(_Event, Node) ->
    delete_and_stop({restarting, Node}, {error, {unhandled_event, _Event}}).

-spec started(event(), node_state()) -> state_cb_return().
started(_Event, Node) ->
    delete_and_stop({started, Node}, {error, {unhandled_event, _Event}}).

-spec leaving(event(), node_state()) -> state_cb_return().
leaving(timeout, Node) ->
    %% check riak_explorer_client:status(NodeHost:NodePort, FullyQualifiedNodeName@Hostname)
    %% if this node believes it's the only node in the cluster, drop into shutting_down
    %% else stay in leaving, with timeout
    lager:debug("Leaving timeout reached"),
    {next_state, leaving, Node, ?LEAVING_TIMEOUT};
leaving(_Event, Node) ->
    delete_and_stop({leaving, Node}, {error, {unhandled_event, _Event}}).

-spec shutting_down(event(), node_state()) -> state_cb_return().
shutting_down(_Event, Node) ->
    delete_and_stop({shutting_down, Node}, {error, {unhandled_event, _Event}}).

-spec shutdown(event(), node_state()) -> state_cb_return().
%% TODO This state may no longer be required. Investigate.
shutdown(timeout, Node) ->
    delete_and_stop({shutdown, Node}, normal);
shutdown(_Event, Node) ->
    {stop, {unhandled_event, _Event}, Node}.

-spec handle_event(event(), StateName :: atom(), node_state()) -> state_cb_return().
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%% Sync per-state event handling
%%% Note that, as of now, there is none.
-type state_cb_reply() ::
        state_cb_return()
      | {stop, reason(), reply(), New::node_state()}
      | {reply, reply(), Next::state(), New::node_state()}
      | {reply, reply(), Next::state(), New::node_state(), state_timeout()}.

-spec requested(event(), from(), node_state()) -> state_cb_reply().
requested({set_reserve, Hostname, AgentIdValue, PersistenceId, Attributes}, _From, Node) ->
    Node1 = Node#node{hostname = Hostname,
                      agent_id_value = AgentIdValue,
                      persistence_id = PersistenceId,
                      attributes = Attributes},
    update_and_reply({requested, Node}, {reserved, Node1});
requested({status_update, StatusUpdate, _}, _From, Node) ->
    case StatusUpdate of
        'TASK_FAILED' -> {reply, ok, requested, Node};
        'TASK_LOST' -> {reply, ok, requested, Node};
        'TASK_ERROR' -> {reply, ok, requested, Node};
        _ ->
            lager:debug("Unexpected status_update [~p]: ~p", [requested, StatusUpdate]),
            {reply, ok, requested, Node}
    end;
requested(can_be_scheduled, _From, Node) ->
    {reply, {ok, true}, requested, Node};
requested(can_be_shutdown, _From, Node) ->
    {reply, {ok, false}, requested, Node};
requested({destroy, _}, _From, Node) ->
    update_and_reply({requested, Node}, {shutting_down, Node});
requested(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, requested, Node}.

-spec reserved(event(), from(), node_state()) -> state_cb_reply().
reserved(set_unreserve, _From, Node) ->
    lager:info("Removing reservation for node: ~p", [Node]),
    unreserve(reserved, requested, Node);
reserved({set_agent_info,
          NodeName,
          Hostname,
          HttpPort,
          PbPort,
          DisterlPort,
          AgentIdValue,
          ExecutorIdValue,
          ContainerPath},
          _From, Node) ->
    Node1 = Node#node{hostname = Hostname,
                      node_name = NodeName,
                      http_port = HttpPort,
                      pb_port = PbPort,
                      disterl_port = DisterlPort,
                      agent_id_value = AgentIdValue,
                      executor_id_value = ExecutorIdValue,
                      container_path = ContainerPath},
    lager:info("Setting agent info for node to ~p", [Node1]),
    update_and_reply({reserved, Node}, {reserved, Node1});
reserved({status_update, StatusUpdate, _}, _From, Node) ->
    case StatusUpdate of
        'TASK_FAILED' -> {reply, ok, reserved, Node};
        %% TODO Maybe these should swing back to reserved?
        'TASK_LOST' -> unreserve(reserved, requested, Node);
        'TASK_ERROR' -> unreserve(reserved, requested, Node);
        'TASK_STAGING' -> update_and_reply({reserved, Node}, {starting, Node});
        'TASK_STARTING' -> update_and_reply({reserved, Node}, {starting, Node});
        %% TODO This seems wrong: shouldn't it be 'started'? Maybe this never happens?
        'TASK_RUNNING' -> join(starting, Node);
        _ ->
            lager:debug("Unexpected status_update [~p]: ~p", [reserved, StatusUpdate]),
            {reply, ok, reserved, Node}
    end;
reserved(can_be_scheduled, _From, Node) ->
    {reply, {ok, true}, reserved, Node};
reserved(can_be_shutdown, _From, Node) ->
    {reply, {ok, false}, reserved, Node};
reserved({destroy, _}, _From, Node) ->
    update_and_reply({reserved, Node}, {shutting_down, Node});
reserved(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, reserved, Node}.

-spec starting(event(), from(), node_state()) -> state_cb_reply().
starting({status_update, StatusUpdate, _}, _From, Node) ->
    case StatusUpdate of
        'TASK_FAILED' -> update_and_reply({starting, Node}, {reserved, Node});
        'TASK_LOST' -> update_and_reply({starting, Node}, {reserved, Node});
        'TASK_ERROR' -> update_and_reply({starting, Node}, {reserved, Node});
        'TASK_STARTING' -> {reply, ok, starting, Node};
        'TASK_RUNNING' -> join(starting, Node);
        _ ->
            lager:debug("Unexpected status_update [~p]: ~p", [starting, StatusUpdate]),
            {reply, ok, starting, Node}
    end;
starting(can_be_scheduled, _From, Node) ->
    {reply, {ok, false}, starting, Node};
starting(can_be_shutdown, _From, Node) ->
    {reply, {ok, false}, starting, Node};
starting({destroy, _}, _From, Node) ->
    update_and_reply({starting, Node}, {shutting_down, Node});
starting(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, starting, Node}.

-spec restarting(event(), from(), node_state()) -> state_cb_reply().
restarting({status_update, StatusUpdate, _}, _From, Node) ->
    case StatusUpdate of
        'TASK_FINISHED' -> update_and_reply({restarting, Node}, {reserved, Node});
        'TASK_FAILED' ->   update_and_reply({restarting, Node}, {reserved, Node});
        'TASK_LOST' ->     update_and_reply({restarting, Node}, {reserved, Node});
        'TASK_ERROR' ->    update_and_reply({restarting, Node}, {reserved, Node});
        %% TODO I'm not convinced this should be allowed.
        'TASK_RUNNING' -> join(restarting, Node);
        _ ->
            lager:debug("Unexpected status_update [~p]: ~p", [restarting, StatusUpdate]),
            {reply, ok, restarting, Node}
    end;
restarting(can_be_shutdown, _From, Node) ->
    {reply, {ok, true}, restarting, Node};
restarting(can_be_scheduled, _From, Node) ->
    {reply, {ok, false}, restarting, Node};
restarting({destroy, _}, _From, Node) ->
    update_and_reply({restarting, Node}, {shutting_down, Node});
restarting(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, restarting, Node}.

-spec started(event(), from(), node_state()) -> state_cb_reply().
started({status_update, StatusUpdate, _}, _From, Node) ->
    case StatusUpdate of
        'TASK_FAILED' -> update_and_reply({started, Node}, {reserved, Node});
        'TASK_LOST' -> update_and_reply({started, Node}, {reserved, Node});
        'TASK_ERROR' -> update_and_reply({started, Node}, {reserved, Node});
        'TASK_KILLED' -> update_and_reply({started, Node}, {reserved, Node});
        'TASK_FINISHED' -> update_and_reply({started, Node}, {reserved, Node});
        _ ->
            lager:debug("Unexpected status_update [~p]: ~p", [started, StatusUpdate]),
            {reply, ok, started, Node}
    end;
started(restart, _From, Node) ->
    update_and_reply({started, Node}, {restarting, Node});
started(can_be_scheduled, _From, Node) ->
    {reply, {ok, false}, started, Node};
started(can_be_shutdown, _From, Node) ->
    {reply, {ok, false}, started, Node};
started({destroy, true}, _From, Node) ->
    update_and_reply({started, Node}, {shutting_down, Node});
started({destroy, false}, _From, Node) ->
    leave(started, Node, ?LEAVING_TIMEOUT);
started(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, started, Node}.

-spec leaving(event(), from(), node_state()) -> state_cb_reply().
leaving({status_update, StatusUpdate, _}, _From, Node) ->
    case StatusUpdate of
        'TASK_FINISHED' ->
            delete_reply_stop({leaving, Node}, ok, normal);
        'TASK_FAILED' ->
            delete_reply_stop({leaving, Node}, ok, normal);
        'TASK_KILLED' ->
            delete_reply_stop({leaving, Node}, ok, normal);
        'TASK_LOST' ->
            delete_reply_stop({leaving, Node}, ok, normal);
        'TASK_ERROR' ->
            delete_reply_stop({leaving, Node}, ok, normal);
        _ ->
            lager:debug("Unexpected status_update [~p]: ~p", [leaving, StatusUpdate]),
            {reply, ok, leaving, Node}
    end;
leaving(can_be_shutdown, _From, Node) ->
    {reply, {ok, false}, leaving, Node};
leaving(can_be_scheduled, _From, Node) ->
    {reply, {ok, false}, leaving, Node};
leaving(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, leaving, Node}.

-spec shutting_down(event(), from(), node_state()) -> state_cb_reply().
shutting_down({status_update, StatusUpdate, _}, _From, Node) ->
    case StatusUpdate of
        'TASK_FINISHED' -> delete_reply_stop({shutting_down, Node}, ok, normal);
        %% TODO Perhaps the non-standard statuses should lead an abnormal stop?
        'TASK_KILLED' -> delete_reply_stop({shutting_down, Node}, ok, normal);
        'TASK_FAILED' -> delete_reply_stop({shutting_down, Node}, ok, normal);
        'TASK_LOST' -> delete_reply_stop({shutting_down, Node}, ok, normal);
        'TASK_ERROR' -> delete_reply_stop({shutting_down, Node}, ok, normal);
        _ ->
            lager:debug("Unexpected status_update [~p]: ~p", [shutting_down, StatusUpdate]),
            {reply, ok, shutting_down, Node}
    end;
shutting_down(can_be_shutdown, _From, Node) ->
    {reply, {ok, true}, shutting_down, Node};
shutting_down(can_be_scheduled, _From, Node) ->
    {reply, {ok, false}, shutting_down, Node};
shutting_down({destroy, false}, _From, Node) ->
    {reply, ok, shutting_down, Node};
shutting_down({destroy, true}, _From, Node) ->
    %% There are situations where a node is deleted before it ever gets launched
    delete_reply_stop({shutdown, Node}, ok, normal);
shutting_down(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, shutting_down, Node}.

-spec shutdown(event(), from(), node_state()) -> state_cb_reply().
shutdown({status_update, StatusUpdate, _}, _From, Node) ->
    lager:debug("Unexpected status_update [~p]: ~p", [shutdown, StatusUpdate]),
    {reply, ok, shutdown, Node};
shutdown(can_be_shutdown, _From, Node) ->
    {reply, {ok, false}, shutdown, Node};
shutdown(can_be_scheduled, _From, Node) ->
    {reply, {ok, false}, shutdown, Node};
shutdown({destroy, _}, _From, Node) ->
    %% TODO Maybe this isn't quite right? I feel like this is side-effect-y
    delete_reply_stop({shutdown, Node}, ok, normal);
shutdown(_Event, _From, Node) ->
    {reply, {error, unhandled_event}, shutdown, Node}.

-spec handle_sync_event(event(), from(), state(), node_state()) ->
                               state_cb_reply().
handle_sync_event(set_reconciled, _From, State, Node) ->
    lager:info("Setting reconciliation for node: ~p", [Node]),
    Node1 = Node#node{reconciled = true},
    update_and_reply({State, Node}, {State, Node1});
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, {unhandled_sync_event, _Event}}, StateName, State}.

-spec handle_info(term(), state(), node_state()) -> state_cb_return().
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% FIXME Node cleanup goes here
-spec terminate(term(), state(), node_state()) -> ok.
terminate(_Reason, _StateName, _State) ->
    ok.

-spec code_change(term(), state(), node_state(), term()) ->
    {ok, state(), node_state()}.
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

-spec add_node({state(), node_state()}) -> ok | {error, term()}.
add_node({State, Node}) ->
    rms_metadata:add_node(to_list({State, Node})).

-spec update_and_reply({state(), node_state()}, {state(), node_state()}) -> state_cb_reply().
update_and_reply({_, _}=N0, {_, _}=N1) ->
    update_and_reply(N0, N1, infinity).

update_and_reply({State, #node{key = Key} = Node}, {NewState, Node1}, Timeout) ->
    case rms_metadata:update_node(Key, to_list({NewState, Node1})) of
        ok ->
            {reply, ok, NewState, Node1, Timeout};
        {error, Reason} ->
            lager:error("Error updating state for node: ~p, new state: ~p, reason: ~p.", [Node, NewState, Reason]),
            {reply, {error, Reason}, State, Node, Timeout}
    end.

-spec delete_and_stop({state(), node_state()}, Reason :: term()) -> ok | {error, term()}.
delete_and_stop({_State, #node{key = Key} = Node}, Reason) ->
    case rms_metadata:delete_node(Key) of
        ok -> 
            ok = rms_cluster_manager:node_stopped(Node#node.cluster_key, Key),
            {stop, Reason, Node};
        {error, _}=Error ->
            {stop, Error, Node}
    end.

-spec delete_reply_stop({state(), node_state()}, reply(), Reason :: term()) -> ok | {error, term()}.
delete_reply_stop({State, #node{key = Key} = Node}, Reply, Reason) ->
    case rms_metadata:delete_node(Key) of
        ok -> 
            ok = rms_cluster_manager:node_stopped(Node#node.cluster_key, Key),
            {stop, Reason, Reply, Node};
        {error,_} = Error ->
            {reply, Error, State, Node}
    end.

leave(State, #node{cluster_key = Cluster, key = Key} = Node, Timeout) ->
    case rms_cluster_manager:leave(Cluster, Key) of
        ok ->
            lager:info("~p left cluster ~p successfully.", [Key, Cluster]),
            update_and_reply({State, Node}, {leaving, Node}, Timeout);
        {error, no_suitable_nodes} ->
            lager:info("~p was unable to leave cluster ~p because no nodes were available to leave from.", [Key, Cluster]),
            update_and_reply({State, Node}, {shutting_down, Node}, Timeout);
        {error, Reason} ->
            lager:warning("~p was unable to leave cluster ~p because ~p.", [Key, Cluster, Reason]),
            {reply, {error, Reason}, State, Node}
    end.

join(State, #node{cluster_key = Cluster, key = Key} = Node) ->
    case rms_cluster_manager:maybe_join(Cluster, Key) of
        ok ->
            ok = rms_cluster_manager:node_started(Cluster, Key),
            update_and_reply({State, Node}, {started, Node});
        {error, no_suitable_nodes} ->
            ok = rms_cluster_manager:node_started(Cluster, Key),
            update_and_reply({State, Node}, {started, Node});
        {error, Reason} ->
            %% Maybe we should try to kill the node and restart task here?
            {reply, {error, Reason}, State, Node}
    end.

unreserve(State, NewState, Node) ->
    Node1 = Node#node{hostname = "",
                      agent_id_value = "",
                      persistence_id = "",
                      attributes = []},
    update_and_reply({State, Node}, {NewState, Node1}).

-spec from_list(rms_metadata:node_state()) -> {state(), node_state()}.
from_list(NodeList) ->
    {proplists:get_value(status, NodeList),
     #node{key = proplists:get_value(key, NodeList),
           cluster_key = proplists:get_value(cluster_key, NodeList),
           node_name = proplists:get_value(node_name, NodeList),
           hostname = proplists:get_value(hostname, NodeList),
           http_port = proplists:get_value(http_port, NodeList),
           pb_port = proplists:get_value(pb_port, NodeList),
           disterl_port = proplists:get_value(disterl_port, NodeList),
           executor_id_value = proplists:get_value(executor_id_value, NodeList),
           agent_id_value = proplists:get_value(agent_id_value, NodeList),
           container_path = proplists:get_value(container_path, NodeList),
           persistence_id = proplists:get_value(persistence_id, NodeList),
           reconciled = proplists:get_value(reconciled, NodeList),
           attributes = proplists:get_value(attributes, NodeList)}}.

-spec to_list({state(), node_state()}) -> rms_metadata:node_state().
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
         executor_id_value = ExecutorIdValue,
         container_path = ContainerPath,
         persistence_id = PersistenceId,
         reconciled = Reconciled,
         attributes = Attributes}}) ->
    [{key, Key},
     {status, Status},
     {cluster_key, ClusterKey},
     {node_name, NodeName},
     {hostname, Hostname},
     {http_port, HttpPort},
     {pb_port, PbPort},
     {disterl_port, DisterlPort},
     {agent_id_value, AgentIdValue},
     {executor_id_value, ExecutorIdValue},
     {container_path, ContainerPath},
     {persistence_id, PersistenceId},
     {reconciled, Reconciled},
     {attributes, Attributes}].
