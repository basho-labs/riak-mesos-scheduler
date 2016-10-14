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

-behaviour(gen_fsm).

%% API
-export([start_link/1]).
-export([get/1,
         get/2,
         get_field_value/2,
         set_riak_config/2,
         set_advanced_config/2,
         set_generation/2,
         maybe_join/2,
         leave/3,
         destroy/1,
         add_node/2,
         commence_restart/1,
         node_started/2,
         node_stopped/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([
         requested/2,
         requested/3,
         running/2,
         running/3,
         restarting/2,
         restarting/3,
         shutdown/2,
         shutdown/3
        ]).

-record(cluster, {key :: rms_cluster:key(),
                  riak_config = <<>> :: binary(),
                  advanced_config = <<>> :: binary(),
                  generation = 1 :: pos_integer(),
                  to_restart = {[], []} :: {list(rms_node:key()), list(rms_node:key())}}).

%% TODO Validate this timeout length - also move it somewhere more configurable
-define(RESTART_TIMEOUT, 60000).
-define(SHUTDOWN_TIMEOUT, 30000).

-type key() :: string().
-export_type([key/0]).

-type status() :: requested | running | restarting | shutdown.
-export_type([status/0]).

-type cluster_state() :: #cluster{}.
-export_type([cluster_state/0]).

%%% API

-spec start_link(key()) ->
                        {ok, pid()} | {error, term()}.
start_link(Key) ->
    gen_fsm:start_link(?MODULE, Key, []).

-spec get(key()) -> {ok, rms_metadata:cluster_state()} | {error, term()}.
get(Key) ->
    rms_metadata:get_cluster(Key).

-spec get(key(), [atom()]) ->
    {ok, rms_metadata:cluster_state()} | {error, term()}.
get(Key, Fields) ->
    case rms_metadata:get_cluster(Key) of
        {ok, Cluster} ->
            {ok, [Field || {Name, _Value} = Field <- Cluster,
                  lists:member(Name, Fields)]};
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_field_value(atom(), key()) -> {ok, term()} | {error, term()}.
get_field_value(Field, Key) ->
    case rms_metadata:get_cluster(Key) of
        {ok, Cluster} ->
            case proplists:get_value(Field, Cluster, field_not_found) of
                field_not_found ->
                    {error, field_not_found};
                Value ->
                    {ok, Value}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec leave(pid(), rms_node:key(), list(rms_node:key())) -> ok | {error, term()}.
leave(Pid, NodeKey, NodeKeys) ->
    gen_fsm:send_all_state_event(Pid, {leave, NodeKey, NodeKeys}).

-spec set_riak_config(pid(), binary()) -> ok | {error, term()}.
set_riak_config(Pid, RiakConfig) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_riak_config, RiakConfig}).

-spec set_advanced_config(pid(), binary()) -> ok | {error, term()}.
set_advanced_config(Pid, AdvancedConfig) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_advanced_config, AdvancedConfig}).

-spec set_generation(pid(), non_neg_integer()) -> ok | {error, term()}.
set_generation(Pid, Generation) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_generation, Generation}).

-spec maybe_join(pid(), rms_node:key()) -> ok | {error, term()}.
maybe_join(Pid, NodeKey) ->
    gen_fsm:sync_send_all_state_event(Pid, {maybe_join, NodeKey}).

-spec destroy(pid()) -> ok | {error, term()}.
destroy(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, destroy).

-spec add_node(pid(), undefined | rms_node:key()) -> ok | {error, term()}.
add_node(Pid, NodeKey) ->
    gen_fsm:sync_send_all_state_event(Pid, {add_node, NodeKey}).

-spec commence_restart(pid()) -> ok | {error, term()}.
commence_restart(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, commence_restart).

-spec node_started(pid(), rms_node:key()) -> ok.
node_started(Pid, NodeKey) ->
    gen_fsm:send_event(Pid, {node_started, NodeKey}).

-spec node_stopped(pid(), rms_node:key()) -> ok.
node_stopped(Pid, NodeKey) ->
    gen_fsm:send_event(Pid, {node_stopped, NodeKey}).

%%% gen_fsm callbacks
-type state_timeout() :: non_neg_integer() | infinity.
-type state() :: atom().
-type from() :: {pid(), Tag :: term()}.
-type event() :: term().
-type reply() :: term().
-type reason() :: term().
-type state_cb_return() ::
        {stop, reason(), New::cluster_state()}
      | {next_state, Next::state(), New::cluster_state()}
      | {next_state, Next::state(), New::cluster_state(), state_timeout()}.
-type state_cb_reply() ::
        state_cb_return()
      | {stop, reason(), reply(), New::cluster_state()}
      | {reply, reply(), Next::state(), New::cluster_state()}
      | {reply, reply(), Next::state(), New::cluster_state(), state_timeout()}.

-spec init(key()) ->
                  {ok, state(), cluster_state()}
                      | {stop, reason()}.
init(Key) ->
    case get_cluster(Key) of
        {ok, {State, Cluster}} ->
            {ok, State, Cluster};
        {error, not_found} ->
            Cluster = #cluster{key = Key},
            case add_cluster({running, Cluster}) of
                ok ->
                    {ok, running, Cluster};
                {error, Reason} ->
                    {stop, Reason}
            end
    end.

%%% Async per-state handling
-spec requested(event(), cluster_state()) -> state_cb_return().
requested({NodeAction, _}, #cluster{ key = Key } = Cluster)
  when NodeAction == node_started; NodeAction == node_stopped ->
    %% A node can be stopped from any number of states; our state should be
    %% updated accordingly regardless.
    %% TODO Multiple states
    NKeys = rms_node_manager:get_node_keys(Key),
    Starting =
        lists:filter(fun(Node) ->
                                case rms_node_manager:get_node_state(Node) of
                                    {ok, started} -> false;
                                    {error, _} -> false;
                                    {ok, _} -> true
                                end end,
                     NKeys),
    NextState =
        case Starting of
            [] -> running;
            [_|_] -> requested
        end,
    {next_state, NextState, Cluster#cluster{}};
requested(_Event, Cluster) ->
    {stop, {unhandled_event, _Event}, Cluster}.

-spec running(event(), cluster_state()) -> state_cb_return().
running({node_started,_} = Event, #cluster{}=Cluster) ->
    %% NB Copy the transitions from requested
    requested(Event, Cluster);
running({node_stopped, _NodeKey}, Cluster) ->
    {next_state, running, Cluster};
running(_Event, Cluster) ->
    {stop, {unhandled_event, _Event}, Cluster}.

-spec restarting(event(), cluster_state()) -> state_cb_return().
%% TODO Handle timeout when restarting (i.e. when node has not come back soon enougH)
restarting(timeout, #cluster{}=Cluster) ->
    #cluster{ to_restart = { _PendingNodes, [ Waiting | _Restarted]}} = Cluster,
    %% If we've hit this, we tried to restart a node and it's not come back yet...
    lager:info("Node ~p has been restarted but not reported back.", [Waiting]),
    ok = rms_node_manager:restart_node(Waiting),
    {next_state, restarting, Cluster, ?RESTART_TIMEOUT};
%% NB: When restarting, we should only be starting one node at a time
%% TODO What happens if a user issues 'node add' while we're restarting?
restarting({node_started, NodeKey}, #cluster{
                                       to_restart = { [], [NodeKey|_]}
                                      }=Cluster) ->
    {next_state, running, Cluster#cluster{to_restart={[], []}}};
restarting({node_started, NodeKey}, #cluster{
                                       to_restart = { [Next | Pending], [NodeKey|_]=Restarted}
                                      }=Cluster) ->
    Cluster1 = Cluster#cluster{to_restart={Pending, [Next | Restarted]}},
    ok = rms_node_manager:restart_node(Next),
    ok = update_cluster(Cluster#cluster.key, {restarting, Cluster1}),
    {next_state, restarting, Cluster1};
restarting(_Event, Cluster) ->
    {stop, {unhandled_event, _Event}, Cluster}.

-spec shutdown(event(), cluster_state()) -> state_cb_return().
shutdown({node_stopped, _NodeKey}, #cluster{key=Key}=Cluster) ->
    case rms_node_manager:get_active_node_keys(Key) of
        [] ->
            ok = rms_metadata:delete_cluster(Key),
            {stop, normal, Cluster};
        [_|_] = NodeKeys ->
            %% TODO FIXME We might not need this step? Or maybe we need to be more careful with it
            _ = do_destroy(NodeKeys),
            {next_state, shutdown, Cluster#cluster{}, next_timeout(shutdown, Cluster)}
    end;
shutdown(timeout, #cluster{}=Cluster) ->
    #cluster{ key = Key } = Cluster,
    case rms_node_manager:get_active_node_keys(Key) of
        [] ->
            ok = rms_metadata:delete_cluster(Key),
            {stop, normal, Cluster};
        [_|_] = NodeKeys ->
            _ = do_destroy(NodeKeys),
            {next_state, shutdown, Cluster, next_timeout(shutdown, Cluster)}
    end;
shutdown(_Event, Cluster) ->
    {stop, {unhandled_event, _Event}, Cluster}.

%%% Sync per-state handling
%%% Note that there is none.
-spec requested(event(), from(), cluster_state()) -> state_cb_reply().
requested(_Event, _From, Cluster) ->
    {reply, {error, unhandled_event}, requested, Cluster}.

-spec running(event(), from(), cluster_state()) -> state_cb_reply().
running(_Event, _From, Cluster) ->
    {reply, {error, unhandled_event}, running, Cluster}.

-spec restarting(event(), from(), cluster_state()) -> state_cb_reply().
restarting(_Event, _From, Cluster) ->
    {reply, {error, unhandled_event}, restarting, Cluster}.

-spec shutdown(event(), from(), cluster_state()) -> state_cb_reply().
shutdown(_Event, _From, Cluster) ->
    {reply, {error, unhandled_event}, shutdown, Cluster}.

%%% gen_fsm callbacks
-spec handle_event(event(), StateName :: atom(), cluster_state()) ->
                          state_cb_return().
handle_event({leave, NodeKey, NodeKeys}, StateName,
                  #cluster{} = Cluster) ->
    case do_leave(NodeKey, NodeKeys) of
        ok -> ok;
        {error, Reason} ->
            lager:error("Node ~s failed to leave cluster: ~p", [NodeKey, Reason])
    end,
    {next_state, StateName, Cluster};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

-spec handle_sync_event(event(), from(), state(), cluster_state()) ->
                               state_cb_reply().
handle_sync_event({set_riak_config, RiakConfig}, _From, StateName, Cluster) ->
    Cluster1 = Cluster#cluster{riak_config = RiakConfig},
    case update_cluster(Cluster#cluster.key, {StateName, Cluster1}) of
        ok ->
            update_and_reply({StateName, Cluster}, {StateName, Cluster1}, ok);
        {error, _}=Err ->
            {reply, Err, StateName, Cluster}
    end;
handle_sync_event({set_advanced_config, AdvConfig}, _From, StateName, Cluster) ->
    Cluster1 = Cluster#cluster{advanced_config = AdvConfig},
    case update_cluster(Cluster#cluster.key, {StateName, Cluster1}) of
        ok ->
            update_and_reply({StateName, Cluster}, {StateName, Cluster1}, ok);
        {error,_}=Err ->
            {reply, Err, StateName, Cluster}
    end;
handle_sync_event({set_generation, Generation}, _From, StateName, Cluster) ->
    Cluster1 = Cluster#cluster{generation = Generation},
    case update_cluster(Cluster#cluster.key, {StateName, Cluster1}) of
        ok ->
            update_and_reply({StateName, Cluster}, {StateName, Cluster1}, ok);
        {error,_}=Err ->
            {reply, Err, StateName, Cluster}
    end;
handle_sync_event(destroy, _From, State0,
                  #cluster{key = Key} = Cluster) ->
    %% TODO Get rid of this clause in favour of an exponential backoff feeding shutdown(timeout, Cluster)
    case rms_node_manager:get_active_node_keys(Key) of
        [] ->
            {stop, normal, rms_metadata:delete_cluster(Key), Cluster};
        NodeKeys ->
            Reply = do_destroy(NodeKeys),
            update_and_reply({State0, Cluster}, {shutdown, Cluster}, Reply, next_timeout(shutdown, Cluster))
    end;
handle_sync_event({add_node, undefined}, _From, StateName, Cluster) ->
    #cluster{key = Key,
             generation = Generation} = Cluster,
    FrameworkName = rms_config:framework_name(),
    NodeKey = FrameworkName ++ "-" ++ Key ++ "-" ++ integer_to_list(Generation),
    case rms_node_manager:add_node(NodeKey, Key) of
        ok ->
            Cluster1 = Cluster#cluster{generation = (Generation + 1)},
            case update_cluster(Cluster#cluster.key, {StateName, Cluster1}) of
                ok ->
                    update_and_reply({StateName, Cluster}, {requested, Cluster1}, ok);
                {error,_}=Err ->
                    {reply, Err, StateName, Cluster}
            end;
        {error,_}=Err ->
            {reply, Err, StateName, Cluster}
    end;
handle_sync_event({add_node, NodeKey}, _From, StateName, Cluster) ->
    #cluster{key = Key} = Cluster,
    case rms_node_manager:add_node(NodeKey, Key) of
        ok ->
            update_and_reply({StateName, Cluster}, {requested, Cluster}, ok);
        {error,_}=Err ->
            {reply, Err, StateName, Cluster}
    end;
%% TODO This should probably move to be a strict transition
handle_sync_event({maybe_join, NodeKey}, _From, StateName,
                  #cluster{key=Key} = Cluster) ->
    NodeKeys = rms_node_manager:get_running_node_keys(Key),
    case maybe_do_join(NodeKey, NodeKeys) of
        ok ->
            {reply, ok, StateName, Cluster};
        {error, Reason} ->
            {reply, {error, Reason}, StateName, Cluster}
    end;
handle_sync_event(commence_restart, _From, StateName,
                  #cluster{key=Key} = Cluster) ->
    case rms_node_manager:get_running_node_keys(Key) of
        [] -> {reply, ok, StateName, Cluster}; %% TODO Validate this lack of state change
                                               %% Perhaps we should move to 'requested'
        [Node1 | NodeKeys] ->
            %% We'll move nodes from left to right as we confirm they've restarted
            %% and stabilised
            Cluster1 = Cluster#cluster{ to_restart = { NodeKeys, [Node1] } },
            ok = rms_node_manager:restart_node(Node1),
            %% TODO Replace this macro with a function call
            update_and_reply({StateName, Cluster}, {restarting, Cluster1}, ok, ?RESTART_TIMEOUT)
    end;
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, {unhandled_event, _Event}}, StateName, State}.

-spec handle_info(term(), state(), cluster_state()) -> state_cb_return().
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

-spec terminate(term(), state(), cluster_state()) -> ok.
terminate(_Reason, _StateName, _State) ->
    ok.

-spec code_change(term(), state(), cluster_state(), term()) ->
    {ok, state(), cluster_state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%% Internal functions
-spec get_cluster(key()) -> {ok, cluster_state()} | {error, term()}.
get_cluster(Key) ->
    case rms_metadata:get_cluster(Key) of
        {ok, Cluster} ->
            {ok, from_list(Cluster)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec add_cluster({atom(), cluster_state()}) -> ok | {error, term()}.
add_cluster({State, Cluster}) ->
    rms_metadata:add_cluster(to_list({State, Cluster})).

-spec update_cluster(key(), {atom(), cluster_state()}) -> ok | {error, term()}.
update_cluster(Key, {State, Cluster}) ->
    rms_metadata:update_cluster(Key, to_list({State, Cluster})).

update_and_reply({_, _}=Cluster0, {_, _}=Cluster1, Reply) ->
    update_and_reply(Cluster0, Cluster1, Reply, infinity).

update_and_reply({State0, #cluster{key=Key}=Cluster0}, {State1, Cluster1}=New, Reply, Timeout) ->
    case update_cluster(Key, New) of
        ok -> {reply, Reply, State1, Cluster1, Timeout};
        {error, _} = Error ->
            {reply, Error, State0, Cluster0, Timeout}
    end.

-spec maybe_do_join(rms_node:key(), [rms_node:key()]) ->
                           ok | {error, term()}.
maybe_do_join(_, []) ->
    {error, no_suitable_nodes};
maybe_do_join(NodeKey, [NodeKey|Rest]) ->
    maybe_do_join(NodeKey, Rest);
maybe_do_join(NodeKey, [ExistingNodeKey|Rest]) ->
    case {rms_node_manager:get_node_http_url(ExistingNodeKey),
          rms_node_manager:get_node_name(NodeKey),
          rms_node_manager:get_node_name(ExistingNodeKey)} of
        {{ok,U},{ok,N},{ok,E}} when
              is_list(U) and is_list(N) and is_list(E) ->
            case riak_explorer_client:join(
                   list_to_binary(U),
                   list_to_binary(N),
                   list_to_binary(E)) of
                {ok, [{<<"error">>,<<"node_still_starting">>}]} ->
                    timer:sleep(1000),
                    maybe_do_join(NodeKey, [ExistingNodeKey|Rest]);
                {ok, [{<<"error">>,<<"not_single_node">>}]} ->
                    ok;
                {ok, [{_,[{<<"success">>,true}]}]} ->
                    ok;            
                {ok, Json} ->
                    lager:warning("Failed node join attempt from node ~s to node ~s. Reason: ~p", [N, E, Json]),
                    maybe_do_join(NodeKey, Rest);
                {error, Reason} ->
                    lager:warning("Failed node join attempt from node ~s to node ~s. Reason: ~p", [N, E, Reason]),
                    maybe_do_join(NodeKey, Rest)
            end;
        _ ->
            maybe_do_join(NodeKey, Rest)
    end.

-spec do_leave(rms_node:key(), [rms_node:key()]) ->
                      ok | {error, term()}.
do_leave(_, []) ->
    {error, no_suitable_nodes};
do_leave(NodeKey, [NodeKey|Rest]) ->
    do_leave(NodeKey, Rest);
do_leave(NodeKey, [ExistingNodeKey|Rest]) ->
    case {rms_node_manager:get_node_http_url(ExistingNodeKey),
          rms_node_manager:get_node_name(NodeKey),
          rms_node_manager:get_node_name(ExistingNodeKey)} of
        {{ok,U},{ok,N},{ok,E}} when
              is_list(U) and is_list(N) and is_list(E) ->
            case riak_explorer_client:leave(
                   list_to_binary(U),
                   list_to_binary(E),
                   list_to_binary(N)) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    lager:warning("Failed node leave attempt from node ~s to node ~s. Reason: ~p", [N, E, Reason]),
                    do_leave(NodeKey, Rest)
            end;
        _ ->
            do_leave(NodeKey, Rest)
    end.

-spec do_destroy([rms_node:key()]) ->
                       ok | {error, term()}.
do_destroy([]) ->
    ok;
do_destroy([ExistingNodeKey|Rest]) ->
    case rms_node_manager:destroy_node(ExistingNodeKey, true) of
        ok ->
            do_destroy(Rest);
        {error, Reason} ->
            {error, Reason}
    end.

next_timeout(shutdown, #cluster{}=_Cluster) ->
    ?SHUTDOWN_TIMEOUT.

-spec from_list(rms_metadata:cluster_state()) -> {atom(), cluster_state()}.
from_list(ClusterList) ->
    {proplists:get_value(status, ClusterList),
     #cluster{key = proplists:get_value(key, ClusterList),
              riak_config = proplists:get_value(riak_config, ClusterList),
              advanced_config = proplists:get_value(advanced_config,
                                                    ClusterList),
              to_restart = proplists:get_value(to_restart, ClusterList, {[], []}),
              generation = proplists:get_value(generation, ClusterList)}}.

-spec to_list({atom(), cluster_state()}) -> rms_metadata:cluster_state().
to_list({State,
         #cluster{key = Key,
                  riak_config = RiakConf,
                  advanced_config = AdvancedConfig,
                  to_restart = ToRestart,
                  generation = Generation}}) ->
    [{key, Key},
     {status, State},
     {riak_config, RiakConf},
     {advanced_config, AdvancedConfig},
     {to_restart, ToRestart},
     {generation, Generation}].
