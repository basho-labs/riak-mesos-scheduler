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
         get_field_value/2,
         set_riak_config/2,
         set_advanced_config/2,
         maybe_join/2,
         leave/2,
         delete/1,
		 add_node/1,
		 commence_restart/1]).

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
                  node_keys = [] :: [rms_node:key()],
                  generation = 1 :: pos_integer(),
				  to_restart = {[], []} :: {list(rms_node:key()), list(rms_node:key())}}).

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

-spec set_riak_config(pid(), binary()) -> ok | {error, term()}.
set_riak_config(Pid, RiakConfig) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_riak_config, RiakConfig}).

-spec set_advanced_config(pid(), binary()) -> ok | {error, term()}.
set_advanced_config(Pid, AdvancedConfig) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_advanced_config, AdvancedConfig}).

-spec maybe_join(pid(), rms_node:key()) -> ok | {error, term()}.
maybe_join(Pid, NodeKey) ->
    gen_fsm:sync_send_all_state_event(Pid, {maybe_join, NodeKey}).

-spec leave(pid(), rms_node:key()) -> ok | {error, term()}.
leave(Pid, NodeKey) ->
    gen_fsm:sync_send_all_state_event(Pid, {leave, NodeKey}).

-spec delete(pid()) -> ok | {error, term()}.
delete(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, delete).

-spec add_node(pid()) -> ok | {error, term()}.
add_node(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, add_node).

-spec commence_restart(pid()) -> ok | {error, term()}.
commence_restart(Pid) ->
	gen_fsm:sync_send_all_state_event(Pid, commence_restart).

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
            case add_cluster({requested, Cluster}) of
                ok ->
                    {ok, requested, Cluster};
                {error, Reason} ->
                    {stop, Reason}
            end
    end.

%%% Async per-state handling
%%% Note that there is none.
-spec requested(event(), cluster_state()) -> state_cb_return().
requested(_Event, Cluster) ->
    {stop, {unhandled_event, _Event}, Cluster}.

-spec running(event(), cluster_state()) -> state_cb_return().
running(_Event, Cluster) ->
    {stop, {unhandled_event, _Event}, Cluster}.

-spec restarting(event(), cluster_state()) -> state_cb_return().
%% TODO Handle timeout when restarting (i.e. when node has not come back soon enougH)
restarting(_Event, Cluster) ->
    {stop, {unhandled_event, _Event}, Cluster}.

-spec shutdown(event(), cluster_state()) -> state_cb_return().
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
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

-spec handle_sync_event(event(), from(), state(), cluster_state()) ->
                               state_cb_reply().
handle_sync_event({set_riak_config, RiakConfig}, _From, StateName, Cluster) ->
    Cluster1 = Cluster#cluster{riak_config = RiakConfig},
    case update_cluster(Cluster#cluster.key, {StateName, Cluster1}) of
        ok ->
            {reply, ok, StateName, Cluster1};
        {error, _}=Err ->
            {reply, Err, StateName, Cluster}
    end;
handle_sync_event({set_advanced_config, AdvConfig}, _From, StateName, Cluster) ->
    Cluster1 = Cluster#cluster{advanced_config = AdvConfig},
    case update_cluster(Cluster#cluster.key, {StateName, Cluster1}) of
        ok ->
            {reply, ok, StateName, Cluster1};
        {error,_}=Err ->
            {reply, Err, StateName, Cluster}
    end;
handle_sync_event(delete, _From, _StateName,
                  #cluster{key = Key} = Cluster) ->
    NodeKeys = rms_node_manager:get_active_node_keys(Key),
	Reply = do_delete(NodeKeys),
	{reply, Reply, shutdown, Cluster};
handle_sync_event(add_node, _From, StateName, Cluster) ->
    #cluster{key = Key,
             node_keys = NodeKeys,
             generation = Generation} = Cluster,
    FrameworkName = rms_config:framework_name(),
    NodeKey = FrameworkName ++ "-" ++ Key ++ "-" ++ integer_to_list(Generation),
    case rms_node_manager:add_node(NodeKey, Key) of
        ok ->
            Cluster1 = Cluster#cluster{node_keys = [NodeKey | NodeKeys],
                                       generation = (Generation + 1)},
            case update_cluster(Cluster#cluster.key, {StateName, Cluster1}) of
                ok ->
                    {reply, ok, StateName, Cluster1};
                {error,_}=Err ->
                    {reply, Err, StateName, Cluster}
            end;
        {error,_}=Err ->
            {reply, Err, StateName, Cluster}
    end;
handle_sync_event({maybe_join, NodeKey}, _From, StateName, 
                  #cluster{key=Key} = Cluster) ->
    NodeKeys = rms_node_manager:get_running_node_keys(Key),
    case maybe_do_join(NodeKey, NodeKeys) of
        ok ->
            {reply, ok, StateName, Cluster};
        {error, Reason} ->
            {reply, {error, Reason}, StateName, Cluster}
    end;
handle_sync_event({leave, NodeKey}, _From, StateName,
                  #cluster{key=Key} = Cluster) ->
    NodeKeys = rms_node_manager:get_running_node_keys(Key),
    case do_leave(NodeKey, NodeKeys) of
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
			RestartTimeout = 30000, %% TODO Validate this timeout length - also move it somewhere more configurable
			ok = rms_node_manager:restart_node(Node1),
			{reply, ok, restarting, Cluster1, RestartTimeout}
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
                {ok, _} ->
                    ok;
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
                    lager:warning("Failed node join attempt from node ~s to node ~s. Reason: ~p", [N, E, Reason]),
                    do_leave(NodeKey, Rest)
            end;
        _ ->
            do_leave(NodeKey, Rest)
    end.

-spec do_delete([rms_node:key()]) -> 
                       ok | {error, term()}.
do_delete([]) ->
    ok;
do_delete([ExistingNodeKey|Rest]) ->
    case rms_node_manager:delete_node(ExistingNodeKey) of
        ok ->
            do_delete(Rest);
        {error, Reason} ->
            {error, Reason}
    end.

-spec from_list(rms_metadata:cluster_state()) -> {atom(), cluster_state()}.
from_list(ClusterList) ->
    {proplists:get_value(status, ClusterList),
     #cluster{key = proplists:get_value(key, ClusterList),
              riak_config = proplists:get_value(riak_config, ClusterList),
              advanced_config = proplists:get_value(advanced_config,
                                                    ClusterList),
              node_keys = proplists:get_value(node_keys, ClusterList),
              generation = proplists:get_value(generation, ClusterList)}}.

-spec to_list({atom(), cluster_state()}) -> rms_metadata:cluster_state().
to_list({State,
         #cluster{key = Key,
                  riak_config = RiakConf,
                  advanced_config = AdvancedConfig,
                  node_keys = NodeKeys,
                  generation = Generation}}) ->
    [{key, Key},
     {status, State},
     {riak_config, RiakConf},
     {advanced_config, AdvancedConfig},
     {node_keys, NodeKeys},
     {generation, Generation}].
