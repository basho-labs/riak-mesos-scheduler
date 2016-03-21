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
         delete/1]).
-export([add_node/1]).

%% gen_fsm callbacks
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
		 shutting_down/2,
		 shutting_down/3
		]).

-record(cluster, {key :: rms_cluster:key(),
                  riak_config = "" :: string(),
                  advanced_config = "" :: string(),
                  node_keys = [] :: [rms_node:key()],
                  generation = 1 :: pos_integer()}).

-type key() :: string().
-export_type([key/0]).

-type status() :: undefined | requested | shutting_down.
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

-spec set_riak_config(pid(), string()) -> ok | {error, term()}.
set_riak_config(Pid, RiakConfig) ->
	gen_fsm:sync_send_all_state_event(Pid, {set_riak_config, RiakConfig}).

-spec set_advanced_config(pid(), string()) -> ok | {error, term()}.
set_advanced_config(Pid, AdvancedConfig) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_advanced_config, AdvancedConfig}).

-spec delete(pid()) -> ok | {error, term()}.
delete(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, delete).

-spec add_node(pid()) -> ok | {error, term()}.
add_node(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, add_node).

%%% gen_fsm callbacks
-type timeout() :: non_neg_integer() | infinity.
-type state() :: atom().
-type from() :: {pid(), Tag :: term()}.
-type event() :: term().
-type reply() :: term().
-type reason() :: term().
-type state_cb_return() ::
	{stop, reason(), New::cluster_state()}
	| {next_state, Next::state(), New::cluster_state()}
	| {next_state, Next::state(), New::cluster_state(), timeout()}.
-type state_cb_reply() ::
	state_cb_return()
	| {stop, reason(), reply(), New::cluster_state()}
	| {reply, reply(), Next::state(), New::cluster_state()}
	| {reply, reply(), Next::state(), New::cluster_state(), timeout()}.

-spec init(key()) ->
	{ok, state(), cluster_state()}
	| {stop, reason()}.
init(Key) ->
    case get_cluster(Key) of
        {ok, Cluster} ->
            {ok, requested, Cluster};
        {error, not_found} ->
            Cluster = #cluster{key = Key},
            case add_cluster({requested, Cluster}) of
                ok ->
                    {ok, requested, Cluster};
                {error, Reason} ->
                    {stop, Reason}
            end
    end.

% Async per-state handling
% Note that there is none.
-spec undefined(event(), cluster_state()) -> state_cb_return().
undefined(_Event, Cluster) ->
	{stop, {unhandled_event, _Event}, Cluster}.

-spec requested(event(), cluster_state()) -> state_cb_return().
requested(_Event, Cluster) ->
	{stop, {unhandled_event, _Event}, Cluster}.

-spec shutting_down(event(), cluster_state()) -> state_cb_return().
shutting_down(_Event, Cluster) ->
	{stop, {unhandled_event, _Event}, Cluster}.

% Sync per-state handling
% Note that there is none.
-spec requested(event(), from(), cluster_state()) -> state_cb_reply().
requested(_Event, _From, Cluster) ->
	{reply, {error, unhandled_event}, requested, Cluster}.

-spec undefined(event(), from(), cluster_state()) -> state_cb_return().
undefined(_Event, _From, Cluster) ->
	{reply, {error, unhandled_event}, undefined, Cluster}.

-spec shutting_down(event(), from(), cluster_state()) -> state_cb_return().
shutting_down(_Event, _From, Cluster) ->
	{reply, {error, unhandled_event}, shutting_down, Cluster}.

% gen_fsm callbacks
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
handle_sync_event(delete, _From, _StateName, Cluster) ->
	{reply, ok, shutting_down, Cluster};
handle_sync_event(add_node, _From, StateName, Cluster) ->
	#cluster{key = Key,
			 node_keys = NodeKeys,
			 generation = Generation} = Cluster,
    NodeKey = Key ++ "-" ++ integer_to_list(Generation),
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
handle_sync_event(_Event, _From, StateName, State) ->
	{reply, {error, {unhandled_event, _Event}}, StateName, State}.

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

%%% Internal functions
-spec get_cluster(key()) -> {ok, cluster_state()} | {error, term()}.
get_cluster(Key) ->
    case rms_metadata:get_cluster(Key) of
        {ok, Cluster} ->
            {ok, from_list(Cluster)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec add_cluster(cluster_state()) -> ok | {error, term()}.
add_cluster({State, Cluster}) ->
    rms_metadata:add_cluster(to_list({State, Cluster})).

-spec update_cluster(key(), cluster_state()) -> ok | {error, term()}.
update_cluster(Key, {State, Cluster}) ->
    rms_metadata:update_cluster(Key, to_list({State, Cluster})).

-spec from_list(rms_metadata:cluster_state()) -> cluster_state().
from_list(ClusterList) ->
	{proplists:get_value(status, ClusterList),
    #cluster{key = proplists:get_value(key, ClusterList),
             riak_config = proplists:get_value(riak_config, ClusterList),
             advanced_config = proplists:get_value(advanced_config,
                                                   ClusterList),
             node_keys = proplists:get_value(node_keys, ClusterList),
             generation = proplists:get_value(generation, ClusterList)}}.

-spec to_list(cluster_state()) -> rms_metadata:cluster_state().
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
