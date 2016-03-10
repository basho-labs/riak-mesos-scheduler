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

-module(rms_metadata).

-behavior(gen_server).

-export([start_link/0]).

-export([get_scheduler/0, set_scheduler/1]).

-export([get_clusters/0,
         get_cluster/1,
         add_cluster/1,
         update_cluster/2,
         delete_cluster/1]).

-export([get_nodes/0,
         get_node/1,
         add_node/1,
         update_node/2,
         delete_node/1]).

%% TODO: remove after development will be finished.
-export([reset/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {root_node :: string()}).

-type scheduler_state() :: [{atom(), term()}].
-export_type([scheduler_state/0]).

-type cluster_state() :: [{atom(), term()}].
-export_type([cluster_state/0]).

-type node_state() :: [{atom(), term()}].
-export_type([node_state/0]).

-type state() :: #state{}.

-define(CLUSTER_TAB, rms_metadata_clusters).

-define(NODE_TAB, rms_metadata_nodes).

-define(ZK_SCHEDULER_NODE, "scheduler").

-define(ZK_CLUSTER_NODE, "clusters").

-define(ZK_NODE_NODE, "nodes").

%% External functions.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, {}, []).

-spec get_scheduler() -> {ok, scheduler_state()} | {error, term()}.
get_scheduler() ->
    gen_server:call(?MODULE, get_scheduler).

-spec set_scheduler(scheduler_state()) -> ok | {error, term()}.
set_scheduler(Scheduler) ->
    gen_server:call(?MODULE, {set_scheduler, Scheduler}).

-spec get_clusters() -> [{rms_cluster:key(), cluster_state()}].
get_clusters() ->
    ets:tab2list(?CLUSTER_TAB).

-spec get_cluster(rms_cluster:key()) ->
    {ok, cluster_state()} | {error, not_found}.
get_cluster(Key) ->
    case ets:lookup(?CLUSTER_TAB, Key) of
        [{_Key, Cluster}] ->
            {ok, Cluster};
        [] ->
            {error, not_found}
    end.

-spec add_cluster(cluster_state()) -> ok | {error, term()}.
add_cluster(Cluster) ->
    gen_server:call(?MODULE, {add_cluster, Cluster}).

-spec update_cluster(rms_cluster:key(), cluster_state()) ->
    ok | {error, term()}.
update_cluster(Key, Cluster) ->
    gen_server:call(?MODULE, {update_cluster, Key, Cluster}).

-spec delete_cluster(rms_cluster:key()) -> ok | {error, term()}.
delete_cluster(Key) ->
    gen_server:call(?MODULE, {delete_cluster, Key}).

-spec get_nodes() -> [{string(), node_state()}].
get_nodes() ->
    ets:tab2list(?NODE_TAB).

-spec get_node(rms_node:key()) -> {ok, node_state()} | {error, not_found}.
get_node(Key) ->
    case ets:lookup(?NODE_TAB, Key) of
        [{_Key, Node}] ->
            {ok, Node};
        [] ->
            {error, not_found}
    end.

-spec add_node(node_state()) -> ok | {error, term()}.
add_node(Node) ->
    gen_server:call(?MODULE, {add_node, Node}).

-spec update_node(rms_node:key(), node_state()) -> ok | {error, term()}.
update_node(Key, Node) ->
    gen_server:call(?MODULE, {update_node, Key, Node}).

-spec delete_node(rms_node:key()) -> ok | {error, term()}.
delete_node(Key) ->
    gen_server:call(?MODULE, {delete_node, Key}).

reset() ->
    gen_server:call(?MODULE, reset).

%% gen_server callback functions.

-spec init({}) -> {ok, state()}.
init({}) ->
    ets:new(?CLUSTER_TAB, [set, protected, named_table]),
    ets:new(?NODE_TAB, [set, protected, named_table]),
    {ok, RootNode, _Data} = mesos_metadata_manager:get_root_node(),
    State = #state{root_node = RootNode},
    init_clusters(State),
    init_nodes(State),
    restore_clusters(State),
    restore_nodes(State),
    {ok, State}.

handle_call(get_scheduler, _From, State) ->
    {reply, get_scheduler(State), State};
handle_call({set_scheduler, Scheduler}, _From, State) ->
    {reply, set_scheduler(Scheduler, State), State};
handle_call({add_cluster, Cluster}, _From, State) ->
    {reply, add_cluster(Cluster, State), State};
handle_call({update_cluster, Key, Cluster}, _From, State) ->
    {reply, update_cluster(Key, Cluster, State), State};
handle_call({delete_cluster, Key}, _From, State) ->
    {reply, delete_cluster(Key, State), State};
handle_call({add_node, Node}, _From, State) ->
    {reply, add_node(Node, State), State};
handle_call({update_node, Key, Node}, _From, State) ->
    {reply, update_node(Key, Node, State), State};
handle_call({delete_node, Key}, _From, State) ->
    {reply, delete_node(Key, State), State};

%% TODO: remove.
handle_call(reset, _From, #state{root_node = RootNode} = State) ->
    SchedulerBasePath = [RootNode, "/", ?ZK_SCHEDULER_NODE],
    ClusterBasePath = [RootNode, "/", ?ZK_CLUSTER_NODE],
    NodeBasePath = [RootNode, "/", ?ZK_NODE_NODE],
    ok = mesos_metadata_manager:recursive_delete(SchedulerBasePath),
    ok = mesos_metadata_manager:recursive_delete(ClusterBasePath),
    ok = mesos_metadata_manager:recursive_delete(NodeBasePath),
    true = ets:delete_all_objects(?CLUSTER_TAB),
    true = ets:delete_all_objects(?NODE_TAB),
    {reply, ok, State};

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

-spec get_scheduler(state()) -> {ok, scheduler_state()} | {error, term()}.
get_scheduler(#state{root_node = RootNode}) ->
    Path = [RootNode, "/", ?ZK_SCHEDULER_NODE],
    case mesos_metadata_manager:get_node(Path) of
        {ok, _Path, BinaryData} ->
            Data = binary_to_term(BinaryData),
            {ok, Data};
        {error, no_node} ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_scheduler(scheduler_state(), state()) -> ok | {error, term()}.
set_scheduler(Scheduler, #state{root_node = RootNode}) ->
    BinaryData = term_to_binary(Scheduler),
    case mesos_metadata_manager:create_or_set(RootNode, ?ZK_SCHEDULER_NODE,
                                              BinaryData) of
        {ok, _Path, _Data} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec init_clusters(state()) -> ok.
init_clusters(#state{root_node = RootNode}) ->
    mesos_metadata_manager:make_child(RootNode, ?ZK_CLUSTER_NODE),
    ok.

-spec init_nodes(state()) -> ok.
init_nodes(#state{root_node = RootNode}) ->
    mesos_metadata_manager:make_child(RootNode, ?ZK_NODE_NODE),
    ok.

-spec restore_clusters(state()) -> ok.
restore_clusters(#state{root_node = RootNode} = State) ->
    Path = [RootNode, "/", ?ZK_CLUSTER_NODE],
    case mesos_metadata_manager:get_children(Path) of
        {error, no_node} ->
            ok;
        {ok, Keys} ->
            [begin
                 {ok, Cluster} = get_cluster_data(Key, State),
                 ets:insert(?CLUSTER_TAB, {Key, Cluster})
             end || Key <- Keys],
            ok
    end.

-spec restore_nodes(state()) -> ok.
restore_nodes(#state{root_node = RootNode} = State) ->
    Path = [RootNode, "/", ?ZK_NODE_NODE],
    case mesos_metadata_manager:get_children(Path) of
        {error, no_node} ->
            ok;
        {ok, Keys} ->
            [begin
                 {ok, Node} = get_node_data(Key, State),
                 ets:insert(?NODE_TAB, {Key, Node})
             end || Key <- Keys],
            ok
    end.

-spec add_cluster(cluster_state(), state()) -> ok | {error, exists | term()}.
add_cluster(Cluster, State) ->
    Key = proplists:get_value(key, Cluster),
    case ets:lookup(?CLUSTER_TAB, Key) of
        [] ->
            case set_cluster_data(Key, Cluster, State) of
                ok ->
                    ets:insert(?CLUSTER_TAB, {Key, Cluster}),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        [_Cluster] ->
            {error, exists}
    end.

-spec update_cluster(rms_cluster:key(), cluster_state(), state()) ->
    ok | {error, not_found | term()}.
update_cluster(Key, Cluster, State) ->
    case ets:lookup(?CLUSTER_TAB, Key) of
        [_Cluster] ->
            case set_cluster_data(Key, Cluster, State) of
                ok ->
                    ets:insert(?CLUSTER_TAB, {Key, Cluster}),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        [] ->
            {error, not_found}
    end.

-spec delete_cluster(rms_cluster:key(), state()) ->
    ok | {errro, not_found | term()}.
delete_cluster(Key, State) ->
    case ets:lookup(?CLUSTER_TAB, Key) of
        [] ->
            {error, not_found};
        [_Cluster] ->
            case delete_cluster_data(Key, State) of
                ok ->
                    ets:delete(?CLUSTER_TAB, Key),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec add_node(node_state(), state()) -> ok | {error, exists | term()}.
add_node(Node, State) ->
    Key = proplists:get_value(key, Node),
    case ets:lookup(?NODE_TAB, Key) of
        [] ->
            case set_node_data(Key, Node, State) of
                ok ->
                    ets:insert(?NODE_TAB, {Key, Node}),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        [_Node] ->
            {error, exists}
    end.

-spec update_node(rms_node:key(), node_state(), state()) ->
    ok | {error, not_found | term()}.
update_node(Key, Node, State) ->
    case ets:lookup(?NODE_TAB, Key) of
        [_Node] ->
            case set_node_data(Key, Node, State) of
                ok ->
                    ets:insert(?NODE_TAB, {Key, Node}),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        [] ->
            {error, not_found}
    end.

-spec delete_node(rms_node:key(), state()) -> ok | {errro, not_found | term()}.
delete_node(Key, State) ->
    case ets:lookup(?NODE_TAB, Key) of
        [] ->
            {error, not_found};
        [_Cluster] ->
            case delete_node_data(Key, State) of
                ok ->
                    ets:delete(?NODE_TAB, Key),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec get_cluster_data(rms_cluster:key(), state()) ->
    {ok, cluster_state()} | {error, term()}.
get_cluster_data(Key, State) ->
    get_data(?ZK_CLUSTER_NODE, Key, State).

-spec get_node_data(rms_node:key(), state()) ->
    {ok, node_state()} | {error, term()}.
get_node_data(Key, State) ->
    get_data(?ZK_NODE_NODE, Key, State).

-spec get_data(string(), rms_cluster:key() | rms_node:key(), state()) ->
    {ok, term()} | {error, term()}.
get_data(Node, Key, #state{root_node = RootNode}) ->
    Path = [RootNode, "/", Node, "/", Key],
    case mesos_metadata_manager:get_node(Path) of
        {ok, _Path, BinaryData} ->
            Data = binary_to_term(BinaryData),
            {ok, Data};
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_cluster_data(rms_cluster:key(), cluster_state(), state()) ->
    ok | {error, term()}.
set_cluster_data(Key, Cluster, State) ->
    set_data(?ZK_CLUSTER_NODE, Key, Cluster, State).

-spec set_node_data(rms_node:key(), node_state(), state()) ->
    ok | {error, term()}.
set_node_data(Key, Node, State) ->
    set_data(?ZK_NODE_NODE, Key, Node, State).

-spec set_data(string(), rms_cluster:key() | rms_node:key(), term(), state()) ->
    ok | {error, term()}.
set_data(Node, Key, Data, #state{root_node = RootNode}) ->
    Path = [RootNode, "/", Node],
    BinaryData = term_to_binary(Data),
    case mesos_metadata_manager:create_or_set(Path, Key, BinaryData) of
        {ok, _Path, _Data} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_cluster_data(rms_cluster:key(), state()) -> ok | {error, term()}.
delete_cluster_data(Key, State) ->
    delete_data(?ZK_CLUSTER_NODE, Key, State).

-spec delete_node_data(rms_node:key(), state()) -> ok | {error, term()}.
delete_node_data(Key, State) ->
    delete_data(?ZK_NODE_NODE, Key, State).

-spec delete_data(string(), rms_cluster:key() | rms_node:key(), state()) ->
    ok | {error, term()}.
delete_data(Node, Key, #state{root_node = RootNode}) ->
    Path = [RootNode, "/", Node, "/", Key],
    case mesos_metadata_manager:delete_node(Path) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
