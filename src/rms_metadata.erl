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

-export([get_clusters/0,
         get_cluster/1,
         add_cluster/1,
         update_cluster/2,
         delete_cluster/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {root_node :: string()}).

-type scheduler() :: [{atom(), term()}].
-export_type([scheduler/0]).

-type cluster() :: [{atom(), term()}].
-export_type([cluster/0]).

-type node() :: [{atom(), term()}].
-export_type([node/0]).

-type state() :: #state{}.

-define(CLUSTER_TAB, rms_metadata_clusters).

-define(NODE_TAB, rms_metadata_nodes).

-define(ZK_CLUSTER_NODE, "clusters").

-define(ZK_NODE_NODE, "nodes").

%% External functions.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, {}, []).

-spec get_clusters() -> [{string(), [{atom(), term()}]}].
get_clusters() ->
    ets:tab2list(?CLUSTER_TAB).

-spec get_cluster(string()) -> {ok, cluster()} | {error, not_found}.
get_cluster(Key) ->
    case ets:lookup(?CLUSTER_TAB, Key) of
        [{_Key, Cluster}] ->
            {ok, Cluster};
        [] ->
            {error, not_found}
    end.

-spec add_cluster(cluster()) -> ok | {error, term()}.
add_cluster(Cluster) ->
    gen_server:call(?MODULE, {add_cluster, Cluster}).

-spec update_cluster(string(), cluster()) -> ok | {error, term()}.
update_cluster(Key, Cluster) ->
    gen_server:call(?MODULE, {update_cluster, Key, Cluster}).

-spec delete_cluster(string()) -> ok | {error, term()}.
delete_cluster(Key) ->
    gen_server:call(?MODULE, {delete_cluster, Key}).

%% gen_server callback functions.

-spec init({}) -> state().
init({}) ->
    ets:new(?CLUSTER_TAB, [set, protected, named_table]),
    ets:new(?NODE_TAB, [set, protected, named_table]),
    {ok, RootNode, _Data} = mesos_metadata_manager:get_root_node(),
    State = #state{root_node = RootNode},
    restore_clusters(State),
    {ok, State}.

handle_call({add_cluster, Cluster}, _From, State) ->
    {reply, add_cluster(Cluster, State), State};
handle_call({update_cluster, Key, Cluster}, _From, State) ->
    {reply, update_cluster(Key, Cluster, State), State};
handle_call({delete_cluster, Key}, _From, State) ->
    {reply, delete_cluster(Key, State), State};
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

-spec add_cluster(cluster(), state()) -> ok | {error, cluster_exists | term()}.
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
            {error, cluster_exists}
    end.

-spec update_cluster(string(), cluster(), state()) ->
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

-spec delete_cluster(string(), state()) -> ok | {errro, not_found | term()}.
delete_cluster(Key, State) ->
    case ets:lookup(?CLUSTER_TAB, Key) of
        [] ->
            {error, not_found};
        [_Cluster] ->
            case delete_cluster_data(Key, State) of
                ok ->
                    ets:delete(?CLUSTER_TAB, Key),
                    ok;
                {error, Error} ->
                    {error, Error}
            end
    end.

-spec get_cluster_data(string(), state()) -> {ok, cluster()} | {error, term()}.
get_cluster_data(Key, State) ->
    get_data(?ZK_CLUSTER_NODE, Key, State).

%%-spec get_node_data(string(), state()) -> {ok, node()} | {error, term()}.
%%get_node_data(Key, State) ->
%%    get_data(?ZK_NODE_NODE, Key, State).

-spec get_data(string(), string(), state()) -> {ok, term()} | {error, term()}.
get_data(Node, Key, #state{root_node = RootNode}) ->
    Path = [RootNode, "/", Node, "/", Key],
    case mesos_metadata_manager:get_node(Path) of
        {ok, _Path, BinaryData} ->
            Data = binary_to_term(BinaryData),
            {ok, Data};
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_cluster_data(string(), cluster(), state()) -> ok | {error, term()}.
set_cluster_data(Key, Cluster, State) ->
    set_data(?ZK_CLUSTER_NODE, Key, Cluster, State).

%%-spec set_node_data(string(), node(), state()) -> ok | {error, term()}.
%%set_node_data(Key, Node, State) ->
%%    set_data(?ZK_NODE_NODE, Key, Node, State).

-spec set_data(string(), string(), term(), state()) -> ok | {error, term()}.
set_data(Node, Key, Data, #state{root_node = RootNode}) ->
    Path = [RootNode, "/", Node],
    BinaryData = term_to_binary(Data),
    case mesos_metadata_manager:create_or_set(Path, Key, BinaryData) of
        {ok, _Path, _Data} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_cluster_data(string(), state()) -> ok | {error, term()}.
delete_cluster_data(Key, State) ->
    delete_data(?ZK_CLUSTER_NODE, Key, State).

%%-spec delete_node_data(string(), state()) -> ok | {error, term()}.
%%delete_node_data(Key, State) ->
%%    delete_data(?ZK_NODE_NODE, Key, State).

-spec delete_data(string(), string(), state()) -> ok | {error, term()}.
delete_data(Node, Key, #state{root_node = RootNode}) ->
    Path = [RootNode, "/", Node, "/", Key],
    case mesos_metadata_manager:delete_node(Path) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
