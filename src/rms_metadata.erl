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

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {root_node :: string()}).

-type state() :: #state{}.

-define(CLUSTER_TAB, rms_metadata_clusters).

-define(NODE_TAB, rms_metadata_nodes).

-define(ZK_CLUSTER_NODE, "clusters").

-define(ZK_NODE_NODE, "nodes").

%% External functions.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, {}, []).

%% gen_server callback functions.

-spec init({}) -> state().
init({}) ->
    {ok, RootNode, _Data} = mesos_metadata_manager:get_root_node(),
    ets:new(?CLUSTER_TAB, [set, protected, named_table]),
    ets:new(?NODE_TAB, [set, protected, named_table]),
    {ok, #state{root_node = RootNode}}.

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


