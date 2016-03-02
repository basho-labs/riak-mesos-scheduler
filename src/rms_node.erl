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

-module(rms_node).

-export([start_link/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(node, {key :: key(),
               status = requested :: status(),
               cluster_key :: rms_cluster:key(),
               node_name :: node(),
               hostname = "" :: string(),
               http_port :: pos_integer(),
               pb_port :: pos_integer(),
               disterl_port :: pos_integer(),
               agent_id = "" :: string(),
               container_path = "" :: string(),
               persistence_id = "" :: string()}).

-type key() :: string().
-export_type([key/0]).

-type status() :: requested.
-export_type([status/0]).

-type nd() :: #node{}.
-export_type([node/0]).

%% External functions.

-spec start_link(key(), rms_cluster:key()) ->
    {ok, pid()} | {error, term()}.
start_link(Key, ClusterKey) ->
    gen_server:start_link(?MODULE, {Key, ClusterKey}, []).

%% gen_server callback functions.

init({Key, ClusterKey}) ->
    case get_node(Key) of
        {ok, Node} ->
            {ok, Node};
        {error, not_found} ->
            Node = #node{key = Key,
                         cluster_key = ClusterKey},
            case add_node(Node) of
                ok ->
                    {ok, Node};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

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

-spec get_node(key()) -> {ok, nd()} | {error, term()}.
get_node(Key) ->
    case rms_metadata:get_node(Key) of
        {ok, Node} ->
            {ok, from_list(Node)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec add_node(nd()) -> ok | {error, term()}.
add_node(Node) ->
    rms_metadata:add_node(to_list(Node)).

%%-spec update_node_state(node(), node()) ->
%%    {reply, ok | {error, term()}, cluster()}.
%%update_node_state(#node{key = Key} = Node, NewNode) ->
%%    case update_node(Key, NewNode) of
%%        ok ->
%%            {reply, ok, NewNode};
%%        {error, Reason} ->
%%            {reply, {error, Reason}, Node}
%%    end.

%%-spec update_node(key(), node()) -> ok | {error, term()}.
%%update_node(Key, Node) ->
%%    rms_metadata:update_node(Key, to_list(Node)).

-spec from_list(rms_metadata:nd()) -> nd().
from_list(NodeList) ->
    #node{key = proplists:get_value(key, NodeList),
          status = proplists:get_value(status, NodeList),
          cluster_key = proplists:get_value(cluster_key, NodeList),
          node_name = proplists:get_value(node_name, NodeList),
          hostname = proplists:get_value(hostname, NodeList),
          http_port = proplists:get_value(http_port, NodeList),
          pb_port = proplists:get_value(pb_port, NodeList),
          disterl_port = proplists:get_value(disterl_port, NodeList),
          agent_id = proplists:get_value(agent_id, NodeList),
          container_path = proplists:get_value(container_path, NodeList),
          persistence_id = proplists:get_value(persistence_id, NodeList)}.

-spec to_list(nd()) -> rms_metadata:nd().
to_list(#node{key = Key,
              status = Status,
              cluster_key = ClusterKey,
              node_name = NodeName,
              hostname = Hostname,
              http_port = HttpPort,
              pb_port = PbPort,
              disterl_port = DisterlPort,
              agent_id = AgentId,
              container_path = ContainerPath,
              persistence_id = PersistenceId}) ->
    [{key, Key},
     {status, Status},
     {cluster_key, ClusterKey},
     {node_name, NodeName},
     {hostname, Hostname},
     {http_port, HttpPort},
     {pb_port, PbPort},
     {disterl_port, DisterlPort},
     {agent_id, AgentId},
     {container_path, ContainerPath},
     {persistence_id, PersistenceId}].
