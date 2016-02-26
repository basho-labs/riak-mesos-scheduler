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

-module(rms_node_manager).

-export([node_data/5]).

-export([node_keys/0]).

-export([node_needs_to_be_reconciled/1,
         node_can_be_scheduled/1,
         node_has_reservation/1]).

-export([apply_unreserved_offer/3]).

-record(node_data, {cpus :: float(),
                    mem :: float(),
                    disk :: float(),
                    num_ports :: pos_integer(),
                    role :: string(),
                    principal :: string(),
                    container_path :: string()}).

-type node_data() :: #node_data{}.
-export_type([node_data/0]).

-define(NODE_NUM_PORTS, 10).

-define(NODE_CONTAINER_PATH, "root").

-define(CPUS_PER_EXECUTOR, 0.1).

-define(MEM_PER_EXECUTOR, 32.0).

%% External functions.

-spec node_data(float(), float(), float(), string(), string()) -> node_data().
node_data(Cpus, Mem, Disk, Role, Principal) ->
    #node_data{cpus = Cpus,
               mem = Mem,
               disk = Disk,
               num_ports = ?NODE_NUM_PORTS,
               role = Role,
               principal = Principal,
               container_path = ?NODE_CONTAINER_PATH}.

-spec node_keys() -> [rms_node:key()].
node_keys() ->
    %% Tmp solution for testing the resource managment.
    ["test_1"].

-spec node_needs_to_be_reconciled(rms_node:key()) -> boolean().
node_needs_to_be_reconciled(_NodeKey) ->
    %% Tmp solution for testing the resource managment.
    false.

-spec node_can_be_scheduled(rms_node:key()) -> boolean().
node_can_be_scheduled(_NodeKey) ->
    %% Tmp solution for testing the resource managment.
    true.

-spec node_has_reservation(rms_node:key()) -> boolean().
node_has_reservation(_NodeKey) ->
    %% Tmp solution for testing the resource managment.
    case get(reg) of
        undefined ->
            false;
        _ ->
            true
    end.

-spec apply_unreserved_offer(rms_node:key(), rms_offer_helper:offer_helper(),
                             node_data()) ->
    {ok, rms_offer_helper:offer_helper()} | {error, not_enophe_resources}.
apply_unreserved_offer(_NodeKey, OfferHelper,
                       #node_data{cpus = NodeCpus,
                                  mem = NodeMem,
                                  disk = NodeDisk,
                                  num_ports = NodeNumPorts,
                                  role = Role,
                                  principal = Principal,
                                  container_path = ContainerPath}) ->
    case rms_offer_helper:can_fit_unreserved(NodeCpus + ?CPUS_PER_EXECUTOR,
                                             NodeMem + ?MEM_PER_EXECUTOR,
                                             NodeDisk, NodeNumPorts,
                                             OfferHelper) of
        true ->
            %% Remove requirements from offer helper.
            OfferHelper1 =
                rms_offer_helper:apply_unreserved_resources(?CPUS_PER_EXECUTOR,
                                                            ?MEM_PER_EXECUTOR,
                                                            undefined,
                                                            NodeNumPorts,
                                                            OfferHelper),
            %% Reserve resources.
            OfferHelper2 =
                rms_offer_helper:make_reservation(NodeCpus, NodeMem, NodeDisk,
                                                  undefined, Role, Principal,
                                                  OfferHelper1),
            %% Make volume.
            PersistenceId = node_persistence_id(),
            OfferHelper3 =
                rms_offer_helper:make_volume(NodeDisk, Role, Principal,
                                             PersistenceId, ContainerPath,
                                             OfferHelper2),
            %% Tmp solution for testing the resource managment.
            put(reg, true),

            %% TODO:
            %% AgentIdValue = rms_offer_helper:get_agent_id_value(OfferHelper3),
            %% Hostname = rms_offer_helper:get_hostname(OfferHelper3),
            %% Get node pid by node key and send to the node process.
            %% AgentIdValue, Hostname process
            {ok, OfferHelper3};
        false ->
            {error, not_enophe_resources}
    end.

%% Internal functions.

node_persistence_id() ->
    %% Generate uuid here.
    "uuid".
