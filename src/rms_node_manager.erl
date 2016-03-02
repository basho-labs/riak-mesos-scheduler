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

-behaviour(supervisor).

-export([start_link/0]).

-export([get_node/1,
         add_node/2]).

-export([node_data/1]).

-export([node_keys/0]).

-export([node_needs_to_be_reconciled/1,
         node_can_be_scheduled/1,
         node_has_reservation/1]).

-export([apply_unreserved_offer/3, apply_reserved_offer/3]).

-export([init/1]).

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

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

-spec get_node(rms_node:key()) -> {ok, rms_metadata:nd()} | {error, term()}.
get_node(Key) ->
    rms_metadata:get_node(Key).

-spec add_node(rms_node:key(), rms_cluster:key()) -> ok | {error, term()}.
add_node(Key, ClusterKey) ->
    case get_node(Key) of
        {ok, _Node} ->
            {error, exists};
        {error, not_found} ->
            NodeSpec = node_spec(Key, ClusterKey),
            case supervisor:start_child(?MODULE, NodeSpec) of
                {ok, _Pid} ->
                    ok;
                {error, {already_started, _Pid}} ->
                    {error, exists};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec node_data(rms:options()) -> node_data().
node_data(Options) ->
    #node_data{cpus = proplists:get_value(node_cpus, Options),
               mem = proplists:get_value(node_mem, Options),
               disk = proplists:get_value(node_disk, Options),
               num_ports = ?NODE_NUM_PORTS,
               role = proplists:get_value(framework_role, Options),
               principal = proplists:get_value(framework_principal, Options),
               container_path = ?NODE_CONTAINER_PATH}.

-spec node_keys() -> [rms_node:key()].
node_keys() ->
    %% Tmp solution for testing the resource management.
    ["test_1"].

-spec node_needs_to_be_reconciled(rms_node:key()) -> boolean().
node_needs_to_be_reconciled(_NodeKey) ->
    %% Tmp solution for testing the resource management.
    false.

-spec node_can_be_scheduled(rms_node:key()) -> boolean().
node_can_be_scheduled(_NodeKey) ->
    %% Tmp solution for testing the resource management.
    true.

-spec node_has_reservation(rms_node:key()) -> boolean().
node_has_reservation(_NodeKey) ->
    %% Tmp solution for testing the resource management.
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
            %% Tmp solution for testing the resource management.
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

-spec apply_reserved_offer(rms_node:key(), rms_offer_helper:offer_helper(),
    node_data()) ->
    {ok, rms_offer_helper:offer_helper()} | {error, not_enophe_resources}.
apply_reserved_offer(_NodeKey, OfferHelper,
                     #node_data{cpus = NodeCpus,
                                mem = NodeMem,
                                disk = NodeDisk,
                                num_ports = NodeNumPorts,
                                role = _Role,
                                principal = _Principal,
                                container_path = _ContainerPath}) ->
    CanFitReserved = rms_offer_helper:can_fit_reserved(NodeCpus, NodeMem,
                                                       NodeDisk, undefined,
                                                       OfferHelper),
    CanFitUnreserved = rms_offer_helper:can_fit_unreserved(?CPUS_PER_EXECUTOR,
                                                           ?MEM_PER_EXECUTOR,
                                                           undefined,
                                                           NodeNumPorts,
                                                           OfferHelper),


%%             lager:info("Launching node ~p", [Node]),
%%
%%             UrlBase = "file:///vagrant/riak-mesos-erlang",
%%             ExecutorUrlStr = UrlBase ++ "/framework/riak-mesos-executor/packages/"
%%             ++ "riak_mesos_executor-0.1.2-amd64.tar.gz",
%%             RiakExplorerUrlStr = UrlBase ++ "/framework/riak_explorer/packages/"
%%             ++ "riak_explorer-0.1.1.patch-amd64.tar.gz",
%%             RiakUrlStr = UrlBase ++ "/riak/packages/riak-2.1.3-amd64.tar.gz",
%%             CepmdUrlStr = "file:///vagrant/cepmd_linux_amd64",
%%
%%             ExecutorUrl = erl_mesos_utils:command_info_uri(ExecutorUrlStr, false, true),
%%             RiakExplorerUrl = erl_mesos_utils:command_info_uri(RiakExplorerUrlStr, false, true),
%%             RiakUrl = erl_mesos_utils:command_info_uri(RiakUrlStr, false, true),
%%             CepmdUrl = erl_mesos_utils:command_info_uri(CepmdUrlStr, false, true),
%%
%%             CommandInfoValue = "./riak_mesos_executor/bin/ermf-executor",
%%             UrlList = [ExecutorUrl, RiakExplorerUrl, RiakUrl, CepmdUrl],
%%
%%             CommandInfo = erl_mesos_utils:command_info(CommandInfoValue, UrlList),
%%
%%             TaskIdValue = Node#rms_node.key,
%%             TaskId = erl_mesos_utils:task_id(TaskIdValue),
%%
%%             OfferHelper = riak_mesos_offer_helper:new(Offer),
%%             ReservedPorts = riak_mesos_offer_helper:get_reserved_resources_ports(OfferHelper),
%%             %% FIXME Make sure we actually have 3 ports available!
%%             [HTTPPort, PBPort, DisterlPort | _Unused] = ReservedPorts,
%%
%%             FrameworkInfo = framework_info(),
%%             FrameworkName = list_to_binary(FrameworkInfo#'FrameworkInfo'.name),
%%             NodeName = iolist_to_binary([Node#rms_node.key, "@ubuntu.local"]), %% FIXME host name
%%             TaskData = [
%%                         {<<"FullyQualifiedNodeName">>, NodeName},
%%                         {<<"Host">>,                   <<"localhost">>},
%%                         {<<"Zookeepers">>,             [<<"master.mesos:2181">>]},
%%                         {<<"FrameworkName">>,          FrameworkName},
%%                         {<<"URI">>,                    <<"192.168.1.4:9090">>}, %% FIXME URI
%%                         {<<"ClusterName">>,            list_to_binary(Node#rms_node.cluster)},
%%                         {<<"HTTPPort">>,               HTTPPort},
%%                         {<<"PBPort">>,                 PBPort},
%%                         {<<"HandoffPort">>,            0},
%%                         {<<"DisterlPort">>,            DisterlPort}],
%%             TaskDataBin = iolist_to_binary(mochijson2:encode(TaskData)),
%%
%%             ExecutorId = erl_mesos_utils:executor_id("riak"),
%%             ExecutorInfo0 = erl_mesos_utils:executor_info(ExecutorId, CommandInfo),
%%             ExecutorInfo = ExecutorInfo0#'ExecutorInfo'{source = "riak"},
%%
%%             TaskInfo0 = erl_mesos_utils:task_info("riak", TaskId, AgentId, ReservedResources,
%%                                                   ExecutorInfo, undefined),
%%             TaskInfo = TaskInfo0#'TaskInfo'{data = TaskDataBin},
%%             Operation = erl_mesos_utils:launch_offer_operation([TaskInfo]),
    case CanFitReserved and CanFitUnreserved of
        true ->
            {ok, OfferHelper};
        false ->
            {error, not_enophe_resources}
    end.

%% supervisor callback function.

init({}) ->
    Specs = [node_spec(Key, proplists:get_value(cluster_key, Node)) ||
             {Key, Node} <- rms_metadata:get_nodes()],
    {ok, {{one_for_one, 10, 10}, Specs}}.

%% Internal functions.

node_spec(Key, ClusterKey) ->
    {Key,
        {rms_node, start_link, [Key, ClusterKey]},
        transient, 5000, worker, [rms_node]}.

%% Internal functions.

-spec node_persistence_id() -> string().
node_persistence_id() ->
    %% Generate uuid here.
    "uuid".
