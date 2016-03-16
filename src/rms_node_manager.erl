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

-export([get_node_keys/0,
         get_node_keys/1,
         get_node/1,
         add_node/2,
         delete_node/1]).

-export([node_data/1]).

-export([node_needs_to_be_reconciled/1,
         node_can_be_scheduled/1,
         node_has_reservation/1]).

-export([apply_unreserved_offer/3, apply_reserved_offer/3]).

-export([init/1]).

-record(node_data, {cpus :: float(),
                    mem :: float(),
                    disk :: float(),
                    num_ports :: pos_integer(),
                    name :: string(),
                    role :: string(),
                    principal :: string(),
                    container_path :: string(),
                    artifact_urls :: [string()]}).

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

-spec get_node_keys() -> [rms_node:key()].
get_node_keys() ->
    [Key || {Key, _} <- rms_metadata:get_nodes()].

-spec get_node_keys(rms_cluster:key()) -> [rms_node:key()].
get_node_keys(ClusterKey) ->
    [Key || {Key, Node} <- rms_metadata:get_nodes(),
     ClusterKey =:= proplists:get_value(cluster_key, Node)].

-spec get_node(rms_node:key()) ->
    {ok, rms_metadata:node_state()} | {error, term()}.
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

-spec delete_node(rms_node:key()) -> ok | {error, term()}.
delete_node(Key) ->
    case get_node_pid(Key) of
        {ok, Pid} ->
            rms_node:delete(Pid);
        {error, Reason} ->
            {error, Reason}
    end.

-spec node_data(rms:options()) -> node_data().
node_data(Options) ->
    #node_data{cpus = proplists:get_value(node_cpus, Options),
               mem = proplists:get_value(node_mem, Options),
               disk = proplists:get_value(node_disk, Options),
               num_ports = ?NODE_NUM_PORTS,
               name = proplists:get_value(framework_name, Options),
               role = proplists:get_value(framework_role, Options),
               principal = proplists:get_value(framework_principal, Options),
               container_path = ?NODE_CONTAINER_PATH,
               artifact_urls = proplists:get_value(artifact_urls, Options)}.

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
    {ok, rms_offer_helper:offer_helper()} |
    {error, not_enough_resources | term()}.
apply_unreserved_offer(NodeKey, OfferHelper, NodeData) ->
    case get_node_pid(NodeKey) of
        {ok, Pid} ->
            Hostname = rms_offer_helper:get_hostname(OfferHelper),
            AgentIdValue = rms_offer_helper:get_agent_id_value(OfferHelper),
            PersistenceId = node_persistence_id(),
            case rms_node:set_reserved(Pid, Hostname, AgentIdValue,
                                       PersistenceId) of
                ok ->
                    %% Tmp solution for testing the resource management.
                    put(reg, true),

                    apply_unreserved(PersistenceId, OfferHelper, NodeData);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec apply_unreserved(string(), rms_offer_helper:offer_helper(),
                       node_data()) ->
    {ok, rms_offer_helper:offer_helper()} | {error, not_enough_resources}.
apply_unreserved(PersistenceId, OfferHelper,
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
            OfferHelper3 =
                rms_offer_helper:make_volume(NodeDisk, Role, Principal,
                                             PersistenceId, ContainerPath,
                                             OfferHelper2),
            {ok, OfferHelper3};
        false ->
            {error, not_enough_resources}
    end.

-spec apply_reserved_offer(rms_node:key(), rms_offer_helper:offer_helper(),
    node_data()) ->
    {ok, rms_offer_helper:offer_helper()} |
    {error, not_enough_resources | term()}.
apply_reserved_offer(NodeKey, OfferHelper,
                     #node_data{cpus = NodeCpus,
                                mem = NodeMem,
                                disk = NodeDisk,
                                num_ports = NodeNumPorts,
                                name = Name,
                                role = Role,
                                principal = Principal,
                                container_path = ContainerPath,
                                artifact_urls = ArtifactUrls}) ->
    case get_node(NodeKey) of
        {ok, Node} ->
            CanFitReserved =
                rms_offer_helper:can_fit_reserved(NodeCpus, NodeMem, NodeDisk,
                                                  undefined, OfferHelper),
            CanFitUnreserved =
                rms_offer_helper:can_fit_unreserved(?CPUS_PER_EXECUTOR,
                                                    ?MEM_PER_EXECUTOR,
                                                    undefined, NodeNumPorts,
                                                    OfferHelper),
            case CanFitReserved and CanFitUnreserved of
                true ->
                    ClusterKey = proplists:get_value(cluster_key, Node),
                    PersistenceId = proplists:get_value(persistence_id, Node),
                    Hostname = proplists:get_value(hostname, Node),
                    AgentIdValue = proplists:get_value(agent_id_value, Node),

                    %% Apply reserved resources.
                    OfferHelper1 =
                        rms_offer_helper:apply_reserved_resources(
                            NodeCpus, NodeMem, NodeDisk, undefined, Role,
                            Principal, PersistenceId, ContainerPath,
                            OfferHelper),
                    %% Apply unreserved resources.
                    OfferHelper2 =
                        rms_offer_helper:apply_unreserved_resources(
                            undefined, undefined, undefined, NodeNumPorts,
                            OfferHelper1),

                    AgentId = erl_mesos_utils:agent_id(AgentIdValue),

                    [RiakUrlStr, RiakExplorerUrlStr, ExecutorUrlStr] =
                        ArtifactUrls,

                    ExecutorUrl = erl_mesos_utils:command_info_uri(ExecutorUrlStr, false, true),
                    RiakExplorerUrl = erl_mesos_utils:command_info_uri(RiakExplorerUrlStr, false, true),
                    RiakUrl = erl_mesos_utils:command_info_uri(RiakUrlStr, false, true),

                    CommandInfoValue = "./riak_mesos_executor/bin/ermf-executor",
                    UrlList = [ExecutorUrl, RiakExplorerUrl, RiakUrl],
                    CommandInfo = erl_mesos_utils:command_info(CommandInfoValue, UrlList),

                    TaskId = erl_mesos_utils:task_id(NodeKey),

                    ReservedPorts =
                        rms_offer_helper:get_unreserved_resources_ports(OfferHelper),

                    [HTTPPort, PBPort, DisterlPort | _Ports] = ReservedPorts,

                    NodeName = iolist_to_binary([NodeKey, "@", Hostname]),
                    TaskData = [{<<"FullyQualifiedNodeName">>, NodeName},
                                {<<"Host">>,                   Hostname},
                                {<<"Zookeepers">>,             [<<"master.mesos:2181">>]}, %% FIXME
                                {<<"FrameworkName">>,          Name},
                                {<<"URI">>,                    <<"192.168.1.4:9090">>}, %% FIXME
                                {<<"ClusterName">>,            ClusterKey},
                                {<<"HTTPPort">>,               HTTPPort},
                                {<<"PBPort">>,                 PBPort},
                                {<<"HandoffPort">>,            0},
                                {<<"DisterlPort">>,            DisterlPort}],
                    TaskDataBin = iolist_to_binary(mochijson2:encode(TaskData)),

                    ExecutorId = erl_mesos_utils:executor_id("riak"), %% FIXME

                    Source = "riak", %% FIXME
                    ExecutorInfo =
                        erl_mesos_utils:executor_info(ExecutorId, CommandInfo,
                                                      undefined, undefined,
                                                      Source),


                    TaskName = "riak",
                    %% FIXME Is this the appropriate place for ReservedResources?
                    ReservedResources = rms_offer_helper:get_reserved_resources(OfferHelper2),
                    TaskInfo =
                        erl_mesos_utils:task_info(TaskName, TaskId, AgentId,
                                                  ReservedResources,
                                                  ExecutorInfo, undefined,
                                                  TaskDataBin),
                    {ok, rms_offer_helper:add_task_to_launch(TaskInfo,
                                                             OfferHelper)};
                false ->
                    {error, not_enough_resources}
            end;
        {error, Reason} ->
            {error, Reason}
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

-spec get_node_pid(rms_node:key()) -> {ok, pid()} | {error, not_found}.
get_node_pid(Key) ->
    case lists:keyfind(Key, 1, supervisor:which_children(?MODULE)) of
        {_Key, Pid, _, _} ->
            {ok, Pid};
        false ->
            {error, not_found}
    end.

%% Internal functions.

-spec node_persistence_id() -> string().
node_persistence_id() ->
    %% TODO: Generate uuid here.
    "uuid".
