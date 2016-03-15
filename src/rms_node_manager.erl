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

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([start_link/0]).

-export([get_node_keys/0,
         get_node_keys/1,
         get_node/1,
         add_node/2,
         delete_node/1]).

-export([node_keys/0]).

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

-spec get_node_keys() -> [rms_node:key()].
get_node_keys() ->
    [Key || {Key, _} <- rms_metadata:get_nodes()].

-spec get_node_keys(rms_cluster:key()) -> [rms_node:key()].
get_node_keys(ClusterKey) ->
    [Key || {Key, Node} <- rms_metadata:get_nodes(),
     ClusterKey =:= proplists:get_value(cluster_key, Node)].

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

-spec delete_node(rms_node:key()) -> ok | {error, term()}.
delete_node(Key) ->
    case get_node_pid(Key) of
        {ok, Pid} ->
            rms_node:delete(Pid);
        {error, Reason} ->
            {error, Reason}
    end.

%% Tmp solution for testing the resource management.
%% TODO: replace with get_node_keys/0 and remove.
node_keys() ->
    ["test_1"].

-spec node_data(rms:options()) -> node_data().
node_data(Options) ->
    #node_data{cpus = proplists:get_value(node_cpus, Options),
               mem = proplists:get_value(node_mem, Options),
               disk = proplists:get_value(node_disk, Options),
               num_ports = ?NODE_NUM_PORTS,
               role = proplists:get_value(framework_role, Options),
               principal = proplists:get_value(framework_principal, Options),
               container_path = ?NODE_CONTAINER_PATH}.

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
apply_reserved_offer(NodeKey, OfferHelper,
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

    case CanFitReserved and CanFitUnreserved of
        true ->
			%% FIXME FrameworkName needs come from deep in the bowels of the scheduler
			%% specifically erl_mesos_scheduler: 
			%% ((State#state.call_subscribe)#'Call.Subscribe'.framework_info).#'FrameworkInfo'.name
			FrameworkName = <<"riak">>,

			%% FIXME Where do we get the cluster name from?
			%% I think we need to refactor some assumptions made here: it looks like nodes aren't necessarily set up
			%% to have an idea of which cluster they are a part of?
			ClusterName = <<"default">>,

			%% FIXME AgentId? What on earth is that?
			AgentId = erl_mesos_utils:agent_id("LOL_FIXME_some_agent_id"),

			lager:info("Launching node ~p", [NodeKey]),

			[RiakUrlStr, RiakExplorerUrlStr, ExecutorUrlStr] = rms_config:artifact_urls(),

			ExecutorUrl = erl_mesos_utils:command_info_uri(ExecutorUrlStr, false, true),
			RiakExplorerUrl = erl_mesos_utils:command_info_uri(RiakExplorerUrlStr, false, true),
			RiakUrl = erl_mesos_utils:command_info_uri(RiakUrlStr, false, true),

			CommandInfoValue = "./riak_mesos_executor/bin/ermf-executor",
			UrlList = [ExecutorUrl, RiakExplorerUrl, RiakUrl],

			CommandInfo = erl_mesos_utils:command_info(CommandInfoValue, UrlList),

			TaskIdValue = NodeKey,
			TaskId = erl_mesos_utils:task_id(TaskIdValue),

			ReservedPorts = rms_offer_helper:get_reserved_resources_ports(OfferHelper),
			%% FIXME Make sure we actually have 3 ports available!
			[HTTPPort, PBPort, DisterlPort | _Unused] = ReservedPorts,

			NodeName = iolist_to_binary([NodeKey, "@ubuntu.local"]), %% FIXME host name
			TaskData = [
				 {<<"FullyQualifiedNodeName">>, NodeName},
				 {<<"Host">>,                   <<"localhost">>},
				 {<<"Zookeepers">>,             [<<"master.mesos:2181">>]},
				 {<<"FrameworkName">>,          FrameworkName},
				 {<<"URI">>,                    <<"192.168.1.4:9090">>}, %% FIXME URI
				 {<<"ClusterName">>,            ClusterName},
				 {<<"HTTPPort">>,               HTTPPort},
				 {<<"PBPort">>,                 PBPort},
				 {<<"HandoffPort">>,            0},
				 {<<"DisterlPort">>,            DisterlPort}],
			TaskDataBin = iolist_to_binary(mochijson2:encode(TaskData)),

			ExecutorId = erl_mesos_utils:executor_id("riak"),
			ExecutorInfo0 = erl_mesos_utils:executor_info(ExecutorId, CommandInfo),
			ExecutorInfo = ExecutorInfo0#'ExecutorInfo'{source = "riak"},

			%% FIXME TaskName seems like it should be generated or something
			TaskName = "riak",
			%% FIXME Is this the appropriate place for ReservedResources?
			ReservedResources = rms_offer_helper:get_reserved_resources(OfferHelper),
			TaskInfo0 = erl_mesos_utils:task_info(TaskName, TaskId, AgentId, 
												  ReservedResources, ExecutorInfo,
												  undefined),
			TaskInfo = TaskInfo0#'TaskInfo'{data = TaskDataBin},
            {ok, rms_offer_helper:add_task_to_launch(TaskInfo, OfferHelper)};
        false ->
            {error, not_enough_resources}
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
