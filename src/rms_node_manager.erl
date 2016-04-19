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

%% FIXME Remove, debug only
-export([get_node_pid/1]).

-export([start_link/0]).

-export([get_node_hosts/0,
         get_node_attributes/0,
         get_node_keys/0,
         get_unreconciled_node_keys/0,
         get_node_keys/1,
         get_node_names/1,
         get_active_node_keys/1,
         get_running_node_keys/1,
         get_node/1,
         get_node_cluster_key/1,
         get_node_hostname/1,
         get_node_http_port/1,
         get_node_http_url/1,
         get_node_name/1,
         get_node_agent_id_value/1,
         get_node_persistence_id/1,
         node_needs_to_be_reconciled/1,
         node_can_be_scheduled/1,
         node_has_reservation/1,
         node_can_be_shutdown/1,
         add_node/2,
         restart_node/1,
         destroy_node/1,
         destroy_node/2]).

-export([apply_unreserved_offer/2, apply_reserved_offer/2]).

-export([handle_status_update/3]).

-export([init/1]).

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

-spec get_unreconciled_node_keys() -> [rms_node:key()].
get_unreconciled_node_keys() ->
    [Key || Key <- get_node_keys(),
            true =:= node_needs_to_be_reconciled(Key)].

-spec get_node_keys(rms_cluster:key()) -> [rms_node:key()].
get_node_keys(ClusterKey) ->
    [Key || {Key, Node} <- rms_metadata:get_nodes(),
            ClusterKey =:= proplists:get_value(cluster_key, Node)].

-spec get_node_names(rms_cluster:key()) -> [atom()].
get_node_names(ClusterKey) ->
    [ proplists:get_value(node_name, Node) || {_, Node} <- rms_metadata:get_nodes(),
            ClusterKey =:= proplists:get_value(cluster_key, Node),
            shutdown =/= proplists:get_value(status, Node)].

-spec get_active_node_keys(rms_cluster:key()) -> [rms_node:key()].
get_active_node_keys(ClusterKey) ->
    [Key || {Key, Node} <- rms_metadata:get_nodes(),
            ClusterKey =:= proplists:get_value(cluster_key, Node),
            shutdown =/= proplists:get_value(status, Node)].

-spec get_running_node_keys(rms_cluster:key()) -> [rms_node:key()].
get_running_node_keys(ClusterKey) ->
    [Key || {Key, Node} <- rms_metadata:get_nodes(),
            ClusterKey =:= proplists:get_value(cluster_key, Node),
            started =:= proplists:get_value(status, Node)].

-spec get_node(rms_node:key()) ->
                      {ok, rms_metadata:node_state()} | {error, term()}.
get_node(Key) ->
    rms_node:get(Key).

-spec get_node_cluster_key(rms_node:key()) ->
                                  {ok, rms_cluster:key()} | {error, term()}.
get_node_cluster_key(Key) ->
    rms_node:get_field_value(cluster_key, Key).

-spec get_node_hostname(rms_node:key()) -> {ok, string()} | {error, term()}.
get_node_hostname(Key) ->
    rms_node:get_field_value(hostname, Key).

-spec get_node_hosts() -> {ok, rms_offer_helper:hostnames()}.
get_node_hosts() ->
    {ok, lists:foldl(fun({_, Node}, Accum) -> 
                        case {proplists:get_value(status, Node),
                              proplists:get_value(hostname, Node)} of
                            {shutdown, _} -> Accum;
                            {_,{error, _}} -> Accum;
                            {_,undefined} -> Accum;
                            {_,Host} -> [Host|Accum]
                        end
                end, [], rms_metadata:get_nodes())}.

-spec get_node_attributes() -> {ok, [rms_offer_helper:attributes_group()]}.
get_node_attributes() ->
    {ok, lists:foldl(fun({_, Node}, Accum) -> 
                        case {proplists:get_value(status, Node),
                              proplists:get_value(attributes, Node)} of
                            {shutdown, _} -> Accum;
                            {_,{error, _}} -> Accum;
                            {_,undefined} -> Accum;
                            {_,Attributes} -> [Attributes|Accum]
                        end
                end, [], rms_metadata:get_nodes())}.

-spec get_node_http_port(rms_node:key()) -> {ok, pos_integer()} | {error, term()}.
get_node_http_port(Key) ->
    rms_node:get_field_value(http_port, Key).

-spec get_node_name(rms_node:key()) -> {ok, string()} | {error, term()}.
get_node_name(Key) ->
    rms_node:get_field_value(node_name, Key).

-spec get_node_http_url(rms_node:key()) -> {ok, string()} | {error, term()}.
get_node_http_url(Key) ->
    case {get_node_hostname(Key),
          get_node_http_port(Key)} of
        {{ok, H},{ok, P}} when is_list(H) and is_integer(P) ->
            {ok, H ++ ":" ++ integer_to_list(P)};
        _ -> {error, not_found}
    end.

-spec get_node_agent_id_value(rms_node:key()) ->
                                     {ok, string()} | {error, term()}.
get_node_agent_id_value(Key) ->
    rms_node:get_field_value(agent_id_value, Key).

-spec get_node_persistence_id(rms_node:key()) ->
                                     {ok, string()} | {error, term()}.
get_node_persistence_id(Key) ->
    rms_node:get_field_value(persistence_id, Key).

-spec node_needs_to_be_reconciled(rms_node:key()) -> boolean().
node_needs_to_be_reconciled(NodeKey) ->
    case rms_node:needs_to_be_reconciled(NodeKey) of
        {ok, NeedsToBeReconciled} ->
            NeedsToBeReconciled;
        {error, _Reason} ->
            true
    end.

-spec node_can_be_scheduled(rms_node:key()) -> boolean().
node_can_be_scheduled(NodeKey) ->
    case get_node_pid(NodeKey) of
        {ok, Pid} -> rms_node:can_be_scheduled(Pid);
        {error, _}=Error -> Error
    end.

-spec node_has_reservation(rms_node:key()) -> boolean().
node_has_reservation(NodeKey) ->
    case rms_node:has_reservation(NodeKey) of
        {ok, HasReservation} ->
            HasReservation;
        {error, _Reason} ->
            false
    end.

-spec node_can_be_shutdown(rms_node:key()) -> boolean().
node_can_be_shutdown(NodeKey) ->
    case get_node_pid(NodeKey) of
        {ok, Pid} ->
            rms_node:can_be_shutdown(Pid);
        {error, Reason} ->
            {error, Reason}
    end.

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
                {error, already_present} ->
                    ok = supervisor:delete_child(?MODULE, Key),
                    {ok, _Pid} = supervisor:start_child(?MODULE, NodeSpec),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec restart_node(rms_node:key()) -> ok | {error, term()}.
restart_node(Key) ->
    case get_node_pid(Key) of
        {ok, Pid} ->
            rms_node:restart(Pid);
        {error, Reason} ->
            {error, Reason}
    end.

-spec destroy_node(rms_node:key()) -> ok | {error, term()}.
destroy_node(Key) ->
    destroy_node(Key, false).

-spec destroy_node(rms_node:key(), boolean()) -> ok | {error, term()}.
destroy_node(Key, Force) ->
    case get_node_pid(Key) of
        {ok, Pid} ->
            rms_node:destroy(Pid, Force);
        {error, Reason} ->
            {error, Reason}
    end.

-spec apply_unreserved_offer(rms_node:key(), rms_offer_helper:offer_helper()) ->
                                    {ok, rms_offer_helper:offer_helper()} |
                                    {error, not_enough_resources | term()}.
apply_unreserved_offer(NodeKey, OfferHelper) ->
    case get_node_pid(NodeKey) of
        {ok, Pid} ->
            {ok, Role} = rms_metadata:get_option(framework_role),
            {ok, Principal} = rms_metadata:get_option(framework_principal),
            {ok, NodeCpus} = rms_metadata:get_option(node_cpus),
            {ok, NodeMem} = rms_metadata:get_option(node_mem),
            {ok, NodeDisk} = rms_metadata:get_option(node_disk),
            NodeNumPorts = ?NODE_NUM_PORTS,
            ContainerPath = ?NODE_CONTAINER_PATH,
            Hostname = rms_offer_helper:get_hostname(OfferHelper),
            AgentIdValue = rms_offer_helper:get_agent_id_value(OfferHelper),
            PersistenceId = node_persistence_id(),
            case rms_offer_helper:can_fit_unreserved(NodeCpus +
                                                         ?CPUS_PER_EXECUTOR,
                                                     NodeMem +
                                                         ?MEM_PER_EXECUTOR,
                                                     NodeDisk, NodeNumPorts,
                                                     OfferHelper) of
                true ->
                    %% Remove requirements from offer helper.
                    OfferHelper1 =
                        rms_offer_helper:apply_unreserved_resources(
                          ?CPUS_PER_EXECUTOR, ?MEM_PER_EXECUTOR, undefined,
                          NodeNumPorts, OfferHelper),
                    %% Reserve resources.
                    OfferHelper2 =
                        rms_offer_helper:make_reservation(NodeCpus, NodeMem,
                                                          NodeDisk, undefined,
                                                          Role, Principal,
                                                          OfferHelper1),
                    %% Make volume.
                    OfferHelper3 =
                        rms_offer_helper:make_volume(NodeDisk, Role, Principal,
                                                     PersistenceId,
                                                     ContainerPath,
                                                     OfferHelper2),

                    Attributes =
                        rms_offer_helper:get_attributes(OfferHelper3),

                    ok = rms_node:set_reserve(Pid, Hostname, AgentIdValue,
                                      PersistenceId, Attributes),
                    {ok, OfferHelper3};
                false ->
                    {error, not_enough_resources}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec apply_reserved_offer(rms_node:key(), rms_offer_helper:offer_helper()) ->
                                  {ok, rms_offer_helper:offer_helper()} |
                                  {error, not_enough_resources | term()}.
apply_reserved_offer(NodeKey, OfferHelper) ->
    case get_node_pid(NodeKey) of
        {ok, _Pid} ->
            {ok, Id} = rms_metadata:get_option(framework_id),
            {ok, Name} = rms_metadata:get_option(framework_name),
            {ok, Role} = rms_metadata:get_option(framework_role),
            {ok, Principal} = rms_metadata:get_option(framework_principal),
            {ok, WebuiUrl} = rms_metadata:get_option(framework_webui_url),
            {ok, NodeCpus} = rms_metadata:get_option(node_cpus),
            {ok, NodeMem} = rms_metadata:get_option(node_mem),
            {ok, NodeDisk} = rms_metadata:get_option(node_disk),
            {ok, ArtifactUrls} = rms_metadata:get_option(artifact_urls),
            NodeNumPorts = ?NODE_NUM_PORTS,
            ContainerPath = ?NODE_CONTAINER_PATH,
            CanFitReserved =
                rms_offer_helper:can_fit_reserved(NodeCpus, NodeMem, NodeDisk,
                                                  0, OfferHelper),
            CanFitUnreserved =
                rms_offer_helper:can_fit_unreserved(?CPUS_PER_EXECUTOR,
                                                    ?MEM_PER_EXECUTOR,
                                                    0.0, NodeNumPorts,
                                                    OfferHelper),
            case CanFitReserved and CanFitUnreserved of
                true ->
                    {ok, ClusterKey} = get_node_cluster_key(NodeKey),
                    {ok, PersistenceId} = get_node_persistence_id(NodeKey),
                    {ok, NodeHostname} = get_node_hostname(NodeKey),
                    {ok, AgentIdValue} = get_node_agent_id_value(NodeKey),

                    %% Apply reserved resources for task.
                    OfferHelper0 =
                        rms_offer_helper:clean_applied_resources(OfferHelper),
                    OfferHelper1 =
                        rms_offer_helper:apply_reserved_resources(
                          NodeCpus, NodeMem, NodeDisk, undefined, Role,
                          Principal, PersistenceId, ContainerPath,
                          OfferHelper0),
                    %% Apply unreserved resources for task.
                    OfferHelper2 =
                        rms_offer_helper:apply_unreserved_resources(
                          undefined, undefined, undefined, NodeNumPorts,
                          OfferHelper1),
                    %% Grab Task resources from offer helper in current state.
                    TaskInfoReservedResources =
                        rms_offer_helper:get_reserved_applied_resources(OfferHelper2),
                    TaskInfoUnreservedResources =
                        rms_offer_helper:get_unreserved_applied_resources(OfferHelper2),
                    TaskInfoResources = TaskInfoReservedResources ++ TaskInfoUnreservedResources,

                    %% Apply Executor Resources against original offer helper.
                    OfferHelperExec =
                        rms_offer_helper:apply_unreserved_resources(
                          ?CPUS_PER_EXECUTOR, ?MEM_PER_EXECUTOR, undefined, undefined,
                          OfferHelper),
                    %% Grab Executor resources from exec offer helper in current state.
                    ExecutorInfoResources =
                        rms_offer_helper:get_unreserved_applied_resources(OfferHelperExec),
                    %% Apply Executor Resources against the real offer helper.
                    OfferHelper3 =
                        rms_offer_helper:apply_unreserved_resources(
                          ?CPUS_PER_EXECUTOR, ?MEM_PER_EXECUTOR, undefined, undefined,
                          OfferHelper2),

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

                    TaskDataPorts =
                        rms_offer_helper:get_unreserved_applied_resources_ports(OfferHelper3),

                    [HTTPPort, PBPort, DisterlPort | _Ports] = TaskDataPorts,

                    NodeName = iolist_to_binary([NodeKey, "@", NodeHostname]),
                    TaskData = [{<<"FullyQualifiedNodeName">>, NodeName},
                                {<<"Host">>,                   list_to_binary(NodeHostname)},
                                %% TODO: read list of zookeepers with rms_metadata:get_option/1
                                {<<"Zookeepers">>,             [list_to_binary(rms_config:zk())]},
                                {<<"FrameworkName">>,          list_to_binary(Name)},
                                {<<"URI">>,                    list_to_binary(WebuiUrl)},
                                {<<"ClusterName">>,            list_to_binary(ClusterKey)},
                                {<<"HTTPPort">>,               HTTPPort},
                                {<<"PBPort">>,                 PBPort},
                                {<<"HandoffPort">>,            0},
                                {<<"DisterlPort">>,            DisterlPort}],
                    TaskDataBin = iolist_to_binary(mochijson2:encode(TaskData)),

                    ExecutorId = erl_mesos_utils:executor_id(NodeKey),

                    Source = Name,
                    ExecutorInfo =
                        erl_mesos_utils:executor_info(ExecutorId, CommandInfo,
                                                      ExecutorInfoResources, Id,
                                                      Source),

                    TaskName = Name ++ "-" ++ ClusterKey,
                    TaskInfo =
                        erl_mesos_utils:task_info(TaskName, TaskId, AgentId,
                                                  TaskInfoResources,
                                                  ExecutorInfo, undefined,
                                                  TaskDataBin),

                    {ok, N} = get_node_pid(NodeKey),
                    rms_node:set_agent_info(N,
                                            binary_to_list(NodeName),
                                            NodeHostname,
                                            HTTPPort,
                                            PBPort,
                                            DisterlPort,
                                            AgentIdValue,
                                            ContainerPath),

                    {ok, rms_offer_helper:add_task_to_launch(TaskInfo,
                                                             OfferHelper3)};
                false ->
                    {error, not_enough_resources}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec handle_status_update(rms_node:key(), atom(), atom()) ->
    ok | {error, term()}.
handle_status_update(NodeKey, TaskStatus, Reason) ->
    {ok, N} = get_node_pid(NodeKey),
    rms_node:handle_status_update(N, TaskStatus, Reason).

%% supervisor callback function.

-spec init({}) ->
                  {ok, {{supervisor:strategy(), 1, 1}, [supervisor:child_spec()]}}.
init({}) ->
    Specs = [node_spec(Key, proplists:get_value(cluster_key, Node)) ||
                {Key, Node} <- rms_metadata:get_nodes()],
    {ok, {{one_for_one, 1, 1}, Specs}}.

%% Internal functions.

-spec node_spec(rms_node:key(), rms_cluster:key()) -> supervisor:child_spec().
node_spec(Key, ClusterKey) ->
    {Key,
     {rms_node, start_link, [Key, ClusterKey]},
     transient, 5000, worker, [rms_node]}.

-spec get_node_pid(rms_node:key()) -> {ok, pid()} | {error, not_found}.
get_node_pid(Key) ->
    case lists:keyfind(Key, 1, supervisor:which_children(?MODULE)) of
        {_Key, undefined, _, _} ->
            %% TODO Perhaps we should supervisor:delete_child/2 here?
            {error, shutdown};
        {_Key, Pid, _, _} ->
            {ok, Pid};
        false ->
            {error, not_found}
    end.

-spec node_persistence_id() -> string().
node_persistence_id() ->
    uuid:to_string(uuid:uuid4()).
