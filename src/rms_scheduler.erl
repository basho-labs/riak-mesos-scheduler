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

-module(rms_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([init/1,
         registered/3,
         reregistered/2,
         disconnected/2,
         resource_offers/3,
         offer_rescinded/3,
         status_update/3,
         framework_message/3,
         slave_lost/3,
         executor_lost/3,
         error/3,
         handle_info/3,
         terminate/3]).

-record(scheduler, {framework_id :: undefined | erl_mesos:'FrameworkID'(),
                    cluster_keys = [] :: [rms_cluster:key()],
                    removed_cluster_keys = [] :: [rms_cluster:key()]}).

-record(state, {scheduler :: scheduler(),
                node_data :: rms_node_manager:node_data(),
                framework_info :: erl_mesos:'FrameworkInfo'()}).

-type scheduler() :: #scheduler{}.
-export_type([scheduler/0]).

%% -record(state, {offer_mode = reconcile :: accept | reconcile | decline,
%%                 task_id_values = [] :: [string()]}).

%% erl_mesos_scheduler callback functions.

init(Options) ->
    lager:info("Scheduler options: ~p.", [Options]),
    %% TODO: check if framework id exist in metadata manager.
    FrameworkInfo = framework_info(),
    #'FrameworkInfo'{role = Role,
                     principal = Principal} = FrameworkInfo,
    %% TODO: read node data from config in rms_sup and get it here from options.
%%     NodeCpus = rms_config:get_value(node_cpus, 1.0, float),
%%     NodeMem = rms_config:get_value(node_mem, 16000, float),
%%     NodeDisk = rms_config:get_value(node_disk, 20000, float),
    NodeCpus = rms_config:get_value(node_cpus, 1.0, float),
    NodeMem = rms_config:get_value(node_mem, 3000, float),
    NodeDisk = rms_config:get_value(node_disk, 4000, float),
    NodeData = rms_node_manager:node_data(NodeCpus, NodeMem, NodeDisk, Role,
                                          Principal),
    lager:info("Start scheduler with framework info: ~p.", [FrameworkInfo]),
    {ok, FrameworkInfo, true, #state{node_data = NodeData,
                                     framework_info = FrameworkInfo}}.

registered(_SchedulerInfo, EventSubscribed, State) ->
    lager:info("Registered: ~p.", [EventSubscribed]),
    {ok, State}.

reregistered(SchedulerInfo, State) ->
    lager:info("Reregistered: ~p.", [SchedulerInfo]),
    {ok, State}.

disconnected(SchedulerInfo, State) ->
    lager:warning("Disconnected: ~p.", [SchedulerInfo]),
    {ok, State}.

resource_offers(_SchedulerInfo, #'Event.Offers'{offers = Offers},
                #state{node_data = NodeData} = State) ->
    Operations = apply_offers(Offers, NodeData),
    lager:info("OPERATIONS: ~p.", [Operations]),
    {ok, State}.

%%     [apply_offer(Offer, NodeData) || Offer <- Offers],
%%     {ok, State}.

%% resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers},
%%                 State = #state{offer_mode = reconcile,
%%                                task_id_values = TaskIdValues}) ->
%%     lager:info("Resource Offers. Offer mode: ~p.", [State#state.offer_mode]),
%%     %% Reconcile.
%%     CallReconcileTasks = [call_reconcile_task(erl_mesos_utils:task_id(TaskIdValue)) ||
%%                           TaskIdValue <- TaskIdValues],
%%     ok = erl_mesos_scheduler:reconcile(SchedulerInfo, CallReconcileTasks),
%%     %% Decline this offer.
%%     ok = erl_mesos_scheduler:decline(SchedulerInfo, offer_ids(Offers)),
%%     {ok, State#state{offer_mode = accept}};
%% resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers} = EventOffers,
%%                 State=#state{offer_mode = accept, task_id_values = TaskIdValues}) ->
%%     lager:info("Resource offers: ~p.", [EventOffers]),
%%     lager:info("Offer mode: ~p.", [State#state.offer_mode]),
%%
%%     HandlerFun = fun handle_resource_offer/2,
%%     {OfferIds, Operations, NewTaskIdValues} = lists:foldl(HandlerFun, {[], [], []}, Offers),
%%
%%     ok = erl_mesos_scheduler:accept(SchedulerInfo, OfferIds, Operations),
%%
%%     {ok, State#state{offer_mode = accept,
%%                      task_id_values = TaskIdValues ++ NewTaskIdValues}};
%% resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers},
%%                 State=#state{offer_mode = decline}) ->
%%     lager:info("Resource offers. Offer mode: ~p.", [State#state.offer_mode]),
%%     ok = erl_mesos_scheduler:accept(SchedulerInfo, offer_ids(Offers), []),
%%     {ok, State}.

offer_rescinded(_SchedulerInfo, #'Event.Rescind'{} = EventRescind, State) ->
    lager:info("Offer rescinded: ~p.", [EventRescind]),
    {ok, State}.

status_update(_SchedulerInfo, #'Event.Update'{} = _EventUpdate, State) ->
    {ok, State}.

%% status_update(SchedulerInfo, #'Event.Update'{} = EventUpdate, State) ->
%%     #'TaskStatus'{
%%        agent_id = AgentId,
%%        task_id = TaskId,
%%        uuid = Uuid
%%       } = EventUpdate#'Event.Update'.status,
%%     TaskIdValue = TaskId#'TaskID'.value,
%%
%%     %% FIXME might not want to crash if status_update returns not_found?
%%     ok = scheduler_node_fsm:status_update(TaskIdValue, EventUpdate),
%%     erl_mesos_scheduler:acknowledge(SchedulerInfo, AgentId, TaskId, Uuid),
%%     {ok, State}.

framework_message(_SchedulerInfo, #'Event.Message'{} = EventMessage, State) ->
    lager:info("Framework message: ~p.", [EventMessage]),
    {ok, State}.

slave_lost(_SchedulerInfo, #'Event.Failure'{} = EventFailure, State) ->
    lager:info("Slave lost: ~p.", [EventFailure]),
    {ok, State}.

executor_lost(_SchedulerInfo, #'Event.Failure'{} = EventFailure, State) ->
    lager:info("Executor lost: ~p.", [EventFailure]),
    {ok, State}.

error(_SchedulerInfo, #'Event.Error'{} = EventError, State) ->
    lager:info("Error: ~p.", [EventError]),
    {stop, State}.

handle_info(_SchedulerInfo, Info, State) ->
    lager:info("Handle info. Undefined: ~p.", [Info]),
    {ok, State}.

terminate(_SchedulerInfo, Reason, _State) ->
    lager:warning("Terminate: ~p.", [Reason]),
    ok.

%% Internal functions.

framework_info() ->
    User = riak_mesos_scheduler_config:get_value(user, "root"),
    Name = riak_mesos_scheduler_config:get_value(name, "riak", string),
    Role = riak_mesos_scheduler_config:get_value(role, "riak", string),
    Hostname = riak_mesos_scheduler_config:get_value(hostname, undefined, string),
    Principal = riak_mesos_scheduler_config:get_value(principal, "riak", string),

    #'FrameworkInfo'{user = User,
                     name = Name,
                     role = Role,
                     hostname = Hostname,
                     principal = Principal,
                     checkpoint = undefined, %% TODO: We will want to enable checkpointing
                     id = undefined, %% TODO: Will need to check ZK for this for reregistration
                     webui_url = undefined, %% TODO: Get this from webmachine helper probably
                     failover_timeout = undefined}. %% TODO: Add this to configurable options

apply_offers(Offers, NodeData) ->
    apply_offers(Offers, NodeData, []).

apply_offers([Offer | Offers], NodeData, Operations) ->
    Operations1 = apply_offer(Offer, NodeData),
    apply_offers(Offers, NodeData, Operations ++ Operations1);
apply_offers([], _NodeData, Operations) ->
    Operations.

apply_offer(Offer, NodeData) ->
    OfferHelper = rms_offer_helper:new(Offer),
    lager:info("Scheduler recevied offer. Offer id: ~s. Resources: ~p.",
               [rms_offer_helper:get_offer_id_value(OfferHelper),
                rms_offer_helper:resources_to_list(OfferHelper)]),
    OfferHelper1 = rms_cluster_manager:apply_offer(OfferHelper, NodeData),
    rms_offer_helper:operations(OfferHelper1).

%% offer_ids(Offers) ->
%%     [OfferId || #'Offer'{id = OfferId} <- Offers].
%%
%% call_reconcile_task(TaskId) ->
%%     #'Call.Reconcile.Task'{task_id = TaskId}.
%%
%% handle_resource_offer(Offer, Acc) ->
%%     Acc1 = maybe_reserve_resources(Offer, Acc),
%%     Acc2 = maybe_launch_nodes(Offer, Acc1),
%%     Acc2.
%%
%% maybe_reserve_resources(Offer, Acc) ->
%%     OfferId = Offer#'Offer'.id,
%%     OfferHelper = riak_mesos_offer_helper:new(Offer),
%%     OfferFits = riak_mesos_offer_helper:offer_fits(OfferHelper),
%%
%%     RequestedNodes = [N || N <- mesos_scheduler_data:get_all_nodes(),
%%                            N#rms_node.status =:= requested],
%%
%%     case {OfferFits, RequestedNodes} of
%%         {{ok, {NodeCPU, NodeMem, NodeDisk, NodePorts}}, [Node | _]} ->
%%             lager:info("Reserving resources for node ~p", [Node]),
%%
%%             CpuResource = erl_mesos_utils:scalar_resource("cpus", NodeCPU),
%%             MemResource = erl_mesos_utils:scalar_resource("mem", NodeMem),
%%             DiskResource = erl_mesos_utils:scalar_resource("disk", NodeDisk),
%%             PortResource = port_list_to_port_resource(NodePorts),
%%
%%             UnreservedResources = [CpuResource, MemResource, DiskResource, PortResource],
%%             ReservationRequestResources = [erl_mesos_utils:add_resource_reservation(
%%                                              R, "riak", "riak") ||
%%                                            R <- UnreservedResources],
%%             ReserveOp = erl_mesos_utils:reserve_offer_operation(ReservationRequestResources),
%%
%%             DiskResourceReserved = erl_mesos_utils:add_resource_reservation(
%%                                      DiskResource, "riak", "riak"),
%%             CreateOp = erl_mesos_utils:create_offer_operation([DiskResourceReserved]),
%%
%%             lager:info("Sending reserve operation ~p", [ReserveOp]),
%%             lager:info("Sending create operation ~p", [CreateOp]),
%%
%%             {OfferIdAcc, OperationAcc, TaskIdAcc} = Acc,
%%             {[OfferId | OfferIdAcc], [ReserveOp, CreateOp | OperationAcc], TaskIdAcc};
%%         _ ->
%%             Acc
%%     end.
%%
%% maybe_launch_nodes(Offer, Acc) ->
%%     #'Offer'{
%%        id = OfferId,
%%        agent_id = AgentId,
%%        resources = Resources
%%       } = Offer,
%%
%%     ReservedResources = [R || R <- Resources, R#'Resource'.reservation =/= undefined],
%%     HasReservedResources = (ReservedResources =/= []),
%%
%%     RequestedNodes = [N || N <- mesos_scheduler_data:get_all_nodes(),
%%                            N#rms_node.status =:= requested],
%%
%%     case {HasReservedResources, RequestedNodes} of
%%         {true, [Node | _]} ->
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
%%
%%             %% FIXME handle error results here?
%%             ok = mesos_scheduler_data:set_node_status(Node#rms_node.key, starting),
%%             {ok, _Pid} = scheduler_node_fsm_mgr:start_child(TaskIdValue, AgentId),
%%
%%             lager:info("Sending launch operation ~p", [Operation]),
%%
%%             {OfferIdAcc, OperationAcc, TaskIdAcc} = Acc,
%%             {[OfferId | OfferIdAcc],
%%              [Operation | OperationAcc],
%%              [TaskIdValue | TaskIdAcc]};
%%         {true, []} ->
%%             %% Somehow we got into a state where we have reserved resources but no nodes
%%             %% that actually need those resources. Go ahead and unreserve:
%%             lager:info("Got reserved resource offer, but no nodes in 'requested' state"),
%%
%%             Operation = erl_mesos_utils:unreserve_offer_operation(ReservedResources),
%%
%%             {OfferIdAcc, OperationAcc, TaskIdAcc} = Acc,
%%             {[OfferId | OfferIdAcc],
%%              [Operation | OperationAcc],
%%              TaskIdAcc};
%%         _ ->
%%             Acc
%%     end.
%%
%% port_list_to_port_resource(PortList) ->
%%     Ranges = lists:foldl(fun port_to_range_fold/2, [], lists:sort(PortList)),
%%     erl_mesos_utils:ranges_resource("ports", Ranges).
%%
%% port_to_range_fold(Port, []) ->
%%     [{Port, Port}];
%% port_to_range_fold(Port, [{RangeStart, RangeEnd} | Rest]) when Port =:= (RangeEnd + 1) ->
%%     [{RangeStart, Port} | Rest];
%% port_to_range_fold(Port, Ranges) ->
%%     [{Port, Port} | Ranges].
