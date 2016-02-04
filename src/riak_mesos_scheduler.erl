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

-module(riak_mesos_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-include("mesos_scheduler_data.hrl").

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

-record(state, {offer_mode = reconcile :: accept | reconcile | decline,
                task_id_values = [] :: [string()]}).

%% FIXME These should be configurable (and probably higher)
-define(MIN_CPU, 1).
-define(MIN_RAM, 1024).
-define(MIN_DISK, 4096).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(Options) ->
    FrameworkInfo = framework_info(),
    lager:info("Options: ~p.", [Options]),
    lager:info("FrameworkInfo: ~p.", [FrameworkInfo]),
    %% We should always start up in reconcile mode to ensure that
    %% we have the latest update information before acting on offers.
    {ok, FrameworkInfo, true, #state{offer_mode = reconcile}}.

registered(_SchedulerInfo, EventSubscribed, State) ->
    lager:info("Registered: ~p.", [EventSubscribed]),
    {ok, State}.

reregistered(SchedulerInfo, State) ->
    lager:info("Reregistered: ~p.", [SchedulerInfo]),
    {ok, State}.

disconnected(SchedulerInfo, State) ->
    lager:warning("Disconnected: ~p.", [SchedulerInfo]),
    {ok, State}.

resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers},
                State = #state{offer_mode = reconcile,
                               task_id_values = TaskIdValues}) ->
    lager:info("Resource Offers. Offer mode: ~p.", [State#state.offer_mode]),
    %% Reconcile.
    CallReconcileTasks = [call_reconcile_task(erl_mesos_utils:task_id(TaskIdValue)) ||
                          TaskIdValue <- TaskIdValues],
    ok = erl_mesos_scheduler:reconcile(SchedulerInfo, CallReconcileTasks),
    %% Decline this offer.
    ok = erl_mesos_scheduler:decline(SchedulerInfo, offer_ids(Offers)),
    {ok, State#state{offer_mode = accept}};
resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers} = EventOffers,
                State=#state{offer_mode = accept, task_id_values = TaskIdValues}) ->
    lager:info("Resource offers: ~p.", [EventOffers]),
    lager:info("Offer mode: ~p.", [State#state.offer_mode]),

    HandlerFun = fun handle_resource_offer/2,
    {OfferIds, Operations, NewTaskIdValues} = lists:foldl(HandlerFun, {[], [], []}, Offers),

    ok = erl_mesos_scheduler:accept(SchedulerInfo, OfferIds, Operations),
    %% TODO: Manually returing to decline mode for now, but needs to be based on
    %% whether or not we have nodes to launch eventually.
    {ok, State#state{offer_mode = accept,
                     task_id_values = TaskIdValues ++ NewTaskIdValues}};
resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers},
                State=#state{offer_mode = decline}) ->
    lager:info("Resource offers. Offer mode: ~p.", [State#state.offer_mode]),
    ok = erl_mesos_scheduler:accept(SchedulerInfo, offer_ids(Offers), []),
    {ok, State}.

offer_rescinded(_SchedulerInfo, #'Event.Rescind'{} = EventRescind, State) ->
    lager:info("Offer rescinded: ~p.", [EventRescind]),
    {ok, State}.

status_update(_SchedulerInfo, #'Event.Update'{} = EventUpdate, State) ->
    lager:info("Status update: ~p.", [EventUpdate]),
    {ok, State}.

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

%% ====================================================================
%% Private
%% ====================================================================

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
                    failover_timeout = undefined, %% TODO: Add this to configurable options
                    %%capabilities = undefined,
                    labels = undefined}.

offer_ids(Offers) ->
    [OfferId || #'Offer'{id = OfferId} <- Offers].

call_reconcile_task(TaskId) ->
    #'Call.Reconcile.Task'{task_id = TaskId}.

handle_resource_offer(Offer, Acc) ->
    Acc1 = maybe_reserve_resources(Offer, Acc),
    Acc2 = maybe_launch_nodes(Offer, Acc1),
    Acc2.

maybe_reserve_resources(Offer, Acc) ->
    #'Offer'{
       id = OfferId,
       resources = Resources
      } = Offer,
    OfferHelper = riak_mesos_offer_helper:new(Offer),
    OfferFits = riak_mesos_offer_helper:offer_fits(OfferHelper),

    % TODO Figure out how to prioritize which node we choose:
    RequestedNodes = [N || N <- mesos_scheduler_data:get_all_nodes(),
                           N#rms_node.status =:= requested],

    case {OfferFits, RequestedNodes} of
        {true, [Node | _]} ->
            lager:info("Reserving resources for node ~p", [Node]),
            %% TODO - only take as much as we need, instead of using the entire offer?
            UnreservedResources = [R || R <- Resources, R#'Resource'.reservation =:= undefined],
            ReservationRequestResources = [erl_mesos_utils:add_resource_reservation(
                                             R, "riak", "riak") ||
                                           R <- UnreservedResources],
            ReserveOp = erl_mesos_utils:reserve_offer_operation(ReservationRequestResources),

            Disk = riak_mesos_offer_helper:get_unreserved_resources_disk(OfferHelper),
            DiskResource = erl_mesos_utils:add_resource_reservation(
                             erl_mesos_utils:scalar_resource("disk", Disk), "riak", "riak"),
            CreateOp = erl_mesos_utils:create_offer_operation([DiskResource]),

            %% FIXME handle error results here.
            %% FIXME also handle case where we crash or launch message is lost, so we
            %% don't get stuck with a zombie node in the "reserving" state forever
            ok = mesos_scheduler_data:set_node_status(Node#rms_node.key, reserving),

            lager:info("Sending reserve operation ~p", [ReserveOp]),
            lager:info("Sending create operation ~p", [CreateOp]),

            {OfferIdAcc, OperationAcc, TaskIdAcc} = Acc,
            {[OfferId | OfferIdAcc], [ReserveOp, CreateOp | OperationAcc], TaskIdAcc};
        _ ->
            Acc
    end.

maybe_launch_nodes(Offer, Acc) ->
    #'Offer'{
       id = OfferId,
       agent_id = AgentId,
       resources = Resources
      } = Offer,

    HasReservedResources = lists:any(fun(R) -> R#'Resource'.reservation =/= undefined end,
                                     Resources),

    % TODO Figure out how to prioritize which node we choose:
    ReservingNodes = [N || N <- mesos_scheduler_data:get_all_nodes(),
                           N#rms_node.status =:= reserving],

    case {HasReservedResources, ReservingNodes} of
        {true, [Node | _]} ->
            lager:info("Launching node ~p", [Node]),

            UrlBase = "file:///home/vagrant/riak-mesos-erlang",
            ExecutorUrlStr = UrlBase ++ "/framework/riak-mesos-executor/packages/"
            ++ "riak_mesos_executor-0.1.2-amd64.tar.gz",
            RiakExplorerUrlStr = UrlBase ++ "/framework/riak_explorer/packages/"
            ++ "riak_explorer-0.1.1.patch-amd64.tar.gz",
            RiakUrlStr = UrlBase ++ "/riak/packages/riak-2.1.3-amd64.tar.gz",

            ExecutorUrl = erl_mesos_utils:command_info_uri(ExecutorUrlStr, false, true),
            RiakExplorerUrl = erl_mesos_utils:command_info_uri(RiakExplorerUrlStr, false, true),
            RiakUrl = erl_mesos_utils:command_info_uri(RiakUrlStr, false, true),

            CommandInfoValue = "./riak_mesos_executor/bin/ermf-executor",
            UrlList = [ExecutorUrl, RiakExplorerUrl, RiakUrl],

            CommandInfo = erl_mesos_utils:command_info(CommandInfoValue, UrlList),

            TaskIdValue = binary_to_list(Node#rms_node.key),
            TaskId = erl_mesos_utils:task_id(TaskIdValue),

            ReservedResources = [R || R <- Resources, R#'Resource'.reservation =/= undefined],

            ExecutorId = erl_mesos_utils:executor_id("riak"),
            ExecutorInfo = erl_mesos_utils:executor_info(ExecutorId, CommandInfo),

            TaskInfo = erl_mesos_utils:task_info("riak", TaskId, AgentId, ReservedResources,
                                                 ExecutorInfo, undefined),
            Operation = erl_mesos_utils:launch_offer_operation([TaskInfo]),

            %% FIXME handle error results here.
            %% FIXME also handle case where we crash or launch message is lost, so we
            %% don't get stuck with a zombie node in the "starting" state forever
            ok = mesos_scheduler_data:set_node_status(Node#rms_node.key, starting),

            lager:info("Sending launch operation ~p", [Operation]),

            {OfferIdAcc, OperationAcc, TaskIdAcc} = Acc,
            {[OfferId | OfferIdAcc],
             [Operation | OperationAcc],
             [TaskIdValue | TaskIdAcc]};
        _ ->
            Acc
    end.
