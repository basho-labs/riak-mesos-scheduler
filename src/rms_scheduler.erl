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

-record(state, {scheduler :: scheduler_state(),
                node_data :: rms_node_manager:node_data()}).

-type scheduler_state() :: #scheduler{}.
-export_type([scheduler_state/0]).

-type state() :: #state{}.

-type framework_info() :: [{atom(), term()}].

-define(OFFER_INTERVAL, 5).

%% erl_mesos_scheduler callback functions.

init(Options) ->
    lager:info("Scheduler options: ~p.", [Options]),
    case get_scheduler() of
        {ok, #scheduler{framework_id = FrameworkId} = Scheduler} ->
            Options1 = [{framework_id, FrameworkId} | Options],
            init(Options1, Scheduler);
        {error, not_found} ->
            init(Options, #scheduler{});
        {error, Reason} ->
            lager:error("Error during retrving sheduler state. Reason: ~p.",
                        [Reason]),
            {stop, {error, Reason}}
    end.

registered(_SchedulerInfo, #'Event.Subscribed'{framework_id = FrameworkId},
           #state{scheduler = #scheduler{framework_id = undefined} =
                  Scheduler} = State) ->
    Scheduler1 = Scheduler#scheduler{framework_id = FrameworkId},
    case set_scheduler(Scheduler1) of
        ok ->
            lager:info("New scheduler registered. Framework id: ~p.",
                       [framework_id_value(FrameworkId)]),
            {ok, State#state{scheduler = Scheduler1}};
        {error, Reason} ->
            lager:error("Error during saving sheduler state. Reason: ~p.",
                        [Reason]),
            {ok, State}
    end;
registered(SchedulerInfo, _EventSubscribed,
           #state{scheduler = #scheduler{framework_id = FrameworkId}} =
           State) ->
    lager:info("Scheduler registered. Framework id: ~p.",
               [framework_id_value(FrameworkId)]),
    TaskIdValues = rms_node_manager:get_node_keys(),
    lager:info("Scheduler task ids to reconcile: ~p.", [TaskIdValues]),
    ReconcileTasks = reconcile_tasks(TaskIdValues),
    ok = erl_mesos_scheduler:reconcile(SchedulerInfo, ReconcileTasks),
    {ok, State}.

reregistered(_SchedulerInfo, State) ->
    lager:warning("Scheduler reregistered.", []),
    {ok, State}.

disconnected(_SchedulerInfo, State) ->
    lager:warning("Scheduler disconnected.", []),
    {ok, State}.

resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers},
                #state{node_data = NodeData} = State) ->
    {OfferIds, Operations} = apply_offers(Offers, NodeData),
    Filters = #'Filters'{refuse_seconds = ?OFFER_INTERVAL},
    ok = erl_mesos_scheduler:accept(SchedulerInfo, OfferIds, Operations,
                                    Filters),
    {ok, State}.

offer_rescinded(_SchedulerInfo, #'Event.Rescind'{} = EventRescind, State) ->
    lager:info("Scheduler received offer rescinded event. "
               "Rescind: ~p.",
               [EventRescind]),
    {ok, State}.

status_update(_SchedulerInfo, EventUpdate, State) ->
    %% TODO: handle update event and update node state via cluster manager.
    lager:info("Scheduler received stauts update event. "
               "Update: ~p~n", [EventUpdate]),
    {ok, State}.

framework_message(_SchedulerInfo, EventMessage, State) ->
    lager:info("Scheduler received framework message. "
               "Framework message: ~p.",
               [EventMessage]),
    {ok, State}.

slave_lost(_SchedulerInfo, EventFailure, State) ->
    lager:info("Scheduler received slave lost event. Failure: ~p.",
               [EventFailure]),
    {ok, State}.

executor_lost(_SchedulerInfo, EventFailure, State) ->
    lager:info("Scheduler received executor lost event. Failure: ~p.",
               [EventFailure]),
    {ok, State}.

error(_SchedulerInfo, EventError, State) ->
    lager:info("Scheduler received error event. Error: ~p.", [EventError]),
    {stop, State}.

handle_info(_SchedulerInfo, Info, State) ->
    lager:info("Scheduler received unexpected message. Message: ~p.", [Info]),
    {ok, State}.

terminate(_SchedulerInfo, Reason, _State) ->
    lager:warning("Scheduler terminate. Reason: ~p.", [Reason]),
    ok.

%% Internal functions.

-spec init(rms:options(), scheduler_state()) ->
    {ok, erl_mesos:'FrameworkInfo'(), true, state()}.
init(Options, Scheduler) ->
    FrameworkInfo = framework_info(Options),
    lager:info("Start scheduler. Framework info: ~p.",
               [framework_info_to_list(FrameworkInfo)]),
    NodeData = rms_node_manager:node_data(Options),
    {ok, FrameworkInfo, true, #state{scheduler = Scheduler,
                                     node_data = NodeData}}.

-spec framework_id_value(undefined | erl_mesos:'FrameworkID'()) ->
    undefined | string().
framework_id_value(undefined) ->
    undefined;
framework_id_value(#'FrameworkID'{value = Value}) ->
    Value.

-spec framework_info(rms:options()) -> erl_mesos:'FrameworkInfo'().
framework_info(Options) ->
    Id = proplists:get_value(framework_id, Options),
    User = proplists:get_value(framework_user, Options),
    Name = proplists:get_value(framework_name, Options),
    Role = proplists:get_value(framework_role, Options),
    Hostname = proplists:get_value(framework_hostname, Options),
    Principal = proplists:get_value(framework_principal, Options),
    FailoverTimeout = proplists:get_value(framework_failover_timeout, Options),
    #'FrameworkInfo'{id = Id,
                     user = User,
                     name = Name,
                     role = Role,
                     hostname = Hostname,
                     principal = Principal,
                     %% TODO: We will want to enable checkpoint.
                     checkpoint = undefined,
                     %% TODO: Get this from wm helper probably.
                     webui_url = rms_config:webui_url(),
                     failover_timeout = FailoverTimeout}.

-spec framework_info_to_list(erl_mesos:'FrameworkInfo'()) -> framework_info().
framework_info_to_list(#'FrameworkInfo'{id = Id,
                                        user = User,
                                        name = Name,
                                        role = Role,
                                        hostname = Hostname,
                                        principal = Principal,
                                        checkpoint = Checkpoint,
                                        webui_url = WebuiUrl,
                                        failover_timeout = FailoverTimeout}) ->
    [{id, framework_id_value(Id)},
     {user, User},
     {name, Name},
     {role, Role},
     {hostname, Hostname},
     {principal, Principal},
     {checkpoint, Checkpoint},
     {webui_url, WebuiUrl},
     {failover_timeout, FailoverTimeout}].

-spec get_scheduler() -> {ok, scheduler_state()} | {error, term()}.
get_scheduler() ->
    case rms_metadata:get_scheduler() of
        {ok, Scheduler} ->
            {ok, from_list(Scheduler)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_scheduler(scheduler_state()) -> ok | {error, term()}.
set_scheduler(Scheduler) ->
    rms_metadata:set_scheduler(to_list(Scheduler)).

-spec to_list(scheduler_state()) -> rms_metadata:scheduler_state().
to_list(#scheduler{framework_id = FrameworkId,
                   cluster_keys = ClusterKeys,
                   removed_cluster_keys = RemovedClusterKeys}) ->
    [{framework_id, FrameworkId},
     {cluster_keys, ClusterKeys},
     {removed_cluster_keys, RemovedClusterKeys}].

-spec from_list(rms_metadata:scheduler_state()) -> scheduler_state().
from_list(SchedulerList) ->
    #scheduler{framework_id = proplists:get_value(framework_id, SchedulerList),
               cluster_keys = proplists:get_value(cluster_keys, SchedulerList),
               removed_cluster_keys =
                   proplists:get_value(removed_cluster_keys, SchedulerList)}.

-spec apply_offers([erl_mesos:'Offer'()], rms_node_manager:node_data()) ->
    {[erl_mesos:'OfferID'()], [erl_mesos:'Offer.Operation'()]}.
apply_offers(Offers, NodeData) ->
    apply_offers(Offers, NodeData, [], []).

-spec apply_offers([erl_mesos:'Offer'()], rms_node_manager:node_data(),
                   [erl_mesos:'OfferID'()], [erl_mesos:'Offer.Operation'()]) ->
    {[erl_mesos:'OfferID'()], [erl_mesos:'Offer.Operation'()]}.
apply_offers([Offer | Offers], NodeData, OfferIds, Operations) ->
    {OfferId, Operations1} = apply_offer(Offer, NodeData),
    apply_offers(Offers, NodeData, [OfferId | OfferIds],
                 Operations ++ Operations1);
apply_offers([], _NodeData, OfferIds, Operations) ->
    {OfferIds, Operations}.

-spec apply_offer(erl_mesos:'Offer'(), rms_node_manager:node_data()) ->
    {erl_mesos:'OfferID'(), [erl_mesos:'Offer.Operation'()]}.
apply_offer(Offer, NodeData) ->
    OfferHelper = rms_offer_helper:new(Offer),
    lager:info("Scheduler recevied offer. "
               "Offer id: ~s. "
               "Resources: ~p.",
               [rms_offer_helper:get_offer_id_value(OfferHelper),
                rms_offer_helper:resources_to_list(OfferHelper)]),
    OfferHelper1 = rms_cluster_manager:apply_offer(OfferHelper, NodeData),
    OfferId = rms_offer_helper:get_offer_id(OfferHelper1),
    Operations = rms_offer_helper:operations(OfferHelper1),
    {OfferId, Operations}.

-spec reconcile_tasks([string()]) -> [erl_mesos:'Call.Reconcile.Task'()].
reconcile_tasks(TaskIdValues) ->
    [begin
         TaskId = erl_mesos_utils:task_id(TaskIdValue),
         #'Call.Reconcile.Task'{task_id = TaskId}
     end || TaskIdValue <- TaskIdValues].

