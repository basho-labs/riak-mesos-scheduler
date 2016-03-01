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

-define(OFFER_INTERVAL, 5).

%% erl_mesos_scheduler callback functions.

init(Options) ->
    lager:info("Scheduler options: ~p.", [Options]),
    %% TODO: check if framework id exist in metadata manager.
    FrameworkInfo = framework_info(Options),
    NodeData = rms_node_manager:node_data(Options),
    lager:info("Start scheduler with framework info: ~p.", [FrameworkInfo]),
    {ok, FrameworkInfo, true, #state{node_data = NodeData,
                                     framework_info = FrameworkInfo}}.

registered(_SchedulerInfo, EventSubscribed, State) ->
    lager:info("Scheduler registered: ~p.", [EventSubscribed]),
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
    lager:info("~n~nOfferIds: ~p.~n~n", [OfferIds]),
    lager:info("~n~nOperations: ~p.~n~n", [Operations]),
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

-spec framework_info(rms:options()) -> erl_mesos:'FrameworkInfo'().
framework_info(Options) ->
    #'FrameworkInfo'{user = proplists:get_value(framework_user, Options),
                     name = proplists:get_value(framework_name, Options),
                     role = proplists:get_value(framework_role, Options),
                     hostname =
                         proplists:get_value(framework_hostname, Options),
                     principal =
                         proplists:get_value(framework_principal, Options),
                     %% TODO: We will want to enable checkpoint.
                     checkpoint = undefined,
                     %% TODO: Will need to check ZK for this for reregistration.
                     id = undefined,
                     %% TODO: Get this from wm helper probably.
                     webui_url = undefined,
                     %% TODO: Add this to configurable options.
                     failover_timeout = undefined}.

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
