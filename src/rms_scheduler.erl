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

-record(scheduler, {options :: rms:options()}).

-record(state, {scheduler :: scheduler_state(),
                calls_queue :: erl_mesos_calls_queue:calls_queue()}).

-type scheduler_state() :: #scheduler{}.
-export_type([scheduler_state/0]).

-type state() :: #state{}.

-type framework_info() :: [{atom(), term()}].

-define(OFFER_INTERVAL, 5).

%% erl_mesos_scheduler callback functions.

-spec init(rms:options()) ->
    {ok, erl_mesos:'FrameworkInfo'(), true, state()} | {stop, term()}.
init(Options) ->
    lager:info("Scheduler options: ~p.", [Options]),
    case get_scheduler() of
        {ok, #scheduler{options = Options1} = Scheduler} ->
            FrameworkId = proplists:get_value(framework_id, Options1,
                                              undefined),
            Options2 = [{framework_id, FrameworkId} | Options],
            Scheduler1 = Scheduler#scheduler{options = Options2},
            init_scheduler(Scheduler1);
        {error, not_found} ->
            Scheduler = #scheduler{options = Options},
            init_scheduler(Scheduler)
    end.

-spec registered(erl_mesos_scheduler:scheduler_info(),
                 erl_mesos:'Event.Subscribed'(), state()) ->
    {ok, state()} | {stop, state()}.
registered(SchedulerInfo, #'Event.Subscribed'{framework_id = FrameworkId},
           #state{scheduler = #scheduler{options = Options} = Scheduler} =
               State) ->
    case proplists:get_value(framework_id, Options, undefined) of
        undefined ->
            Options1 = [{framework_id, FrameworkId} |
                        proplists:delete(framework_id, Options)],
            Scheduler1 = Scheduler#scheduler{options = Options1},
            State1 = State#state{scheduler = Scheduler1},
            case set_scheduler(Scheduler1) of
                ok ->
                    lager:info("New scheduler registered. Framework id: ~p.",
                               [framework_id_value(FrameworkId)]),
                    {ok, State1};
                {error, Reason} ->
                    lager:error("Error during saving scheduler state. Reason: ~p.",
                                [Reason]),
                    {stop, State1}
            end;
        _FrameworkId ->
            lager:info("Scheduler registered. Framework id: ~p.",
                       [framework_id_value(FrameworkId)]),
            TaskIdValues = rms_node_manager:get_node_keys(),
            case length(TaskIdValues) of
                0 ->
                    ok;
                _Len ->
                    lager:info("Scheduler task ids to reconcile: ~p.",
                               [TaskIdValues])
            end,
            ReconcileTasks = reconcile_tasks(TaskIdValues),
            call(reconcile, [SchedulerInfo, ReconcileTasks], State)
    end.

-spec reregistered(erl_mesos_scheduler:scheduler_info(), state()) ->
    {ok, state()} | {stop, state()}.
reregistered(_SchedulerInfo, State) ->
    lager:warning("Scheduler reregistered.", []),
    exec_calls(State).

-spec disconnected(erl_mesos_scheduler:scheduler_info(), state()) ->
    {ok, state()} | {stop, state()}.
disconnected(_SchedulerInfo, State) ->
    lager:warning("Scheduler disconnected.", []),
    {ok, State}.

-spec resource_offers(erl_mesos_scheduler:scheduler_info(),
                      erl_mesos:'Event.Offers'(), state()) ->
                             {ok, state()} | {stop, state()}.
resource_offers(SchedulerInfo, #'Event.Offers'{offers = Offers}, State) ->
    case apply_offers(SchedulerInfo, Offers, State) of
        {ok, State1} ->
            ExecsToShutdown = rms_cluster_manager:executors_to_shutdown(),
            case shutdown_executors(SchedulerInfo, ExecsToShutdown, State1) of
                {ok, State2} -> 
                    %% Attempt to drain queue
                    exec_calls(State2);
                Response2 -> Response2
            end;            
        Response1 ->
            Response1
    end.

-spec offer_rescinded(erl_mesos_scheduler:scheduler_info(),
                      erl_mesos:'Event.Rescind'(), state()) ->
    {ok, state()}.
offer_rescinded(_SchedulerInfo, #'Event.Rescind'{} = EventRescind, State) ->
    lager:info("Scheduler received offer rescinded event. "
               "Rescind: ~p.",
               [EventRescind]),
    {ok, State}.

-spec status_update(erl_mesos_scheduler:scheduler_info(),
                    erl_mesos:'Event.Update'(), state()) ->
    {ok, state()} | {stop, state()}.
status_update(SchedulerInfo, #'Event.Update'{
                                 status=#'TaskStatus'{
                                           reason=Reason,
                                           task_id=TaskId, 
                                           agent_id=AgentId, 
                                           state=NodeState, 
                                           uuid=Uuid}} = EventUpdate, State)->
    lager:info("Scheduler received status update event. "
               "Update: ~p~n", [EventUpdate]),
    {ok, NodeName} = node_name_from_task_id(TaskId),
    {ok, ClusterName} = rms_node_manager:get_node_cluster_key(NodeName),
    case rms_cluster_manager:handle_status_update(ClusterName, NodeName, NodeState, Reason) of
        ok ->
            case Uuid of 
                undefined -> 
                    {ok, State};
                _ ->
                    call(acknowledge, [SchedulerInfo, AgentId, TaskId, Uuid], State)
            end;
        {error, Reason} ->
            lager:warning("Error while attempting to process status update: ~p.", [Reason]),
            {ok, State}
    end.

-spec framework_message(erl_mesos_scheduler:scheduler_info(),
                        erl_mesos:'Event.Message'(), state()) ->
    {ok, state()}.
framework_message(_SchedulerInfo, EventMessage, State) ->
    lager:info("Scheduler received framework message. "
               "Framework message: ~p.",
               [EventMessage]),
    {ok, State}.

-spec slave_lost(erl_mesos_scheduler:scheduler_info(),
                 erl_mesos:'Event.Failure'(), state()) ->
    {ok, state()}.
slave_lost(_SchedulerInfo, EventFailure, State) ->
    lager:info("Scheduler received slave lost event. Failure: ~p.",
               [EventFailure]),
    {ok, State}.

-spec executor_lost(erl_mesos_scheduler:scheduler_info(),
                    erl_mesos:'Event.Failure'(), state()) ->
    {ok, state()}.
executor_lost(_SchedulerInfo, EventFailure, State) ->
    lager:info("Scheduler received executor lost event. Failure: ~p.",
               [EventFailure]),
    {ok, State}.

-spec error(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Event.Error'(),
            state()) ->
    {stop, state()}.
error(_SchedulerInfo, #'Event.Error'{message = Message}, State) ->
    lager:info("Scheduler received error event. Error message: ~s.", [Message]),
    may_be_delete_scheduler(Message),
    {stop, State}.

-spec handle_info(erl_mesos_scheduler:scheduler_info(), term(), state()) ->
    {ok, state()}.
handle_info(_SchedulerInfo, Info, State) ->
    lager:info("Scheduler received unexpected message. Message: ~p.", [Info]),
    {ok, State}.

-spec terminate(erl_mesos_scheduler:scheduler_info(), term(), state()) ->
    ok.
terminate(_SchedulerInfo, Reason, _State) ->
    lager:warning("Scheduler terminate. Reason: ~p.", [Reason]),
    ok.

%% Internal functions.

-spec init_scheduler(scheduler_state()) ->
    {ok, erl_mesos:'FrameworkInfo'(), true, state()} | {stop, term()}.
init_scheduler(#scheduler{options = Options} = Scheduler) ->
    case set_scheduler(Scheduler) of
        ok ->
            FrameworkInfo = framework_info(Options),
            lager:info("Start scheduler. Framework info: ~p.",
                       [framework_info_to_list(FrameworkInfo)]),
            CallsQueue = erl_mesos_calls_queue:new(),
            {ok, FrameworkInfo, true, #state{scheduler = Scheduler,
                                             calls_queue = CallsQueue}};
        {error, Reason} ->
            lager:error("Error during saving scheduler state. Reason: ~p.",
                        [Reason]),
            {stop, {error, Reason}}
    end.

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
    WebUIURL = proplists:get_value(framework_webui_url, Options),
    #'FrameworkInfo'{id = Id,
                     user = User,
                     name = Name,
                     role = Role,
                     hostname = Hostname,
                     principal = Principal,
                     checkpoint = true,
                     webui_url = WebUIURL,
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
to_list(#scheduler{options = Options}) ->
    [{options, Options}].

-spec from_list(rms_metadata:scheduler_state()) -> scheduler_state().
from_list(SchedulerList) ->
    #scheduler{options = proplists:get_value(options, SchedulerList)}.

-spec call(atom(), [term()], state()) -> {ok, state()} | {stop, state()}.
call(Function, Args, #state{calls_queue = CallsQueue} = State) ->
    Call = {erl_mesos_scheduler, Function, Args},
    case erl_mesos_calls_queue:exec_or_push_call(Call, CallsQueue) of
        {ok, CallsQueue1} ->
            State1 = State#state{calls_queue = CallsQueue1},
            {ok, State1};
        {exec_error, Reason, CallsQueue1} ->
            lager:warning("Scheduler api call error. "
                          "Put call to the queue. "
                          "Function: ~p, "
                          "Args: ~p, "
                          "Call error reason: ~p.",
                          [Function, Args, Reason]),
            State1 = State#state{calls_queue = CallsQueue1},
            {ok, State1};
        {error, Reason} ->
            lager:warning("Scheduler api call error. "
                          "Put call to the queue error. "
                          "Function: ~p, "
                          "Args: ~p, "
                          "Queue error reason: ~p.",
                          [Function, Args, Reason]),
            {stop, State}
    end.

-spec exec_calls(state()) -> {ok, state()} | {stop, state()}.
exec_calls(#state{calls_queue = CallsQueue} = State) ->
    case erl_mesos_calls_queue:exec_calls(CallsQueue) of
        {exec_error, Reason, CallsQueue1} ->
            lager:warning("Scheduler api call from queue error. "
                          "Call error reason: ~p.",
                          [Reason]),
            {ok, State#state{calls_queue = CallsQueue1}};
        {error, Reason} ->
            lager:warning("Scheduler queue error."
                          "Queue error reason: ~p.",
                          [Reason]),
            {stop, State};
        calls_queue_empty ->
            State1 = State#state{calls_queue = erl_mesos_calls_queue:new()},
            {ok, State1}
    end.

-spec apply_offers(erl_mesos_scheduler:scheduler_info(), [erl_mesos:'Offer'()], state()) -> 
                          {ok, state()} | {stop, state()}.
apply_offers(_, [], State) ->
    {ok, State};
apply_offers(SchedulerInfo, [Offer|Offers], #state{scheduler = #scheduler{options = Options}}=State) ->
    Constraints = proplists:get_value(constraints, Options),
    Filters = #'Filters'{refuse_seconds = ?OFFER_INTERVAL},
    {OfferId, Operations} = apply_offer(Offer, Constraints),
    case length(Operations) of
        0 ->
            ok;
        _Len ->
            lager:info("Scheduler accept operations: ~p.", [Operations])
    end,
    case call(accept, [SchedulerInfo, [OfferId], Operations, Filters], State) of
        {ok, State1} -> 
            apply_offers(SchedulerInfo, Offers, State1);
        _ -> {stop, State}
    end.

-spec apply_offer(erl_mesos:'Offer'(), rms_offer_helper:constraints()) ->
                         {erl_mesos:'OfferID'(), [erl_mesos:'Offer.Operation'()]}.
apply_offer(Offer, Constraints) ->
    OfferHelper = rms_offer_helper:new(Offer),
    OfferHelper1 = rms_offer_helper:set_constraints(Constraints, OfferHelper),
    lager:info("Scheduler recevied offer. "
               "Offer id: ~s. "
               "Resources: ~p. "
               "Constraints: ~p. ",
               [rms_offer_helper:get_offer_id_value(OfferHelper1),
                rms_offer_helper:resources_to_list(OfferHelper1),
                rms_offer_helper:get_constraints(OfferHelper1)]),
    OfferHelper2 = rms_cluster_manager:apply_offer(OfferHelper1),
    OfferId = rms_offer_helper:get_offer_id(OfferHelper2),
    Operations = rms_offer_helper:operations(OfferHelper2),
    {OfferId, Operations}.

-spec reconcile_tasks([string()]) -> [erl_mesos:'Call.Reconcile.Task'()].
reconcile_tasks(TaskIdValues) ->
    [begin
         TaskId = erl_mesos_utils:task_id(TaskIdValue),
         #'Call.Reconcile.Task'{task_id = TaskId}
     end || TaskIdValue <- TaskIdValues].

-spec node_name_from_task_id(erl_mesos:'TaskID'()) -> {ok, string()}.
node_name_from_task_id(#'TaskID'{value = NodeName}) ->
    {ok, NodeName}.

-spec may_be_delete_scheduler(string()) -> ok | {error, term()}.
may_be_delete_scheduler("Framework has been removed") ->
    case rms_metadata:set_scheduler([]) of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("Error during saving scheduler state. "
                        "Reason: ~p.", [Reason]),
            {error, Reason}
    end;
may_be_delete_scheduler(_Message) ->
    ok.

-spec shutdown_executors(erl_mesos_scheduler:scheduler_info(),
                         [{string(), string()}], state()) ->
    {ok, state()} | {stop, state()}.
shutdown_executors(_, [], State) ->
    {ok, State};
shutdown_executors(SchedulerInfo, [{NodeKey, AgentIdValue}|Rest], State) ->
    AgentId = erl_mesos_utils:agent_id(AgentIdValue),
    ExecutorId = erl_mesos_utils:executor_id(NodeKey),
    _TaskId = erl_mesos_utils:task_id(NodeKey),
    lager:info("Shutting down ~p.", [NodeKey]),
    case call(message, 
              [SchedulerInfo, AgentId, 
               ExecutorId, <<"finish">>], State) of
        {ok, S1} ->
            lager:info("Finished shutting down ~p.", [NodeKey]),
            shutdown_executors(SchedulerInfo, Rest, S1);
        R -> 
            lager:info("Error shutting node down: ~p.", [R]),
            R
    end.
    %% case call(shutdown, [SchedulerInfo, ExecutorId, AgentId], State) of
    %%     {ok, S1} ->
    %%         case call(kill, [SchedulerInfo, TaskId, AgentId], S1) of
    %%             {ok, S2} ->
    %%                 shutdown_executors(SchedulerInfo, Rest, S2);
    %%             R -> 
    %%                 R
    %%         end;
    %%     R ->
    %%         R
    %% end.
