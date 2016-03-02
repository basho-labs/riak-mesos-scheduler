%% TODO: remove this file and may be move some of the code to the rms_node.

-module(scheduler_node_fsm).

-behavior(gen_fsm).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-define(START_TIMEOUT, 30000).

%% API functions
-export([start_link/2,
         status_update/2,
         stop_node/1
        ]).

%% Required gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4
        ]).

%% gen_fsm state functions
-export([starting/2,
         running/2,
         stopping/2
        ]).

-record(state, {
          task_id :: string(),
          agent_id :: erl_mesos:'AgentID'()
         }).

%% public API

-spec start_link(string(), erl_mesos:'AgentID'()) -> {ok, pid()}.
start_link(TaskId, AgentId) ->
    gen_fsm:start_link(?MODULE, [TaskId, AgentId], []).

-spec status_update(string(), term()) -> ok | {error, not_found}.
status_update(TaskId, Event) ->
    #'Event.Update'{status = #'TaskStatus'{state = Status}} = Event,
    case is_all_state_status(Status) of
        false ->
            send_event(TaskId, {status_update, Status});
        true ->
            send_all_state_event(TaskId, {status_update, Status})
    end.

stop_node(TaskId) ->
    sync_send_all_state_event(TaskId, stop_node).

%% Utility functions for passing events in to the FSM

send_event(TaskId, Event) ->
    do_send_event(TaskId, Event, fun gen_fsm:send_event/2).

send_all_state_event(TaskId, Event) ->
    do_send_event(TaskId, Event, fun gen_fsm:send_all_state_event/2).

sync_send_all_state_event(TaskId, Event) ->
    do_send_event(TaskId, Event, fun gen_fsm:sync_send_all_state_event/2).

do_send_event(TaskId, Event, SendFun) ->
    case scheduler_node_fsm_mgr:get_pid(TaskId) of
        {ok, Pid} ->
            SendFun(Pid, Event);
        {error, not_found} ->
            {error, not_found}
    end.

%% gen_fsm required callbacks

init([TaskId, AgentId]) ->
    lager:md([{task_id, TaskId}]),
    StateData = #state{task_id = TaskId, agent_id = AgentId},
    {ok, starting, StateData, ?START_TIMEOUT}.

handle_event(Event, StateName, StateData) ->
    lager:warning("Unhandled all-state event ~p at state ~p!", [Event, StateName]),
    {next_state, StateName, StateData}.

handle_sync_event(stop_node, _From, _StateName, StateData) ->
    lager:info("Got stop_node message. Stopping..."),

    %% TODO handle errors here instead of matching on 'ok':
    ok = mesos_scheduler_data:set_node_status(StateData#state.task_id, stopping),

    %% It is incorrect. Scheduler may be disconnected.
    %% We should do the queue inside rms_scheduler and make all mesos calls from rms_scheduler.
    SchedulerInfo = erl_mesos_scheduler:get_scheduler_info(),

    AgentId = StateData#state.agent_id,
    ExecutorId = erl_mesos_utils:executor_id("riak"),
    erl_mesos_scheduler:message(SchedulerInfo, AgentId, ExecutorId, <<"finish">>),

    {reply, ok, stopping, StateData};

handle_sync_event(Event, _From, StateName, StateData) ->
    lager:warning("Unhandled all-state sync event ~p at state ~p!", [Event, StateName]),
    {reply, ok, StateName, StateData}.

handle_info(Info, StateName, StateData) ->
    lager:warning("Unhandled message ~p at state ~p!", [Info, StateName]),
    {next_state, StateName, StateData}.

terminate(_Reason, _StateName, _StateData) ->
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%% --- 'starting' state functions ---

starting({status_update, Status}, StateData) ->
    starting_event_update(Status, StateData);
starting(timeout, StateData) ->
    lager:error("Failed to receive task status update after ~p seconds. Aborting...",
                [?START_TIMEOUT]),
    die(StateData).

starting_event_update('TASK_RUNNING', StateData) ->
    lager:info("Node startup successful. Current status: 'TASK_RUNNING'"),
    %% TODO handle errors here somehow? Maybe wait and retry setting node status before giving up?
    ok = mesos_scheduler_data:set_node_status(StateData#state.task_id, active),
    {next_state, running, StateData};
starting_event_update(Status, StateData) when Status =:= 'TASK_STAGING' ;
                                              Status =:= 'TASK_STARTING' ->
    lager:info("Got task event update ~p - continuing to wait for startup to complete", [Status]),
    {next_state, starting, StateData, ?START_TIMEOUT};
starting_event_update(Status, StateData) ->
    lager:warning("Got unexpected task status ~p. Aborting startup...", [Status]),
    die(StateData).

%% --- 'running' state functions ---

running({status_update, Status}, StateData) ->
    lager:info("Got status update ~p", [Status]),
    {next_state, running, StateData}.

%% --- 'stopping' state functions ---

stopping({status_update, 'TASK_KILLED'}, StateData) ->
    lager:info("Got 'TASK_KILLED' update for stopping node. Deleting node from metadata."),
    ok = mesos_scheduler_data:delete_node(StateData#state.task_id),
    {stop, normal, StateData}.

%% Misc helper functions

is_all_state_status('TASK_STAGING')  -> false;
is_all_state_status('TASK_STARTING') -> false;
is_all_state_status('TASK_RUNNING')  -> false;
is_all_state_status('TASK_KILLED') -> false;
is_all_state_status(_) -> true.

die(StateData) ->
    mesos_scheduler_data:set_node_status(StateData#state.task_id, requested),
    {stop, normal, StateData}.
