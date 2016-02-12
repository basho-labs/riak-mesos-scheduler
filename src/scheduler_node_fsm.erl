-module(scheduler_node_fsm).

-behavior(plain_fsm).

-include_lib("plain_fsm/include/plain_fsm.hrl").

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([start_link/1]).
-export([send_msg/2]).
-export([code_change/3]).

-record(state, {
          task_id :: string()
         }).

%% public API

-spec start_link(string()) -> {ok, pid()}.
start_link(TaskId) ->
    Pid = plain_fsm:spawn_link(?MODULE, fun() -> init(TaskId) end),
    {ok, Pid}.

-spec send_msg(string(), term()) -> ok | {error, not_found}.
send_msg(TaskId, Msg) ->
    case scheduler_node_fsm_mgr:get_pid(TaskId) of
        {ok, Pid} ->
            Pid ! Msg,
            ok;
        {error, not_found} ->
            {error, not_found}
    end.

%% plain_fsm behavior callbacks

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% FSM state functions:

init(TaskId) ->
    lager:md([{task_id, TaskId}]),
    starting(#state{
                task_id = TaskId
               }).

%% --- 'starting' state functions ---

starting(State) ->
    plain_fsm:extended_receive(
      receive
          #'Event.Update'{status = #'TaskStatus'{state = Status}} ->
              starting_event_update(Status, State)
      after
          30000 ->
              lager:error("Failed to receive task status update after ~p seconds. Aborting...",
                          [30000]),
              die(State)
      end
     ).

starting_event_update('TASK_RUNNING', State) ->
    lager:info("Node startup successful. Current status: 'TASK_RUNNING'"),
    %% TODO handle errors here somehow? Maybe wait and retry setting node status before giving up?
    ok = mesos_scheduler_data:set_node_status(State#state.task_id, active),
    running(State);
starting_event_update(Status, State) when Status =:= 'TASK_STAGING' ;
                                          Status =:= 'TASK_STARTING' ->
    lager:info("Got task event update ~p - continuing to wait for startup to complete", [Status]),
    starting(State);
starting_event_update(Status, State) ->
    lager:warning("Got unexpected task status ~p. Aborting startup...", [Status]),
    die(State).

%% --- 'running' state functions ---

running(State) ->
    plain_fsm:extended_receive(
      receive
          #'Event.Update'{status = #'TaskStatus'{state = Status}} ->
              running_event_update(Status, State),
              running(State)
      end
     ).

running_event_update(Status, _State) ->
    lager:info("Got status update ~p", [Status]).

%% Misc helper functions

die(State) ->
    mesos_scheduler_data:set_node_status(State#state.task_id, requested).
