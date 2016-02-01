-module(scheduler_node_fsm).

-behavior(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
          task_id
         }).

%% public API

start_link(TaskId) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [TaskId], []).

%% gen_server implementation

init([TaskId]) ->
    {ok, #state{
            task_id = TaskId
           }}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.
