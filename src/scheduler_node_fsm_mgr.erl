%% TODO: remove this file.

-module(scheduler_node_fsm_mgr).

-behavior(gen_server).

-export([start_link/0,
         start_child/2,
         get_pid/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {}).

%% public API

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, #state{}, []).

-spec start_child(string(), erl_mesos:'AgentID'()) -> {ok, pid()} | {error, term()}.
start_child(Id, AgentId) ->
    gen_server:call(?MODULE, {start_child, Id, AgentId}).

-spec get_pid(term()) -> {ok, pid()} | {error, not_found}.
get_pid(Id) ->
    case ets:lookup(?MODULE, Id) of
        [] -> {error, not_found};
        [{Id, Pid}] -> {ok, Pid}
    end.

%% gen_server implementation

init(State) ->
    ets:new(?MODULE, [protected, named_table]),
    {ok, State}.

handle_call({start_child, Id, AgentId}, _From, State) ->
    case supervisor:start_child(scheduler_node_fsm_child_sup, [Id, AgentId]) of
        {ok, Pid} ->
            erlang:monitor(process, Pid),
            ets:insert(?MODULE, {Id, Pid}),
            {reply, {ok, Pid}, State};
        Error ->
            {reply, Error, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Info}, State) ->
    Keys = ets:match(?MODULE, {'$1', Pid}),
    [ets:delete(?MODULE, K) || K <- Keys],
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.
