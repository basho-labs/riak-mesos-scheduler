-module(scheduler_node_fsm_mgr).

-behavior(gen_server).

-export([start_link/0,
         start_child/1,
         get_pid/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {}).

%% public API

start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, #state{}, []).

start_child(Id) ->
    gen_server:call(?MODULE, {start_child, Id}).

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

handle_call({start_child, Id}, _From, State) ->
    {ok, Pid} = supervisor:start_child(scheduler_node_fsm_child_sup, [Id]),
    erlang:monitor(process, Pid),
    ets:insert(?MODULE, {Id, Pid}),
    {reply, ok, State}.

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
