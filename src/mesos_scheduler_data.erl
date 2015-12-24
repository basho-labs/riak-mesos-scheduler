-module(mesos_scheduler_data).

-behavior(gen_server).

-export([
         start_link/0,
         add_cluster/3,
         get_cluster/1,
         set_cluster_status/2,
         join_node_to_cluster/2,
         delete_cluster/1,
         add_node/3,
         set_node_status/2,
         delete_node/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type key() :: term(). %% Keys used to identify nodes/clusters

%% We might not need all of these, it's just a best guess as to what states we might expect to see:
-type node_status() :: requested | starting | active | down | stopping | stopped.
-type cluster_status() :: requested | active | stopping | stopped.

-record(state, {
          cluster_table :: ets:tid(),
          node_table :: ets:tid()
         }).

-record(cluster, {
          key :: key(),
          status :: node_status(),
          nodes = [] :: [key()]
}).

-record(node, {
          key :: key(),
          status :: node_status(),
          location :: term() %% FIXME - determine how we track node location?
}).

-define(CLUSTER_TAB, mesos_scheduler_cluster_data).
-define(NODE_TAB, mesos_scheduler_node_data).

%% public API

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, nil, []).

-spec add_cluster(key(), cluster_status(), [key()]) -> ok | {error, term()}.
add_cluster(Key, Status, Nodes) ->
    gen_server:call(?MODULE, {add_cluster, Key, Status, Nodes}).

-spec get_cluster(key()) -> {ok, cluster_status(), [key()]} | {error, not_found}.
get_cluster(Key) ->
    gen_server:call(?MODULE, {get_cluster, Key}).

-spec set_cluster_status(key(), cluster_status()) -> ok | {error, term()}.
set_cluster_status(Key, Status) ->
    gen_server:call(?MODULE, {set_cluster_status, Key, Status}).

-spec join_node_to_cluster(key(), key()) -> ok | {error, term()}.
join_node_to_cluster(ClusterKey, NodeKey) ->
    gen_server:call(?MODULE, {join_node_to_cluster, ClusterKey, NodeKey}).

-spec delete_cluster(key()) -> ok | {error, term()}.
delete_cluster(Key) ->
    gen_server:call(?MODULE, {delete_cluster, Key}).

-spec add_node(key(), node_status(), location()) -> ok | {error, term()}.
add_node(Key, Status, Location) ->
    gen_server:call(?MODULE, {add_node, Key, Status, Location}).

-spec set_node_status(key(), node_status()) -> ok | {error, term()}.
set_node_status(Key, Status) ->
    gen_server:call(?MODULE, {set_node_status, Key, Status}).

-spec delete_node(key()) -> ok | {error, term()}.
delete_node(Key) ->
    gen_server:call(?MODULE, {delete_node, Key}).

%% gen_server implementation

init(State) ->
    ClusterTid = ets:create_table(?CLUSTER_TAB, [set, private]),
    NodeTid = ets:create_table(?NODE_TAB, [set, private]),
    {ok, #state{
            cluster_table = ClusterTid,
            node_table = NodeTid
           }
    }.

handle_call({add_cluster, Key, Status, Nodes}, _From, State) ->
    {reply, fixme, State};
handle_call({get_cluster, Key}, _From, State) ->
    {reply, fixme, State};
handle_call({set_cluster_status, Key, Status}, _From, State) ->
    {reply, fixme, State};
handle_call({join_node_to_cluster, ClusterKey, NodeKey}, _From, State) ->
    {reply, fixme, State};
handle_call({delete_cluster, Key}, _From, State) ->
    {reply, fixme, State};
handle_call({add_node, Key, Status, Location}, _From, State) ->
    {reply, fixme, State};
handle_call({set_node_status, Key, Status}, _From, State) ->
    {reply, fixme, State};
handle_call({delete_node, Key}, _From, State) ->
    {reply, fixme, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.
