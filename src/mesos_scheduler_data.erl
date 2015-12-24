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

-type location() :: term(). %% FIXME determine how we track node location? Or just leave it opaque?

%% We might not need all of these, it's just a best guess as to what states we might expect to see:
-type node_status() :: requested | starting | active | down | stopping | stopped.
-type cluster_status() :: requested | active | stopping | stopped.

-record(state, {
         }).

-record(cluster, {
          key :: key(),
          status :: cluster_status(),
          nodes = [] :: [key()]
}).

-record(node, {
          key :: key(),
          status :: node_status(),
          location :: term()
}).

-define(CLUST_TAB, mesos_scheduler_cluster_data).
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

init(_) ->
    ets:create_table(?CLUST_TAB, [set, private, named_table, {keypos, #cluster.key}]),
    ets:create_table(?NODE_TAB, [set, private, named_table, {keypos, #node.key}]),
    {ok, #state{
           }
    }.

handle_call({add_cluster, Key, Status, Nodes}, _From, State) ->
    Result = do_add_cluster(Key, Status, Nodes),
    {reply, Result, State};
handle_call({get_cluster, Key}, _From, State) ->
    Result = do_get_cluster(Key),
    {reply, Result, State};
handle_call({set_cluster_status, Key, Status}, _From, State) ->
    Result = do_set_cluster_status(Key, Status),
    {reply, Result, State};
handle_call({join_node_to_cluster, ClusterKey, NodeKey}, _From, State) ->
    Result = do_join_node_to_cluster(ClusterKey, NodeKey),
    {reply, Result, State};
handle_call({delete_cluster, Key}, _From, State) ->
    Result = do_delete_cluster(Key),
    {reply, Result, State};
handle_call({add_node, Key, Status, Location}, _From, State) ->
    Result = do_add_node(Key, Status, Location),
    {reply, Result, State};
handle_call({set_node_status, Key, Status}, _From, State) ->
    Result = do_set_node_status(Key, Status),
    {reply, Result, State};
handle_call({delete_node, Key}, _From, State) ->
    Result = do_delete_node(Key),
    {reply, Result, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Private implementation functions

do_add_cluster(Key, Status, Nodes) ->
    NewCluster = #cluster{
                    key = Key,
                    status = Status,
                    nodes = Nodes
                   },
    case ets:insert_new(?CLUST_TAB, NewCluster) of
        false ->
            {error, {cluster_exists, Key}};
        true ->
            ok
    end.

do_get_cluster(Key) ->
    case ets:lookup(?CLUST_TAB, Key) of
        [] ->
            {error, {not_found, Key}};
        [Cluster] ->
            {ok, Cluster#cluster.status, Cluster#cluster.nodes}
    end.

do_set_cluster_status(Key, Status) ->
    case ets:lookup(?CLUST_TAB, Key) of
        [] ->
            {error, {not_found, Key}};
        [Cluster] ->
            NewCluster = Cluster#cluster{status = Status},
            ets:insert(?CLUST_TAB, NewCluster),
            ok
    end.

do_join_node_to_cluster(ClusterKey, NodeKey) ->
    case ets:lookup(?NODE_TAB, NodeKey) of
        [] ->
            {error, {node_not_found, NodeKey}};
        [Node] when Node#node.status =/= active ->
            {error, {node_not_active, NodeKey, Node#node.status}};
        [_Node] ->
            case ets:lookup(?CLUST_TAB, ClusterKey) of
                [] ->
                    {error, {cluster_not_found, ClusterKey}};
                [Cluster] when Cluster#cluster.status =/= active ->
                    {error, {cluster_not_active, ClusterKey, Cluster#cluster.status}};
                [Cluster] ->
                    ClusterNodes = Cluster#cluster.nodes,
                    case lists:member(NodeKey, ClusterNodes) of
                        true ->
                            {error, {node_already_joined, ClusterKey, NodeKey}};
                        false ->
                            NewNodes = [NodeKey | ClusterNodes],
                            NewCluster = Cluster#cluster{nodes = NewNodes},
                            ets:insert(?CLUST_TAB, NewCluster)
                    end
            end
    end.

do_delete_cluster(Key) ->
    do_delete(Key, ?CLUST_TAB).

do_add_node(Key, Status, Location) ->
    NewNode = #node{
                 key = Key,
                 status = Status,
                 location = Location
                },
    case ets:insert_new(?NODE_TAB, NewNode) of
        true ->
            ok;
        false ->
            {error, {node_exists, Key}}
    end.

do_set_node_status(Key, Status) ->
    case ets:lookup(?NODE_TAB, Key) of
        [] ->
            {error, {not_found, Key}};
        [Node] ->
            NewNode = Node#node{status = Status},
            ets:insert(?NODE_TAB, NewNode),
            ok
    end.

do_delete_node(Key) ->
    do_delete(Key, ?NODE_TAB).

do_delete(Key, Table) ->
    case ets:lookup(Table, Key) of
        [] ->
            {error, {not_found, Key}};
        [_] ->
            ets:delete(Table, Key)
    end.
