-module(mesos_scheduler_data).

-behavior(gen_server).

-export([
         start_link/0,
         stop/0,
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

%% Debug/testing use only?
-export([reset_all_data/0]).

-type key() :: iolist(). %% Keys used to identify nodes/clusters

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

-define(ZK_CLUSTER_NODE, "clusters").
-define(ZK_NODE_NODE, "nodes").

%% public API

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, nil, []).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop).

-spec add_cluster(key(), cluster_status(), [key()]) -> ok | {error, term()}.
add_cluster(Key, Status, Nodes) ->
    gen_server:call(?MODULE, {add_cluster, Key, Status, Nodes}).

-spec get_cluster(key()) -> {ok, cluster_status(), [key()]} | {error, {not_found, key()}}.
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

%% debug/test API

reset_all_data() ->
    gen_server:call(?MODULE, reset_all_data).

%% gen_server implementation

init(_) ->
    init_ets(),
    load_or_init_persistent_data(),
    {ok, #state{
           }
    }.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
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
    {reply, Result, State};

handle_call(reset_all_data, _From, State) ->
    RootPath = root_path(),
    ClusterBasePath = [RootPath, "/", ?ZK_CLUSTER_NODE],
    NodeBasePath = [RootPath, "/", ?ZK_NODE_NODE],
    ok = mesos_metadata_manager:recursive_delete(ClusterBasePath),
    ok = mesos_metadata_manager:recursive_delete(NodeBasePath),
    true = ets:delete_all_objects(?CLUST_TAB),
    true = ets:delete_all_objects(?NODE_TAB),
    load_or_init_persistent_data(),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Private implementation functions

init_ets() ->
    ets:new(?CLUST_TAB, [set, private, named_table, {keypos, #cluster.key}]),
    ets:new(?NODE_TAB, [set, private, named_table, {keypos, #node.key}]).

%% Since the root path will never change, it's not too evil to save it in the process dictionary,
%% so that we can avoid having to thread the value all over the code everywhere.
%% But we will at least wrap everything in getter/setter functions for cleanliness.
set_root_path(RootPath) ->
    put(root_zk_path, RootPath).
root_path() ->
    get(root_zk_path).

load_or_init_persistent_data() ->
    {ok, RootPath, _Data} = mesos_metadata_manager:get_root_node(),

    set_root_path(RootPath),

    ClusterPath = load_or_init_persistent_data(
                    RootPath, ?ZK_CLUSTER_NODE, fun load_persistent_cluster_data/1),
    NodePath = load_or_init_persistent_data(
                 RootPath, ?ZK_NODE_NODE, fun load_persistent_node_data/1),
    {ClusterPath, NodePath}.

load_or_init_persistent_data(RootPath, ZooKeeperNode, LoadFunc) ->
    %% XXX Do the metadata functions support iolists or do we need to flatten this? I forget
    Path = [RootPath, "/", ZooKeeperNode],
    case mesos_metadata_manager:get_children(Path) of
        {error, no_node} ->
            {ok, NewPath, <<>>} = mesos_metadata_manager:make_child(RootPath, ZooKeeperNode),
            NewPath;
        {ok, Children} ->
            laod_persistent_data(Path, Children, LoadFunc),
            Path
    end.

laod_persistent_data(Path, Children, LoadFunc) ->
    %% XXX Do the metadata functions support iolists or do we need to flatten these? I forget
    _ = [LoadFunc([Path, "/", Child]) || Child <- Children],
    ok.

load_persistent_cluster_data(ZKPath) ->
    {ok, _Path, ClusterRecordBin} = mesos_metadata_manager:get_node(ZKPath),
    ClusterRecord = erlang:binary_to_term(ClusterRecordBin),
    %% TODO check against data corruption? Verify no duplicate keys? etc.
    ets:insert(?CLUST_TAB, ClusterRecord).

load_persistent_node_data(ZKPath) ->
    {ok, _Path, NodeRecordBin} = mesos_metadata_manager:get_node(ZKPath),
    NodeRecord = erlang:binary_to_term(NodeRecordBin),
    %% TODO check against data corruption? Verify no duplicate keys? etc.
    ets:insert(?NODE_TAB, NodeRecord).

persist_record(Rec) when is_record(Rec, cluster) ->
    persist_record(Rec, ?ZK_CLUSTER_NODE, Rec#cluster.key);
persist_record(Rec) when is_record(Rec, node) ->
    persist_record(Rec, ?ZK_NODE_NODE, Rec#node.key).

persist_record(Rec, Node, Key) ->
    Path = [root_path(), "/", Node],
    Data = term_to_binary(Rec),
    {ok, _, _} = mesos_metadata_manager:create_or_set(Path, Key, Data).

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
            persist_record(NewCluster),
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
            persist_record(NewCluster),
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
                            ets:insert(?CLUST_TAB, NewCluster),
                            persist_record(NewCluster),
                            ok
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
            persist_record(NewNode),
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
            persist_record(NewNode),
            ok
    end.

do_delete_node(Key) ->
    do_delete(Key, ?NODE_TAB).

do_delete(Key, Table) ->
    case ets:lookup(Table, Key) of
        [] ->
            {error, {not_found, Key}};
        [_] ->
            ets:delete(Table, Key),
            ok
    end.
