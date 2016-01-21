-module(mesos_scheduler_data).

-include("mesos_scheduler_data.hrl").

-behavior(gen_server).

-export([
         start_link/0,
         stop/0,
         add_cluster/1,
         get_cluster/1,
         update_cluster/2,
         set_cluster_status/2,
         delete_cluster/1,
         add_node/1,
         get_node/1,
         set_node_status/2,
         delete_node/1,
         get_all_clusters/0,
         get_all_nodes/0
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Debug/testing use only?
-export([reset_all_data/0]).

-type key() :: string(). %% Keys used to identify nodes/clusters
-type cluster_key() :: key().
-type node_key() :: key().

%% We might not need all of these, it's just a best guess as to what states we might expect to see:
-type node_status() :: requested | starting | active | down | stopping | stopped.
-type cluster_status() :: requested | active | stopping | stopped.

-export_type([
              cluster_key/0,
              node_key/0,
              node_status/0,
              cluster_status/0
             ]).

-record(state, {
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

-spec add_cluster(#rms_cluster{}) -> ok | {error, term()}.
add_cluster(ClusterRec) ->
    gen_server:call(?MODULE, {add_cluster, ClusterRec}).

-spec get_cluster(key()) -> {ok, #rms_cluster{}} | {error, {not_found, key()}}.
get_cluster(Key) ->
    gen_server:call(?MODULE, {get_cluster, Key}).

-spec update_cluster(key(), fun((#rms_cluster{}) -> #rms_cluster{})) ->
    {ok, #rms_cluster{}} | {error, term()}.
update_cluster(Key, UpdateFun) ->
    gen_server:call(?MODULE, {update_cluster, Key, UpdateFun}).

-spec set_cluster_status(key(), cluster_status()) -> ok | {error, term()}.
set_cluster_status(Key, Status) ->
    UpdateFun = fun(Cluster) -> Cluster#rms_cluster{status = Status} end,
    case update_cluster(Key, UpdateFun) of
        {ok, _NewCluster} ->
            ok;
        Error ->
            Error
    end.

-spec delete_cluster(key()) -> ok | {error, term()}.
delete_cluster(Key) ->
    gen_server:call(?MODULE, {delete_cluster, Key}).

-spec add_node(#rms_node{}) -> ok | {error, term()}.
add_node(NodeRec) when NodeRec#rms_node.cluster =/= undefined ->
    gen_server:call(?MODULE, {add_node, NodeRec}).

-spec get_node(key()) -> {ok, #rms_node{}} | {error, term()}.
get_node(Key) ->
    gen_server:call(?MODULE, {get_node, Key}).

-spec set_node_status(key(), node_status()) -> ok | {error, term()}.
set_node_status(Key, Status) ->
    gen_server:call(?MODULE, {set_node_status, Key, Status}).

-spec delete_node(key()) -> ok | {error, term()}.
delete_node(Key) ->
    gen_server:call(?MODULE, {delete_node, Key}).

-spec get_all_clusters() -> [#rms_cluster{}].
get_all_clusters() ->
    gen_server:call(?MODULE, get_all_clusters).

-spec get_all_nodes() -> [#rms_node{}].
get_all_nodes() ->
    gen_server:call(?MODULE, get_all_nodes).

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
handle_call({add_cluster, ClusterRec}, _From, State) ->
    Result = do_add_cluster(ClusterRec),
    {reply, Result, State};
handle_call({get_cluster, Key}, _From, State) ->
    Result = do_get_cluster(Key),
    {reply, Result, State};
handle_call({update_cluster, Key, UpdateFun}, _From, State) ->
    Result = do_update_cluster(Key, UpdateFun),
    {reply, Result, State};
handle_call({delete_cluster, Key}, _From, State) ->
    Result = do_delete_cluster(Key),
    {reply, Result, State};
handle_call({add_node, NodeRec}, _From, State) ->
    Result = do_add_node(NodeRec),
    {reply, Result, State};
handle_call({get_node, Key}, _From, State) ->
    Result = do_get_node(Key),
    {reply, Result, State};
handle_call({set_node_status, Key, Status}, _From, State) ->
    Result = do_set_node_status(Key, Status),
    {reply, Result, State};
handle_call({delete_node, Key}, _From, State) ->
    Result = do_delete_node(Key),
    {reply, Result, State};
handle_call(get_all_clusters, _From, State) ->
    Result = do_get_all_clusters(),
    {reply, Result, State};
handle_call(get_all_nodes, _From, State) ->
    Result = do_get_all_nodes(),
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
    ets:new(?CLUST_TAB, [set, private, named_table, {keypos, #rms_cluster.key}]),
    ets:new(?NODE_TAB, [set, private, named_table, {keypos, #rms_node.key}]).

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

persist_record(Rec) when is_record(Rec, rms_cluster) ->
    persist_record(Rec, ?ZK_CLUSTER_NODE, Rec#rms_cluster.key);
persist_record(Rec) when is_record(Rec, rms_node) ->
    persist_record(Rec, ?ZK_NODE_NODE, Rec#rms_node.key).

persist_record(Rec, Node, Key) ->
    Path = [root_path(), "/", Node],
    Data = term_to_binary(Rec),
    case mesos_metadata_manager:create_or_set(Path, Key, Data) of
        {ok, _, _} ->
            ok;
        {error, closed} ->
            {error, closed}
    end.

delete_persistent_record(#rms_cluster{key = Key}) ->
    delete_persistent_record(?ZK_CLUSTER_NODE, Key);
delete_persistent_record(#rms_node{key = Key}) ->
    delete_persistent_record(?ZK_NODE_NODE, Key).

delete_persistent_record(Node, Key) ->
    Path = [root_path(), "/", Node, "/", Key],
    %% This case statement looks silly but we just want to make sure we're getting one
    %% of the return values that we're expecting. Anything other than ok or {error, closed}
    %% indicates a problem, so we want to crash immediately rather than pass the buck.
    case mesos_metadata_manager:delete_node(Path) of
        ok ->
            ok;
        {error, closed} ->
            {error, closed}
    end.

do_add_cluster(ClusterRec) ->
    Key = ClusterRec#rms_cluster.key,

    case ets:lookup(?CLUST_TAB, Key) of
        [] ->
            case persist_record(ClusterRec) of
                ok ->
                    ets:insert(?CLUST_TAB, ClusterRec),
                    ok;
                {error, Error} ->
                    {error, Error}
            end;
        [_ExistingCluster] ->
            {error, {cluster_exists, ClusterRec#rms_cluster.key}}
    end.

do_get_cluster(Key) ->
    do_get_record(?CLUST_TAB, Key).

do_get_record(Tab, Key) ->
    case ets:lookup(Tab, Key) of
        [] ->
            {error, {not_found, Key}};
        [Result] ->
            {ok, Result}
    end.

do_update_cluster(Key, UpdateFun) ->
    case ets:lookup(?CLUST_TAB, Key) of
        [] ->
            {error, {not_found, Key}};
        [Cluster] ->
            #rms_cluster{} = NewCluster = UpdateFun(Cluster),
            case persist_record(NewCluster) of
                ok ->
                    ets:insert(?CLUST_TAB, NewCluster),
                    {ok, NewCluster};
                {error, Error} ->
                    {error, Error}
            end
    end.

do_delete_cluster(Key) ->
    case ets:lookup(?CLUST_TAB, Key) of
        [] ->
            {error, {not_found, Key}};
        [Cluster] ->
            case delete_persistent_record(Cluster) of
                ok ->
                    %% Should we also delete any associated nodes here?
                    %%[do_delete_node(NodeKey) || NodeKey <- Cluster#rms_cluster.nodes],
                    ets:delete(?CLUST_TAB, Key),
                    ok;
                {error, Error} ->
                    {error, Error}
            end
    end.

do_add_node(NodeRec) ->
    #rms_node{
       key = NodeKey,
       cluster = ClusterKey
      } = NodeRec,
    case ets:lookup(?CLUST_TAB, ClusterKey) of
        [] ->
            {error, {no_such_cluster, ClusterKey}};
        [Cluster] ->
            case ets:lookup(?NODE_TAB, NodeKey) of
                [] ->
                    NewMembership = add_member(NodeKey, Cluster#rms_cluster.nodes),
                    NewCluster = Cluster#rms_cluster{nodes = NewMembership},
                    %% N.B. It's possible we might succeed in persisting the new cluster
                    %% record but fail to persist the node record. This should work out
                    %% okay in the end though, since we've written the add_member function
                    %% to be idempotent. Once the operation is retried, everything will
                    %% turn out the way its supposed to be.
                    case persist_record(NewCluster) of
                        ok ->
                            case persist_record(NodeRec) of
                                ok ->
                                    ets:insert(?CLUST_TAB, NewCluster),
                                    ets:insert(?NODE_TAB, NodeRec),
                                    ok;
                                {error, Error} ->
                                    {error, Error}
                            end;
                        {error, Error} ->
                            {error, Error}
                    end;
                [_ExistingNode] ->
                    {error, {node_exists, NodeKey}}
            end
    end.

add_member(NodeKey, Nodes) ->
    case lists:member(NodeKey, Nodes) of
        true -> Nodes;
        false -> [NodeKey | Nodes]
    end.

do_get_node(Key) ->
    do_get_record(?NODE_TAB, Key).

do_set_node_status(Key, Status) ->
    case ets:lookup(?NODE_TAB, Key) of
        [] ->
            {error, {not_found, Key}};
        [Node] ->
            NewNode = Node#rms_node{status = Status},
            case persist_record(NewNode) of
                ok ->
                    ets:insert(?NODE_TAB, NewNode),
                    ok;
                {error, Error} ->
                    {error, Error}
            end
    end.

do_delete_node(Key) ->
    case ets:lookup(?NODE_TAB, Key) of
        [] ->
            {error, {not_found, Key}};
        [Node] ->
            #rms_node{cluster = ClusterKey} = Node,
            case delete_persistent_record(Node) of
                ok ->
                    ets:delete(?NODE_TAB, Key),
                    case ets:lookup(?CLUST_TAB, ClusterKey) of
                        [] ->
                            ok; %% Shouldn't ever hit this case, but if we do just ignore
                        [Cluster] ->
                            NewMembership = lists:delete(Key, Cluster#rms_cluster.nodes),
                            NewCluster = Cluster#rms_cluster{nodes = NewMembership},
                            case persist_record(NewCluster) of
                                ok ->
                                    ets:insert(?CLUST_TAB, NewCluster),
                                    ok;
                                {error, Error} ->
                                    {error, Error}
                            end
                    end;
                {error, Error} ->
                    {error, Error}
            end
    end.

do_get_all_clusters() ->
    ets:tab2list(?CLUST_TAB).

do_get_all_nodes() ->
    ets:tab2list(?NODE_TAB).
