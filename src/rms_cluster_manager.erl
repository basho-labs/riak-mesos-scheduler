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

-module(rms_cluster_manager).

-behaviour(supervisor).

-export([start_link/0]).

-export([get_cluster_keys/0,
         get_cluster/1,
         get_cluster_riak_config/1,
         get_cluster_advanced_config/1,
         add_cluster/1,
         set_cluster_riak_config/2,
         set_cluster_advanced_config/2,
         delete_cluster/1]).

-export([add_node/1]).

-export([apply_offer/2]).

-export([init/1]).

-define(CONFIG_ROOT, "priv/").
-define(RIAK_CONFIG, ?CONFIG_ROOT ++ "riak.conf.default").
-define(ADVANCED_CONFIG, ?CONFIG_ROOT ++ "advanced.config.default").

%% External functions.

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

-spec get_cluster_keys() -> [rms_cluster:key()].
get_cluster_keys() ->
    [Key || {Key, _} <- rms_metadata:get_clusters()].

-spec get_cluster(rms_cluster:key()) ->
    {ok, rms_metadata:cluster_state()} | {error, term()}.
get_cluster(Key) ->
    rms_cluster:get(Key).

-spec get_cluster_riak_config(rms_cluster:key()) ->
    {ok, string()} | {error, term()}.
get_cluster_riak_config(Key) ->
    rms_cluster:get_field_value(riak_config, Key).

-spec get_cluster_advanced_config(rms_cluster:key()) ->
    {ok, string()} | {error, term()}.
get_cluster_advanced_config(Key) ->
    rms_cluster:get_field_value(advanced_config, Key).

-spec add_cluster(rms_cluster:key()) -> ok | {error, term()}.
add_cluster(Key) ->
    case get_cluster(Key) of
        {ok, _Cluster} ->
            {error, exists};
        {error, not_found} ->
            ClusterSpec = cluster_spec(Key),
            case supervisor:start_child(?MODULE, ClusterSpec) of
                {ok, _Pid} ->
                    {ok, RiakConfig} = file:read_file(?RIAK_CONFIG),
                    {ok, AdvancedConfig} = file:read_file(?ADVANCED_CONFIG),
                    ok = set_cluster_riak_config(Key, RiakConfig),
                    ok = set_cluster_advanced_config(Key, AdvancedConfig),
                    ok;
                {error, {already_started, _Pid}} ->
                    {error, exists};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec set_cluster_riak_config(rms_cluster:key(), string()) ->
    ok | {error, term()}.
set_cluster_riak_config(Key, RiakConfig) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:set_riak_config(Pid, RiakConfig);
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_cluster_advanced_config(rms_cluster:key(), string()) ->
    ok | {error, term()}.
set_cluster_advanced_config(Key, AdvancedConfig) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:set_advanced_config(Pid, AdvancedConfig);
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_cluster(rms_cluster:key()) -> ok | {error, term()}.
delete_cluster(Key) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:delete(Pid);
        {error, Reason} ->
            {error, Reason}
    end.

-spec add_node(rms_cluster:key()) -> ok | {error, term()}.
add_node(Key) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:add_node(Pid);
        {error, Reason} ->
            {error, Reason}
    end.

-spec apply_offer(rms_offer_helper:offer_helper(),
                  rms_node_manager:node_data()) ->
    rms_offer_helper:offer_helper().
apply_offer(OfferHelper, NodeData) ->
    NodeKeys = rms_node_manager:get_node_keys(),
    case apply_offer(NodeKeys, false, OfferHelper, NodeData) of
        {true, OfferHelper1} ->
            OfferHelper1;
        {false, OfferHelper1} ->
            case rms_offer_helper:has_tasks_to_launch(OfferHelper1) of
                true ->
                    OfferHelper1;
                false ->
                    unreserve_volumes(unreserve_resources(OfferHelper1))
            end
    end.

%% supervisor callback function.

-spec init({}) ->
    {ok, {{supervisor:strategy(), 1, 1}, [supervisor:child_spec()]}}.
init({}) ->
    Specs = [cluster_spec(Key) ||
             {Key, _Cluster} <- rms_metadata:get_clusters()],
    {ok, {{one_for_one, 1, 1}, Specs}}.

%% Internal functions.

-spec cluster_spec(rms_cluster:key()) -> supervisor:child_spec().
cluster_spec(Key) ->
    {Key,
        {rms_cluster, start_link, [Key]},
        transient, 5000, worker, [rms_cluster]}.

-spec get_cluster_pid(rms_cluster:key()) -> {ok, pid()} | {error, not_found}.
get_cluster_pid(Key) ->
    case lists:keyfind(Key, 1, supervisor:which_children(?MODULE)) of
        {_Key, Pid, _, _} ->
            {ok, Pid};
        false ->
            {error, not_found}
    end.

-spec apply_offer([rms_node:key()], boolean(), rms_offer_helper:offer_helper(),
                  rms_node_manager:node_data()) ->
    {boolean(), rms_offer_helper:offer_helper()}.
apply_offer([NodeKey | NodeKeys], NeedsReconciliation, OfferHelper, NodeData) ->
    case rms_node_manager:node_needs_to_be_reconciled(NodeKey) of
        true ->
            apply_offer(NodeKeys, true, OfferHelper, NodeData);
        false ->
            case rms_node_manager:node_can_be_scheduled(NodeKey) of
                true ->
                    schedule_node(NodeKey, NodeKeys, NeedsReconciliation,
                                  OfferHelper, NodeData);
                false ->
                    apply_offer(NodeKeys, NeedsReconciliation, OfferHelper,
                                NodeData)
            end
    end;
apply_offer([], NeedsReconciliation, OfferHelper, _FrameworkInfo) ->
    {NeedsReconciliation, OfferHelper}.

-spec schedule_node(rms_node:key(), [rms_node:key()], boolean(),
                    rms_offer_helper:offer_helper(),
                    rms_node_manager:node_data()) ->
    {boolean(), rms_offer_helper:offer_helper()}.
schedule_node(NodeKey, NodeKeys, NeedsReconciliation, OfferHelper, NodeData) ->
    case rms_node_manager:node_has_reservation(NodeKey) of
        true ->
            %% Apply reserved resources.
            apply_reserved_offer(NodeKey, NodeKeys, NeedsReconciliation,
                                 OfferHelper, NodeData);
        false ->
            %% New node.
            apply_unreserved_offer(NodeKey, NodeKeys, NeedsReconciliation,
                                   OfferHelper, NodeData)
    end.

-spec apply_unreserved_offer(rms_node:key(), [rms_node:key()], boolean(),
                             rms_offer_helper:offer_helper(),
                             rms_node_manager:node_data()) ->
    {boolean(), rms_offer_helper:offer_helper()}.
apply_unreserved_offer(NodeKey, NodeKeys, NeedsReconciliation, OfferHelper,
                       NodeData) ->
    case rms_node_manager:apply_unreserved_offer(NodeKey, OfferHelper,
                                                 NodeData) of
        {ok, OfferHelper1} ->
            lager:info("Found new offer for node. "
                       "Node key: ~s. "
                       "Offer id: ~s. "
                       "Offer resources: ~p.",
                       [NodeKey,
                        rms_offer_helper:get_offer_id_value(OfferHelper),
                        rms_offer_helper:resources_to_list(OfferHelper)]),
            apply_offer(NodeKeys, NeedsReconciliation, OfferHelper1, NodeData);
        {error, Reason} ->
            lager:warning("Appling of unreserved resources error. "
                          "Node key: ~s. "
                          "Offer id: ~s. "
                          "Error reason: ~p.",
                          [NodeKey,
                           rms_offer_helper:get_offer_id_value(OfferHelper),
                           Reason]),
            apply_offer(NodeKeys, NeedsReconciliation, OfferHelper, NodeData)
    end.

-spec apply_reserved_offer(rms_node:key(), [rms_node:key()], boolean(),
                           rms_offer_helper:offer_helper(),
                           rms_node_manager:node_data()) ->
    {boolean(), rms_offer_helper:offer_helper()}.
apply_reserved_offer(NodeKey, NodeKeys, NeedsReconciliation, OfferHelper,
                     NodeData) ->
    {ok, PersistenceId} = rms_node_manager:get_node_persistence_id(NodeKey),
    OfferIdValue = rms_offer_helper:get_offer_id_value(OfferHelper),
    case rms_offer_helper:has_persistence_id(PersistenceId, OfferHelper) of
        true ->
            %% Found reserved resources for node.
            %% Persistence id matches.
            %% Try to launch the node.
            case rms_node_manager:apply_reserved_offer(NodeKey, OfferHelper,
                                                       NodeData) of
                {ok, OfferHelper1} ->
                    ResourcesList =
                        rms_offer_helper:resources_to_list(OfferHelper),
                    lager:info("New node added for scheduling. "
                               "Node has persistence id. "
                               "Node key: ~s. "
                               "Persistence id: ~s. "
                               "Offer id: ~s. "
                               "Offer resources: ~p.",
                               [NodeKey, PersistenceId, OfferIdValue,
                                ResourcesList]),
                    apply_offer(NodeKeys, NeedsReconciliation, OfferHelper1,
                                NodeData);
                {error, Reason} ->
                    lager:warning("Adding node for scheduling error. "
                                  "Node has persistence id. "
                                  "Node key: ~s. "
                                  "Persistence id: ~s. "
                                  "Offer id: ~s. "
                                  "Error reason: ~p.",
                                  [NodeKey, PersistenceId, OfferIdValue,
                                   Reason]),
                    apply_offer(NodeKeys, true, OfferHelper, NodeData)
            end;
        false ->
            %% TODO: Why is any of the stuff below here happening if the persistence id isn't there?
            {ok, Hostname} = rms_node_manager:get_node_hostname(NodeKey),
            {ok, AgentIdValue} =
                rms_node_manager:get_node_agent_id_value(NodeKey),
            OfferHostname = rms_offer_helper:get_hostname(OfferHelper),
            OfferAgentIdValue =
                rms_offer_helper:get_agent_id_value(OfferHelper),
            case Hostname =:= OfferHostname andalso
                 AgentIdValue =:= OfferAgentIdValue of
                true ->
                    %% Found reserved resources for node.
                    %% Agent id and hostname matches.
                    %% Try to launch the node.
                    %% If apply fails unreserve the resources for the node.
                    case rms_node_manager:apply_reserved_offer(NodeKey,
                                                               OfferHelper,
                                                               NodeData) of
                        {ok, OfferHelper1} ->
                            ResourcesList =
                                rms_offer_helper:resources_to_list(OfferHelper),
                            lager:info("New node added for scheduling. "
                                       "Agent id and hostname matches. "
                                       "Node key: ~s. "
                                       "Agent id: ~s. "
                                       "Hostname: ~s. "
                                       "Offer id: ~s. "
                                       "Offer resources: ~p.",
                                       [NodeKey, AgentIdValue, Hostname,
                                        PersistenceId, OfferIdValue,
                                        ResourcesList]),
                            apply_offer(NodeKeys, NeedsReconciliation,
                                        OfferHelper1, NodeData);
                        {error, Reason} ->
                            %% TODO: unreserve node here,
                            lager:warning("Adding node for scheduling error. "
                                          "Agent id and hostname matches. "
                                          "Node key: ~s. "
                                          "Agent id: ~s. "
                                          "Hostname: ~s. "
                                          "Persistence id: ~s"
                                          "Offer id: ~s. "
                                          "Error reason: ~p.",
                                          [NodeKey, AgentIdValue, Hostname,
                                           PersistenceId, OfferIdValue,
                                           Reason]),
                            apply_offer(NodeKeys, NeedsReconciliation,
                                        OfferHelper, NodeData)
                    end;
                false ->
                    lager:info("Hostname or AgentId didn't match. "
                               "Node key: ~s. "
                               "Agent id: ~s. "
                               "Offer Agent id: ~s. "
                               "Hostname: ~s. "
                               "Offer id: ~s. ",
                               [NodeKey, AgentIdValue, OfferAgentIdValue, 
                                Hostname, PersistenceId, OfferIdValue]),
                    apply_offer(NodeKeys, NeedsReconciliation, OfferHelper,
                                NodeData)
            end
    end.

-spec unreserve_resources(rms_offer_helper:offer_helper()) ->
    rms_offer_helper:offer_helper().
unreserve_resources(OfferHelper) ->
    case rms_offer_helper:has_reservations(OfferHelper) of
        true ->
            lager:info("Offer has reserved resources, but no nodes can use it. "
                       "Unreserve resources. "
                       "Offer id: ~s. "
                       "Resources: ~p.",
                       [rms_offer_helper:get_offer_id_value(OfferHelper),
                        rms_offer_helper:resources_to_list(OfferHelper)]),
            rms_offer_helper:unreserve_resources(OfferHelper);
        false ->
            OfferHelper
    end.

-spec unreserve_volumes(rms_offer_helper:offer_helper()) ->
    rms_offer_helper:offer_helper().
unreserve_volumes(OfferHelper) ->
    case rms_offer_helper:has_volumes(OfferHelper) of
        true ->
            lager:info("Offer has persisted volumes, but no nodes can use it. "
                       "Destroy volumes. "
                       "Offer id: ~s. "
                       "Resources: ~p.",
                       [rms_offer_helper:get_offer_id_value(OfferHelper),
                        rms_offer_helper:resources_to_list(OfferHelper)]),
            rms_offer_helper:unreserve_volumes(OfferHelper);
        false ->
            OfferHelper
    end.
