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
         restart_cluster/1,
         node_started/2,
         destroy_cluster/1]).

-export([add_node/1]).

-export([apply_offer/1]).

-export([executors_to_shutdown/0]).

-export([maybe_join/2, leave/2]).

-export([handle_status_update/4]).

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
    %% FIXME This is why we should use the process states not what's in ZK.
    %% This will need to query each cluster's FSM for status != 'shutdown'
    [Key || {Key, _} <- rms_metadata:get_clusters()].

-spec get_cluster(rms_cluster:key()) ->
    {ok, rms_metadata:cluster_state()} | {error, term()}.
get_cluster(Key) ->
    rms_cluster:get(Key).

-spec get_cluster_riak_config(rms_cluster:key()) ->
    {ok, binary()} | {error, term()}.
get_cluster_riak_config(Key) ->
    rms_cluster:get_field_value(riak_config, Key).

-spec get_cluster_advanced_config(rms_cluster:key()) ->
    {ok, binary()} | {error, term()}.
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
                    ok = set_cluster_config(Key);
                {error, already_present} ->
                    %% This happens when this cluster was already created then destroyed.
                    %% No state is restored, so we can just restart it and use it.
                    {ok, _Pid} = supervisor:restart_child(?MODULE, Key),
                    ok = set_cluster_config(Key);
                {error, {already_started, _Pid}} ->
                    {error, exists};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec set_cluster_config(rms_cluster:key()) ->
    ok | {error, term()}.
set_cluster_config(Key) ->
    {ok, RiakConfig} = file:read_file(?RIAK_CONFIG),
    {ok, AdvancedConfig} = file:read_file(?ADVANCED_CONFIG),
    ok = set_cluster_riak_config(Key, RiakConfig),
    ok = set_cluster_advanced_config(Key, AdvancedConfig).

-spec set_cluster_riak_config(rms_cluster:key(), binary()) ->
    ok | {error, term()}.
set_cluster_riak_config(Key, RiakConfig) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:set_riak_config(Pid, RiakConfig);
        {error, Reason} ->
            {error, Reason}
    end.

-spec set_cluster_advanced_config(rms_cluster:key(), binary()) ->
    ok | {error, term()}.
set_cluster_advanced_config(Key, AdvancedConfig) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:set_advanced_config(Pid, AdvancedConfig);
        {error, Reason} ->
            {error, Reason}
    end.

%% TODO Rename this to be clearer that returning 'ok' just means we've
%% *commenced* restarting, not *completed*
-spec restart_cluster(rms_cluster:key()) -> ok | {error, term()}.
restart_cluster(Key) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:commence_restart(Pid);
        {error, Reason} ->
            {error, Reason}
    end.

-spec node_started(rms_cluster:key(), rms_node:key()) -> ok.
node_started(Key, NodeKey) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:node_started(Pid, NodeKey);
        {error, Reason} ->
            {error, Reason}
    end.

-spec destroy_cluster(rms_cluster:key()) -> ok | {error, term()}.
destroy_cluster(Key) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:destroy(Pid);
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

-spec executors_to_shutdown() -> [{string(), string()}].
executors_to_shutdown() ->
    NodeKeys = rms_node_manager:get_node_keys(),
    executors_to_shutdown(NodeKeys, []).

-spec executors_to_shutdown([{string(), string()}], [{string(), string()}]) ->
    [{string(), string()}].
executors_to_shutdown([], Accum) ->
    Accum;
executors_to_shutdown([NodeKey|Rest], Accum) ->
  case rms_node_manager:node_can_be_shutdown(NodeKey) of
      {ok, true} ->
          {ok, AgentIdValue} = rms_node_manager:get_node_agent_id_value(NodeKey),
          executors_to_shutdown(Rest, [{NodeKey, AgentIdValue}|Accum]);
      {ok, false} ->
          executors_to_shutdown(Rest, Accum)
  end.

-spec maybe_join(rms_cluster:key(), rms_node:key()) -> ok | {error, term()}.
maybe_join(Key, NodeKey) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:maybe_join(Pid, NodeKey);
        {error, Reason} ->
            {error, Reason}
    end.

-spec leave(rms_cluster:key(), rms_node:key()) -> ok | {error, term()}.
leave(Key, NodeKey) ->
    case get_cluster_pid(Key) of
        {ok, Pid} ->
            rms_cluster:leave(Pid, NodeKey);
        {error, Reason} ->
            {error, Reason}
    end.

-spec handle_status_update(rms_cluster:key(), rms_node:key(), atom(), atom()) ->
    ok | {error, term()}.
handle_status_update(_Key, NodeKey, TaskStatus, Reason) ->
    rms_node_manager:handle_status_update(NodeKey, TaskStatus, Reason).

-spec apply_offer(rms_offer_helper:offer_helper()) ->
    rms_offer_helper:offer_helper().
apply_offer(OfferHelper) ->
    {ok, NodeHosts} = rms_node_manager:get_node_hosts(),
    {ok, NodeAttributes} = rms_node_manager:get_node_attributes(),
    OfferHelper1 = rms_offer_helper:set_node_hostnames(NodeHosts, OfferHelper),
    OfferHelper2 = rms_offer_helper:set_node_attributes(NodeAttributes, OfferHelper1),
    case rms_node_manager:get_unreconciled_node_keys() of
        N when length(N) > 0 ->
            OfferHelper2;
        _ ->
            NodeKeys = rms_node_manager:get_node_keys(),
            OfferHelper3 = apply_offer(NodeKeys, OfferHelper2),
            case rms_offer_helper:has_tasks_to_launch(OfferHelper3) of
                true ->
                    OfferHelper3;
                false ->
                    unreserve_volumes(unreserve_resources(OfferHelper3))
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

-spec apply_offer([rms_node:key()],
                  rms_offer_helper:offer_helper()) ->
                         rms_offer_helper:offer_helper().
apply_offer([NodeKey | NodeKeys], OfferHelper) ->
    %% One launch at a time for now
    case rms_offer_helper:has_tasks_to_launch(OfferHelper) of
        true ->
            OfferHelper;
        false ->
            case rms_node_manager:node_can_be_scheduled(NodeKey) of
                % TODO How do we clean up the node keys here?
                {error, shutdown} ->
                    apply_offer(NodeKeys, OfferHelper);
                {ok, true} ->
                    schedule_node(NodeKey, NodeKeys,
                                  OfferHelper);
                {ok, false} ->
                    apply_offer(NodeKeys, OfferHelper)
            end
    end;
apply_offer([], OfferHelper) ->
    OfferHelper.

-spec schedule_node(rms_node:key(), [rms_node:key()],
                    rms_offer_helper:offer_helper()) ->
    rms_offer_helper:offer_helper().
schedule_node(NodeKey, NodeKeys, OfferHelper) ->
    case rms_node_manager:node_has_reservation(NodeKey) of
        true ->
            %% Apply reserved resources.
            apply_reserved_offer(NodeKey, NodeKeys,
                                 OfferHelper);
        false ->
            %% New Node
            case rms_offer_helper:can_fit_constraints(OfferHelper) of
                true ->
                    apply_unreserved_offer(NodeKey, NodeKeys,
                                           OfferHelper);
                false ->
                    apply_offer(NodeKeys, OfferHelper)
            end
    end.

-spec apply_unreserved_offer(rms_node:key(), [rms_node:key()],
                             rms_offer_helper:offer_helper()) ->
    rms_offer_helper:offer_helper().
apply_unreserved_offer(NodeKey, NodeKeys, OfferHelper) ->
    case rms_node_manager:apply_unreserved_offer(NodeKey, OfferHelper) of
        {ok, OfferHelper1} ->
            lager:info("Found new offer for node. "
                       "Node key: ~s. "
                       "Offer id: ~s. "
                       "Offer resources: ~p.",
                       [NodeKey,
                        rms_offer_helper:get_offer_id_value(OfferHelper1),
                        rms_offer_helper:resources_to_list(OfferHelper1)]),
            OfferHelper1;
        {error, Reason} ->
            lager:warning("Applying of unreserved resources error. "
                          "Node key: ~s. "
                          "Offer id: ~s. "
                          "Error reason: ~p.",
                          [NodeKey,
                           rms_offer_helper:get_offer_id_value(OfferHelper),
                           Reason]),
            apply_offer(NodeKeys, OfferHelper)
    end.

-spec apply_reserved_offer(rms_node:key(), [rms_node:key()],
                           rms_offer_helper:offer_helper()) ->
    rms_offer_helper:offer_helper().
apply_reserved_offer(NodeKey, NodeKeys, OfferHelper) ->
    {ok, PersistenceId} = rms_node_manager:get_node_persistence_id(NodeKey),
    OfferIdValue = rms_offer_helper:get_offer_id_value(OfferHelper),
    case rms_offer_helper:has_persistence_id(PersistenceId, OfferHelper) of
        true ->
            %% Found reserved resources for node.
            %% Persistence id matches.
            %% Try to launch the node.
            case rms_node_manager:apply_reserved_offer(NodeKey, OfferHelper) of
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
                    OfferHelper1;
                {error, Reason} ->
                    lager:warning("Adding node for scheduling error. "
                                  "Node has persistence id. "
                                  "Node key: ~s. "
                                  "Persistence id: ~s. "
                                  "Offer id: ~s. "
                                  "Error reason: ~p.",
                                  [NodeKey, PersistenceId, OfferIdValue,
                                   Reason]),
                    apply_offer(NodeKeys, OfferHelper)
            end;
        false ->
            apply_offer(NodeKeys, OfferHelper)
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

-spec cluster_spec(rms_cluster:key()) -> supervisor:child_spec().
cluster_spec(Key) ->
    {Key,
        {rms_cluster, start_link, [Key]},
        transient, 5000, worker, [rms_cluster]}.

-spec get_cluster_pid(rms_cluster:key()) -> {ok, pid()} | {error, not_found}.
get_cluster_pid(Key) ->
    case lists:keyfind(Key, 1, supervisor:which_children(?MODULE)) of
        {_Key, undefined, _, _} ->
            %% TODO Maybe we should supervisor:delete_child/2 here?
            {error, shutdown};
        {_Key, Pid, _, _} ->
            {ok, Pid};
        false ->
            {error, not_found}
    end.
