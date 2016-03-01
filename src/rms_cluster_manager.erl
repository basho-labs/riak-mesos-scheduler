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

%-behaviour(gen_server).

-export([start_link/0]).

-export([apply_offer/2]).

-export([init/1]).

-record(state, {}).

%-type state() :: #state{}.

%% External functions.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

-spec apply_offer(rms_offer_helper:offer_helper(),
                  rms_node_manager:node_data()) ->
    rms_offer_helper:offer_helper().
apply_offer(OfferHelper, NodeData) ->
    NodeKeys = rms_node_manager:node_keys(),
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

%% gen_server callback functions.

init({}) ->
    {ok, #state{}}.

%% Internal functions.

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

schedule_node(NodeKey, NodeKeys, NeedsReconciliation, OfferHelper, NodeData) ->
    case rms_node_manager:node_has_reservation(NodeKey) of
        true ->
            %% TODO: launch Riak node.
            apply_offer(NodeKeys, false, OfferHelper, NodeData);
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
        {error, _Reason} ->
            apply_offer(NodeKeys, NeedsReconciliation, OfferHelper, NodeData)
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
