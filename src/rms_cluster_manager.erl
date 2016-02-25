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

-export([apply_offer/2]).

%% External functions.

-spec apply_offer(rms_offer_helper:offer_helper(),
                  rms_node_manager:node_data()) ->
    rms_offer_helper:offer_helper().
apply_offer(OfferHelper, NodeData) ->
    NodeKeys = rms_node_manager:node_keys(),
    case apply_offer(NodeKeys, false, OfferHelper, NodeData) of
        {true, OfferHelper1} ->
            OfferHelper1;
        {false, OfferHelper1} ->
            %% TODO: may be unreserve resources here.
            OfferHelper1
    end.

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
            ok;
        false ->
            %% New node. We need to reserve the resources.
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
