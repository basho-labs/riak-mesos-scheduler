-module(riak_mesos_offer_helper).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([new/1]).

-record(offer_helper, {offer :: erl_mesos:'Offer'(),
                       offer_id_value :: string(),
                       persistence_ids = [] :: [string()],
                       reserved_resources :: erl_mesos_utils:resources(),
                       unreserved_resources :: erl_mesos_utils:resources(),
                       resources_to_reserve = [] :: [erl_mesos:'Resource'()],
                       resources_to_uneserve = [] :: [erl_mesos:'Resource'()],
                       volumes_to_create = [] :: [erl_mesos:'Resource'()],
                       volumes_to_destroy = [] :: [erl_mesos:'Resource'()],
                       tasks_to_launch = [] :: [erl_mesos:'TaskInfo'()]}).

%% ====================================================================
%% API
%% ====================================================================

new(#'Offer'{id = #'OfferID'{value = OfferIdValue}} = Offer) ->
    #offer_helper{offer = Offer,
                  offer_id_value = OfferIdValue}.

%% ====================================================================
%% Private
%% ====================================================================


