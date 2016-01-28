-module(riak_mesos_offer_helper).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([new/1,
         get_offer_id_value/1,
         get_persistence_ids/1]).

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

new(#'Offer'{id = #'OfferID'{value = OfferIdValue},
             resources = Resources} = Offer) ->
    #offer_helper{offer = Offer,
                  offer_id_value = OfferIdValue,
                  persistence_ids = get_persistence_ids(Resources, [])}.

get_offer_id_value(#offer_helper{offer_id_value = OfferIdValue}) ->
    OfferIdValue.

get_persistence_ids(#offer_helper{persistence_ids = PersistenceIds}) ->
    PersistenceIds.

%% ====================================================================
%% Private
%% ====================================================================

get_persistence_ids([#'Resource'{name = "disk",
                                 reservation = Reservation,
                                 disk =
                                     #'Resource.DiskInfo'{persistence =
                                                              Persistence}} |
                     Resources], PersistenceIds) when
  Reservation =/= undefined ->
    #'Resource.DiskInfo.Persistence'{id = PersistenceId} = Persistence,
    get_persistence_ids(Resources, [PersistenceId | PersistenceIds]);
get_persistence_ids([_Resource | Resources], PersistenceIds) ->
    get_persistence_ids(Resources, PersistenceIds);
get_persistence_ids([], PersistenceIds) ->
    lists:reverse(PersistenceIds).


