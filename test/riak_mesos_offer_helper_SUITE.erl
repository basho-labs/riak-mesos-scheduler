-module(riak_mesos_offer_helper_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([all/0]).

-export([new/1]).

all() ->
    [new].

new(_Config) ->
    Offer1 = offer("offer_1", []),
    OfferHelper1 = riak_mesos_offer_helper:new(Offer1),
    "offer_1" = riak_mesos_offer_helper:get_offer_id_value(OfferHelper1),
    [] = riak_mesos_offer_helper:get_persistence_ids(OfferHelper1),

    Offer2 = offer("offer_2", resources_with_persistence_ids()),
    OfferHelper2 = riak_mesos_offer_helper:new(Offer2),
    "offer_2" = riak_mesos_offer_helper:get_offer_id_value(OfferHelper2),
    ["id_1", "id_2"] =
        riak_mesos_offer_helper:get_persistence_ids(OfferHelper2),

    ok.


resources_with_persistence_ids() ->
    [erl_mesos_utils:volume_resource_reservation(0.1, "id_1", "path_1", 'RW',
                                                 "*", "principal"),
     erl_mesos_utils:volume_resource_reservation(0.2, "id_2", "path_2", 'RW',
                                                 "*", "principal")].

offer(OfferIdValue, Resources) ->
    #'Offer'{id = #'OfferID'{value = OfferIdValue},
             resources = Resources}.
