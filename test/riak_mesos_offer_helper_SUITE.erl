-module(riak_mesos_offer_helper_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([all/0]).

-export([new/1,
         has_reservations/1,
         has_volumes/1]).

all() ->
    [new,
     has_reservations,
     has_volumes].

new(_Config) ->
    OfferResources1 = [],
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = riak_mesos_offer_helper:new(Offer1),
    "offer_1" = riak_mesos_offer_helper:get_offer_id_value(OfferHelper1),
    [] = riak_mesos_offer_helper:get_persistence_ids(OfferHelper1),
    0.0 = riak_mesos_offer_helper:get_reserved_resources_cpus(OfferHelper1),
    0.0 = riak_mesos_offer_helper:get_reserved_resources_mem(OfferHelper1),
    0.0 = riak_mesos_offer_helper:get_reserved_resources_disk(OfferHelper1),
    [] = riak_mesos_offer_helper:get_reserved_resources_ports(OfferHelper1),

    OfferResources2 = cpus_resources_reservation() ++
                      mem_resources_reservation() ++
                      ports_resources_reservation() ++
                      volume_resources_reservation() ++
                      cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    Offer2 = offer("offer_2", OfferResources2),
    OfferHelper2 = riak_mesos_offer_helper:new(Offer2),
    "offer_2" = riak_mesos_offer_helper:get_offer_id_value(OfferHelper2),
    ["id_1", "id_2"] =
        riak_mesos_offer_helper:get_persistence_ids(OfferHelper2),
    CpusReservation = 0.2 + 0.3 + 0.4,
    CpusReservation =
        riak_mesos_offer_helper:get_reserved_resources_cpus(OfferHelper2),
    MemReservation = 256.0 + 512.0 + 1024.0,
    MemReservation =
        riak_mesos_offer_helper:get_reserved_resources_mem(OfferHelper2),
    VolumeReservation = 1024.0 + 2048.0,
    VolumeReservation =
        riak_mesos_offer_helper:get_reserved_resources_disk(OfferHelper2),
    PortsReservation = lists:seq(1, 6),
    PortsReservation =
        riak_mesos_offer_helper:get_reserved_resources_ports(OfferHelper2),
    Cpus = 0.1 + 0.2 + 0.3,
    Cpus = riak_mesos_offer_helper:get_unreserved_resources_cpus(OfferHelper2),
    Mem = 128.0 + 256.0 + 512.0,
    Mem = riak_mesos_offer_helper:get_unreserved_resources_mem(OfferHelper2),
    Ports = lists:seq(7, 12),
    Ports =
        riak_mesos_offer_helper:get_unreserved_resources_ports(OfferHelper2),
    Volume = 2048.0 + 4096.0,
    Volume =
        riak_mesos_offer_helper:get_unreserved_resources_disk(OfferHelper2).

has_reservations(_Config) ->
    OfferResources1 = cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = riak_mesos_offer_helper:new(Offer1),
    false = riak_mesos_offer_helper:has_reservations(OfferHelper1),

    OfferResources2 = cpus_resources_reservation() ++ OfferResources1,
    Offer2 = offer("offer_2", OfferResources2),
    OfferHelper2 = riak_mesos_offer_helper:new(Offer2),
    true = riak_mesos_offer_helper:has_reservations(OfferHelper2),

    OfferResources3 = mem_resources_reservation() ++ OfferResources1,
    Offer3 = offer("offer_3", OfferResources3),
    OfferHelper3 = riak_mesos_offer_helper:new(Offer3),
    true = riak_mesos_offer_helper:has_reservations(OfferHelper3),

    OfferResources4 = ports_resources_reservation() ++ OfferResources1,
    Offer4 = offer("offer_4", OfferResources4),
    OfferHelper4 = riak_mesos_offer_helper:new(Offer4),
    true = riak_mesos_offer_helper:has_reservations(OfferHelper4),

    OfferResources5 = volume_resources_reservation() ++ OfferResources1,
    Offer5 = offer("offer_5", OfferResources5),
    OfferHelper5 = riak_mesos_offer_helper:new(Offer5),
    true = riak_mesos_offer_helper:has_reservations(OfferHelper5),

    ok.

has_volumes(_Config) ->
    OfferResources1 = [],
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = riak_mesos_offer_helper:new(Offer1),
    false = riak_mesos_offer_helper:has_volumes(OfferHelper1),

    OfferResources2 = volume_resources_reservation(),
    Offer2 = offer("offer_2", OfferResources2),
    OfferHelper2 = riak_mesos_offer_helper:new(Offer2),
    true = riak_mesos_offer_helper:has_volumes(OfferHelper2).

cpus_resources_reservation() ->
    [erl_mesos_utils:scalar_resource_reservation("cpus", 0.2, "*",
                                                 "principal"),
     erl_mesos_utils:scalar_resource_reservation("cpus", 0.3, "*",
                                                 "principal"),
     erl_mesos_utils:scalar_resource_reservation("cpus", 0.4, "*",
                                                 "principal")].

mem_resources_reservation() ->
    [erl_mesos_utils:scalar_resource_reservation("mem", 256.0, "*",
                                                 "principal"),
     erl_mesos_utils:scalar_resource_reservation("mem", 512.0, "*",
                                                 "principal"),
     erl_mesos_utils:scalar_resource_reservation("mem", 1024.0, "*",
                                                 "principal")].

ports_resources_reservation() ->
    [erl_mesos_utils:ranges_resource_reservation("ports", [{1, 3}], "*",
                                                 "principal"),
     erl_mesos_utils:ranges_resource_reservation("ports", [{4, 6}], "*",
                                                 "principal")].

volume_resources_reservation() ->
    [erl_mesos_utils:volume_resource_reservation(1024.0, "id_1", "path_1", 'RW',
                                                 "*", "principal"),
     erl_mesos_utils:volume_resource_reservation(2048.0, "id_2", "path_2", 'RW',
                                                 "*", "principal")].

cpus_resources() ->
    [erl_mesos_utils:scalar_resource("cpus", 0.1),
     erl_mesos_utils:scalar_resource("cpus", 0.2),
     erl_mesos_utils:scalar_resource("cpus", 0.3)].

mem_resources() ->
    [erl_mesos_utils:scalar_resource("mem", 128.0),
     erl_mesos_utils:scalar_resource("mem", 256.0),
     erl_mesos_utils:scalar_resource("mem", 512.0)].

ports_resources() ->
    [erl_mesos_utils:ranges_resource("ports", [{7, 9}]),
     erl_mesos_utils:ranges_resource("ports", [{10, 12}])].

volume_resources() ->
    [erl_mesos_utils:volume_resource(2048.0, "id_1", "path_1", 'RW'),
     erl_mesos_utils:volume_resource(4096.0, "id_2", "path_2", 'RW')].

offer(OfferIdValue, Resources) ->
    #'Offer'{id = #'OfferID'{value = OfferIdValue},
             resources = Resources}.
