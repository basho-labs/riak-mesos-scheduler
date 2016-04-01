-module(rms_offer_helper_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([all/0]).

-export([new/1,
         can_fit_hostname_constraints/1,
         can_fit_attribute_constraints/1,
         has_reservations/1,
         has_volumes/1,
         make_reservation/1,
         make_volume/1,
         apply_resources/1]).

all() ->
    [new,
     can_fit_hostname_constraints,
     can_fit_attribute_constraints,
     has_reservations,
     has_volumes,
     make_reservation,
     make_volume,
     apply_resources].

new(_Config) ->
    OfferResources1 = [],
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = rms_offer_helper:new(Offer1),
    "offer_1" = rms_offer_helper:get_offer_id_value(OfferHelper1),
    [] = rms_offer_helper:get_persistence_ids(OfferHelper1),
    0.0 = rms_offer_helper:get_reserved_resources_cpus(OfferHelper1),
    0.0 = rms_offer_helper:get_reserved_resources_mem(OfferHelper1),
    0.0 = rms_offer_helper:get_reserved_resources_disk(OfferHelper1),
    [] = rms_offer_helper:get_reserved_resources_ports(OfferHelper1),

    OfferResources2 = cpus_resources_reservation() ++
                      mem_resources_reservation() ++
                      ports_resources_reservation() ++
                      volume_resources_reservation() ++
                      cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    Offer2 = offer("offer_2", OfferResources2),
    OfferHelper2 = rms_offer_helper:new(Offer2),
    "offer_2" = rms_offer_helper:get_offer_id_value(OfferHelper2),
    ["id_1", "id_2"] =
        rms_offer_helper:get_persistence_ids(OfferHelper2),
    CpusReservation = 0.2 + 0.3 + 0.4,
    CpusReservation =
        rms_offer_helper:get_reserved_resources_cpus(OfferHelper2),
    MemReservation = 256.0 + 512.0 + 1024.0,
    MemReservation =
        rms_offer_helper:get_reserved_resources_mem(OfferHelper2),
    VolumeReservation = 1024.0 + 2048.0,
    VolumeReservation =
        rms_offer_helper:get_reserved_resources_disk(OfferHelper2),
    PortsReservation = lists:seq(1, 6),
    PortsReservation =
        rms_offer_helper:get_reserved_resources_ports(OfferHelper2),
    Cpus = 0.1 + 0.2 + 0.3,
    Cpus = rms_offer_helper:get_unreserved_resources_cpus(OfferHelper2),
    Mem = 128.0 + 256.0 + 512.0,
    Mem = rms_offer_helper:get_unreserved_resources_mem(OfferHelper2),
    Ports = lists:seq(7, 20),
    Ports = rms_offer_helper:get_unreserved_resources_ports(OfferHelper2),
    Volume = 2048.0 + 4096.0,
    Volume = rms_offer_helper:get_unreserved_resources_disk(OfferHelper2).

has_reservations(_Config) ->
    OfferResources1 = cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = rms_offer_helper:new(Offer1),
    false = rms_offer_helper:has_reservations(OfferHelper1),

    OfferResources2 = cpus_resources_reservation() ++ OfferResources1,
    Offer2 = offer("offer_2", OfferResources2),
    OfferHelper2 = rms_offer_helper:new(Offer2),
    true = rms_offer_helper:has_reservations(OfferHelper2),

    OfferResources3 = mem_resources_reservation() ++ OfferResources1,
    Offer3 = offer("offer_3", OfferResources3),
    OfferHelper3 = rms_offer_helper:new(Offer3),
    true = rms_offer_helper:has_reservations(OfferHelper3),

    OfferResources4 = ports_resources_reservation() ++ OfferResources1,
    Offer4 = offer("offer_4", OfferResources4),
    OfferHelper4 = rms_offer_helper:new(Offer4),
    true = rms_offer_helper:has_reservations(OfferHelper4),

    OfferResources5 = volume_resources_reservation() ++ OfferResources1,
    Offer5 = offer("offer_5", OfferResources5),
    OfferHelper5 = rms_offer_helper:new(Offer5),
    true = rms_offer_helper:has_reservations(OfferHelper5).

has_volumes(_Config) ->
    OfferResources1 = [],
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = rms_offer_helper:new(Offer1),
    false = rms_offer_helper:has_volumes(OfferHelper1),

    OfferResources2 = volume_resources_reservation(),
    Offer2 = offer("offer_2", OfferResources2),
    OfferHelper2 = rms_offer_helper:new(Offer2),
    true = rms_offer_helper:has_volumes(OfferHelper2).

make_reservation(_Config) ->
    OfferResources1 = cpus_resources_reservation() ++
                      mem_resources_reservation() ++
                      ports_resources_reservation() ++
                      volume_resources_reservation() ++
                      cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = rms_offer_helper:new(Offer1),
    Cpus = 0.1 + 0.2 + 0.3,
    ReserveCpus = 0.4,
    UnreservedCpus = Cpus - ReserveCpus,
    Cpus = rms_offer_helper:get_unreserved_resources_cpus(OfferHelper1),
    Mem = 128.0 + 256.0 + 512.0,
    ReserveMem = 384.0,
    UnreservedMem = Mem - ReserveMem,
    Mem = rms_offer_helper:get_unreserved_resources_mem(OfferHelper1),
    Disk = 2048.0 + 4096.0,
    ReserveDisk = 512.0,
    UnreservedDisk = Disk - ReserveDisk,
    Disk = rms_offer_helper:get_unreserved_resources_disk(OfferHelper1),
    Ports = lists:seq(7, 20),
    ReservePorts = 5,
    Ports = rms_offer_helper:get_unreserved_resources_ports(OfferHelper1),
    OfferHelper2 = rms_offer_helper:make_reservation(ReserveCpus,
                                                     ReserveMem,
                                                     ReserveDisk,
                                                     ReservePorts, "*",
                                                     "principal",
                                                     OfferHelper1),
    UnreservedCpus =
        rms_offer_helper:get_unreserved_resources_cpus(OfferHelper2),
    UnreservedMem =
        rms_offer_helper:get_unreserved_resources_mem(OfferHelper2),
    UnreservedDisk =
        rms_offer_helper:get_unreserved_resources_disk(OfferHelper2),
    UnreservedPorts =
        rms_offer_helper:get_unreserved_resources_ports(OfferHelper2),
    CpusResourceToReserve =
        erl_mesos_utils:scalar_resource_reservation("cpus", ReserveCpus, "*",
                                                    "principal"),
    MemResourceToReserve =
        erl_mesos_utils:scalar_resource_reservation("mem", ReserveMem, "*",
                                                    "principal"),
    DiskResourceToReserve =
        erl_mesos_utils:scalar_resource_reservation("disk", ReserveDisk, "*",
                                                    "principal"),
    [CpusResourceToReserve,
     MemResourceToReserve,
     DiskResourceToReserve,
     PortsResourceToReserve] =
        rms_offer_helper:get_resources_to_reserve(OfferHelper2),
    #'Resource'{ranges = #'Value.Ranges'{range = PortsToReserveRanges}} =
        PortsResourceToReserve,
    [#'Value.Range'{'begin' = PortsToReserveBegin,
                    'end' = PortsToReserveEnd}] = PortsToReserveRanges,
    PortsToReserve = lists:seq(PortsToReserveBegin, PortsToReserveEnd),
    PortsLength = length(Ports),
    PortsToReserveLength = length(PortsToReserve),
    UnreservedPortsLength = length(UnreservedPorts),
    PortsLength = PortsToReserveLength + UnreservedPortsLength.

make_volume(_Config) ->
    OfferResources1 = cpus_resources_reservation() ++
                      mem_resources_reservation() ++
                      ports_resources_reservation() ++
                      volume_resources_reservation() ++
                      cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = rms_offer_helper:new(Offer1),
    VolumeDisk1 = 128.0,
    VolumeResource1 =
        erl_mesos_utils:volume_resource_reservation(VolumeDisk1, "id_1",
                                                    "path_1", 'RW', "*",
                                                    "principal"),
    VolumeDisk2 = 256.0,
    VolumeResource2 =
        erl_mesos_utils:volume_resource_reservation(VolumeDisk2, "id_2",
                                                    "path_2", 'RW', "*",
                                                    "principal"),
    OfferHelper2 = rms_offer_helper:make_volume(VolumeDisk1, "*",
                                                "principal", "id_1",
                                                "path_1", OfferHelper1),
    OfferHelper3 = rms_offer_helper:make_volume(VolumeDisk2, "*",
                                                "principal", "id_2",
                                                "path_2", OfferHelper2),
    [VolumeResource1, VolumeResource2] =
        rms_offer_helper:get_volumes_to_create(OfferHelper3).

apply_resources(_Config) ->
    OfferResources1 = cpus_resources_reservation() ++
                      mem_resources_reservation() ++
                      ports_resources_reservation() ++
                      volume_resources_reservation() ++
                      cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    Offer1 = offer("offer_1", OfferResources1),
    OfferHelper1 = rms_offer_helper:new(Offer1),
    CpusReservation = 0.2 + 0.3 + 0.4,
    ApplyCpusReservation = 0.5,
    AfterApplyCpusReservation = CpusReservation - ApplyCpusReservation,
    MemReservation = 256.0 + 512.0 + 1024.0,
    ApplyMemReservation = 256.0 + 512.0,
    AfterApplyMemReservation = MemReservation - ApplyMemReservation,
    VolumeReservation = 1024.0 + 2048.0,
    ApplyVolumeReservation = 512.0 + 1024.0,
    AfterApplyVolumeReservation = VolumeReservation - ApplyVolumeReservation,
    PortsReservation = lists:seq(1, 6),
    ApplyPortsReservationLength = 2,
    AfterApplyPortsReservationLength =
        length(PortsReservation) - ApplyPortsReservationLength,
    OfferHelper2 =
        rms_offer_helper:apply_reserved_resources(
            ApplyCpusReservation, ApplyMemReservation,
            AfterApplyVolumeReservation, ApplyPortsReservationLength, "*",
            "principal", "id", "path", OfferHelper1),
    AfterApplyCpusReservation =
        rms_offer_helper:get_reserved_resources_cpus(OfferHelper2),
    AfterApplyMemReservation =
        rms_offer_helper:get_reserved_resources_mem(OfferHelper2),
    AfterApplyVolumeReservation =
        rms_offer_helper:get_reserved_resources_disk(OfferHelper2),
    AfterApplyPortsReservation =
        rms_offer_helper:get_reserved_resources_ports(OfferHelper2),
    AfterApplyPortsReservationLength = length(AfterApplyPortsReservation),
    Cpus = 0.1 + 0.2 + 0.3,
    ApplyCpus = 0.5,
    AfterApplyCpus = Cpus - ApplyCpus,
    Mem = 128.0 + 256.0 + 512.0,
    ApplyMem = 384.0,
    AfterApplyMem = Mem - ApplyMem,
    Volume = 2048.0 + 4096.0,
    ApplyVolume = 1024.0 + 2048.0,
    AfterApplyVolume = Volume - ApplyVolume,
    Ports = lists:seq(7, 20),
    ApplyPortsLength = 5,
    AfterApplyPortsLength = length(Ports) - ApplyPortsLength,
    OfferHelper3 =
        rms_offer_helper:apply_unreserved_resources(ApplyCpus, ApplyMem,
                                                    ApplyVolume,
                                                    ApplyPortsLength,
                                                    OfferHelper1),
    AfterApplyCpus =
        rms_offer_helper:get_unreserved_resources_cpus(OfferHelper3),
    AfterApplyMem =
        rms_offer_helper:get_unreserved_resources_mem(OfferHelper3),
    AfterApplyVolume =
        rms_offer_helper:get_unreserved_resources_disk(OfferHelper3),
    AfterApplyPorts =
        rms_offer_helper:get_unreserved_resources_ports(OfferHelper3),
    AfterApplyPortsLength = length(AfterApplyPorts).

can_fit_hostname_constraints(_Config) ->
    false = test_hostname_constraint(
              [["hostname", "UNIQUE"]],
              "ubuntu1.local",
              ["ubuntu1.local"]),
    true = test_hostname_constraint(
              [["hostname", "UNIQUE"]],
              "ubuntu2.local",
              ["ubuntu1.local"]),
    true = test_hostname_constraint(
              [["hostname", "GROUP_BY"]],
              "ubuntu1.local",
              ["ubuntu1.local"]),
    true = test_hostname_constraint(
              [["hostname", "GROUP_BY"]],
              "ubuntu2.local",
              ["ubuntu1.local"]),
    false = test_hostname_constraint(
              [["hostname", "GROUP_BY", "2"]],
              "ubuntu1.local",
              ["ubuntu1.local"]),
    true = test_hostname_constraint(
              [["hostname", "GROUP_BY", "1"]],
              "ubuntu2.local",
              ["ubuntu1.local"]),
    true = test_hostname_constraint(
              [["hostname", "CLUSTER", "ubuntu1.local"]],
              "ubuntu1.local",
              ["ubuntu1.local"]),
    false = test_hostname_constraint(
              [["hostname", "CLUSTER", "ubuntu1.local"]],
              "ubuntu2.local",
              ["ubuntu1.local"]),
    false = test_hostname_constraint(
              [["hostname", "LIKE", "ubuntu[1-2].local"]],
              "ubuntu3.local",
              ["ubuntu1.local"]),
    true = test_hostname_constraint(
              [["hostname", "LIKE", "ubuntu[1-2].local"]],
              "ubuntu1.local",
              ["ubuntu1.local"]),
    true = test_hostname_constraint(
              [["hostname", "UNLIKE", "ubuntu[1-2].local"]],
              "ubuntu3.local",
              ["ubuntu1.local"]),
    false = test_hostname_constraint(
              [["hostname", "UNLIKE", "ubuntu[1-2].local"]],
              "ubuntu1.local",
              ["ubuntu1.local"]).

can_fit_attribute_constraints(_Config) ->
    false = test_attribute_constraint(
              [["rack_id", "UNIQUE"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]),
    true = test_attribute_constraint(
              [["rack_id", "UNIQUE"]],
              [{"rack_id", "2"}],
              [[{"rack_id", "1"}]]),
    true = test_attribute_constraint(
              [["rack_id", "GROUP_BY"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]),
    true = test_attribute_constraint(
              [["rack_id", "GROUP_BY"]],
              [{"rack_id", "2"}],
              [[{"rack_id", "1"}]]),
    false = test_attribute_constraint(
              [["rack_id", "GROUP_BY", "2"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]),
    true = test_attribute_constraint(
              [["rack_id", "GROUP_BY", "1"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]),
    true = test_attribute_constraint(
              [["rack_id", "CLUSTER", "1"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]),
    false = test_attribute_constraint(
              [["rack_id", "CLUSTER", "2"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]),
    false = test_attribute_constraint(
              [["rack_id", "LIKE", "[1-2]"]],
              [{"rack_id", "3"}],
              [[{"rack_id", "1"}]]),
    true = test_attribute_constraint(
              [["rack_id", "LIKE", "[1-2]"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]),
    true = test_attribute_constraint(
              [["rack_id", "UNLIKE", "[1-2]"]],
              [{"rack_id", "3"}],
              [[{"rack_id", "1"}]]),
    false = test_attribute_constraint(
              [["rack_id", "UNLIKE", "[1-2]"]],
              [{"rack_id", "1"}],
              [[{"rack_id", "1"}]]).

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
     erl_mesos_utils:ranges_resource("ports", [{10, 12}, {13, 20}])].

volume_resources() ->
    [erl_mesos_utils:volume_resource(2048.0, "id_1", "path_1", 'RW'),
     erl_mesos_utils:volume_resource(4096.0, "id_2", "path_2", 'RW')].

offer(OfferIdValue, Resources, Hostname, Attributes) ->
    Offer1 = offer(OfferIdValue, Resources),
    Offer1#'Offer'{hostname=Hostname,
                  attributes=Attributes}.

offer(OfferIdValue, Resources) ->
    #'Offer'{id = #'OfferID'{value = OfferIdValue},
             resources = Resources}.

test_hostname_constraint(Constraints, Hostname, NodeHosts) ->
    OfferResources1 = cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    rms_offer_helper:can_fit_constraints(
            rms_offer_helper:set_constraints(Constraints, 
            rms_offer_helper:set_node_hostnames(NodeHosts,
            rms_offer_helper:set_node_attributes([[]],
            rms_offer_helper:new(offer("offer_1", OfferResources1, 
              Hostname, 
              []
            )))))).

test_attribute_constraint(Constraints, [{Attr,AttrVal}], NodeAttrs) ->
    OfferResources1 = cpus_resources() ++
                      mem_resources() ++
                      ports_resources() ++
                      volume_resources(),
    rms_offer_helper:can_fit_constraints(
            rms_offer_helper:set_constraints(Constraints, 
            rms_offer_helper:set_node_hostnames([],
            rms_offer_helper:set_node_attributes(NodeAttrs,
            rms_offer_helper:new(offer("offer_1", OfferResources1, 
              "ubuntu.local", 
              [#'Attribute'{name=Attr,type='TEXT',scalar=#'Value.Text'{value=AttrVal}}]
            )))))).
