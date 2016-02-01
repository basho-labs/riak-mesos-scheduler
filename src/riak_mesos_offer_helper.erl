-module(riak_mesos_offer_helper).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-include_lib("erl_mesos/include/utils.hrl").

-export([new/1,
         get_offer_id_value/1,
         get_persistence_ids/1,
         get_reserved_resources/1,
         get_reserved_resources_cpus/1,
         get_reserved_resources_mem/1,
         get_reserved_resources_disk/1,
         get_reserved_resources_ports/1,
         get_unreserved_resources_cpus/1,
         get_unreserved_resources_mem/1,
         get_unreserved_resources_disk/1,
         get_unreserved_resources_ports/1,
         get_resources_to_reserve/1]).

-export([has_reservations/1,
         has_volumes/1]).

-export([make_reservation/7]).

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
    PersistenceIds = get_persistence_ids(Resources, []),
    ReservedCpus = get_scalar_resource_value("cpus", true, Resources),
    ReservedMem = get_scalar_resource_value("mem", true, Resources),
    ReservedDisk = get_scalar_resource_value("disk", true, Resources),
    ReservedPorts = get_ranges_resource_values("ports", true, Resources),
    ReservedResources = resources(ReservedCpus, ReservedMem, ReservedDisk,
                                  ReservedPorts),
    UnreservedCpus = get_scalar_resource_value("cpus", false, Resources),
    UnreservedMem = get_scalar_resource_value("mem", false, Resources),
    UnreservedDisk = get_scalar_resource_value("disk", false, Resources),
    UnreservedPorts = get_ranges_resource_values("ports", false, Resources),
    UnreservedResources = resources(UnreservedCpus, UnreservedMem,
                                    UnreservedDisk, UnreservedPorts),
    #offer_helper{offer = Offer,
                  offer_id_value = OfferIdValue,
                  persistence_ids = PersistenceIds,
                  reserved_resources = ReservedResources,
                  unreserved_resources = UnreservedResources}.

get_offer_id_value(#offer_helper{offer_id_value = OfferIdValue}) ->
    OfferIdValue.

get_persistence_ids(#offer_helper{persistence_ids = PersistenceIds}) ->
    PersistenceIds.

get_reserved_resources(#offer_helper{reserved_resources = ReservedResources}) ->
    ReservedResources.

get_reserved_resources_cpus(OfferHelper) ->
    erl_mesos_utils:resources_cpus(get_reserved_resources(OfferHelper)).

get_reserved_resources_mem(OfferHelper) ->
    erl_mesos_utils:resources_mem(get_reserved_resources(OfferHelper)).

get_reserved_resources_disk(OfferHelper) ->
    erl_mesos_utils:resources_disk(get_reserved_resources(OfferHelper)).

get_reserved_resources_ports(OfferHelper) ->
    erl_mesos_utils:resources_ports(get_reserved_resources(OfferHelper)).

get_unreserved_resources(#offer_helper{unreserved_resources =
                                           UnreservedResources}) ->
    UnreservedResources.

get_unreserved_resources_cpus(OfferHelper) ->
    erl_mesos_utils:resources_cpus(get_unreserved_resources(OfferHelper)).

get_unreserved_resources_mem(OfferHelper) ->
    erl_mesos_utils:resources_mem(get_unreserved_resources(OfferHelper)).

get_unreserved_resources_disk(OfferHelper) ->
    erl_mesos_utils:resources_disk(get_unreserved_resources(OfferHelper)).

get_unreserved_resources_ports(OfferHelper) ->
    erl_mesos_utils:resources_ports(get_unreserved_resources(OfferHelper)).

get_resources_to_reserve(#offer_helper{resources_to_reserve =
                                       ResourcesToReserve}) ->
    ResourcesToReserve.

has_reservations(OfferHelper) ->
    get_reserved_resources_cpus(OfferHelper) > 0.0 orelse
    get_reserved_resources_mem(OfferHelper) > 0.0 orelse
    get_reserved_resources_disk(OfferHelper) > 0.0 orelse
    length(get_reserved_resources_ports(OfferHelper)) > 0.

has_volumes(OfferHelper) ->
    length(get_persistence_ids(OfferHelper)) > 0.

make_reservation(Cpus, Mem, Disk, Ports, Role, Principal,
                 #offer_helper{unreserved_resources = UnreservedResources} =
                 OfferHelper) ->
    {UnreservedResources1, Resources} =
        apply(Cpus, Mem, Disk, Ports, Role, Principal, undefined, undefined,
              UnreservedResources),
    OfferHelper#offer_helper{unreserved_resources = UnreservedResources1,
                             resources_to_reserve = Resources}.

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

get_scalar_resource_value(Name, WithReservation, Resources) ->
    get_scalar_resource_value(Name, WithReservation, Resources, 0.0).

get_scalar_resource_value(Name, true,
                          [#'Resource'{name = Name,
                                       type = 'SCALAR',
                                       scalar = #'Value.Scalar'{value =
                                                                  ScalarValue},
                                       reservation = Reservation} |
                           Resources], Value)
  when Reservation =/= undefined  ->
    get_scalar_resource_value(Name, true, Resources, Value + ScalarValue);
get_scalar_resource_value(Name, false,
                          [#'Resource'{name = Name,
                                       type = 'SCALAR',
                                       scalar = #'Value.Scalar'{value =
                                                                  ScalarValue},
                                       reservation = undefined} |
                           Resources], Value) ->
    get_scalar_resource_value(Name, false, Resources, Value + ScalarValue);
get_scalar_resource_value(Name, WithReservation, [_Resource | Resources],
                          Value) ->
    get_scalar_resource_value(Name, WithReservation, Resources, Value);
get_scalar_resource_value(_Name, _WithReservation, [], Value) ->
    Value.

get_ranges_resource_values(Name, WithReservation, Resources) ->
    get_ranges_resource_values(Name, WithReservation, Resources, []).

get_ranges_resource_values(Name, true,
                           [#'Resource'{name = Name,
                                        type = 'RANGES',
                                        ranges = #'Value.Ranges'{range =
                                                                     Ranges},
                                        reservation = Reservation} |
                           Resources], Values)
  when Reservation =/= undefined  ->
    Values1 = add_ranges_values(Ranges, Values),
    get_ranges_resource_values(Name, true, Resources, Values1);
get_ranges_resource_values(Name, false,
                           [#'Resource'{name = Name,
                                        type = 'RANGES',
                                        ranges = #'Value.Ranges'{range =
                                                                     Ranges},
                                        reservation = undefined} |
                            Resources], Values) ->
    Values1 = add_ranges_values(Ranges, Values),
    get_ranges_resource_values(Name, false, Resources, Values1);
get_ranges_resource_values(Name, WithReservation, [_Resource | Resources],
                           Values) ->
    get_ranges_resource_values(Name, WithReservation, Resources, Values);
get_ranges_resource_values(_Name, _WithReservation, [], Values) ->
    Values.

add_ranges_values(Ranges, Values) ->
    lists:foldl(fun(#'Value.Range'{'begin' = Begin,
                                   'end' = End}, Acc) ->
                    Acc ++ lists:seq(Begin, End)
                end, Values, Ranges).

resources(Cpus, Mem, Disk, Ports) ->
    #resources{cpus = Cpus,
               mem = Mem,
               disk = Disk,
               ports = Ports}.

apply(Cpus, Mem, Disk, _Ports, Role, Principal, PersistenceId, ContainerPath,
      Res) ->
    Resources = [],
    {Res1, Resources1} = apply_cpus(Cpus, Role, Principal, Res, Resources),
    {Res2, Resources2} = apply_mem(Mem, Role, Principal, Res1, Resources1),
    {Res3, Resources3} = apply_disk(Disk, Role, Principal, PersistenceId,
                                    ContainerPath, Res2, Resources2),
    {Res3, lists:reverse(Resources3)}.


apply_cpus(Cpus, Role, Principal, #resources{cpus = ResCpus} = Res,
           Resources) ->
    case Cpus of
        undefined ->
            {Res, Resources};
        _Cpus when Role =/= undefined, Principal =/= undefined ->
            Res1 = Res#resources{cpus = ResCpus - Cpus},
            Resource = erl_mesos_utils:scalar_resource_reservation("cpus", Cpus,
                                                                   Role,
                                                                   Principal),
            Resource1 = [Resource | Resources],
            {Res1, Resource1};
        _Cpus ->
            Res1 = Res#resources{cpus = ResCpus - Cpus},
            Resource = erl_mesos_utils:scalar_resource("cpus", Cpus),
            Resource1 = [Resource | Resources],
            {Res1, Resource1}
    end.

apply_mem(Mem, Role, Principal, #resources{mem = ResMem} = Res,
          Resources) ->
    case Mem of
        undefined ->
            {Res, Resources};
        _Mem when Role =/= undefined, Principal =/= undefined ->
            Res1 = Res#resources{mem = ResMem - Mem},
            Resource = erl_mesos_utils:scalar_resource_reservation("mem", Mem,
                                                                   Role,
                                                                   Principal),
            Resource1 = [Resource | Resources],
            {Res1, Resource1};
        _Mem ->
            Res1 = Res#resources{mem = ResMem - Mem},
            Resource = erl_mesos_utils:scalar_resource("mem", Mem),
            Resource1 = [Resource | Resources],
            {Res1, Resource1}
    end.

apply_disk(Disk, Role, Principal, PersistenceId, ContainerPath,
           #resources{disk = ResDisk} = Res, Resources) ->
    case Disk of
        undefined ->
            {Res, Resources};
        _Disk when Role =/= undefined, Principal =/= undefined,
                   PersistenceId =/= undefined, ContainerPath =/= undefined ->
            Res1 = Res#resources{disk = ResDisk - Disk},
            Resource =
                erl_mesos_utils:volume_resource_reservation(Disk, PersistenceId,
                                                            ContainerPath, 'RW',
                                                            Role, Principal),
            Resource1 = [Resource | Resources],
            {Res1, Resource1};
        _Disk when Role =/= undefined, Principal =/= undefined ->
            Res1 = Res#resources{disk = ResDisk - Disk},
            Resource = erl_mesos_utils:scalar_resource_reservation("disk", Disk,
                                                                   Role,
                                                                   Principal),
            Resource1 = [Resource | Resources],
            {Res1, Resource1};
        _Disk ->
            Res1 = Res#resources{disk = ResDisk - Disk},
            Resource = erl_mesos_utils:scalar_resource("disk", Disk),
            Resource1 = [Resource | Resources],
            {Res1, Resource1}
    end.

%% apply_ports(Ports, Role, Principal, #resources{cpus = ResCpus} = Res,
%%             Resources) ->
%%     case Cpus of
%%         undefined ->
%%             {Res, Resources};
%%         _Cpus when Role =/= undefined, Principal =/= undefined ->
%%             Res1 = Res#resources{cpus = ResCpus - Cpus},
%%             Resource = erl_mesos_utils:scalar_resource_reservation("cpus", Cpus,
%%                 Role,
%%                 Principal),
%%             Resource1 = [Resource | Resources],
%%             {Res1, Resource1};
%%         _Cpus ->
%%             Res1 = Res#resources{cpus = ResCpus - Cpus},
%%             Resource = erl_mesos_utils:scalar_resource("cpus", Cpus),
%%             Resource1 = [Resource | Resources],
%%             {Res1, Resource1}
%%     end.
