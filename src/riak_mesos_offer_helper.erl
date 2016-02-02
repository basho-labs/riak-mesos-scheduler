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
         get_unreserved_resources_ports/1]).

-export([has_reservations/1,
         has_volumes/1]).

-export([offer_fits/1]).

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

has_reservations(OfferHelper) ->
    get_reserved_resources_cpus(OfferHelper) > 0.0 orelse
    get_reserved_resources_mem(OfferHelper) > 0.0 orelse
    get_reserved_resources_disk(OfferHelper) > 0.0 orelse
    length(get_reserved_resources_ports(OfferHelper)) > 0.

has_volumes(OfferHelper) ->
    length(get_persistence_ids(OfferHelper)) > 0.

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

-spec offer_fits(#offer_helper{}) -> boolean().
offer_fits(OfferHelper) ->
    UnreservedResources = get_unreserved_resources(OfferHelper),
    OfferedCPU = erl_mesos_utils:resources_cpus(UnreservedResources),
    OfferedMem = erl_mesos_utils:resources_mem(UnreservedResources),
    %% TODO check disk and ports as well?
    %% TODO make these limits configurable?
    OfferedCPU >= 1.0 andalso OfferedMem > 1024.
