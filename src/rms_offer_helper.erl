-module(rms_offer_helper).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-include_lib("erl_mesos/include/utils.hrl").

-export([new/1,
         get_offer_id/1,
         get_offer_id_value/1,
         get_attributes/1,
         get_agent_id/1,
         get_agent_id_value/1,
         get_hostname/1,
         get_constraints/1,
         get_persistence_ids/1,
         has_reserved_resources/1,
         get_reserved_resources/1,
         get_reserved_resources/2,
         get_reserved_resources_cpus/1,
         get_reserved_resources_mem/1,
         get_reserved_resources_disk/1,
         get_reserved_resources_ports/1,
         has_unreserved_resources/1,
         get_unreserved_resources/1,
         get_unreserved_resources/2,
         get_unreserved_resources_cpus/1,
         get_unreserved_resources_mem/1,
         get_unreserved_resources_disk/1,
         get_unreserved_resources_ports/1,
         get_unreserved_applied_resources_ports/1,
         get_resources_to_reserve/1,
         get_resources_to_unreserve/1,
         get_volumes_to_create/1,
         get_volumes_to_destroy/1,
         get_tasks_to_launch/1]).

-export([set_node_hostnames/2,
         set_node_attributes/2,
         set_constraints/2,
         can_fit_constraints/1,
         add_task_to_launch/2,
         has_reservations/1,
         has_volumes/1,
         make_reservation/7,
         make_volume/6,
         apply_reserved_resources/9,
         apply_unreserved_resources/5,
         get_reserved_applied_resources/1,
         get_unreserved_applied_resources/1,
         clean_applied_resources/1,
         unfit_for_reserved/2,
         unfit_for_unreserved/2,
         has_persistence_id/2,
         has_tasks_to_launch/1,
         set_sufficient_resources/2,
         should_unreserve_resources/1,
         unreserve_resources/1,
         unreserve_volumes/1,
         operations/1,
         resources_to_list/1]).

-record(offer_helper, {offer :: erl_mesos:'Offer'(),
                       persistence_ids = [] :: [string()],
                       attributes = [] :: attributes(),
                       constraints = [] :: constraints(),
                       node_attributes = [] :: attributes_group(),
                       node_hostnames = [] :: hostnames(),
                       sufficient_resources = true :: boolean(),
                       applied_reserved_resources = [] :: [erl_mesos:'Resource'()],
                       applied_unreserved_resources = [] :: [erl_mesos:'Resource'()],
                       reserved_resources :: erl_mesos_utils:resources(),
                       unreserved_resources :: erl_mesos_utils:resources(),
                       resources_to_reserve = [] :: [erl_mesos:'Resource'()],
                       resources_to_unreserve = [] :: [erl_mesos:'Resource'()],
                       volumes_to_create = [] :: [erl_mesos:'Resource'()],
                       volumes_to_destroy = [] :: [erl_mesos:'Resource'()],
                       tasks_to_launch = [] :: [erl_mesos:'TaskInfo'()]}).

-type offer_helper() :: #offer_helper{}.
-export_type([offer_helper/0]).

-type constraint() :: [string()].
-type constraints() :: [constraint()].
-export_type([constraints/0]).

-type attribute() :: {string(), string()}.
-type attributes() :: [attribute()].
-type attributes_group() :: [attributes()].
-export_type([attributes/0, attributes_group/0]).

-type hostname() :: string().
-type hostnames() :: [hostname()].
-export_type([hostnames/0]).

%% External functions.

-spec new(erl_mesos:'Offer'()) -> offer_helper().
new(#'Offer'{resources = Resources, attributes = OfferAttributes} = Offer) ->
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
    Attributes = attributes_to_list(OfferAttributes),
    #offer_helper{offer = Offer,
                  persistence_ids = PersistenceIds,
                  reserved_resources = ReservedResources,
                  unreserved_resources = UnreservedResources,
                  attributes = Attributes}.

-spec set_node_hostnames(hostnames(), offer_helper()) -> offer_helper().
set_node_hostnames(NodeHostnames, OfferHelper) ->
    OfferHelper#offer_helper{node_hostnames=NodeHostnames}.

-spec set_node_attributes(attributes_group(), offer_helper()) -> offer_helper().
set_node_attributes(NodeAttributes, OfferHelper) ->
    OfferHelper#offer_helper{node_attributes=NodeAttributes}.

-spec set_constraints(constraints(), offer_helper()) -> offer_helper().
set_constraints(Constraints, OfferHelper) ->
    OfferHelper#offer_helper{constraints=Constraints}.

-spec can_fit_constraints(offer_helper()) -> boolean().
can_fit_constraints(#offer_helper{
                       constraints=Constraints,
                       node_hostnames=NodeHostnames,
                       node_attributes=NodeAttributes}=OfferHelper) ->
    can_fit_constraints(Constraints, NodeHostnames, NodeAttributes, OfferHelper).

-spec can_fit_constraints(constraints(), hostnames(), attributes_group(),
                          offer_helper()) -> ok | {error, Reason :: term()}.
can_fit_constraints([], _, _, _) ->
    ok;
can_fit_constraints([["hostname"|Constraint]|Rest], NodeHosts, NodeAttributes,
                    OfferHelper) ->
    OfferHostname = get_hostname(OfferHelper),
    case fit_constraint(Constraint, OfferHostname, NodeHosts) of
        ok -> can_fit_constraints(Rest, NodeHosts, NodeAttributes, OfferHelper);
        {error, Constraint} -> {error, ["hostname" | Constraint]}
    end;
can_fit_constraints([[Name|Constraint]|Rest], NodeHosts, NodeAttributes, OfferHelper) ->
    Attributes = get_attributes(OfferHelper),
    A = proplists:get_value(Name, Attributes),
    As = lists:foldl(
           fun(X, Accum) ->
                   [proplists:get_value(Name, X)|Accum]
           end, [], NodeAttributes),
    case fit_constraint(Constraint, A, As) of
        ok -> can_fit_constraints(Rest, NodeHosts, NodeAttributes, OfferHelper);
        {error, Constraint} -> {error, [Name | Constraint]}
    end.

-spec get_offer_id(offer_helper()|erl_mesos:'Offer'()) -> erl_mesos:'OfferID'().
get_offer_id(#'Offer'{id = OfferId}) ->
    OfferId;
get_offer_id(#offer_helper{offer = #'Offer'{id = OfferId}}) ->
    OfferId.

-spec get_offer_id_value(offer_helper()) -> string().
get_offer_id_value(OfferHelper) ->
    #'OfferID'{value = OfferIdValue} = get_offer_id(OfferHelper),
    OfferIdValue.

-spec get_attributes(offer_helper()) -> attributes().
get_attributes(#offer_helper{attributes = Attributes}) ->
    Attributes.

-spec get_agent_id(offer_helper()) -> erl_mesos:'AgentID'().
get_agent_id(#offer_helper{offer = #'Offer'{agent_id = AgentId}}) ->
    AgentId.

-spec get_agent_id_value(offer_helper()) -> string().
get_agent_id_value(OfferHelper) ->
    #'AgentID'{value = AgentIdValue} = get_agent_id(OfferHelper),
    AgentIdValue.

-spec get_hostname(offer_helper()) -> string().
get_hostname(#offer_helper{offer = #'Offer'{hostname = Hostname}}) ->
    Hostname.

-spec get_constraints(offer_helper()) -> string().
get_constraints(#offer_helper{constraints = Constraints}) ->
    Constraints.

-spec get_persistence_ids(offer_helper()) -> [string()].
get_persistence_ids(#offer_helper{persistence_ids = PersistenceIds}) ->
    PersistenceIds.

-spec get_reserved_resources(offer_helper()) -> erl_mesos_utils:resources().
get_reserved_resources(#offer_helper{reserved_resources = ReservedResources}) ->
    ReservedResources.

%% Returns true iff any reserved resource > 0
-spec has_reserved_resources(offer_helper()) -> boolean().
has_reserved_resources(OfferHelper) ->
    Rsrcs = [ cpus, mem, disk, ports ],
    lists:sum([ get_reserved_resources(R, OfferHelper) || R <- Rsrcs ]) > 0.

-spec get_reserved_resources(cpus | mem | disk | ports, offer_helper()) -> float() | [non_neg_integer()].
get_reserved_resources(cpus, OfferHelper) ->
    get_reserved_resources_cpus(OfferHelper);
get_reserved_resources(mem, OfferHelper) ->
    get_reserved_resources_mem(OfferHelper);
get_reserved_resources(disk, OfferHelper) ->
    get_reserved_resources_disk(OfferHelper);
get_reserved_resources(ports, OfferHelper) ->
    get_reserved_resources_ports(OfferHelper).

-spec get_reserved_resources_cpus(offer_helper()) -> float().
get_reserved_resources_cpus(OfferHelper) ->
    erl_mesos_utils:resources_cpus(get_reserved_resources(OfferHelper)).

-spec get_reserved_resources_mem(offer_helper()) -> float().
get_reserved_resources_mem(OfferHelper) ->
    erl_mesos_utils:resources_mem(get_reserved_resources(OfferHelper)).

-spec get_reserved_resources_disk(offer_helper()) -> float().
get_reserved_resources_disk(OfferHelper) ->
    erl_mesos_utils:resources_disk(get_reserved_resources(OfferHelper)).

-spec get_reserved_resources_ports(offer_helper()) -> [non_neg_integer()].
get_reserved_resources_ports(OfferHelper) ->
    erl_mesos_utils:resources_ports(get_reserved_resources(OfferHelper)).

-spec get_unreserved_resources(offer_helper()) -> erl_mesos_utils:resources().
get_unreserved_resources(#offer_helper{unreserved_resources =
                                       UnreservedResources}) ->
    UnreservedResources.

-spec has_unreserved_resources(offer_helper()) -> boolean().
has_unreserved_resources(OfferHelper) ->
    Rsrcs = [ cpus, mem, disk, ports ],
    lists:sum([ get_unreserved_resources(R, OfferHelper) || R <- Rsrcs ]) > 0.

-spec get_unreserved_resources(cpus | mem | disk | ports, offer_helper()) -> float() | [non_neg_integer()].
get_unreserved_resources(cpus, OfferHelper) ->
    get_unreserved_resources_cpus(OfferHelper);
get_unreserved_resources(mem, OfferHelper) ->
    get_unreserved_resources_mem(OfferHelper);
get_unreserved_resources(disk, OfferHelper) ->
    get_unreserved_resources_disk(OfferHelper);
get_unreserved_resources(ports, OfferHelper) ->
    get_unreserved_resources_ports(OfferHelper).

-spec get_unreserved_resources_cpus(offer_helper()) -> float().
get_unreserved_resources_cpus(OfferHelper) ->
    erl_mesos_utils:resources_cpus(get_unreserved_resources(OfferHelper)).

-spec get_unreserved_resources_mem(offer_helper()) -> float().
get_unreserved_resources_mem(OfferHelper) ->
    erl_mesos_utils:resources_mem(get_unreserved_resources(OfferHelper)).

-spec get_unreserved_resources_disk(offer_helper()) -> float().
get_unreserved_resources_disk(OfferHelper) ->
    erl_mesos_utils:resources_disk(get_unreserved_resources(OfferHelper)).

-spec get_unreserved_resources_ports(offer_helper()) -> [non_neg_integer()].
get_unreserved_resources_ports(OfferHelper) ->
    erl_mesos_utils:resources_ports(get_unreserved_resources(OfferHelper)).

-spec get_unreserved_applied_resources_ports(offer_helper()) ->
    [non_neg_integer()].
get_unreserved_applied_resources_ports(OfferHelper) ->
    UnreservedPorts = get_ranges_resource_values("ports", false,
                          get_unreserved_applied_resources(OfferHelper)),
    UnreservedResources = resources(0.0, 0.0, 0.0, UnreservedPorts),
    erl_mesos_utils:resources_ports(UnreservedResources).

-spec clean_applied_resources(offer_helper()) -> offer_helper().
clean_applied_resources(OfferHelper) ->
  OfferHelper#offer_helper{
     applied_reserved_resources=[],
     applied_unreserved_resources=[]
    }.

-spec get_resources_to_reserve(offer_helper()) -> [erl_mesos:'Resource'()].
get_resources_to_reserve(#offer_helper{resources_to_reserve =
                                       ResourcesToReserve}) ->
    ResourcesToReserve.

-spec get_resources_to_unreserve(offer_helper()) -> [erl_mesos:'Resource'()].
get_resources_to_unreserve(#offer_helper{resources_to_unreserve =
                                         ResourcesToUnreserve}) ->
    ResourcesToUnreserve.

-spec get_volumes_to_create(offer_helper()) -> [erl_mesos:'Resource'()].
get_volumes_to_create(#offer_helper{volumes_to_create = VolumesToCreate}) ->
    VolumesToCreate.

-spec get_volumes_to_destroy(offer_helper()) -> [erl_mesos:'Resource'()].
get_volumes_to_destroy(#offer_helper{volumes_to_destroy = VolumesToDestroy}) ->
    VolumesToDestroy.

-spec get_tasks_to_launch(offer_helper()) -> [erl_mesos:'TaskInfo'()].
get_tasks_to_launch(#offer_helper{tasks_to_launch = TasksToLaunch}) ->
    TasksToLaunch.

-spec add_task_to_launch(erl_mesos:'TaskInfo'(), offer_helper()) ->
    offer_helper().
add_task_to_launch(TaskInfo, #offer_helper{tasks_to_launch = TasksToLaunch} =
                   OfferHelper) ->
    OfferHelper#offer_helper{tasks_to_launch = [TaskInfo | TasksToLaunch]}.

-spec has_reservations(offer_helper()) -> boolean().
has_reservations(OfferHelper) ->
    get_reserved_resources_cpus(OfferHelper) > 0.0 orelse
    get_reserved_resources_mem(OfferHelper) > 0.0 orelse
    get_reserved_resources_disk(OfferHelper) > 0.0 orelse
    length(get_reserved_resources_ports(OfferHelper)) > 0.

-spec has_volumes(offer_helper()) -> boolean().
has_volumes(OfferHelper) ->
    length(get_persistence_ids(OfferHelper)) > 0.

-spec make_reservation(undefined | float(), undefined | float(),
                       undefined | float(), undefined | pos_integer(),
                       undefined | string(), undefined | string(),
                       offer_helper()) ->
    offer_helper().
make_reservation(Cpus, Mem, Disk, NumPorts, Role, Principal,
                 #offer_helper{unreserved_resources = UnreservedResources,
                               resources_to_reserve = Resources} =
                 OfferHelper) ->
    {UnreservedResources1, Resources1} =
        apply(Cpus, Mem, Disk, NumPorts, Role, Principal, undefined, undefined,
              UnreservedResources),
    OfferHelper#offer_helper{unreserved_resources = UnreservedResources1,
                             resources_to_reserve = Resources ++ Resources1}.

-spec make_volume(undefined | float(), undefined | string(),
                  undefined | string(), undefined | string(),
                  undefined | string(), offer_helper()) ->
    offer_helper().
make_volume(Disk, Role, Principal, PersistenceId, ContainerPath,
            #offer_helper{unreserved_resources = UnreservedResources,
                          volumes_to_create = Volumes} = OfferHelper) ->
    {_, Volumes1} =
        apply_disk(Disk, Role, Principal, PersistenceId, ContainerPath,
                   UnreservedResources, []),
    %% Maybe decrease unreserved disk here.
    OfferHelper#offer_helper{volumes_to_create = Volumes ++ Volumes1}.

-spec apply_reserved_resources(undefined | float(), undefined | float(),
                               undefined | float(), undefined | pos_integer(),
                               undefined | string(), undefined | string(),
                               undefined | string(), undefined | string(),
                               offer_helper()) ->
    offer_helper().
apply_reserved_resources(Cpus, Mem, Disk, NumPorts, Role, Principal,
                         PersistenceId, ContainerPath,
                         #offer_helper{reserved_resources = ReservedResources,
                                       applied_reserved_resources =
                                           AppliedReservedResources} =
                         OfferHelper) ->
    {ReservedResources1, AppliedReservedResources1} =
        apply(Cpus, Mem, Disk, NumPorts, Role, Principal, PersistenceId,
              ContainerPath, ReservedResources),
    OfferHelper#offer_helper{reserved_resources = ReservedResources1,
                             applied_reserved_resources =
                                 AppliedReservedResources ++ AppliedReservedResources1}.

-spec apply_unreserved_resources(undefined | float(), undefined | float(),
                                 undefined | float(), undefined | pos_integer(),
                                 offer_helper()) ->
    offer_helper().
apply_unreserved_resources(Cpus, Mem, Disk, NumPorts,
                           #offer_helper{unreserved_resources =
                                             UnreservedResources,
                                         applied_unreserved_resources =
                                             AppliedUnreservedResources} =
                           OfferHelper) ->
    {UnreservedResources1, AppliedUnreservedResources1} =
        apply(Cpus, Mem, Disk, NumPorts, undefined, undefined, undefined,
              undefined, UnreservedResources),
    OfferHelper#offer_helper{unreserved_resources = UnreservedResources1,
                             applied_unreserved_resources =
                                 AppliedUnreservedResources ++ AppliedUnreservedResources1}.

-spec get_reserved_applied_resources(offer_helper()) ->
    [erl_mesos:'Resource'()].
get_reserved_applied_resources(#offer_helper{applied_reserved_resources =
                                                 AppliedReservedResources}) ->
    AppliedReservedResources.

-spec get_unreserved_applied_resources(offer_helper()) ->
    [erl_mesos:'Resource'()].
get_unreserved_applied_resources(#offer_helper{applied_unreserved_resources =
                                                 AppliedUnreservedResources}) ->
    AppliedUnreservedResources.

-spec unfit_for_reserved(list({cpus, float()} | {mem, float()} | {disk, float()} | {ports, non_neg_integer()}),
                     offer_helper()) ->
    list(cpus | mem | disk | ports).
unfit_for_reserved(Wanted, OfferHelper) ->
    lists:filtermap(
      fun({Rsrc, Need}) ->
              not unfit_for_reserved(Rsrc, Need, OfferHelper) andalso {true, Rsrc}
      end,
      Wanted).

-spec unfit_for_reserved(cpus | mem | disk | ports, float() | non_neg_integer(), offer_helper()) ->
    boolean().
unfit_for_reserved(ports, NumPorts, OfferHelper) ->
    length(get_reserved_resources(ports, OfferHelper)) >= NumPorts;
unfit_for_reserved(Rsrc, Count, OfferHelper) ->
    get_reserved_resources(Rsrc, OfferHelper) >= Count.

-spec unfit_for_unreserved(list({cpus, float()} | {mem, float()} | {disk, float()} | {ports, non_neg_integer()}),
                     offer_helper()) ->
    list(cpus | mem | disk | ports).
unfit_for_unreserved(Wanted, OfferHelper) ->
    lists:filtermap(
      fun({Rsrc, Need}) ->
              %% andalso doesn't care what the RHS is, just returns it iff LHS
              not unfit_for_unreserved(Rsrc, Need, OfferHelper) andalso {true, Rsrc}
      end,
      Wanted).

-spec unfit_for_unreserved(cpus | mem | disk | ports, float() | non_neg_integer(), offer_helper()) ->
    boolean().
unfit_for_unreserved(ports, NumPorts, OfferHelper) ->
    length(get_unreserved_resources(ports, OfferHelper)) >= NumPorts;
unfit_for_unreserved(Rsrc, Count, OfferHelper) ->
    get_unreserved_resources(Rsrc, OfferHelper) >= Count.

-spec has_persistence_id(string(), offer_helper()) -> boolean().
has_persistence_id(PersistenceId,
                   #offer_helper{persistence_ids = PersistenceIds}) ->
    lists:member(PersistenceId, PersistenceIds).

-spec has_tasks_to_launch(offer_helper()) -> boolean().
has_tasks_to_launch(#offer_helper{tasks_to_launch = TasksToLaunch}) ->
    length(TasksToLaunch) > 0.

-spec unreserve_resources(offer_helper()) -> offer_helper().
unreserve_resources(#offer_helper{offer = #'Offer'{resources = Resources},
                                  resources_to_unreserve =
                                      ResourcesToUnreserve} =
                    OfferHelper) ->
    ResourcesToUnreserve1 = ResourcesToUnreserve ++
                                unreserve_resources(Resources, []),
    OfferHelper#offer_helper{resources_to_unreserve = ResourcesToUnreserve1}.

-spec unreserve_volumes(offer_helper()) -> offer_helper().
unreserve_volumes(#offer_helper{offer = #'Offer'{resources = Resources},
                                volumes_to_destroy = VolumesToDestroy} =
                  OfferHelper) ->
    VolumesToDestroy1 = VolumesToDestroy ++ unreserve_volumes(Resources, []),
    OfferHelper#offer_helper{volumes_to_destroy = VolumesToDestroy1}.

-spec operations(offer_helper()) -> [erl_mesos:'Offer.Operation'()].
operations(#offer_helper{resources_to_reserve = ResourcesToReserve,
                         resources_to_unreserve = ResourcesToUnreserve,
                         volumes_to_create = VolumesToCreate,
                         volumes_to_destroy = VolumesToDestroy,
                         tasks_to_launch = TasksToLaunch}) ->
    AppendOperationFun = fun({OperationFun, Operations}, OfferOperations) ->
                             case Operations of
                                 [] ->
                                     OfferOperations;
                                 _Operations ->
                                     [OperationFun(Operations) |
                                      OfferOperations]
                             end
                         end,
    OfferOperations = [],
    Funs =
        [{fun erl_mesos_utils:launch_offer_operation/1, TasksToLaunch},
         {fun erl_mesos_utils:destroy_offer_operation/1, VolumesToDestroy},
         {fun erl_mesos_utils:create_offer_operation/1, VolumesToCreate},
         {fun erl_mesos_utils:unreserve_offer_operation/1, ResourcesToUnreserve},
         {fun erl_mesos_utils:reserve_offer_operation/1, ResourcesToReserve}
         ],
    lists:foldl(AppendOperationFun, OfferOperations, Funs).

-spec resources_to_list(offer_helper()) ->
    [{reserved | unreserved, [{atom(), term()}]}].
resources_to_list(OfferHelper) ->
    ReservedCpus = get_reserved_resources_cpus(OfferHelper),
    ReservedMem = get_reserved_resources_mem(OfferHelper),
    ReservedDisk = get_reserved_resources_disk(OfferHelper),
    ReservedNumPorts = length(get_reserved_resources_ports(OfferHelper)),
    ReservedNumPersistenceIds = length(get_persistence_ids(OfferHelper)),
    UnreservedCpus = get_unreserved_resources_cpus(OfferHelper),
    UnreservedMem = get_unreserved_resources_mem(OfferHelper),
    UnreservedDisk = get_unreserved_resources_disk(OfferHelper),
    UnreservedNumPorts = length(get_unreserved_resources_ports(OfferHelper)),
    [{reserved, [{cpus, ReservedCpus},
                 {mem, ReservedMem},
                 {disk, ReservedDisk},
                 {num_ports, ReservedNumPorts},
                 {num_persistence_ids, ReservedNumPersistenceIds}]},
     {unreserved, [{cpus, UnreservedCpus},
                   {mem, UnreservedMem},
                   {disk, UnreservedDisk},
                   {num_ports, UnreservedNumPorts}]}].

%% Internal functions.

-spec get_persistence_ids([erl_mesos:'Resource'()], [string()]) -> [string()].
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

-spec get_scalar_resource_value(string(), boolean(),
                                [erl_mesos:'Resource'()]) ->
    float().
get_scalar_resource_value(Name, WithReservation, Resources) ->
    get_scalar_resource_value(Name, WithReservation, Resources, 0.0).

-spec get_scalar_resource_value(string(), boolean(), [erl_mesos:'Resource'()],
                                float()) ->
    float().
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

-spec get_ranges_resource_values(string(), boolean(),
                                 [erl_mesos:'Resource'()]) ->
    [non_neg_integer()].
get_ranges_resource_values(Name, WithReservation, Resources) ->
    get_ranges_resource_values(Name, WithReservation, Resources, []).

-spec get_ranges_resource_values(string(), boolean(),
                                 [erl_mesos:'Resource'()],
                                 [non_neg_integer()]) ->
    [non_neg_integer()].
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

-spec add_ranges_values([erl_mesos:'Value.Range'()], [non_neg_integer()]) ->
    [non_neg_integer()].
add_ranges_values(Ranges, Values) ->
    lists:foldl(fun(#'Value.Range'{'begin' = Begin,
                                   'end' = End}, Acc) ->
                    Acc ++ lists:seq(Begin, End)
                end, Values, Ranges).

-spec resources(float(), float(), float(), [non_neg_integer()]) ->
    erl_mesos_utils:resources().
resources(Cpus, Mem, Disk, Ports) ->
    #resources{cpus = Cpus,
               mem = Mem,
               disk = Disk,
               ports = Ports}.

-spec apply(undefined | float(), undefined | float(), undefined | float(),
            undefined | pos_integer(), undefined | string(),
            undefined | string(), undefined | string(), undefined | string(),
            erl_mesos_utils:resources()) ->
    {erl_mesos_utils:resources(), [erl_mesos:'Resource'()]}.
apply(Cpus, Mem, Disk, NumPorts, Role, Principal, PersistenceId, ContainerPath,
      Res) ->
    Resources = [],
    {Res1, Resources1} = apply_cpus(Cpus, Role, Principal, Res, Resources),
    {Res2, Resources2} = apply_mem(Mem, Role, Principal, Res1, Resources1),
    {Res3, Resources3} = apply_disk(Disk, Role, Principal, PersistenceId,
                                    ContainerPath, Res2, Resources2),
    {Res4, Resources4} = apply_ports(NumPorts, Role, Principal, Res3,
                                     Resources3),
    {Res4, lists:reverse(Resources4)}.

-spec apply_cpus(undefined | float(), undefined | string(),
                 undefined | string(), erl_mesos_utils:resources(),
                 [erl_mesos:'Resource'()]) ->
    {erl_mesos_utils:resources(), [erl_mesos:'Resource'()]}.
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

-spec apply_mem(undefined | float(), undefined | string(),
                undefined | string(), erl_mesos_utils:resources(),
                [erl_mesos:'Resource'()]) ->
    {erl_mesos_utils:resources(), [erl_mesos:'Resource'()]}.
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

-spec apply_disk(undefined | float(), undefined | string(),
                 undefined | string(), undefined | string(),
                 undefined | string(), erl_mesos_utils:resources(),
                 [erl_mesos:'Resource'()]) ->
    {erl_mesos_utils:resources(), [erl_mesos:'Resource'()]}.
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

-spec apply_ports(undefined | pos_integer(), undefined | string(),
                  undefined | string(), erl_mesos_utils:resources(),
                  [erl_mesos:'Resource'()]) ->
    {erl_mesos_utils:resources(), [erl_mesos:'Resource'()]}.
apply_ports(NumPorts, Role, Principal, #resources{ports = ResPorts} = Res,
            Resources) ->
    case NumPorts of
        undefined ->
            {Res, Resources};
        _NumPorts when Role =/= undefined, Principal =/= undefined ->
            {PortsSlice, ResPorts1} = ports_slice(NumPorts, ResPorts),
            Ranges = rms_utils:list_to_ranges(PortsSlice),
            Res1 = Res#resources{ports = ResPorts1},
            Resource = erl_mesos_utils:ranges_resource_reservation("ports",
                                                                   Ranges,
                                                                   Role,
                                                                   Principal),
            Resource1 = [Resource | Resources],
            {Res1, Resource1};
        _NumPorts ->
            {PortsSlice, ResPorts1} = ports_slice(NumPorts, ResPorts),
            Ranges = rms_utils:list_to_ranges(PortsSlice),
            Res1 = Res#resources{ports = ResPorts1},
            Resource = erl_mesos_utils:ranges_resource("ports", Ranges),
            Resource1 = [Resource | Resources],
            {Res1, Resource1}
    end.

-spec ports_slice(pos_integer(), [pos_integer()]) ->
    {[non_neg_integer()], [non_neg_integer()]}.
ports_slice(NumPorts, Ports) when NumPorts < length(Ports) ->
    SliceStart = random:uniform(length(Ports) - NumPorts),
    ports_slice(SliceStart, NumPorts, Ports);
ports_slice(NumPorts, Ports) ->
    ports_slice(1, NumPorts, Ports).

-spec ports_slice(pos_integer(), pos_integer(), [non_neg_integer()]) ->
    {[non_neg_integer()], [non_neg_integer()]}.
ports_slice(SliceStart, NumPorts, Ports) ->
    PortsSlice = lists:sublist(Ports, SliceStart, NumPorts),
    {PortsSlice, Ports -- PortsSlice}.

-spec set_sufficient_resources(boolean(), offer_helper()) -> offer_helper().
set_sufficient_resources(HasSufficientResources, OfferHelper) ->
    OfferHelper#offer_helper{sufficient_resources=HasSufficientResources}.

-spec should_unreserve_resources(offer_helper()) -> boolean().
should_unreserve_resources(OfferHelper=
                               #offer_helper{
                                  sufficient_resources=HasSufficientResources}) ->
    case has_tasks_to_launch(OfferHelper) of
        true ->
            false;
        false ->
            HasSufficientResources
    end.

-spec unreserve_resources([erl_mesos:'Resource'()], [erl_mesos:'Resource'()]) ->
    [erl_mesos:'Resource'()].
unreserve_resources([#'Resource'{name = Name,
                                 type = 'SCALAR',
                                 scalar = #'Value.Scalar'{value = Value},
                                 role = Role,
                                 reservation = Reservation} | Resources],
                    ResourcesToUnreserver)
  when Role =/= undefined, Reservation =/= undefined ->
    #'Resource.ReservationInfo'{principal = Principal} = Reservation,
    ResourceToUnreserver =
        erl_mesos_utils:scalar_resource_reservation(Name, Value, Role,
                                                    Principal),
    ResourcesToUnreserver1 = [ResourceToUnreserver | ResourcesToUnreserver],
    unreserve_resources(Resources, ResourcesToUnreserver1);
unreserve_resources([_Resource | Resources], ResourcesToUnreserver) ->
    unreserve_resources(Resources, ResourcesToUnreserver);
unreserve_resources([], ResourcesToUnreserver) ->
    ResourcesToUnreserver.

-spec unreserve_volumes([erl_mesos:'Resource'()], [erl_mesos:'Resource'()]) ->
    [erl_mesos:'Resource'()].
unreserve_volumes([#'Resource'{name = "disk",
                               type = 'SCALAR',
                               scalar = #'Value.Scalar'{value = Value},
                               role = Role,
                               reservation = Reservation,
                               disk = Disk} | Resources],
                  VolumesToDestroy)
  when Role =/= undefined, Reservation =/= undefined, Disk =/= undefined ->
    #'Resource.ReservationInfo'{principal = Principal} = Reservation,
    #'Resource.DiskInfo'{persistence = Persistence,
                         volume = Volume} = Disk,
    #'Resource.DiskInfo.Persistence'{id = PersistenceId} = Persistence,
    #'Volume'{container_path = ContainerPath} = Volume,
    VolumeToDestroy =
        erl_mesos_utils:volume_resource_reservation(Value, PersistenceId,
                                                    ContainerPath, 'RW', Role,
                                                    Principal),
    VolumesToDestroy1 = [VolumeToDestroy | VolumesToDestroy],
    unreserve_volumes(Resources, VolumesToDestroy1);
unreserve_volumes([_Resource | Resources], VolumesToDestroy) ->
    unreserve_volumes(Resources, VolumesToDestroy);
unreserve_volumes([], VolumesToDestroy) ->
    VolumesToDestroy.

-spec attributes_to_list([erl_mesos:'Attribute'()]) -> attributes().
attributes_to_list(RawAttributes) ->
    attributes_to_list(RawAttributes, []).

-spec attributes_to_list([erl_mesos:'Attribute'()],
                         attributes()) -> attributes().
attributes_to_list([], Accum) ->
    Accum;
attributes_to_list([#'Attribute'{
                     name=Name,
                     type='SCALAR',
                     scalar=#'Value.Scalar'{value=Value}}|Rest],
                   Accum) when is_float(Value) ->
    attributes_to_list(Rest, [{Name, float_to_list(Value)}|Accum]);
attributes_to_list([#'Attribute'{
                     type='RANGES',
                     ranges=#'Value.Scalar'{}}|Rest], Accum) ->
    %% TODO: Deal with range attributes
    attributes_to_list(Rest, Accum);
attributes_to_list([#'Attribute'{
                     type='SET',
                     scalar=#'Value.Set'{item=[]}}|Rest],
                   Accum) ->
    attributes_to_list(Rest, Accum);
attributes_to_list([#'Attribute'{
                     name=Name,
                     type='SET',
                     scalar=#'Value.Set'{item=[V1|_]=Value}}|Rest],
                   Accum) when is_list(V1) ->
    NewValues = lists:map(fun(X) -> {Name, X} end, Value),
    attributes_to_list(Rest, Accum ++ NewValues);
attributes_to_list([#'Attribute'{
                     name=Name,
                     type='TEXT',
                     scalar=#'Value.Text'{value=Value}}|Rest],
                   Accum) when is_list(Value) ->
    attributes_to_list(Rest, [{Name, Value}|Accum]);
attributes_to_list([_|Rest], Accum) ->
    attributes_to_list(Rest, Accum).

-spec fit_constraint(constraint(), string(), [string()]) ->
    ok | {error, constraint()}.
fit_constraint(Constraint, V, Vs) ->
    case can_fit_constraint(Constraint, V, Vs) of
        true -> ok;
        false -> {error, Constraint}
    end.

-spec can_fit_constraint(constraint(), string(), [string()]) -> boolean().
can_fit_constraint(["UNIQUE"], V, Vs) ->
    %% Unique Value
    not lists:member(V, Vs);
can_fit_constraint(["GROUP_BY"], V, Vs) ->
    %% Groupby Value. If the value isn't unique, then attempt to
    %% schedule it on a different offer. If there are no other offers,
    %% go ahead and schedule it on this one. In other words, attempt
    %% to spread across all values, but don't refuse offers.
    %% TODO: when false is returned, we should only
    %% apply when there are no other offers being considered?
    case can_fit_constraint(["UNIQUE"], V, Vs) of
        true -> true;
        false -> true
    end;
can_fit_constraint(["GROUP_BY", Param], V, Vs) ->
    %% Compare total number of hosts to number of scheduled hosts
    case {list_to_integer(Param), length(Vs)} of
        %% There should still be unclaimed values, wait if not unique
        {N1, N2} when N1 > N2 ->
            can_fit_constraint(["UNIQUE"], V, Vs);
        %% There's a node on every host already, go ahead and use the offer
        _ ->
            %% TODO: get more sophisticated about attempting to spread
            %% nodes evenly, because we already know how many nodes there are
            true
    end;
can_fit_constraint(["CLUSTER", V], V, _) ->
    %% Cluster on value, values match
    true;
can_fit_constraint(["CLUSTER", _], _, _) ->
    %% Cluster on value, hosts do not match
    false;
can_fit_constraint(["LIKE", Param], V, _) ->
    %% Value is like regex
    case re:run(V, Param) of
        {match, _} -> true;
        nomatch -> false
    end;
can_fit_constraint(["UNLIKE", Param], V, Vs) ->
    %% Value is not like regex
    not can_fit_constraint(["LIKE", Param], V, Vs);
can_fit_constraint(_, _, _) ->
    %% Undefined constraint, just schedule it
    true.
