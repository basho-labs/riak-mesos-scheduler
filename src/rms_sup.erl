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

-module(rms_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init([]) ->
    Ip = rms_config:get_value(ip, "0.0.0.0"),
    %% TODO: Will need to get this dynamically... somehow.
    Port = rms_config:get_value(port, 9090, integer),
    WebConfig = rms_wm_resource:dispatch(Ip, Port),

    ZooKeeperHost = rms_config:get_value(zk_host, "localhost"),
    ZooKeeperPort = rms_config:get_value(zk_port, 2181),

    %% TODO: need to turn this into a list if it contains commas.
    Master = rms_config:get_value(master, <<"localhost:5050">>, binary),

    %% TODO: move all possible options init to rms:start/2.
    FrameworkUser = rms_config:get_value(user, "root"),
    FrameworkName = rms_config:get_value(name, "riak", string),
    FrameworkRole = rms_config:get_value(role, "riak", string),
    FrameworkHostname = rms_config:get_value(hostname, undefined, string),
    FrameworkPrincipal = rms_config:get_value(principal, "riak", string),
    NodeCpus = rms_config:get_value(node_cpus, 1.0, float),
    NodeMem = rms_config:get_value(node_mem, 1024.0, float),
    NodeDisk = rms_config:get_value(node_disk, 4000.0, float),

    Ref = riak_mesos_scheduler,
    Scheduler = rms_scheduler,
    SchedulerOptions = [{framework_user, FrameworkUser},
                        {framework_name, FrameworkName},
                        {framework_role, FrameworkRole},
                        {framework_hostname, FrameworkHostname},
                        {framework_principal, FrameworkPrincipal},
                        {node_cpus, NodeCpus},
                        {node_mem, NodeMem},
                        {node_disk, NodeDisk}],
    Options = [{master_hosts, [Master]}],



    MetadataManagerSpec = {mesos_metadata_manager,
                               {mesos_metadata_manager, start_link,
                               [[{ZooKeeperHost, ZooKeeperPort}],
                                "rms_scheduler"]},
                               permanent, 5000, worker,
                               [mesos_metadata_manager]},
    MetadataSpec = {rms_metadata,
                        {rms_metadata, start_link, []},
                        permanent, 5000, worker, [rms_metadata]},
    ClusterManagerSpec = {rms_cluster_manager,
                              {rms_cluster_manager, start_link, []},
                              permanent, 5000, supervisor,
                              [rms_cluster_manager]},
    NodeManagerSpec = {rms_node_manager,
                           {rms_node_manager, start_link, []},
                           permanent, 5000, supervisor,
                           [rms_node_manager]},
    _SchedulerSpec = {rms_scheduler,
                        {erl_mesos_scheduler, start_link, [Ref, Scheduler,
                                                           SchedulerOptions,
                                                           Options]},
                        permanent, 5000, worker, [rms_scheduler]},



    WebmachineSpec = {webmachine_mochiweb,
                      {webmachine_mochiweb, start, [WebConfig]},
                      permanent, 5000, worker, [mochiweb_socket_server]},
%%    MdMgrSpec = {mesos_metadata_manager,
%%                 {mesos_metadata_manager, start_link,
%%                  [[{ZooKeeperHost, ZooKeeperPort}], "riak_mesos_scheduler"]},
%%                 permanent, 5000, worker, [mesos_metadata_manager]},
    %% TODO: remove SchedulerDataSpec.
    SchedulerDataSpec = {mesos_scheduler_data,
                         {mesos_scheduler_data, start_link, []},
                         permanent, 5000, worker, [mesos_scheduler_data]},
%%    NodeFsmSup = {scheduler_node_fsm_sup,
%%                  {scheduler_node_fsm_sup, start_link, []},
%%                  permanent, 5000, worker, [scheduler_node_fsm_sup]},

    Specs = [MetadataManagerSpec, MetadataSpec, ClusterManagerSpec,
             NodeManagerSpec, WebmachineSpec,
             SchedulerDataSpec],
    {ok, {{one_for_one, 10, 10}, Specs}}.
