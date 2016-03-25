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

%% External functions.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor callback function.

init([]) ->
    Ip = rms_config:get_value(ip, "0.0.0.0"),
    Port = rms_config:get_value(port, 9090, integer),
    WebConfig = rms_wm_resource:dispatch(Ip, Port),

    ZooKeeper = rms_config:zk(),
    [ZooKeeperHost,P] = string:tokens(ZooKeeper, ":"),
    ZooKeeperPort = list_to_integer(P),

    %% TODO: need to turn this into a list if it contains commas.
    Master = rms_config:get_value(master, <<"localhost:5050">>, binary),

    %% TODO: move all possible options init to rms:start/2.
    FrameworkUser = rms_config:get_value(user, "root"),
    FrameworkName = rms_config:framework_name(),
    FrameworkRole = rms_config:get_value(role, "riak", string),
    FrameworkHostname = rms_config:framework_hostname(),
    FrameworkPrincipal = rms_config:get_value(principal, "riak", string),
    FrameworkFailoverTimeout =
        rms_config:get_value(failover_timeout, 10000.0, float),
    FrameworkWebUIURL = rms_config:webui_url(),

    %% TODO: use these if they are set
    _FrameworkAuthProvider = rms_config:get_value(provider, "", string),
    _FrameworkAuthSecret = rms_config:get_value(secret_file, "", string),

    NodeCpus = rms_config:get_value(node_cpus, 0.5, float),
    NodeMem = rms_config:get_value(node_mem, 1024.0, float),
    NodeDisk = rms_config:get_value(node_disk, 4000.0, float),

    ExecutorCpus = rms_config:get_value(executor_cpus, 0.1, float),
    ExecutorMem = rms_config:get_value(executor_mem, 512.0, float),

    ArtifactUrls = rms_config:artifact_urls(),

    Ref = riak_mesos_scheduler,
    Scheduler = rms_scheduler,

    SchedulerOptions = [{framework_user, FrameworkUser},
                        {framework_name, FrameworkName},
                        {framework_role, FrameworkRole},
                        {framework_hostname, FrameworkHostname},
                        {framework_principal, FrameworkPrincipal},
                        {framework_failover_timeout, FrameworkFailoverTimeout},
                        {framework_webui_url, FrameworkWebUIURL},
                        {node_cpus, NodeCpus},
                        {node_mem, NodeMem},
                        {node_disk, NodeDisk},
                        {executor_cpus, ExecutorCpus},
                        {executor_mem, ExecutorMem},
                        {artifact_urls, ArtifactUrls}],
    Options = [{master_hosts, [Master]}],

    MetadataManagerSpec = {mesos_metadata_manager,
                               {mesos_metadata_manager, start_link,
                               [[{ZooKeeperHost, ZooKeeperPort}],
                                FrameworkName]},
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
    SchedulerSpec = {rms_scheduler,
                        {erl_mesos_scheduler, start_link, [Ref, Scheduler,
                                                           SchedulerOptions,
                                                           Options]},
                        permanent, 5000, worker, [rms_scheduler]},
    WebmachineSpec = {webmachine_mochiweb,
                      {webmachine_mochiweb, start, [WebConfig]},
                      permanent, 5000, worker, [mochiweb_socket_server]},
    Specs = [MetadataManagerSpec, MetadataSpec, ClusterManagerSpec,
             NodeManagerSpec, SchedulerSpec, WebmachineSpec],
    {ok, {{one_for_one, 1, 1}, Specs}}.
