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

-module(riak_mesos_sup).
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
    Ip = riak_mesos_scheduler_config:get_value(ip, "0.0.0.0"),
    Port = riak_mesos_scheduler_config:get_value(port, 9090, integer), %% TODO: Will need to get this dynamically... somehow
    WebConfig = riak_mesos_wm_util:dispatch(Ip, Port),

    ZooKeeperHost = riak_mesos_scheduler_config:get_value(zk_host, "localhost"),
    ZooKeeperPort = riak_mesos_scheduler_config:get_value(zk_port, 2181),

    %% TODO: need to turn this into a list if it contains commas
    Master = riak_mesos_scheduler_config:get_value(master, <<"localhost:5050">>, binary),

    Ref = {riak_mesos, scheduler},
    Scheduler = riak_mesos_scheduler,
    SchedulerOptions = [],
    Options = [{master_hosts, [Master]}],

    SchedulerSpec = {riak_mesos_scheduler,
                     {erl_mesos, start_scheduler, [Ref, Scheduler, SchedulerOptions, Options]},
                     permanent, 5000, worker, [riak_mesos_scheduler]},
    WebmachineSpec = {webmachine_mochiweb,
                      {webmachine_mochiweb, start, [WebConfig]},
                      permanent, 5000, worker, [mochiweb_socket_server]},
    MdMgrSpec = {mesos_metadata_manager,
                 {mesos_metadata_manager, start_link,
                  [[{ZooKeeperHost, ZooKeeperPort}], "riak_mesos_scheduler"]},
                 permanent, 5000, worker, [mesos_metadata_manager]},
    Processes = [MdMgrSpec, SchedulerSpec, WebmachineSpec],

    {ok, { {one_for_one, 10, 10}, Processes} }.
