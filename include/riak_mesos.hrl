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

%%%===================================================================
%%% Macros
%%%===================================================================

-define(RIAK_MESOS_BASE_ROUTE, "api").
-define(RIAK_MESOS_API_VERSION, "v1").

-record(riak_node, {
    id :: binary(),
    cluster_id :: binary(),
    container_path :: binary(),
    persistence_id :: binary() | undefined,
    task_status :: [{atom(), binary()}] | undefined,
    slave_id :: binary() | undefined,
    hostname :: binary() | undefined,
    http_port :: integer() | undefined,
    protobuf_port :: integer() | undefined
}).

-type riak_node() :: #riak_node{}.

-record(riak_cluster, {
    id :: binary(),
    nodes :: [riak_node()],
    node_cpus :: float(),
    node_mem :: float(),
    node_disk :: float(),
    node_ports :: integer()
}).
