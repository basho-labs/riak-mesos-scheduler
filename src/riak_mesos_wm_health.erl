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

-module(riak_mesos_wm_health).
-export([routes/0, dispatch/0]).
-export([init/1]).
-export([service_available/2,
         allowed_methods/2,
         content_types_provided/2,
         content_types_accepted/2,
         resource_exists/2,
         provide_content/2,
         accept_content/2]).

-record(ctx, {}).

-include_lib("webmachine/include/webmachine.hrl").

%%%===================================================================
%%% API
%%%===================================================================

routes() ->
    riak_mesos_wm_util:build_routes([[]],[
        ["healthcheck"]
    ]).

dispatch() -> lists:map(fun(Route) -> {Route, ?MODULE, []} end, routes()).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, #ctx{}}.

service_available(RD, Ctx) ->
    {true, RD, Ctx#ctx{}}.

allowed_methods(RD, Ctx) -> {['GET'], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    {[{"application/json", provide_content}], RD, Ctx}.

content_types_accepted(RD, Ctx) -> {[], RD, Ctx}.

resource_exists(RD, Ctx) -> {true, RD, Ctx}.

provide_content(RD, Ctx) ->
    Data = healthcheck(),
    {mochijson2:encode(Data), RD, Ctx}.

accept_content(RD, Ctx) -> {false, RD, Ctx}.

%% ====================================================================
%% Private
%% ====================================================================

healthcheck() -> [{success, true}].
