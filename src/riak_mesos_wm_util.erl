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

-module(riak_mesos_wm_util).

-export([
  path_part/2,
  dispatch/2,
  base_route/0,
  build_routes/1, build_routes/2,
  halt/3, halt/4, halt/5,
  halt_json/4]).

-define(RIAK_MESOS_BASE_ROUTE, "api").
-define(RIAK_MESOS_API_VERSION, "v1").

%%%===================================================================
%%% API
%%%===================================================================

path_part(Idx, RD) ->
    Path = wrq:path(RD),
    Tokens = string:tokens(Path, "/"),
    Idx1 = length(base_route()) + Idx,

    case {Idx1, length(Tokens)} of
        {I, T} when I =< T -> lists:nth(I, Tokens);
        _ -> undefined
    end.

dispatch(Ip, Port) ->
    Resources = [
        riak_mesos_wm_cluster:dispatch(),
        riak_mesos_wm_node:dispatch(),
        riak_mesos_wm_health:dispatch()
    ],
    [
        {ip, Ip},
        {port, Port},
        {nodelay, true},
        {log_dir, "log"},
        {dispatch, lists:flatten(Resources)}
    ].

base_route() -> [?RIAK_MESOS_BASE_ROUTE, ?RIAK_MESOS_API_VERSION].

build_routes(Routes) ->
    build_routes([
        base_route()
    ], Routes, []).

build_routes(Prefixes, Routes) ->
    build_routes(Prefixes, Routes, []).

halt(Code, RD, Ctx) ->
    {{halt, Code}, RD, Ctx}.
halt(Code, Headers, RD, Ctx) ->
    {{halt, Code}, wrq:set_resp_headers(Headers, RD), Ctx}.
halt(Code, Headers, Data, RD, Ctx) ->
    {{halt, Code}, wrq:set_resp_headers(Headers, wrq:set_resp_body(Data, RD)), Ctx}.

halt_json(Code, Data, RD, Ctx) ->
    halt(Code, [{<<"Content-Type">>, <<"application/json">>}],
         mochijson2:encode(Data), RD, Ctx).

%%%===================================================================
%%% Private
%%%===================================================================

build_routes([], _, Acc) ->
    Acc;
build_routes([P|Prefixes], Routes, Acc) ->
    PRoutes = build_prefixed_routes(P, Routes, []),
    build_routes(Prefixes, Routes, Acc ++ PRoutes).

build_prefixed_routes(_, [], Acc) ->
    lists:reverse(Acc);
build_prefixed_routes(Prefix, [R|Routes], Acc) ->
    R0 = Prefix ++ R,
    build_prefixed_routes(Prefix, Routes, [R0|Acc]).
