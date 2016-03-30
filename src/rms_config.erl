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

-module(rms_config).

-export([
         constraints/0,
         zk/0,
         framework_name/0,
         webui_url/0, 
         artifact_urls/0, 
         framework_hostname/0]).

-export([get_value/2, get_value/3]).

-define(DEFAULT_NAME, "riak").
-define(DEFAULT_HOSTNAME, "riak.mesos").
-define(DEFAULT_ZK, "master.mesos:2181").
-define(DEFAULT_CONSTRAINTS, "[[\"hostname\", \"GROUP_BY\"]]").

%% Helper functions.

constraints() ->
    ConstraintsStr = get_value(constraints, ?DEFAULT_CONSTRAINTS, string),
    ConstraintsBin = mochijson2:decode(ConstraintsStr),
    lists:foldr(
      fun(X1, Accum1) -> 
              [lists:foldr(
                fun(X2, Accum2) -> 
                        [binary_to_list(X2)|Accum2]
                end, [], X1)|Accum1]
      end, [], ConstraintsBin).

zk() ->
    get_value(zk, ?DEFAULT_ZK, string).

framework_name() ->
    get_value(name, ?DEFAULT_NAME, string).

framework_hostname() ->
    case get_value(hostname, undefined, string) of
        undefined ->
            case inet:gethostname() of
                {ok, LH} ->
                    case inet:gethostbyname(LH) of
                        {ok, {_, FullHostname, _, _, _, _}} ->
                            FullHostname;
                        _ -> ?DEFAULT_HOSTNAME
                    end;
                _ ->
                    ?DEFAULT_HOSTNAME
            end;
        HN -> HN
    end.

webui_url() ->
    Hostname = framework_hostname(),
    Port = rms_config:get_value(port, 9090, integer),
    Hostname ++ ":" ++ integer_to_list(Port).

artifact_urls() ->
    Base = "http://" ++ webui_url() ++ "/static/",
    [
     Base ++ get_value(riak_pkg, "riak.tar.gz", string),
     Base ++ get_value(explorer_pkg, "riak_explorer.tar.gz", string),
     Base ++ get_value(executor_pkg, "riak_mesos_executor.tar.gz", string)
    ].

%% External functions.

get_value(Key, Default) ->
    case get_env_value(Key) of
        false ->
            application:get_env(rms, Key, Default);
        Value ->
            Value
    end.

get_value(Key, Default, Type) ->
    case get_value(Key, Default) of
        Default ->
            Default;
        Value ->
            convert_value(Value, Type)
    end.

%% Internal functions.

convert_value(Value, integer) when is_list(Value) ->
    list_to_integer(Value);
convert_value(Value, float) when is_list(Value) ->
    list_to_float(Value);
convert_value(Value, boolean) when is_list(Value) ->
    list_to_atom(Value);
convert_value(Value, atom) when is_list(Value) ->
    list_to_atom(Value);
convert_value(Value, binary) when is_list(Value) ->
    list_to_binary(Value);
convert_value(Value, _Type) ->
    Value.

get_env_value(Key) ->
    Key1 = "RIAK_MESOS_" ++ string:to_upper(atom_to_list(Key)),
    os:getenv(Key1).
