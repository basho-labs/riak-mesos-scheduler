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

-export([master_hosts/0,
         static_root/0,
         constraints/0,
         zk/0,
         framework_name/0,
         framework_role/0,
         webui_url/0, 
         artifacts/0,
         artifact_urls/0, 
         persistent_path/0,
         container_path/0,
         framework_hostname/0]).

-export([get_value/2, get_value/3]).

-define(DEFAULT_NAME, "riak").
-define(DEFAULT_HOSTNAME_SUFFIX, ".marathon.mesos").
-define(DEFAULT_MASTER, "master.mesos:5050").
-define(DEFAULT_ZK, "master.mesos:2181").
-define(DEFAULT_CONSTRAINTS, "[]").
-define(STATIC_ROOT, "../artifacts/").
-define(DEFAULT_CONTAINER_PATH, "root").

 % The path-tail in a Riak archive, for which we search
-define(RIAK_BIN, "riak/bin/riak").

%% Helper functions.

-spec master_hosts() -> [string()].
master_hosts() ->
    {Hosts, _} = split_hosts(get_value(master, ?DEFAULT_MASTER, string)),
    Hosts.

-spec static_root() -> string().
static_root() -> ?STATIC_ROOT.

-spec constraints() -> rms_offer_helper:constraints().
constraints() ->
    ConstraintsRaw = get_value(constraints, ?DEFAULT_CONSTRAINTS),
    %% constraints might be double-string-encoded
    ConstraintsStr =
        case {re:run(ConstraintsRaw, "\\\\"), re:run(ConstraintsRaw, "&quot;")} of
            {nomatch, nomatch} -> % plain JSON-as-a-string "[[ \"hostname\", \"UNIQUE\" ]]"
                convert_value(ConstraintsRaw, string);
            {nomatch, {match, _}} -> % plain JSON as an html-encoded string e.g. "[[&quot;hostname&quot;, &quot;UNIQUE&quot;]]"
                convert_value(ConstraintsRaw, html_string);
            {{match, _}, nomatch} -> % double-encoded string e.g. "\"[[\\\"hostname\\\", \\\"UNIQUE\\\"]]\""
                mochijson2:decode(convert_value(ConstraintsRaw, string))
        end,
    ConstraintsBin = case mochijson2:decode(ConstraintsStr) of
        [] -> [];
        [[]] -> [];
        [[F|_]|_]=C when is_binary(F) -> C;
        [F|_]=C when is_binary(F) -> [C];
        _ -> []
    end,
    lists:foldr(
      fun(X1, Accum1) -> 
              [lists:foldr(
                 fun(X2, Accum2) -> 
                         [binary_to_list(X2)|Accum2]
                 end, [], X1)|Accum1]
      end, [], ConstraintsBin).

-spec zk() -> string().
zk() ->
    split_hosts(get_value(zk, ?DEFAULT_ZK, string)).

-spec framework_name() -> string().
framework_name() ->
    get_value(name, ?DEFAULT_NAME, string).

-spec framework_role() -> string().
framework_role() ->
    get_value(name, framework_name(), string).

-spec framework_hostname() -> string().
framework_hostname() ->
    case get_value(hostname, undefined, string) of
        undefined ->
            framework_name() ++ ?DEFAULT_HOSTNAME_SUFFIX;
        Hostname ->
            Hostname
    end.

-spec webui_url() -> string().
webui_url() ->
    Hostname = framework_hostname(),
    Port = rms_config:get_value(port, 9090, integer),
    "http://" ++ Hostname ++ ":" ++ integer_to_list(Port) ++ "/".

-spec artifacts() -> [string()].
artifacts() ->
    [
     get_value(riak_pkg, "riak.tar.gz", string),
     get_value(explorer_pkg, "riak_explorer.tar.gz", string),
     get_value(patches_pkg, "riak_erlpmd_patches.tar.gz", string),
     get_value(executor_pkg, "riak_mesos_executor.tar.gz", string)
    ].

-spec container_path() -> string().
container_path() ->
    RiakPkg = get_value(riak_pkg, "riak.tar.gz", string),
    ArtifactDir = "../artifacts",
    Filename = filename:join([ArtifactDir, RiakPkg]),
    {ok, TarTable} = erl_tar:table(Filename, [compressed]),
    find_root_path(TarTable).

%% TODO Should we log something in this case?
-spec find_root_path(list(string())) -> string().
find_root_path([]) -> ?DEFAULT_CONTAINER_PATH;
find_root_path([P | Paths]) ->
    case lists:suffix(?RIAK_BIN, P) of
        true ->
            %% Strip the known tail, leave only the prefix
            find_prefix(P, ?RIAK_BIN);
        false -> find_root_path(Paths)
    end.

-spec find_prefix(string(), string()) -> string().
find_prefix(FullPath, Tail) ->
    % We know that FullPath = Prefix ++ Tail
    % How to find Prefix?
    SplitPath = filename:split(FullPath),
    SplitTail = filename:split(Tail),
    % Reverse the path components
    LiatTilps = lists:reverse(SplitTail),
    HtapTilps = lists:reverse(SplitPath),
    % Find the common path-tail (list-head), reverse and join
    filename:join(lists:reverse(drop_common_prefix(HtapTilps, LiatTilps))).

% Drops from A the leading elements common to A and B.
-spec drop_common_prefix(A::list(), B::list()) -> list().
drop_common_prefix([], _) -> [];
drop_common_prefix([X | Rest1], [X | Rest2]) -> drop_common_prefix(Rest1, Rest2);
drop_common_prefix(Rest, _) -> Rest.
    
-spec artifact_urls() -> [string()].
artifact_urls() ->
    %% TODO "static" is magic
    Base = webui_url() ++ "static/",
    [ Base ++ Artifact || Artifact <- artifacts() ].

-spec persistent_path() -> string().
persistent_path() ->
    get_value(persistent_path, "data", string).

%% External functions.

-spec get_value(atom(), term()) -> term().
get_value(Key, Default) ->
    case get_env_value(Key) of
        false ->
            application:get_env(rms, Key, Default);
        Value ->
            Value
    end.

-spec get_value(atom(), term(), atom()) -> term().
get_value(Key, Default, Type) ->
    case get_value(Key, Default) of
        Default ->
            Default;
        Value ->
            convert_value(Value, Type)
    end.

%% Internal functions.

-spec convert_value(term(), atom()) -> term().
convert_value(Value, number) when is_list(Value) ->
    try
        list_to_float(Value)
    catch 
        error:badarg -> list_to_integer(Value)
    end;
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
convert_value(Value, html_string) when is_binary(Value) ->
    unescape_html(binary_to_list(Value));
convert_value(Value, html_string) when is_list(Value) ->
    unescape_html(Value);
convert_value(Value, _Type) ->
    Value.

-spec unescape_html(string()) -> string().
unescape_html([]) -> [];
unescape_html("&quot;"++Rest) -> "\"" ++ unescape_html(Rest);
unescape_html("&lt;" ++ Rest) -> "<" ++ unescape_html(Rest);
unescape_html("&gt;" ++ Rest) -> ">" ++ unescape_html(Rest);
unescape_html("&amp;"++ Rest) -> "&" ++ unescape_html(Rest);
unescape_html([C | Rest]) -> [ C | unescape_html(Rest) ].

-spec get_env_value(atom()) -> string() | false.
get_env_value(Key) ->
    Key1 = "RIAK_MESOS_" ++ string:to_upper(atom_to_list(Key)),
    os:getenv(Key1).

-spec split_hosts(string()) -> {[string()], undefined | string()}.
split_hosts("zk://" ++ Uri) ->
    [Hosts | Path] = string:tokens(Uri, "/"),
    {string:tokens(Hosts, ","), "/" ++ string:join(Path, "/")};
split_hosts(Hosts) ->
    {string:tokens(Hosts, ","), undefined}.
