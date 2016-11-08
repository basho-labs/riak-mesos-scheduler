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

-export([root/0,
         static_root/0,
         master_hosts/0,
         zk/0,
         constraints/0,
         framework_name/0,
         framework_role/0,
         webui_url/0,
         resource_urls/0,
         persistent_path/0,
         framework_hostname/0]).

-export([get_value/2, get_value/3]).

-define(ROOT, "../").
-define(STATIC_ROOT, "../artifacts/").
-define(DEFAULT_MASTER, "master.mesos:5050").
-define(DEFAULT_ZK, "master.mesos:2181").
-define(DEFAULT_NAME, "riak").
-define(DEFAULT_HOSTNAME_SUFFIX, ".marathon.mesos").
-define(DEFAULT_CONSTRAINTS, "[]").

%% Helper functions.

-spec root() -> string().
root() ->
    ?ROOT.

-spec static_root() -> string().
static_root() ->
    ?STATIC_ROOT.

-spec master_hosts() -> [string()].
master_hosts() ->
    {Hosts, _} = split_hosts(get_value(master, ?DEFAULT_MASTER, string)),
    Hosts.

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

-spec resource_urls() -> [{string(), string()}].
resource_urls() ->
    Resources = mochijson2:decode(get_value(resource_urls, undefined, string),
                                  [{format, proplist}]),
    [{binary_to_list(Key), binary_to_list(Url)} ||
     {Key, Url} <- Resources].

-spec persistent_path() -> string().
persistent_path() ->
    get_value(persistent_path, "data", string).

-spec constraints() -> rms_offer_helper:constraints().
constraints() ->
    ConstraintsRaw = get_value(constraints, ?DEFAULT_CONSTRAINTS),
    %% constraints might be double-string-encoded
    ConstraintsStr =
        case {re:run(ConstraintsRaw, "\\\\"), re:run(ConstraintsRaw, "&quot;")} of
            {nomatch, nomatch} ->
                %% plain JSON-as-a-string "[[ \"hostname\", \"UNIQUE\" ]]"
                convert_value(ConstraintsRaw, string);
            {nomatch, {match, _}} ->
                %% plain JSON as an html-encoded string e.g. "[[&quot;hostname&quot;, &quot;UNIQUE&quot;]]"
                convert_value(ConstraintsRaw, html_string);
            {{match, _}, nomatch} ->
                %% double-encoded string e.g. "\"[[\\\"hostname\\\", \\\"UNIQUE\\\"]]\""
                mochijson2:decode(convert_value(ConstraintsRaw, string))
        end,
    ConstraintsBin = case mochijson2:decode(ConstraintsStr) of
                         [] -> [];
                         [[]] -> [];
                         [[F | _] | _] = C when is_binary(F) -> C;
                         [F | _] = C when is_binary(F) -> [C];
                         _ -> []
                     end,
    lists:foldr(fun(X1, Accum1) ->
                    [lists:foldr(fun(X2, Accum2) ->
                                     [binary_to_list(X2)|Accum2]
                                 end, [], X1)|Accum1]
                end, [], ConstraintsBin).

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
unescape_html([C | Rest]) -> [C | unescape_html(Rest)].

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
