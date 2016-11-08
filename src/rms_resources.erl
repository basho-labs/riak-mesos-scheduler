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

-module(rms_resources).

-export([init_artifacts/4,
         artifact_urls/2,
         riak_artifact_urls/1,
         riak_root_path/2]).

-define(EXCLUDE_ARTIFACT_KEYS, ["scheduler"]).

-define(DEFAULT_RIAK_ROOT_PATH, "root").

-define(RIAK_BIN, "riak/bin/riak").

%% External functions.

init_artifacts(RootDir, ArtifactDir, ResourceUrls, PersistentPath) ->
    ok = filelib:ensure_dir(ArtifactDir),
    Artifacts = artifacts(ResourceUrls),
    ok = move_artifacts(RootDir, ArtifactDir, Artifacts),
    case is_persistent_volume_safe(PersistentPath, ArtifactDir, Artifacts) of
        {error, {path_clash, Package}} ->
            lager:error("Unable to start scheduler: a path in ~p will overwrite"
                        " the persistent volume path (~p) in executors. "
                        "Refusing to start.", [Package, PersistentPath]),
            {error, path_clash};
        {error, Reason} = Error ->
            lager:error("Unable to validate archives against persistent volume "
                        "path, because: ~p", [Reason]),
            Error;
        ok ->
            ok
    end.

-spec artifact_urls(string(), string()) -> [{string(), string()}].
artifact_urls(BaseUrl, ResourceUrls) ->
    Artifacts = artifacts(ResourceUrls),
    [{Key, BaseUrl ++ Package} || {Key, Package} <- Artifacts].

-spec riak_artifact_urls([{string(), string()}]) -> [{string(), string()}].
riak_artifact_urls(ArtifactUrls) ->
    [ArtifactUrl || {Key, _Url} = ArtifactUrl <- ArtifactUrls,
     case Key of
         "riak-" ++ _Version ->
             true;
         _Key ->
             false
     end].

-spec riak_root_path(string(), string()) -> string().
riak_root_path(ArtifactDir, RiakUrl) ->
    Package = get_package_from_url(RiakUrl),
    Filename = filename:join([ArtifactDir, Package]),
    {ok, Tar} = erl_tar:table(Filename, [compressed]),
    find_riak_root_path(Tar).

%% Internal functions.

-spec artifacts([{string(), string()}]) -> [{string(), string()}].
artifacts(ResourceUrls) ->
    [{Key, get_package_from_url(Url)} || {Key, Url} <- ResourceUrls].

-spec get_package_from_url(string()) -> string().
get_package_from_url(Url) ->
    lists:last(string:tokens(Url, "/")).

-spec move_artifacts(string(), string(), [{string(), string()}]) ->
    ok | {error, term()}.
move_artifacts(RootDir, ArtifactDir, [{Key, Package} | Artifacts]) ->
    PackagePath = filename:join([RootDir, Package]),
    case lists:member(Key, ?EXCLUDE_ARTIFACT_KEYS) of
        true ->
            file:delete(PackagePath),
            move_artifacts(RootDir, ArtifactDir, Artifacts);
        false ->
            ArtifactPath = filename:join([ArtifactDir, Package]),
            file:rename(PackagePath, ArtifactPath),
            move_artifacts(RootDir, ArtifactDir, Artifacts)
    end;
move_artifacts(_RootDir, _ArtifactDir, []) ->
    ok.

-spec is_persistent_volume_safe(string(), string(), [{string(), string()}]) ->
    ok | {error, term()}.
is_persistent_volume_safe(Path, ArtifactDir, [{Key, Package} | Artifacts]) ->
    case lists:member(Key, ?EXCLUDE_ARTIFACT_KEYS) of
        true ->
            is_persistent_volume_safe(Path, ArtifactDir, Artifacts);
        false ->
            ArtifactPath = filename:join([ArtifactDir, Package]),
            case erl_tar:table(ArtifactPath, [compressed]) of
                {ok, Content} ->
                    case lists:member(Path, Content) orelse
                         lists:member(Path ++ "/", Content) of
                        true ->
                            {error, {path_clash, Package}};
                        false ->
                            is_persistent_volume_safe(Path, ArtifactDir,
                                                      Artifacts)
                    end;
                {error, _Reason} = Error ->
                    Error
            end
    end;
is_persistent_volume_safe(_Path, _ArtifactDir, []) ->
    ok.

-spec find_riak_root_path([string()]) -> string().
find_riak_root_path([]) ->
    ?DEFAULT_RIAK_ROOT_PATH;
find_riak_root_path([Path | Paths]) ->
    case lists:suffix(?RIAK_BIN, Path) of
        true ->
            %% Strip the known tail, leave only the prefix.
            find_prefix(Path, ?RIAK_BIN);
        false ->
            find_riak_root_path(Paths)
    end.

-spec find_prefix(string(), string()) -> string().
find_prefix(FullPath, Tail) ->
    % We know that FullPath = Prefix ++ Tail
    % How to find Prefix?
    SplitPath = filename:split(FullPath),
    SplitTail = filename:split(Tail),
    % Reverse the path components
    ReverseSplitTail = lists:reverse(SplitTail),
    ReverseSplitPath = lists:reverse(SplitPath),
    % Find the common path-tail (list-head), reverse and join
    filename:join(lists:reverse(drop_common_prefix(ReverseSplitPath,
                                                   ReverseSplitTail))).

% Drops from A the leading elements common to A and B.
-spec drop_common_prefix([string()], [string()]) -> [string()].
drop_common_prefix([Component | Path1], [Component | Path2]) ->
    drop_common_prefix(Path1, Path2);
drop_common_prefix([], _) ->
    [];
drop_common_prefix(Path1, _) ->
    Path1.
