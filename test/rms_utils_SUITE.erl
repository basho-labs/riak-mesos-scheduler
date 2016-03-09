-module(rms_utils_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([all/0]).

-export([list_to_ranges/1]).

all() ->
    [list_to_ranges].

list_to_ranges(_Config) ->
    [] = rms_utils:list_to_ranges([]),
    [{1, 1}] = rms_utils:list_to_ranges([1]),
    [{1, 2}] = rms_utils:list_to_ranges([1, 2]),
    [{1, 4}, {6, 6}, {9, 11}] =
        rms_utils:list_to_ranges([1, 2, 3, 4, 6, 9, 10, 11]).
