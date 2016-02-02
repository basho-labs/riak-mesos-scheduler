-module(riak_mesos_utils_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([all/0]).

-export([list_to_ranges/1]).

all() ->
    [list_to_ranges].

list_to_ranges(_Config) ->
    [] = riak_mesos_utils:list_to_ranges([]),
    [{1, 1}] = riak_mesos_utils:list_to_ranges([1]),
    [{1, 2}] = riak_mesos_utils:list_to_ranges([1, 2]),
    [{1, 4}, {6, 6}, {9, 11}] =
        riak_mesos_utils:list_to_ranges([1, 2, 3, 4, 6, 9, 10, 11]).
