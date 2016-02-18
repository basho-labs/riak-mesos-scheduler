-module(riak_mesos_utils).

-export([list_to_ranges/1]).

%% ====================================================================
%% API
%% ====================================================================

list_to_ranges([]) ->
    [];
list_to_ranges([Begin | List]) ->
    list_to_ranges(List, {Begin, Begin}, []).

%% ====================================================================
%% Private
%% ====================================================================

list_to_ranges([End1 | List], {Begin, End}, Ranges)
  when End1 == End + 1 ->
    list_to_ranges(List, {Begin, End1}, Ranges);
list_to_ranges([Begin | List], ValueRange, Ranges) ->
    list_to_ranges(List, {Begin, Begin}, [ValueRange | Ranges]);
list_to_ranges([], ValueRange, Ranges) ->
    lists:reverse([ValueRange | Ranges]).
