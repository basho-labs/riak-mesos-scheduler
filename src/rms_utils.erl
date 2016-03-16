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

-module(rms_utils).

-export([list_to_ranges/1]).

%% External functions.

-spec list_to_ranges([non_neg_integer()]) ->
    [{non_neg_integer(), non_neg_integer()}].
list_to_ranges([]) ->
    [];
list_to_ranges([Begin | List]) ->
    list_to_ranges(List, {Begin, Begin}, []).

%% Internal functions.

-spec list_to_ranges([non_neg_integer()],
                     {non_neg_integer(), non_neg_integer()},
                     [{non_neg_integer(), non_neg_integer()}]) ->
    [{non_neg_integer(), non_neg_integer()}].
list_to_ranges([End1 | List], {Begin, End}, Ranges)
  when End1 == End + 1 ->
    list_to_ranges(List, {Begin, End1}, Ranges);
list_to_ranges([Begin | List], ValueRange, Ranges) ->
    list_to_ranges(List, {Begin, Begin}, [ValueRange | Ranges]);
list_to_ranges([], ValueRange, Ranges) ->
    lists:reverse([ValueRange | Ranges]).
