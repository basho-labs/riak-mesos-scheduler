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

-module(rms_constraint_helper).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

-export([can_accept/5,
         attributes_to_list/1]).

-type constraint() :: [string()].
-type attribute() :: {string(), string()}.
-type attributes() :: [attribute()].
-export_type([attributes/0]).

-spec attributes_to_list([erl_mesos:'Attribute'()]) -> attributes().
attributes_to_list(RawAttributes) ->
    attributes_to_list(RawAttributes, []).
    
-spec attributes_to_list([erl_mesos:'Attribute'()],
                         attributes()) -> attributes().
attributes_to_list([], Accum) ->
    Accum;
attributes_to_list([#'Attribute'{
                     name=Name, 
                     type='SCALAR', 
                     scalar=#'Value.Scalar'{value=Value}}|Rest], Accum) ->
    attributes_to_list(Rest, [{Name, Value}|Accum]);
attributes_to_list([#'Attribute'{
                     type='RANGES', 
                     ranges=#'Value.Scalar'{}}|Rest], Accum) ->
    %% TODO: Deal with range attributes
    attributes_to_list(Rest, Accum);
attributes_to_list([#'Attribute'{
                     name=Name, 
                     type='SET', 
                     scalar=#'Value.Set'{item=Value}}|Rest], Accum) ->
    attributes_to_list(Rest, [{Name, Value}|Accum]);
attributes_to_list([#'Attribute'{
                     name=Name, 
                     type='TEXT', 
                     scalar=#'Value.Text'{value=Value}}|Rest], Accum) ->
    attributes_to_list(Rest, [{Name, Value}|Accum]).

-spec can_accept(string(), [erl_mesos:'Attribute'()], 
                 [constraint()], 
                 [string()], 
                 [attributes()]) -> boolean() | maybe.
can_accept(OfferHostname, OfferAttributes, Constraints, NodeHosts, NodeAttributes) ->
    can_accept(OfferHostname, OfferAttributes, Constraints, NodeHosts, NodeAttributes, true).

-spec can_accept(string(), [erl_mesos:'Attribute'()], 
                 [constraint()], 
                 [string()], 
                 [attributes()],
                 true | maybe) -> boolean() | maybe.
can_accept(_, _,[], _, _, Last) ->
    Last;
can_accept(OfferHostname, OfferAttributes, 
           [["hostname"|Constraint]|Rest], 
           NodeHosts, NodeAttributes, Last) ->
    case check_constraint(Constraint, OfferHostname, NodeHosts) of
        false -> 
            false;
        maybe ->
            can_accept(OfferHostname, OfferAttributes, Rest, NodeHosts, 
                       NodeAttributes, maybe);
        true ->
            can_accept(OfferHostname, OfferAttributes, Rest, NodeHosts, 
                       NodeAttributes, Last)
    end;
can_accept(OfferHostname, OfferAttributes, 
           [[Name|Constraint]|Rest], 
           NodeHosts, NodeAttributes, Last) ->
    Attributes = attributes_to_list(OfferAttributes, []),
    A = proplists:get_value(Name, Attributes),
    As = lists:foldl(fun(X, Accum) -> 
                             [proplists:get_value(Name, X)|Accum]
                     end, [], NodeAttributes),
    case check_constraint(Constraint, A, As) of
        false -> 
            false;
        maybe ->
            can_accept(OfferHostname, OfferAttributes, Rest, NodeHosts, 
                       NodeAttributes, maybe);
        true ->
            can_accept(OfferHostname, OfferAttributes, Rest, NodeHosts, 
                       NodeAttributes, Last)
    end.

-spec check_constraint(constraint(), string(), [string()]) -> boolean()|maybe.
check_constraint(["UNIQUE"], V, Vs) -> 
    %% Unique Value
    not lists:member(V, Vs);
check_constraint(["GROUP_BY"], V, Vs) ->
    %% Groupby Value. If the value isn't unique, then attempt to
    %% schedule it on a different offer. If there are no other offers,
    %% go ahead and schedule it on this one. In other words, attempt
    %% to spread across all values, but don't refuse offers.
    case check_constraint(["UNIQUE"], V, Vs) of
        true -> true;
        false -> maybe
    end;
check_constraint(["GROUP_BY", Param], V, Vs) ->
    %% Compare total number of hosts to number of scheduled hosts
    case {list_to_integer(Param), length(Vs)} of
        %% There should still be unclaimed values, wait if not unique
        {N1, N2} when N1 > N2 ->
            check_constraint(["UNIQUE"], V, Vs);
        %% There's a node on every host already, go ahead and use the offer
        _ ->
            %% TODO: get more sophisticated about attempting to spread
            %% nodes evenly, because we already know how many nodes there are
            true
    end;
check_constraint(["CLUSTER", V], V, _) -> 
    %% Cluster on value, values match
    true;
check_constraint(["CLUSTER", _], _, _) -> 
    %% Cluster on value, hosts do not match
    false;
check_constraint(["LIKE", Param], V, _) ->
    %% Value is like regex
    case re:run(V, Param) of
        {match, _} -> true;
        nomatch -> false
    end;
check_constraint(["UNLIKE", Param], V, Vs) -> 
    %% Value is not like regex
    not check_constraint(["LIKE", Param], V, Vs);
check_constraint(_, _, _) -> 
    %% Undefined constraint, just schedule it
    true.
