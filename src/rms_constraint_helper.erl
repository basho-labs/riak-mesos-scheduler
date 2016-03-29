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

-export([new/3,
         can_accept/2]).

-record(constraint, {field :: string(),
                     operator :: string(),
                     parameter :: string()}).

-type constraint() :: #constraint{}.

-record(constraint_helper, {cluster_constraints :: [constraint()],
                            cluster_hostnames :: [string()],
                            cluster_attributes :: [{string(), string()}]
                            }).

-type constraint_helper() :: #constraint_helper{}.
-export_type([constraint_helper/0]).

new(RawConstraints, ClusterHostnames, ClusterAttributes) ->
    ClusterConstraints = parse_constraints(RawConstraints, []),
    #constraint_helper{
       cluster_constraints=ClusterConstraints,
       cluster_hostnames=ClusterHostnames,
       cluster_attributes=ClusterAttributes
      }.

parse_constraints([], Accum) ->
    Accum;
parse_constraints([[Field, Operator]|Rest], Accum) ->
    parse_constraints(Rest, 
                      [#constraint{
                          field=Field,
                          operator=Operator}|Accum]);
parse_constraints([[Field, Operator, Parameter]|Rest], Accum) ->
    parse_constraints(Rest, 
                      [#constraint{
                          field=Field,
                          operator=Operator,
                          parameter=Parameter}|Accum]).

can_accept(#'Offer'{hostname=Hostname, attributes=RawAttributes},
           #constraint_helper{cluster_constraints=Constraints}=Helper) ->
    Attributes = parse_attributes(RawAttributes, []),
    process_constraints(Constraints, Hostname, Attributes, Helper).

parse_attributes([#'Attribute'{
                     name=Name, 
                     type='SCALAR', 
                     scalar=#'Value.Scalar'{value=Value}}|Rest], Accum) ->
    parse_attributes(Rest, [{Name, Value}|Accum]);
parse_attributes([#'Attribute'{
                     type='RANGES', 
                     ranges=#'Value.Scalar'{}}|Rest], Accum) ->
    %% TODO: Deal with range attributes
    parse_attributes(Rest, Accum);
parse_attributes([#'Attribute'{
                     name=Name, 
                     type='SET', 
                     scalar=#'Value.Set'{item=Value}}|Rest], Accum) ->
    parse_attributes(Rest, [{Name, Value}|Accum]);
parse_attributes([#'Attribute'{
                     name=Name, 
                     type='TEXT', 
                     scalar=#'Value.Text'{value=Value}}|Rest], Accum) ->
    parse_attributes(Rest, [{Name, Value}|Accum]).

process_constraints([], _, _, _) ->
    true;
process_constraints([#constraint{
                        field="hostname", 
                        operator="UNIQUE",
                        parameter=undefined}|Rest], 
                    Hostname, Attributes, 
                    #constraint_helper{
                       cluster_hostnames=ClusterHostnames
                      }=Helper) ->
    case lists:member(Hostname, ClusterHostnames) of
        true -> false;
        false -> process_constraints(Rest, Hostname, Attributes, Helper)
    end;
process_constraints([#constraint{
                        field="hostname", 
                        operator="CLUSTER",
                        parameter=Parameter}|Rest], 
                    Hostname, Attributes, Helper) ->
    case Parameter =:= Hostname of
        true -> process_constraints(Rest, Hostname, Attributes, Helper);
        false -> false
    end;
process_constraints([#constraint{
                        field=Name, 
                        operator="CLUSTER",
                        parameter=Parameter}|Rest], 
                    Hostname, Attributes, Helper) ->
    case proplists:get_value(Name, Attributes) of
        Parameter -> process_constraints(Rest, Hostname, Attributes, Helper);
        _ -> false
    end;
process_constraints([#constraint{
                        field=Name,
                        operator="GROUP_BY",
                        parameter=ParameterStr}|Rest], 
                    Hostname, Attributes, 
                    #constraint_helper{
                       cluster_attributes=ClusterAttributes
                      }=Helper) ->
    Parameter = list_to_integer(ParameterStr),
    case group_by(ClusterAttributes, Attributes, Name, Parameter) of
        true -> process_constraints(Rest, Hostname, Attributes, Helper);
        false -> false
    end.

group_by(_ClusterAttributes, _Attributes, _Name, undefined) ->
    %% We don't have a way of knowing how many available distinct values there are,
    %% So basically always return true, but... return EXTRA true if the offer's
    %% attribute value for Name is unique!
    true;
group_by(_ClusterAttributes, _Attributes, _Name, _Parameter) ->
    %% TODO: attempt to find a suggested attribute value, 
    %% and then match it against the attribute value from the offer
    true.



%% {'Event.Offers',[

%% {'Offer',
%%   id={'OfferID',"9af79c87-3ba0-4aac-99c6-49a8a07343ab-O493"},
%%   framework_id={'FrameworkID',"293cad27-15ce-4480-bacd-e6c075c8fdbe-0006"},
%%   agent_id={'AgentID',"293cad27-15ce-4480-bacd-e6c075c8fdbe-S0"},
%%   hostname="ubuntu.local",
%%   url={'URL',
%%     scheme="http",
%%     address={'Address',"ubuntu.local","127.0.1.1",5051},
%%     path="/slave(1)",
%%     query=[],
%%     fragment=undefined},
%%   resources=[
%%     {'Resource',"cpus",'SCALAR',{'Value.Scalar',2.6999999999999997},undefined,undefined,"*",undefined,undefined,undefined},
%%     {'Resource',"mem",'SCALAR',{'Value.Scalar',4531.0},undefined,undefined,"*",undefined,undefined,undefined},
%%     {'Resource',"disk",'SCALAR',{'Value.Scalar',27164.0},undefined,undefined,"*",undefined,undefined,undefined},
%%     {'Resource',"ports",'RANGES',undefined,{'Value.Ranges',[{'Value.Range',31000,31162},{'Value.Range',31164,31491},{'Value.Range',31502,31936},{'Value.Range',31947,32000}]},undefined,"*",undefined,undefined,undefined}
%%   ],
%%   attributes=[
%%     {'Attribute',
%%       name="something",
%%       type='TEXT'|'SCALAR'|'RANGES'|'SET',
%%       scalar={'Value.Scalar',2.6999999999999997},
%%       ranges={'Value.Ranges',[{'Value.Range',31000,31162},{'Value.Range',31164,31491},{'Value.Range',31502,31936},{'Value.Range',31947,32000}]},
%%       set={'Value.Set',
%%              item=["something1", "something2", "something3"]}
%%       text={'Value.Text',
%%         value="something"}],
%%   executor_ids=[
%%     {'ExecutorID',"riak-default-8"},
%%     {'ExecutorID',"riak-default-7"}
%%   ],undefined}],
%%   unavailability=[]}.
