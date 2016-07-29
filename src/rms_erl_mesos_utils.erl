-module(rms_erl_mesos_utils).

-export([
         environment_variable/2,
         environment/1,
         set_command_info_environment/2
        ]).

-include_lib("erl_mesos/include/scheduler_protobuf.hrl").

set_command_info_environment(#'CommandInfo'{}=CI0, #'Environment'{}=E0) ->
    CI0#'CommandInfo'{environment = E0}.

environment_variable(Name, Value) ->
    #'Environment.Variable'{
       name = Name,
       value = Value
    }.

environment(Variables) ->
    #'Environment'{
       variables = Variables
      }.
