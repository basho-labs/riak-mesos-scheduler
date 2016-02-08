-module(riak_mesos_config).

-export([get_default_riak_conf/0,
         get_default_advanced_config/0]).

get_default_riak_conf() ->
    read_priv_file("riak.conf.default").

get_default_advanced_config() ->
    read_priv_file("advanced.config.default").

read_priv_file(FileName) ->
    case code:priv_dir(riak_mesos_scheduler) of
        {error, bad_name} ->
            PrivDir = "priv";
        PrivDir ->
            ok
    end,
    {ok, Bin} = file:read_file(filename:join([PrivDir, FileName])),
    binary_to_list(Bin).
