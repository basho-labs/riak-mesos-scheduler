-module(riak_explorer_client_test).

-include_lib("eunit/include/eunit.hrl").

riak_explorer_client_test_() ->
    application:ensure_all_started(hackney),
    case hackney:request(get,
                         <<"http://localhost:8098/admin">>, 
                         [], <<>>, []) of
        {ok, 200, _, _} ->
            SetupFun = fun() ->
                               ok
                       end,
            TeardownFun = fun(_) ->
                                 ok
                          end,
            {foreach,
             SetupFun,
             TeardownFun,
             [
              fun aae_status/0
              %% fun status/0,
              %% fun ringready/0,
              %% fun transfers/0,
              %% fun bucket_types/0,
              %% fun create_bucket_type/0,
              %% fun join/0,
              %% fun leave/0
             ]};
        _ ->
            {}
    end.

aae_status() ->
    {ok, Res} = riak_explorer_client:aae_status(<<"localhost:8098">>, <<"riak@127.0.0.1">>),
    ?assertMatch({struct, [{<<"aae-status">>, {struct, [{<<"exchanges">>, _}]}},_]}, mochijson2:decode(Res)).
