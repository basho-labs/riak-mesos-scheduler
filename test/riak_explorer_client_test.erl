-module(riak_explorer_client_test).

-include_lib("eunit/include/eunit.hrl").

riak_explorer_client_test_() ->
    SetupFun = fun() ->
                       ok
               end,
    TeardownFun = fun(_) ->
                          ok
                  end,
    application:ensure_all_started(hackney),
    case hackney:request(get,
                         <<"http://localhost:8098/admin">>, 
                         [], <<>>, []) of
        {ok, 200, _, _} ->
            {foreach,
             SetupFun,
             TeardownFun,
             [
              fun aae_status/0,
              fun status/0,
              fun ringready/0,
              fun transfers/0,
              fun bucket_types/0
              %% fun create_bucket_type/0,
              %% fun join/0,
              %% fun leave/0
             ]};
        _ ->
            {foreach,
             SetupFun,
             TeardownFun,
             []}
        end.

aae_status() ->
    {ok, Res} = riak_explorer_client:aae_status(<<"localhost:8098">>, <<"riak@127.0.0.1">>),
    ?assertMatch({struct,[{<<"aae-status">>,_},_]}, mochijson2:decode(Res)).

status() ->
    {ok, Res} = riak_explorer_client:status(<<"localhost:8098">>, <<"riak@127.0.0.1">>),
    ?assertMatch({struct,[{<<"status">>,_},_]}, mochijson2:decode(Res)).

ringready() ->
    {ok, Res} = riak_explorer_client:ringready(<<"localhost:8098">>, <<"riak@127.0.0.1">>),
    ?assertMatch({struct,[{<<"ringready">>,_},_]}, mochijson2:decode(Res)).

transfers() ->
    {ok, Res} = riak_explorer_client:transfers(<<"localhost:8098">>, <<"riak@127.0.0.1">>),
    ?assertMatch({struct,[{<<"transfers">>,_},_]}, mochijson2:decode(Res)).

bucket_types() ->
    {ok, Res} = riak_explorer_client:bucket_types(<<"localhost:8098">>, <<"riak@127.0.0.1">>),
    ?assertMatch({struct,[{<<"bucket_types">>,_},_]}, mochijson2:decode(Res)).
