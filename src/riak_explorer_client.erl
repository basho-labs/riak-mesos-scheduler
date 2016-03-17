
-module(riak_explorer_client).

-export([
         aae_status/2,
         status/2,
         ringready/2,
         transfers/2,
         bucket_types/2,
         create_bucket_type/4,
         join/3,
         leave/3
        ]).

%% @doc Gets AAE status for a node from Explorer.
-spec aae_status(binary(), binary()) ->
    {ok, binary()} | {error, term()}.
aae_status(Host, Node) ->
    ReqUri = <<"control/nodes/", Node/binary, "/aae-status">>,
    do_get(Host, ReqUri).

%% @doc Gets status for a node from Explorer.
-spec status(binary(), binary()) ->
    {ok, binary()} | {error, term()}.
status(Host, Node) ->
    ReqUri = <<"control/nodes/", Node/binary, "/status">>,
    do_get(Host, ReqUri).

%% @doc Gets status for a node from Explorer.
-spec ringready(binary(), binary()) ->
    {ok, binary()} | {error, term()}.
ringready(Host, Node) ->
    ReqUri = <<"control/nodes/", Node/binary, "/ringready">>,
    do_get(Host, ReqUri).

%% @doc Gets status for a node from Explorer.
-spec transfers(binary(), binary()) ->
    {ok, binary()} | {error, term()}.
transfers(Host, Node) ->
    ReqUri = <<"control/nodes/", Node/binary, "/transfers">>,
    do_get(Host, ReqUri).

%% @doc Gets status for a node from Explorer.
-spec bucket_types(binary(), binary()) ->
    {ok, binary()} | {error, term()}.
bucket_types(Host, Node) ->
    ReqUri = <<"explore/nodes/", Node/binary, "/bucket_types">>,
    do_get(Host, ReqUri).

%% @doc Gets status for a node from Explorer.
-spec create_bucket_type(binary(), binary(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
create_bucket_type(Host, Node, Type, Props) ->
    ReqUri = <<"explore/nodes/", Node/binary, "/bucket_types/", Type/binary>>,
    do_put(Host, ReqUri, Props).

%% @doc Gets status for a node from Explorer.
-spec join(binary(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
join(Host, FromNode, ToNode) ->
    ReqUri = <<"control/nodes/", FromNode/binary, "/join/", ToNode/binary>>,
    do_get(Host, ReqUri).

%% @doc Gets status for a node from Explorer.
-spec leave(binary(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
leave(Host, StayingNode, LeavingNode) ->
    ReqUri = <<"control/nodes/", StayingNode/binary, "/leave/", LeavingNode/binary>>,
    do_get(Host, ReqUri).

%% @doc Returns request options.
%% @private
-spec request_options() -> [{atom(), term()}].
request_options() ->
    [].

%% @doc Returns request url.
%% @private
-spec request_url(binary(), binary()) -> binary().
request_url(Host, Uri) ->
    <<"http://", Host/binary, "/admin/", Uri/binary>>.

%% @doc Returns request headers.
%% @private
-spec request_headers(erl_mesos_data_format:data_format()) ->
    erl_mesos_http:headers().
request_headers(ContentType) ->
    [{<<"Content-Type">>, ContentType},
     {<<"Accept">>, ContentType},
     {<<"Connection">>, <<"close">>}].

%% @doc Sends http request.
-spec request(atom(), binary(), [{binary(), binary()}], binary(), [{atom(), term()}]) ->
    {ok, hackney:client_ref()} | {ok, non_neg_integer(), [{binary(), binary()}], hackney:client_ref()} |
    {error, term()}.
request(Method, Url, Headers, Body, Options) ->
    hackney:request(Method, Url, Headers, Body, Options).

%% @doc Perform get and return body.
%% @private
-spec do_get(binary(), binary()) ->
    {ok, binary()} | {error, term()}.
do_get(Host, Uri) ->
    ReqUrl = request_url(Host, Uri),
    ReqHeaders = request_headers(<<"application/json">>),
    ReqBody = <<>>,
    ReqOptions = request_options(),
    case request(get, ReqUrl, ReqHeaders, ReqBody, ReqOptions) of
        {ok, Ref} ->
            body(Ref);
        {ok, _Code, _Headers, Ref} ->
            body(Ref);
        {error, _}=Error ->
            Error
    end.

%% @doc Perform get and return body.
%% @private
-spec do_put(binary(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
do_put(Host, Uri, Data) ->
    ReqUrl = request_url(Host, Uri),
    ReqHeaders = request_headers(<<"application/json">>),
    ReqOptions = request_options(),
    case request(put, ReqUrl, ReqHeaders, Data, ReqOptions) of
        {ok, Ref} ->
            body(Ref);
        {ok, _Code, _Headers, Ref} ->
            body(Ref);
        {error, _}=Error ->
            Error
    end.

%% @doc Receives http request body.
%% @private
-spec body(hackney:client_ref()) -> {ok, binary()} | {error, term()}.
body(Ref) ->
    hackney:body(Ref).

