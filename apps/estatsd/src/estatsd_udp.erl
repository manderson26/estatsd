-module(estatsd_udp).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(to_int(Value), list_to_integer(binary_to_list(Value))).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state, {port          :: non_neg_integer(),
                socket        :: inet:socket(),
                batch = []    :: [binary()],
                batch_max     :: non_neg_integer(),
                batch_max_age :: non_neg_integer()
               }).

init([]) ->
    {ok, Port} = application:get_env(estatsd, udp_listen_port),
    {ok, RecBuf} = application:get_env(estatsd, udp_recbuf),
    {ok, BatchMax} = application:get_env(estatsd, udp_max_batch_size),
    {ok, BatchAge} = application:get_env(estatsd, udp_max_batch_age),
    error_logger:info_msg("estatsd will listen on UDP ~p with recbuf ~p~n",
                          [Port, RecBuf]),
    error_logger:info_msg("batch size ~p with max age of ~pms~n",
                          [BatchMax, BatchAge]),
    {ok, Socket} = gen_udp:open(Port, [binary, {active, once},
                                       {recbuf, RecBuf}]),
    {ok, #state{port = Port, socket = Socket,
                batch = [],
                batch_max = BatchMax,
                batch_max_age = BatchAge}}.

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({udp, Socket, _Host, _Port, Bin},
            #state{batch=Batch, batch_max=Max}=State) when length(Batch) == Max ->
    error_logger:info_msg("spawn batch ~p FULL~n", [Max]),
    start_batch_worker(Batch),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{batch=[Bin]}};
handle_info({udp, Socket, _Host, _Port, Bin}, #state{batch=Batch,
                                                     batch_max_age=MaxAge}=State) ->
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{batch=[Bin|Batch]}, MaxAge};
handle_info(timeout, #state{batch=Batch}=State) ->
    error_logger:info_msg("spawn batch ~p TIMEOUT~n", [length(Batch)]),
    start_batch_worker(Batch),
    {noreply, State#state{batch=[]}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
start_batch_worker(Batch) ->
    %% Make sure we process messages in the order received
    proc_lib:spawn(fun() -> handle_messages(lists:reverse(Batch)) end).

handle_messages(Batch) ->
    [ handle_message(M) || M <- Batch ],
    ok.

handle_message(Bin) ->
    try
        [Header | Lines] = binary:split(Bin, <<"\n">>, [global]),
	case binary:split(Header, [<<":">>, <<"|">>], [global]) of
	    [_MsgCount, _Length] ->
		%% new format with length and size
%		MsgCount = length(Lines-1), % trailing empty line expected
		[ parse_line(L) || L <- Lines ];
	    [_Key, _Value, _Type] ->
		% old format w/o length and size
		[ parse_line(L) || L <- [Header | Lines] ]
	end,
        ok
    catch
        error:Why ->
            error_logger:error_report({error, "handle_message failed",
                                       Bin, Why, erlang:get_stacktrace()})
    end.

parse_line(<<>>) ->
    skip;
parse_line(Bin) ->
    [Key, Value, Type] = binary:split(Bin, [<<":">>, <<"|">>], [global]),
    send_metric(Type, Key, Value),
    ok.

send_metric(Type, Key, Value) ->
    FolsomKey = folsom_key_name(Type, Key),
    {ok, Status} = estatsd_folsom:ensure_metric(FolsomKey, Type),
    send_folsom_metric(Status, Type, FolsomKey, Value),
    send_estatsd_metric(Type, Key, Value).

send_estatsd_metric(Type = <<"ms">>, Key, Value) ->
    estatsd:timing(Key, convert_value(Type, Value));
send_estatsd_metric(Type = <<"c">>, Key, Value) ->
    estatsd:increment(Key, convert_value(Type, Value));
send_estatsd_metric(_Type, _Key, _Value) ->
    % if it isn't one of the above types, we ignore the request.
    ignored.

send_folsom_metric(blacklisted, _, _, _) ->
    skipped;
send_folsom_metric({ok, _}, Type = <<"c">>, Key, Value) ->
    % TODO: avoid event handler, go direct to folsom
    folsom_metrics_meter:mark(Key, convert_value(Type, Value));
send_folsom_metric({ok, _}, Type = <<"ms">>, Key, Value) ->
    folsom_metrics_histogram:update(Key, convert_value(Type, Value));
send_folsom_metric({ok, _}, Type = <<"mr">>, Key, Value) ->
    folsom_metrics_meter_reader:mark(Key, convert_value(Type, Value));
send_folsom_metric({ok, _}, Type, Key, Value) ->
    folsom_metrics:notify({Key, convert_value(Type, Value)}).

convert_value(<<"e">>, Value) ->
    Value;
convert_value(_Type, Value) ->
    ?to_int(Value).

folsom_key_name(<<"c">>, Key) ->
    iolist_to_binary([<<"stats.">>, Key]);
folsom_key_name(<<"ms">>, Key) ->
    iolist_to_binary([<<"stats.timers.">>, Key]);
folsom_key_name(_Type, Key) ->
    Key.
