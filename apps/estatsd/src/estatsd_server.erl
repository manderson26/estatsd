%% Stats aggregation process that periodically dumps data to graphite
%% Will calculate 90th percentile etc.
%% Inspired by etsy statsd:
%% http://codeascraft.etsy.com/2011/02/15/measure-anything-measure-everything/
%%
%% This could be extended to take a callback for reporting mechanisms.
%% Right now it's hardcoded to stick data into graphite.
%%
%% Richard Jones <rj@metabrew.com>
%%
-module(estatsd_server).
-behaviour(gen_server).

-export([start_link/0]).

%-export([key2str/1,flush/0]). %% export for debugging 

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
         terminate/2, code_change/3]).

-record(state, {timers,             % gb_tree of timer data
                flush_interval,     % ms interval between stats flushing
                flush_timer,        % TRef of interval timer
                graphite_host,      % graphite server host
                graphite_port       % graphite server port
               }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%

init([]) ->
    {ok, FlushIntervalMs} = application:get_env(estatsd, flush_interval),
    {ok, GraphiteHost} = application:get_env(estatsd, graphite_host),
    {ok, GraphitePort} = application:get_env(estatsd, graphite_port),
    error_logger:info_msg("estatsd will flush stats to ~p:~w every ~wms\n", 
                          [ GraphiteHost, GraphitePort, FlushIntervalMs ]),
    ets:new(statsd, [named_table, set]),
    %% Flush out stats to graphite periodically
    {ok, Tref} = timer:apply_interval(FlushIntervalMs, gen_server, cast, 
                                                       [?MODULE, flush]),
    State = #state{ timers          = gb_trees:empty(),
                    flush_interval  = FlushIntervalMs,
                    flush_timer     = Tref,
                    graphite_host   = GraphiteHost,
                    graphite_port   = GraphitePort
                  },
    {ok, State}.

handle_cast({increment, Key, Delta0, Sample}, State) when Sample >= 0, Sample =< 1 ->
    Delta = Delta0 * ( 1 / Sample ), %% account for sample rates < 1.0
    case ets:lookup(statsd, Key) of
        [] ->
            ets:insert(statsd, {Key, {Delta,1}});
        [{Key,{Tot,Times}}] ->
            ets:insert(statsd, {Key,{Tot+Delta, Times+1}}),
            ok
    end,
    {noreply, State};

handle_cast({timing, Key, Duration}, State) ->
    case gb_trees:lookup(Key, State#state.timers) of
        none ->
            {noreply, State#state{timers = gb_trees:insert(Key, [Duration], State#state.timers)}};
        {value, Val} ->
            {noreply, State#state{timers = gb_trees:update(Key, [Duration|Val], State#state.timers)}}
    end;

handle_cast(flush, State) ->
    All = ets:tab2list(statsd),
    spawn( fun() -> do_report(All, State) end ),
    %% WIPE ALL
    ets:delete_all_objects(statsd),
    NewState = State#state{timers = gb_trees:empty()},
    {noreply, NewState}.

handle_call(_,_,State)      -> {reply, ok, State}.

handle_info(_Msg, State)    -> {noreply, State}.

code_change(_, _, State)    -> {noreply, State}.

terminate(_, _)             -> ok.

%% INTERNAL STUFF

send_to_graphite(Msg, State) ->
    error_logger:info_msg("sending data to graphite~n"),
    % io:format("SENDING: ~s\n", [Msg]),
    case gen_tcp:connect(State#state.graphite_host,
                         State#state.graphite_port,
                         [list, {packet, 0}]) of
        {ok, Sock} ->
            gen_tcp:send(Sock, Msg),
            gen_tcp:close(Sock),
            ok;
        E ->
            %error_logger:error_msg("Failed to connect to graphite: ~p", [E]),
            E
    end.

% this string munging is damn ugly compared to javascript :(
key2str(K) when is_atom(K) -> 
    atom_to_list(K);
key2str(K) when is_binary(K) -> 
    key2str(binary_to_list(K));
key2str(K) when is_list(K) ->
    {ok, R1} = re:compile("\\s+"),
    {ok, R2} = re:compile("/"),
    {ok, R3} = re:compile("[^a-zA-Z_\\-0-9\\.]"),
    Opts = [global, {return, list}],
    S1 = re:replace(K,  R1, "_", Opts),
    S2 = re:replace(S1, R2, "-", Opts),
    S3 = re:replace(S2, R3, "", Opts),
    S3.

num2str(NN) -> lists:flatten(io_lib:format("~w",[NN])).

unixtime()  -> {Meg,S,_Mic} = erlang:now(), Meg*1000000 + S.

%% Aggregate the stats and generate a report to send to graphite
do_report(All, State) ->
    % One time stamp string used in all stats lines:
    TsStr = num2str(unixtime()),
    {MsgCounters, NumCounters} = do_report_counters(All, TsStr, State),
    {MsgTimers,   NumTimers}   = do_report_timers(TsStr, State),
    %% REPORT TO GRAPHITE
    case NumTimers + NumCounters of
        0 -> nothing_to_report;
        NumStats ->
            FinalMsg = [ MsgCounters,
                         MsgTimers,
                         %% Also graph the number of graphs we're graphing:
                         "statsd.numStats ", num2str(NumStats), " ", TsStr, "\n"
                       ],
            send_to_graphite(FinalMsg, State)
    end.

do_report_counters(All, TsStr, State) ->
    Msg = lists:foldl(
                fun({Key, {Val0,NumVals}}, Acc) ->
                        KeyS = key2str(Key),
                        Val = Val0 / (State#state.flush_interval/1000),
                        %% Build stats string for graphite
                        Fragment = [ "stats.", KeyS, " ", 
                                     io_lib:format("~w", [Val]), " ", 
                                     TsStr, "\n",

                                     "stats_counts.", KeyS, " ", 
                                     io_lib:format("~w",[NumVals]), " ", 
                                     TsStr, "\n"
                                   ],
                        [ Fragment | Acc ]                    
                end, [], All),
    {Msg, length(All)}.

do_report_timers(TsStr, State) ->
    Timings = gb_trees:to_list(State#state.timers),
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
                KeyS = key2str(Key),
                Values          = lists:sort(Vals),
                Count           = length(Values),
                Min             = hd(Values),
                Max             = lists:last(Values),
                PctThreshold    = 90,
                ThresholdIndex  = erlang:round(((100-PctThreshold)/100)*Count),
                NumInThreshold  = Count - ThresholdIndex,
                Values1         = lists:sublist(Values, NumInThreshold),
                MaxAtThreshold  = lists:nth(NumInThreshold, Values),
                Mean            = lists:sum(Values1) / NumInThreshold,
                %% Build stats string for graphite
                Startl          = [ "stats.timers.", KeyS, "." ],
                Endl            = [" ", TsStr, "\n"],
                Fragment        = [ [Startl, Name, " ", num2str(Val), Endl] || {Name,Val} <-
                                  [ {"mean", Mean},
                                    {"upper", Max},
                                    {"upper_"++num2str(PctThreshold), MaxAtThreshold},
                                    {"lower", Min},
                                    {"count", Count}
                                  ]],
                [ Fragment | Acc ]
        end, [], Timings),
    {Msg, length(Msg)}.
