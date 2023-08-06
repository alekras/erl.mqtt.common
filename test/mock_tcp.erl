%% @author alexei
%% @doc @todo Add description to mock_tcp.

-module(mock_tcp).

-include_lib("eunit/include/eunit.hrl").
-include("test.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	start/0, 
	stop/0, 
	set_expectation/1, 
	connect/4, 
	send/2, 
	controlling_process/2, 
	close/1, 
	loop/1, 
	wait_mock_tcp/1,
	wait_mock_tcp/2,
	wait_no_mock_tcp/1
]).

start() ->
	Pid = spawn_link(?MODULE, loop, [{self(), [undefined]}]),
	register(mock_tcp_srv, Pid),
	?debug_Fmt("~n::test:: MOCK_TCP_SRV process is started.~n         (PID: ~p, registered under mock_tcp_srv). Parent process:~p~n", [Pid, self()]),
	ok.

stop() -> 
	mock_tcp_srv ! stop,
	unregister(mock_tcp_srv).

set_expectation(Expect) when is_list(Expect)->
	mock_tcp_srv ! {expect, self(), Expect};
set_expectation(Expect) ->
	mock_tcp_srv ! {expect, self(), [Expect]}.

connect(_Host, _Port, _Options, _Timeout) ->
%%	?debug_Fmt("~n >>> mock_tcp:connect(~p, ~p, ~p, ~p)~n", [_Host, _Port, _Options, _Timeout]),
	{ok, list_to_port("#Port<0.7>")}.

send(_Socket, Binary) ->
%	?debug_Fmt("~n >>> mock_tcp:send(~p, ~p)~n", [_Socket, Binary]),
	mock_tcp_srv ! Binary,
	ok.

controlling_process(_Socket, _Pid) ->
	ok.

close(_Socket) -> ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Implementation of mock server
loop({Pid, ExpList}) ->
	receive
		stop -> ok;
		{expect, Caller, Expect} -> 
			loop({Caller, Expect});
		Msg -> 
			IsMember = lists:member(Msg, ExpList),
			if IsMember -> 
%%						?debug_Fmt("~n Mock expected = ~256p~n", [ExpectValue]),
					?debug_Fmt("~n::test:: Mock received msg = ~256p~n", [Msg]),
					Pid ! {mock_tcp, true},
					loop({Pid, lists:delete(Msg, ExpList)});
				true ->
					?debug_Fmt("~n**test** Received msg = ~p~n not in list of expected = ~n~256p~n", [Msg, ExpList]),
					Pid ! {mock_tcp, false},
%					?assert(false)
					loop({Pid, []})
			end
	end.

wait_mock_tcp(R) ->
	wait_mock_tcp(R, fun(M) ->
			?debug_Fmt("**test** while waiting ~p mock_tcp got unexpected msg = ~p~n", [R, M]),
			?assert(false) end
	).

wait_mock_tcp(R, Func) ->
	receive
		{mock_tcp, true} ->
			?debug_Fmt("::test:: mock_tcp ~p acknowledge~n", [R]),
			?assert(true);
		{mock_tcp, false} ->
			?assert(false);
		M ->
			Func(M)
%% 			?debug_Fmt("**test** while waiting ~p mock_tcp got unexpected msg = ~p~n", [R, M]),
%% 			?assert(false)
	after 200 ->
			?debug_Fmt("**test** Timeout while waiting ~p from mock_tcp~n", [R]),
			?assert(false)
	end.

wait_no_mock_tcp(R) ->
	receive
		{mock_tcp, true} ->
			?debug_Fmt("**test** while waiting ~p mock_tcp got unexpected msg~n", [R]),
			?assert(false);
		{mock_tcp, false} ->
			?assert(false);
		M ->
			?debug_Fmt("**test** while waiting ~p mock_tcp got unexpected msg = ~p~n", [R, M]),
			?assert(false)
	after 200 ->
			?debug_Fmt("::test:: mock_tcp ~p acknowledge (expected timeout)~n", [R]),
			?assert(true)
	end.
