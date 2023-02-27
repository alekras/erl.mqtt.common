%% @author alexei
%% @doc @todo Add description to mock_tcp.

-module(mock_tcp).

-include_lib("eunit/include/eunit.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/0, stop/0, set_expectation/1, connect/4, send/2, close/1, loop/1]).

start() ->
	Pid = spawn_link(?MODULE, loop, [{self(), [undefined]}]),
	register(mock_tcp_srv, Pid),
	ok.

stop() -> 
	mock_tcp_srv ! stop,
	unregister(mock_tcp_srv).

set_expectation(Expect) when is_list(Expect)->
	mock_tcp_srv ! {expect, self(), Expect};
set_expectation(Expect) ->
	mock_tcp_srv ! {expect, self(), [Expect]}.

connect(Host, Port, Options, Timeout) ->
	io:format(user, "~n >>> mock_tcp:connect(~p, ~p, ~p, ~p)~n", [Host, Port, Options, Timeout]),
	{ok, list_to_pid("<0.7.7>")}.

send(_Socket, Binary) ->
%	io:format(user, "~n >>> mock_tcp:send(~p, ~p)~n", [_Socket, Binary]),
	mock_tcp_srv ! Binary,
	ok.

close(_Socket) -> ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Implementation of mock server
loop({Pid, []} = _State) ->
%	io:format(user, "~n >>> mock_tcp:loop() = ~256p~n", [_State]),
	loop({Pid, [undefined]});
loop({Pid, [ExpectValue | EVList]} = State) ->
	receive
		stop -> ok;
		{expect, Caller, Expect} -> 
			loop({Caller, Expect});
		Msg -> 
			case ExpectValue of
				undefined -> loop(State);
				_ ->
					if ExpectValue == Msg -> 
								io:format(user, "~n Sent to mock = ~256p~n", [Msg]),
								Pid ! {mock_tcp, true};
						 true ->
								io:format(user, "~n Expected = ~256p~n    Value = ~256p~n", [ExpectValue, Msg]),
								Pid ! {mock_tcp, false}
%								?assert(false)
					end,
					loop({Pid, EVList})
			end
	end.
