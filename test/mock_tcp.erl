%% @author alexei
%% @doc @todo Add description to mock_tcp.

-module(mock_tcp).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/0, stop/0, send/2, close/1, loop/1]).

start() ->
	Pid = spawn_link(?MODULE, loop, [undefined]),
	register(mock_tcp_srv, Pid),
	ok.

stop() -> ok.

send(_Socket, Binary) ->
	io:format(user, "~n >>> mock_tcp:send(~p, ~p)~n", [_Socket, Binary]),
	mock_tcp_srv ! Binary,
	ok.

close(_Socket) -> ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

loop(State) ->
	receive
		Msg -> io:format(user, "~n >>> mock_tcp:loop(~p), Message=~p)~n", [State, Msg])
	end,
	loop(State).
