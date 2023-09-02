%%
%% Copyright (C) 2015-2023 by krasnop@bellsouth.net (Alexei Krasnopolski)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License. 
%%

%% @hidden
%% @since 2023-07-10
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for subscribe service side operations.

-module(mqtt_disconnect_srv_tests).
%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
-include_lib("mqtt_property.hrl").
-include("test.hrl").

%%
%% Import modules
%%
-import(mqtt_connection_srv_tests, [
	connect/2
]).
-import(mock_tcp, [wait_mock_tcp/1, wait_no_mock_tcp/1]).

%%
%% Exported Functions
%%
-export([]).

%%
%% API Functions
%%
connect_genServer_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				 {'3.1.1', fun disconnect_test_1/2}
				,{'5.0',   fun disconnect_test_1/2}
				,{'5.0',   fun disconnect_test_2/2}
				,{'3.1.1', fun disconnect_normal_no_will_test/2}
				,{'5.0',   fun disconnect_normal_no_will_test/2}
				,{'3.1.1', fun disconnect_shutdown_with_will_test/2}
				,{'5.0', fun disconnect_shutdown_with_will_test/2}
			]
		}
	 }
	].

do_start() ->
	?debug_Fmt("::test:: >>> do_start() ~n", []),
	lager:start(),

	mqtt_dets_storage:start(server),
	mqtt_dets_storage:cleanup(server),

	Storage = mqtt_dets_storage,
	Storage:user(save, #user{user_id = <<"guest">>, password = <<"guest">>}),

	self().

create_server_process() ->
	Transport = mock_tcp,
	Storage = mqtt_dets_storage,
	Socket = list_to_port("#Port<0.7>"),
	State = #connection_state{socket = Socket, transport = Transport, storage = Storage, end_type = server},
	Pid = proc_lib:spawn(fun() -> mqtt_connection:init(State) end),
	register(conn_server, Pid),
	?debug_Fmt("::test:: Create Server process (conn_server) with Pid=~p and SELF=~p~n", [Pid, self()]),
	Socket.

create_server_subsc_process() ->
	Transport = mock_tcp,
	Storage = mqtt_dets_storage,
	Socket = list_to_port("#Port<0.8>"),
	State = #connection_state{socket = Socket, transport = Transport, storage = Storage, end_type = server},
	Pid = proc_lib:spawn(fun() -> mqtt_connection:init(State) end),
	register(conn_server_subs, Pid),
	?debug_Fmt("::test:: Create Server Subscriber process (conn_server_subs) with Pid=~p and SELF=~p~n", [Pid, self()]),
	Socket.

do_stop(Pid) ->
	?debug_Fmt("::test:: >>> do_stop(~p) ~n", [Pid]),
	mqtt_dets_storage:cleanup(server),	
	mqtt_dets_storage:close(server).	

setup('3.1.1') ->
	?debug_Fmt("::test:: >>> setup('3.1.1')~n", []),
	mock_tcp:start(),
	{create_server_process(), create_server_subsc_process()};
setup('5.0') ->
	?debug_Fmt("::test:: >>> setup('5.0')~n", []),
	mock_tcp:start(),
	{create_server_process(), create_server_subsc_process()}.

cleanup('3.1.1'=X, {Y, Z}) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p,~p) ~n", [X,Y,Z]),
% Close connection - stop the conn_server process.
	case whereis(conn_server_subs) of
		undefined -> ok;
		_ -> 
			case gen_server:call(conn_server_subs, status) of
				[{connected, 1},_,_] ->
					conn_server_subs ! {tcp, Z, <<224,0>>};
				_ -> unregister(conn_server_subs)
			end
	end,
	mqtt_dets_storage:connect_pid(remove, <<"test0Client"/utf8>>, server),
	mqtt_dets_storage:retain(clean, server),

	mock_tcp:stop();
cleanup('5.0'=X, {Y, Z}) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p,~p) ~n", [X,Y,Z]),
% Close connection - stop the conn_server process.
	case whereis(conn_server_subs) of
		undefined -> ok;
		_ -> 
			case gen_server:call(conn_server_subs, status) of
				[{connected, 1},_,_] ->
					mock_tcp:set_expectation(<<224,34,2, 32, 31,29:16,"Initiated by client or server"/utf8>>),
					conn_server_subs ! {tcp, Z, <<224,2,2,0>>},
					wait_mock_tcp("disconnect subs");
				_ -> unregister(conn_server_subs)
			end
	end,
	mqtt_dets_storage:connect_pid(remove, <<"test0Client"/utf8>>, server),
	mqtt_dets_storage:retain(clean, server),

	mock_tcp:stop().

%% ====================================================================
%% API functions
%% ====================================================================

disconnect_test_1('3.1.1' = Version, {Socket, _Socket_Subs}) -> {"Disconnect test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	mock_tcp:set_expectation([<<32,2,0,0>>]), %% Connack packet SP=1 and re-sent pubrec packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket, <<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
		11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack"),

%%	mock_tcp:set_expectation(<<224,0>>),
	conn_server ! {tcp, Socket, <<224,0>>},
%	gen_server:cast(conn_server, {disconnect,0,[]}),
	wait_no_mock_tcp("disconnect"),

	?passed
end};
disconnect_test_1('5.0' = Version, {Socket, _Socket_Subs}) -> {"Disconnect test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	mock_tcp:set_expectation(<<32,8,0,0,5,17,0,0,0,27>>), %% Connack packet
% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,43,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
			5,17,0,0,0,27,
			11:16,"test0Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),

	mock_tcp:set_expectation(
	 <<224,34,2, 32, 31,29:16,"Initiated by client or server"/utf8>>),
	conn_server ! {tcp, Socket, <<224,7,2,5,17,0,0,0,25>>},
	wait_mock_tcp("disconnect"),

	?passed
end}.

disconnect_test_2('5.0' = Version, {Socket, _Socket_Subs}) -> {"Disconnect test 2 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	mock_tcp:set_expectation(<<32,3,0,0,0>>), %% Connack packet
% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,38,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
			0,
			11:16,"test0Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),

	mock_tcp:set_expectation(
	 <<224,19,130, 17, 31,14:16,"Protocol Error"/utf8>>),
	conn_server ! {tcp, Socket, <<224,7,2,5,17,0,0,0,25>>},
	wait_mock_tcp("disconnect"),

	?passed
end}.

disconnect_normal_no_will_test('3.1.1' = Version, {Socket, Socket_Subs}) -> {"Disconnect Will 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p, ~p) test process PID=~p~n", [Version, Socket, Socket_Subs, self()]),
%% ---- connect to main process ----
	mock_tcp:set_expectation([<<32,2,0,0>>]), %% Connack packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,63,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 1:1, 1:1, 0:1, 60000:16, 
			11:16,"test0Client"/utf8,
			10:16, "Will_Topic"/utf8,
			12:16, "Will Payload"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),
%% ---- connect to subscriber process ----
	mock_tcp:set_expectation([<<32,2,0,0>>]), %% Connack packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server_subs ! {tcp, Socket_Subs,
		<<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
			11:16,"test1Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),
	
	subscribe(Version, Socket_Subs, 0),

%%	mock_tcp:set_expectation(<<224,0>>),
	conn_server ! {tcp, Socket, <<224,0>>},
%	gen_server:cast(conn_server, {disconnect,0,[]}),
	wait_no_mock_tcp("disconnect"),

	?passed
end};
disconnect_normal_no_will_test('5.0' = Version, {Socket, Socket_Subs}) -> {"Disconnect Will 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
%% ---- connect to main process ----
	mock_tcp:set_expectation(<<32,11,0,0,8,17,255,255,255,255,33,0,11>>), %% Connack packet
% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,75,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 1:1, 1:1, 0:1, 60000:16, 
			8,17,255,255,255,255,33,11:16,
			11:16,"test0Client"/utf8,
			2, 1, 0,
			10:16, "Will_Topic"/utf8,
			12:16, "Will Payload"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack 1"),
%% ---- connect to subscriber process ----
	mock_tcp:set_expectation(<<32,11,0,0,8,17,0,0,0,0,33,0,11>>), %% Connack packet
% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server_subs ! {tcp, Socket_Subs,
		<<16,46,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
			8,17,0,0,0,0,33,11:16,
			11:16,"test1Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack 2"),

	subscribe(Version, Socket_Subs, {0,0,0,2}),

	mock_tcp:set_expectation(
	 <<224,34,2, 32, 31,29:16,"Initiated by client or server"/utf8>>),
	conn_server ! {tcp, Socket, <<224,7,2,5,17,0,0,0,25>>},
	wait_mock_tcp("disconnect"),
	wait_no_mock_tcp("will"),

	?passed
end}.

disconnect_shutdown_with_will_test('3.1.1' = Version, {Socket, Socket_Subs}) -> {"Disconnect Will 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p, ~p) test process PID=~p~n", [Version, Socket, Socket_Subs, self()]),
%% ---- connect to main process ----
	mock_tcp:set_expectation([<<32,2,0,0>>]), %% Connack packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,63,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 1:1, 1:1, 0:1, 60000:16, 
			11:16,"test0Client"/utf8,
			10:16, "Will_Topic"/utf8,
			12:16, "Will Payload"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),
%% ---- connect to subscriber process ----
	mock_tcp:set_expectation([<<32,2,0,0>>]), %% Connack packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server_subs ! {tcp, Socket_Subs,
		<<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
			11:16,"test1Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),
	
	subscribe(Version, Socket_Subs, 0),

	mock_tcp:set_expectation(<<48,24,10:16,"Will_Topic"/utf8,"Will Payload"/utf8>>),
	conn_server ! {tcp_closed, Socket},
	wait_mock_tcp("will"),
	wait_no_mock_tcp("disconnect"),

	?passed
end};
disconnect_shutdown_with_will_test('5.0' = Version, {Socket, Socket_Subs}) -> {"Disconnect Will 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
%% ---- connect to main process ----
	mock_tcp:set_expectation(<<32,11,0,0,8,17,255,255,255,255,33,0,11>>), %% Connack packet
% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,75,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 1:1, 1:1, 0:1, 60000:16, 
			8,17,255,255,255,255,33,11:16,
			11:16,"test0Client"/utf8,
			2, 1, 0,
			10:16, "Will_Topic"/utf8,
			12:16, "Will Payload"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack 1"),
%% ---- connect to subscriber process ----
	mock_tcp:set_expectation(<<32,11,0,0,8,17,0,0,0,0,33,0,11>>), %% Connack packet
% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server_subs ! {tcp, Socket_Subs,
		<<16,46,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
			8,17,0,0,0,0,33,11:16,
			11:16,"test1Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack 2"),

	subscribe(Version, Socket_Subs, {0,0,0,2}),

	mock_tcp:set_expectation(<<48,27,10:16,"Will_Topic"/utf8,2,1,0,"Will Payload"/utf8>>),
	conn_server ! {tcp_closed, Socket},
	wait_mock_tcp("will"),
	wait_no_mock_tcp("disconnect"),

	?passed
end}.

%% ====================================================================
%% Internal functions
%% ====================================================================
subscribe('3.1.1', Socket, _) ->
	mock_tcp:set_expectation(<<144,4,0,100,2,1>>), %% Suback packet
	conn_server_subs ! {tcp, Socket, <<130,23,0,100, 0,5,"Topic"/utf8,2, 10:16,"Will_Topic"/utf8,1>>}, %% Subscription request
	wait_mock_tcp("suback");
subscribe('5.0', Socket, {Retain_handling,Retain_as_published,No_local,Max_qos}) ->
	mock_tcp:set_expectation(<<144,5, 0,100, 0, 2,2>>), %% Suback packet
	conn_server_subs ! {tcp, Socket,
		<<130,24,0,100,0,
			5:16,"Topic"/utf8,0:2,Retain_handling:2,Retain_as_published:1,No_local:1,Max_qos:2,
			10:16,"Will_Topic"/utf8,0:2,Retain_handling:2,Retain_as_published:1,No_local:1,Max_qos:2>>
	}, %% Subscription request
	wait_mock_tcp("suback").

