%%
%% Copyright (C) 2015-2022 by krasnop@bellsouth.net (Alexei Krasnopolski)
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
%% @since 2016-09-08
%% @copyright 2015-2022 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_connection_srv_tests).

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
%-import(helper_common, []).

%%
%% Exported Functions
%%
-export([
]).

%%
%% API Functions
%%

connection_genServer_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				{'3.1.1', fun connection_test/2}
				,{'5.0',   fun connection_test/2}
				,{'5.0',   fun connection_props_test/2}
				,{'3.1.1', fun subscribe_test/2}
				,{'5.0', fun subscribe_test/2}
				,{'5.0', fun subscribe_props_test/2}
				,{'3.1.1', fun unsubscribe_test/2}
				,{'5.0', fun unsubscribe_test/2}
				,{'5.0', fun unsubscribe_props_test/2}
				,{'3.1.1', fun publish_0_test/2}
				,{'5.0', fun publish_0_test/2}
				,{'5.0', fun publish_0_props_test/2}
				,{'3.1.1', fun publish_1_test/2}
				,{'5.0', fun publish_1_test/2}
				,{'5.0', fun publish_1_props_test/2}
				,{'3.1.1', fun publish_2_test/2}
				,{'5.0', fun publish_2_test/2}
				,{'5.0', fun publish_2_props_test/2}
			]
		}
	 }
	].

do_start() ->
	?debug_Fmt("::test:: >>> do_start() ~n", []),
	lager:start(),

	mqtt_dets_dao:start(server),
	mqtt_dets_dao:cleanup(server),
	Transport = mock_tcp,

	Storage = mqtt_dets_dao,
	Storage:save(server, #user{user_id = <<"guest">>, password = <<"guest">>}),

	Socket = list_to_port("#Port<0.7>"),
	State = #connection_state{socket = Socket, transport = Transport, storage = Storage, end_type = server},
%	{ok, Pid} = gen_server:start_link({local, conn_server}, mqtt_connection, State, [{timeout, ?MQTT_GEN_SERVER_TIMEOUT}]),
	Pid = proc_lib:spawn_link(fun() -> mqtt_connection:init(State) end),
	?debug_Fmt("::test:: >>> do_start() Pid=~p~n", [Pid]),
	register(conn_server, Pid),
	Pid.


do_stop(_Pid) ->
	?debug_Fmt("::test:: >>> do_stop(~p) ~n", [_Pid]),
% Close connection - stop the conn_server process.
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<224,0>>}, %% Disconnect packet
	unregister(conn_server),
	mqtt_dets_dao:cleanup(server),	
	mqtt_dets_dao:close(server).	

setup('3.1.1') ->
	?debug_Fmt("::test:: >>> setup('3.1.1')~n", []),
	mock_tcp:start(),
	#connect{client_id = "test0Client", user_name = ?TEST_USER, password = ?TEST_PASSWORD, keep_alive = 60000, version = '3.1.1'};
setup('5.0') ->
	?debug_Fmt("::test:: >>> setup('5.0')~n", []),
	mock_tcp:start(),
	#connect{client_id = "test0Client", user_name = ?TEST_USER, password = ?TEST_PASSWORD, keep_alive = 60000, version = '5.0'}.

cleanup(X, Y) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p) ~n", [X,Y#connect.client_id]),
	mqtt_dets_dao:remove(server, {client_id, Y#connect.client_id}),

	mock_tcp:stop().

connection_test('3.1.1'=Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p~n", [Version, Conn_config, self()]),
	mock_tcp:set_expectation(<<32,2,0,0>>), %% Connack packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack"),

	mock_tcp:set_expectation(<<16#D0:8, 0:8>>), %% PingResp packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<192,0>>}, %% PingReq
	wait_mock_tcp("pingresp"),

	Conn_State2 = sys:get_state(conn_server),
	?debug_Fmt("::test:: ping_count = ~p ~n", [Conn_State2#connection_state.ping_count]),
	?assertEqual(0, Conn_State2#connection_state.ping_count),

% Close connection - stop the conn_server process.
%	conn_server ! {tcp, undefined, <<224,0>>}, %% Disconnect packet
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
connection_test('5.0' = Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	mock_tcp:set_expectation(<<32,3,0,0,0>>), %% Connack packet
%%	conn_server ! {tcp, undefined, <<16,38, 4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<16>>},
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<38>>},
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack"),

	mock_tcp:set_expectation([<<16#D0:8,0:8>>,<<16#D0:8,0:8>>]), %% PingResp packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<192,0,192,0>>}, %% PingReq
	wait_mock_tcp("pingresp"),

	Conn_State2 = sys:get_state(conn_server),
	?debug_Fmt("::test:: ping_count = ~p ~n", [Conn_State2#connection_state.ping_count]),
	?assertEqual(0, Conn_State2#connection_state.ping_count),

% Close connection - stop the conn_server process.
%	conn_server ! {tcp, undefined, <<224,0>>}, %% Disconnect packet
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

connection_props_test('5.0' = Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	mock_tcp:set_expectation(<<32,13,0,0, 10, 17, 16#FFFFFFFF:32, 39, 65000:32>>), %% Connack packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, 
			<<16,93, 4:16,"MQTT"/utf8,5,246,234,96, 
				10, 17, 16#FFFFFFFF:32, 39, 65000:32, %% properties
				11:16,"test0Client"/utf8, 
				23, 8, 15:16,"AfterClose/Will"/utf8, 24, 6000:32, %% will properties
				8:16,"Last_msg"/utf8, 9:16,"Good bye!",
				5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack"),

	Conn_State = sys:get_state(conn_server),
	?debug_Fmt("::test:: will properties = ~p ~n", [Conn_State#connection_state.config#connect.will_publish#publish.properties]),
	?debug_Fmt("::test:: properties = ~p ~n", [Conn_State#connection_state.config#connect.properties]),
	?assertEqual([{?Will_Delay_Interval, 6000},{?Response_Topic, <<"AfterClose/Will">>}], Conn_State#connection_state.config#connect.will_publish#publish.properties),
	?assertEqual([{?Maximum_Packet_Size, 65000}, {?Session_Expiry_Interval, 16#FFFFFFFF}], Conn_State#connection_state.config#connect.properties),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

subscribe_test('3.1.1'=Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(),

	mock_tcp:set_expectation(<<144,3,0,100,2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,10,0,100,0,5,84,111,112,105,99,2>>}, %% Subscription request
	wait_mock_tcp("suback"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
subscribe_test('5.0' = Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),

	mock_tcp:set_expectation(<<144,4, 0,100, 0, 2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,11,0,100,0,0,5,84,111,112,105,99,2>>}, %% Subscription request
	wait_mock_tcp("suback"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

subscribe_props_test('5.0' = Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),

%%	mock_tcp:set_expectation(<<144,21, 100:16, 17, 11,233,230,10, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 2>>), %% Suback packet
	mock_tcp:set_expectation(<<144,4,100:16, 0,2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,28,100:16, 17,11,233,230,10, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 5:16,"Topic"/utf8,2>>}, %% Subscribe request
	wait_mock_tcp("suback"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

unsubscribe_test('3.1.1'=Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(),

	mock_tcp:set_expectation(<<144,3,0,100,2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,10,0,100,0,5,84,111,112,105,99,2>>}, %% Subscription request
	wait_mock_tcp("suback"),

	mock_tcp:set_expectation(<<176,2,0,101>>), %% Unsuback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<162,9,0,101,0,5,84,111,112,105,99>>}, %% Unsubscription request
	wait_mock_tcp("unsuback"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
unsubscribe_test('5.0' = Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),

	mock_tcp:set_expectation(<<144,4, 0,100, 0, 2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,11,0,100,0,0,5,84,111,112,105,99,2>>}, %% Subscription request
	wait_mock_tcp("suback"),

	mock_tcp:set_expectation(<<176,4,0,101,0,0>>), %% Unsuback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<162,10,0,101,0,0,5,84,111,112,105,99>>}, %% Unsubscription request
	wait_mock_tcp("unsuback"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

unsubscribe_props_test('5.0' = Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),

	mock_tcp:set_expectation(<<144,4, 0,100, 0, 2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,11,0,100,0,0,5,84,111,112,105,99,2>>}, %% Subscription request
	wait_mock_tcp("suback"),

	mock_tcp:set_expectation(<<176,4,101:16, 0,0>>), %% Unsuback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<162,23,0,101,
										13, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8,
										0,5,84,111,112,105,99>>}, %% Unsubscription request
	wait_mock_tcp("unsuback"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_0_test('3.1.1'=Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(),
	subscribe_v3(),

	mock_tcp:set_expectation(<<48,14,0,5,84,111,112,105,99,80,97,121,108,111,97,100>>), %% Publish packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<48,14,0,5,84,111,112,105,99,80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("publish<0>"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
publish_0_test('5.0' = Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),
	subscribe_v5(),

	mock_tcp:set_expectation(<<48,15,0,5,84,111,112,105,99,0,80,97,121,108,111,97,100>>), %% Publish packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<48,15,0,5,84,111,112,105,99,0,80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("publish<0>"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_0_props_test('5.0' = Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),
	subscribe_v5(),

	mock_tcp:set_expectation(<<48,25,0,5,84,111,112,105,99,10, 9,4:16,1,2,3,4, 35,300:16,80,97,121,108,111,97,100>>), %% Publish packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<48,25,0,5,84,111,112,105,99, 10, 9,4:16,1,2,3,4, 35,300:16, 80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("publish<0>"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_1_test('3.1.1'=Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(),
	subscribe_v3(),
	
	mock_tcp:set_expectation([<<64,2,0,100>>,<<50,16,0,5,84,111,112,105,99,0,100,80,97,121,108,111,97,100>>]), % puback packet, publish packet server -> subscriber
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<50,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("(puback sent -> client)"),
	wait_mock_tcp("(publish sent -> subscriber)"),

	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<64,2,0,101>>}, %% Puback packet from subscriber -> server
	timer:sleep(1000),
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
publish_1_test('5.0' = Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 2, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),
	subscribe_v5(),

	mock_tcp:set_expectation([<<64,2,100:16>>,<<50,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>]),
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<50,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("(puback sent -> client)"),
	wait_mock_tcp("(publish sent -> subscriber)"),

	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<64,3,101:16,10>>}, %% Puback packet from subscriber -> server
	timer:sleep(1000),
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_1_props_test('5.0' = Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 2, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),
	subscribe_v5(),

	mock_tcp:set_expectation([<<64,2,100:16>>,
														<<50,19, 5:16,84,111,112,105,99, 100:16, 2,1,1, 80,97,121,108,111,97,100>>]),
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<50,19, 5:16,84,111,112,105,99, 100:16, 2,1,1, 80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("(puback sent -> client)"),
	wait_mock_tcp("(publish sent -> subscriber)"),

	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<64,57,101:16,10, 53, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 31, 23:16,"No matching subscribers"/utf8>>}, %% Puback packet from subscriber -> server
	timer:sleep(1000),
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_2_test('3.1.1'=Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 2, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(),
	subscribe_v3(),

	mock_tcp:set_expectation(<<80,2,0,100>>), % pubrec packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<52,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("(pubrec sent -> client)"),

	mock_tcp:set_expectation([<<112,2,0,100>>,<<52,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>]), %% expect pubcomp packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<98,2,0,100>>}, %% Pubrel packet from client -> server
	wait_mock_tcp("(pubcomp sent -> client)"),
	wait_mock_tcp("(publish sent -> subscriber)"),

	mock_tcp:set_expectation(<<98,2,0,100>>), % pubrel packet from server -> subscriber
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<80,2,0,100>>}, %% pubrec packet from subscriber -> server
	wait_mock_tcp("(pubrel sent -> subscriber)"),

	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<112,2,0,100>>}, %% pubcomp packet from subscriber -> server
	timer:sleep(1000),
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
publish_2_test('5.0' = Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 2, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),
	subscribe_v5(),

	mock_tcp:set_expectation(<<80,2,0,100>>), % pubrec packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<52,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("(pubrec sent -> client)"),

	mock_tcp:set_expectation([<<112,2,0,100>>,<<52,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>]), %% expect pubcomp packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<98,3,0,100,0>>}, %% Pubrel packet from client -> server
	wait_mock_tcp("(pubcomp sent -> client)"),
	wait_mock_tcp("(publish sent -> subscriber)"),

	mock_tcp:set_expectation(<<98,2,0,100>>), % pubrel packet from server -> subscriber
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<80,3,0,100,0>>}, %% pubrec packet from subscriber -> server
	wait_mock_tcp("(pubrel sent -> subscriber)"),

	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<112,3,0,100,0>>}, %% pubcomp packet from subscriber -> server
	timer:sleep(1000),
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_2_props_test('5.0' = Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 2, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) ~n", [Version, Conn_config]),
	connect_v5(),
	subscribe_v5(),

	mock_tcp:set_expectation(<<80,2,0,100>>), % pubrec packet from server -> client
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<52,27,0,5,84,111,112,105,99,100:16,10,9,0,4,1,2,3,4,35,1,44,80,97,121,108,111,97,100>>}, %% Publish packet from client -> server
	wait_mock_tcp("(pubrec sent -> client)"),

	mock_tcp:set_expectation([<<112,2,0,100>>, %% expect pubcomp packet from server -> client
			<<52,27,0,5,84,111,112,105,99,100:16,10,9,0,4,1,2,3,4,35,1,44,80,97,121,108,111,97,100>>]), %% expect publish packet from server -> subscriber
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<98,43,0,100,16,39, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 31,23:16,"No matching subscribers"/utf8>>}, %% Pubrel packet from client -> server
	wait_mock_tcp("(pubcomp sent -> client)"),
	wait_mock_tcp("(publish sent -> subscriber)"),

	mock_tcp:set_expectation(<<98,3,0,100,16>>), % pubrel packet from server -> subscriber
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<80,43,100:16,16, 39, 
																	 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 
																	 31,23:16,"No matching subscribers"/utf8>>}, %% pubrec packet from subscriber -> server
	wait_mock_tcp("(pubrel sent -> subscriber)"),

%% <<112,61,0,100,1,57, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 
%% 																	 31, 27:16,"Packet Identifier not found"/utf8>>
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<112,61,0,100,1,57, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 
																	 31, 27:16,"Packet Identifier not found"/utf8>>}, %% pubcomp packet from subscriber -> server
	timer:sleep(1000),
	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

%% ====================================================================
%% Internal functions
%% ====================================================================
wait_mock_tcp(R) ->
	receive
		{mock_tcp, true} ->
			?debug_Fmt("::test:: mock_tcp ~p acknowledge ~n", [R]);
		{mock_tcp, false} ->
			?assert(false);
		M ->
			?debug_Fmt("::test:: while waiting ~p mock_tcp got unexpected msg = ~p~n", [R, M]),
			?assert(false)
	end.

connect_v3() ->
	mock_tcp:set_expectation(<<32,2,0,0>>), %% Connack packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack").

connect_v5() ->
	mock_tcp:set_expectation(<<32,3,0,0,0>>), %% Connack packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<16,38, 4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack").

subscribe_v3() ->
	mock_tcp:set_expectation(<<144,3,0,100,2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,10,0,100,0,5,84,111,112,105,99,2>>}, %% Subscription request
	wait_mock_tcp("suback").

subscribe_v5() ->
	mock_tcp:set_expectation(<<144,4, 0,100, 0, 2>>), %% Suback packet
	conn_server ! {tcp, (sys:get_state(conn_server))#connection_state.socket, <<130,11,0,100,0,0,5,84,111,112,105,99,2>>}, %% Subscription request
	wait_mock_tcp("suback").
