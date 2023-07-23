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

-module(mqtt_subscribe_srv_tests).
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
	connect/2,
	subscribe/3,
	disconnect/0
]).
-import(mock_tcp, [wait_mock_tcp/1, wait_no_mock_tcp/1]).

%%
%% Exported Functions
%%
-export([]).

%%
%% API Functions
%%
subscribe_genServer_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				{'3.1.1', fun storage_test/2}
				,{'5.0',  fun storage_test/2}
				,{'5.0',  fun share_test/2}
				,{'3.1.1', fun retain0_test/2}
				,{'5.0', fun retain0_test/2}
				,{'5.0', fun retain1_test/2}
				,{'5.0', fun retain2_test/2}
				,{'5.0', fun retain3_test/2}
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
	?debug_Fmt("::test:: Create Server process (conn_server) with Pid=~p~n", [Pid]),
	Socket.
	
do_stop(Pid) ->
	?debug_Fmt("::test:: >>> do_stop(~p) ~n", [Pid]),
	mqtt_dets_storage:cleanup(server),	
	mqtt_dets_storage:close(server).	

setup('3.1.1') ->
	?debug_Fmt("::test:: >>> setup('3.1.1')~n", []),
	mock_tcp:start(),
	{create_server_process(), #connect{client_id = "test0Client", user_name = ?TEST_USER, password = ?TEST_PASSWORD, keep_alive = 60000, version = '3.1.1'}};
setup('5.0') ->
	?debug_Fmt("::test:: >>> setup('5.0')~n", []),
	mock_tcp:start(),
	{create_server_process(), #connect{client_id = "test0Client", user_name = ?TEST_USER, password = ?TEST_PASSWORD, keep_alive = 60000, version = '5.0'}}.

cleanup(X, {_, Y}) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p) ~n", [X,Y#connect.client_id]),
% Close connection - stop the conn_server process.
	case whereis(conn_server) of
		undefined -> ok;
		_ -> unregister(conn_server)
	end,
	mqtt_dets_storage:connect_pid(remove, Y#connect.client_id, server),
	mqtt_dets_storage:retain(clean, server),

	mock_tcp:stop().

%% ====================================================================
%% API functions
%% ====================================================================

storage_test('3.1.1'=Version, {Socket, Conn_config}) -> {"Storage test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	subscribe(Version, Socket, ok),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:subscription(get_client_topics,"test0Client",server)]),
	[Record] = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= "test0Client", topicFilter= "Topic"}, server),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),
	?assertEqual(2, (Record#storage_subscription.options)#subscription_options.max_qos),	
	
	mock_tcp:set_expectation(<<176,2,0,101>>), %% Unsuback packet
	conn_server ! {tcp, Socket, <<162,9,0,101,0,5,"Topic"/utf8>>}, %% Unsubscription request
	wait_mock_tcp("unsuback"),
	Record1 = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= "test0Client", topicFilter= "Topic"}, server),
	?assertEqual([], Record1),

	disconnect(),

	?passed
end};
storage_test('5.0' = Version, {Socket, Conn_config}) -> {"Storage test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	subscribe(Version, Socket, {0,0,0,2}),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:subscription(get_client_topics,"test0Client",server)]),
	[Record] = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= "test0Client", topicFilter= "Topic"}, server),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),
	?assertEqual(2, (Record#storage_subscription.options)#subscription_options.max_qos),	

	mock_tcp:set_expectation(<<176,4,0,101,0,0>>), %% Unsuback packet
	conn_server ! {tcp, Socket, <<162,10,0,101,0,0,5,"Topic"/utf8>>}, %% Unsubscription request
	wait_mock_tcp("unsuback"),
	Record1 = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= "test0Client", topicFilter= "Topic"}, server),
	?assertEqual([], Record1),

	disconnect(),

	?passed
end}.

share_test('5.0' = Version, {Socket, Conn_config}) -> {"Share test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	subscribe(Version, Socket, {0,0,0,2}),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:subscription(get_client_topics,"test0Client",server)]),
	[Record] = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= "test0Client", topicFilter= "TopicA", shareName= "A"}, server),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),
	?assertEqual(2, (Record#storage_subscription.options)#subscription_options.max_qos),	
	?assertEqual("A", (Record#storage_subscription.key)#subs_primary_key.shareName),	

	mock_tcp:set_expectation(<<176,4,0,101,0,0>>), %% Unsuback packet
	conn_server ! {tcp, Socket, <<162,10,0,101,0,0,5,"Topic"/utf8>>}, %% Unsubscription request
	wait_mock_tcp("unsuback"),
	Record1 = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= "test0Client", topicFilter= "Topic"}, server),
	?assertEqual([], Record1),

	disconnect(),

	?passed
end}.

%% get messages for handle_retain = 0
retain0_test('3.1.1' = Version, {Socket, Conn_config}) -> {"Retain test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	conn_server ! {tcp, Socket, <<3:4, 0:1, 0:2, 1:1, 14,0,5,"Topic"/utf8,"Payload"/utf8>>}, %% Publish<0> packet from client -> server
	timer:sleep(100),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:retain(get_all,"Topic")]),
	[Record] = mqtt_dets_storage:retain(get, "Topic"),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),

%%	subscribe_and_retain(Version, Socket, {0,0,0,2}),
	mock_tcp:set_expectation([
		<<144,4,0,100,2,1>>, %% Suback packet
		<<49,14,0,5,"Topic"/utf8,"Payload"/utf8>> %% Publish packet from server -> client
	]),
	conn_server ! {tcp, Socket, <<130,19,0,100, 0,5,"Topic"/utf8,2, 0,6,"TopicA"/utf8,1>>}, %% Subscription request
	wait_mock_tcp("suback | publish<0>"),
	wait_mock_tcp("suback | publish<0>"),

	disconnect(),
	?passed
end};
retain0_test('5.0' = Version, {Socket, Conn_config}) -> {"Retain test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	conn_server ! {tcp, Socket, <<3:4, 0:1, 0:2, 1:1, 15,0,5,"Topic"/utf8,0,"Payload"/utf8>>}, %% Publish packet from client -> server
	timer:sleep(100),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:retain(get_all,"Topic")]),
	[Record] = mqtt_dets_storage:retain(get, "Topic"),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),

%%	subscribe_and_retain(Version, Socket, {0,0,0,2}),
	mock_tcp:set_expectation([
		<<144,5, 0,100, 0, 2,2>>,  %% Suback packet
		<<49,15,0,5,"Topic"/utf8,0,"Payload"/utf8>> %% Publish packet from server -> clien
	]),
	conn_server ! {tcp, Socket, <<130,29,0,100,0,0,5,"Topic"/utf8,0:2,0:2,0:1,0:1,2:2,
																0,15,"$share/A/TopicA"/utf8,0:2,0:2,0:1,0:1,2:2>>}, %% Subscription request
	wait_mock_tcp("suback | publish<0>"),
	wait_mock_tcp("suback | publish<0>"),

	disconnect(),
	?passed
end}.

%% no messages for handle_retain = 1 if the subscription was exist
retain1_test('5.0' = Version, {Socket, Conn_config}) -> {"Retain1 test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	conn_server ! {tcp, Socket, <<3:4, 0:1, 0:2, 1:1, 15,0,5,"Topic"/utf8,0,"Payload"/utf8>>}, %% Publish packet from client -> server
	timer:sleep(100),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:retain(get_all,"Topic")]),
	[Record] = mqtt_dets_storage:retain(get, "Topic"),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),

%%	subscribe_and_retain(Version, Socket, {1,0,0,2}),
	mock_tcp:set_expectation([
		<<144,5, 0,100, 0, 2,2>>,  %% Suback packet
		<<49,15,0,5,"Topic"/utf8,0,"Payload"/utf8>> %% Publish packet from server -> clien
	]),
	conn_server ! {tcp, Socket, <<130,29,0,100,0,0,5,"Topic"/utf8,0:2,1:2,0:1,0:1,2:2,
																0,15,"$share/A/TopicA"/utf8,0:2,1:2,0:1,0:1,2:2>>}, %% Subscription request
	wait_mock_tcp("suback | publish<0>"),
	wait_mock_tcp("suback | publish<0>"),

	%%	subscribe_and_retain(Version, Socket, {1,0,0,2}),
	mock_tcp:set_expectation([
		<<144,5, 0,100, 0, 2,2>>  %% Suback packet
		%% No Publish packet from server -> clien
	]),
	conn_server ! {tcp, Socket, <<130,29,0,100,0,0,5,"Topic"/utf8,0:2,1:2,0:1,0:1,2:2,
																0,15,"$share/A/TopicA"/utf8,0:2,1:2,0:1,0:1,2:2>>}, %% Subscription request
	wait_mock_tcp("suback"),
	wait_no_mock_tcp("no publish<0>"),

	disconnect(),
	?passed
end}.

%% no messages for handle_retain = 2
retain2_test('5.0' = Version, {Socket, Conn_config}) -> {"Retain2 test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	conn_server ! {tcp, Socket, <<3:4, 0:1, 0:2, 1:1, 15,0,5,"Topic"/utf8,0,"Payload"/utf8>>}, %% Publish packet from client -> server
	timer:sleep(100),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:retain(get_all,"Topic")]),
	[Record] = mqtt_dets_storage:retain(get, "Topic"),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),

%%	subscribe_and_retain(Version, Socket, {2,0,0,2}),
	mock_tcp:set_expectation([
		<<144,5, 0,100, 0, 2,2>>  %% Suback packet
		%% No Publish packet from server -> clien
	]),
	conn_server ! {tcp, Socket, <<130,29,0,100,0,0,5,"Topic"/utf8,0:2,2:2,0:1,0:1,2:2,
																0,15,"$share/A/TopicA"/utf8,0:2,2:2,0:1,0:1,2:2>>}, %% Subscription request
	wait_mock_tcp("suback | publish<0>"),
	wait_no_mock_tcp("no publish<0>"),

	disconnect(),
	?passed
end}.

%% no messages for Share subscription
retain3_test('5.0' = Version, {Socket, Conn_config}) -> {"Retain2 test [" ++ atom_to_list(Version) ++ "]", timeout, 10, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~128p) PID=~p ~n", [Version, Conn_config, self()]),
	connect(Version, Socket),

	conn_server ! {tcp, Socket, <<3:4, 0:1, 0:2, 1:1, 16,0,6,"TopicA"/utf8,0,"Payload"/utf8>>}, %% Publish packet from client -> server
	timer:sleep(100),
	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:retain(get_all,"TopicA")]),
	[Record] = mqtt_dets_storage:retain(get, "TopicA"),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),

%%	subscribe_and_retain(Version, Socket, {2,0,0,2}),
	mock_tcp:set_expectation([
		<<144,5, 0,100, 0, 2,2>>  %% Suback packet
		%% No Publish packet from server -> clien
	]),
	conn_server ! {tcp, Socket, <<130,29,0,100,0,0,5,"Topic"/utf8,0:2,0:2,0:1,0:1,2:2,
																0,15,"$share/A/TopicA"/utf8,0:2,0:2,0:1,0:1,2:2>>}, %% Subscription request
	wait_mock_tcp("suback | publish<0>"),
	wait_no_mock_tcp("no publish<0>"),

	disconnect(),
	?passed
end}.

%% to do : 2. Retain after subscribe
%% ====================================================================
%% Internal functions
%% ====================================================================

