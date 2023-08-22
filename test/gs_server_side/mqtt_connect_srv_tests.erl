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

-module(mqtt_connect_srv_tests).
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
connect_genServer_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				 {'3.1.1', fun config_setup_test/2}
				,{'5.0',   fun config_setup_test/2}
				,{'3.1.1', fun restore_session_1_test/2}
				,{'5.0',   fun restore_session_1_test/2}
				,{'3.1.1', fun restore_session_2_test/2}
				,{'5.0',   fun restore_session_2_test/2}
				,{'3.1.1', fun restore_session_3_test/2}
				,{'5.0',   fun restore_session_3_test/2}
				,{'3.1.1', fun disconnect_test_1/2}
				,{'5.0',   fun disconnect_test_1/2}
				,{'5.0',   fun disconnect_test_2/2}
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
	{create_server_process(), #connect{client_id = "test0Client", version = '3.1.1'}};
setup('5.0') ->
	?debug_Fmt("::test:: >>> setup('5.0')~n", []),
	mock_tcp:start(),
	{create_server_process(), #connect{client_id = "test0Client", version = '5.0'}}.

cleanup(X, {_, Y}) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p) ~n", [X,Y#connect.client_id]),
% Close connection - stop the conn_server process.
	case whereis(conn_server) of
		undefined -> ok;
		_ -> 
			disconnect()
%			unregister(conn_server)
	end,
	mqtt_dets_storage:connect_pid(remove, Y#connect.client_id, server),
	mqtt_dets_storage:retain(clean, server),

	mock_tcp:stop().

%% ====================================================================
%% API functions
%% ====================================================================
config_setup_test('3.1.1' = Version, {Socket, _Conn_config}) -> {"Config setup test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	mock_tcp:set_expectation(<<32,2,0,0>>), %% Connack packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket, <<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack"),

	Record = mqtt_dets_storage:connect_pid(get, <<"test0Client">>, server),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),
	?assert(is_pid(Record)),

	?passed
end};
config_setup_test('5.0' = Version, {Socket, _Conn_config}) -> {"Config setup test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	mock_tcp:set_expectation(<<32,11,0,0,8,17,0,0,0,7,33,0,11>>), %% Connack packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,75,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 1:1, 1:1, 0:1, 60000:16, 
			8,17,0,0,0,7,33,11:16,
			11:16,"test0Client"/utf8,
			2, 1, 0,
			10:16, "Will_Topic"/utf8,
			12:16, "Will Payload"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),


	Record = mqtt_dets_storage:connect_pid(get, <<"test0Client">>, server),
	?debug_Fmt("::test:: Record = ~p ~n", [Record]),
	?assert(is_pid(Record)),
	
	Record_1 = mqtt_dets_storage:session_state(get, <<"test0Client">>),
	?debug_Fmt("::test:: Record_1 = ~p ~n", [Record_1]),
	?assertEqual(7, Record_1#session_state.session_expiry_interval),
	?assertMatch(#publish{topic="Will_Topic", payload= <<"Will Payload">>, properties = [{?Payload_Format_Indicator, 0}]}, Record_1#session_state.will_publish),

	State = sys:get_state(conn_server),
	?debug_Fmt("::test:: State = ~p ~n", [State]),
	?assertEqual(11, State#connection_state.receive_max),
	?assertEqual(11, State#connection_state.send_quota),
	?passed
end}.

restore_session_1_test('3.1.1' = Version, {Socket, _Conn_config}) -> {"Restore session test 1 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=publish},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, server),

	mock_tcp:set_expectation([<<32,2,1,0>>, <<56,14,0,5,"Topic"/utf8,"Payload"/utf8>>]), %% Connack packet SP=1 and re-published packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket, <<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 0:1, 0:1, 60000:16, 
		11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack | publish packet"),
	wait_mock_tcp("publish packet | connack"),

	?passed
end};
restore_session_1_test('5.0' = Version, {Socket, _Conn_config}) -> {"Restore session test 1 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=publish},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, server),

	mock_tcp:set_expectation([<<32,8,1,0,5,17,0,0,0,7>>, %% Connack packet
			<<56,15,0,5,"Topic"/utf8,0,"Payload"/utf8>> % re-published packet
		]), 
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,43,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 0:1, 0:1, 60000:16, 
			5,17,0,0,0,7,
			11:16,"test0Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack | publish packet"),
	wait_mock_tcp("publish packet | connack"),

	?passed
end}.

restore_session_2_test('3.1.1' = Version, {Socket, _Conn_config}) -> {"Restore session test 2 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrel},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, server),

	mock_tcp:set_expectation([<<32,2,1,0>>, <<98,2,0,100>>]), %% Connack packet SP=1 and re-sent pubrel packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket, <<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 0:1, 0:1, 60000:16, 
		11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack | pubrel packet"),
	wait_mock_tcp("pubrel packet | connack"),

	?passed
end};
restore_session_2_test('5.0' = Version, {Socket, _Conn_config}) -> {"Restore session test 2 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrel},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, server),

	mock_tcp:set_expectation([<<32,8,1,0,5,17,0,0,0,7>>, %% Connack packet
			<<98,2,0,100>> % re-sent pubrel packet
		]), 
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,43,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 0:1, 0:1, 60000:16, 
			5,17,0,0,0,7,
			11:16,"test0Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack | pubrel packet"),
	wait_mock_tcp("pubrel packet | connack"),

	?passed
end}.

restore_session_3_test('3.1.1' = Version, {Socket, _Conn_config}) -> {"Restore session test 3 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrec},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, server),

	mock_tcp:set_expectation([<<32,2,1,0>>, <<80,2,0,100>>]), %% Connack packet SP=1 and re-sent pubrec packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket, <<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 0:1, 0:1, 60000:16, 
		11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack | pubrec packet"),
	wait_mock_tcp("pubrec packet | connack"),

	?passed
end};
restore_session_3_test('5.0' = Version, {Socket, _Conn_config}) -> {"Restore session test 3 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrec},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, server),

	mock_tcp:set_expectation([<<32,8,1,0,5,17,0,0,0,7>>, %% Connack packet
			<<80,2,0,100>> % re-sent pubrec packet
		]), 
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,43,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 0:1, 0:1, 60000:16, 
			5,17,0,0,0,7,
			11:16,"test0Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack | pubrec packet"),
	wait_mock_tcp("pubrec packet | connack"),

	?passed
end}.

disconnect_test_1('3.1.1' = Version, {Socket, _Conn_config}) -> {"Disconnect test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->

	mock_tcp:set_expectation([<<32,2,0,0>>]), %% Connack packet SP=1 and re-sent pubrec packet
	% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket, <<16,37,4:16,"MQTT"/utf8,4, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
		11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>},
	wait_mock_tcp("connack"),

	mock_tcp:set_expectation(<<224,0>>),
	conn_server ! {tcp, Socket, <<224,0>>},
%	gen_server:cast(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
disconnect_test_1('5.0' = Version, {Socket, _Conn_config}) -> {"Disconnect test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->

	mock_tcp:set_expectation(<<32,8,0,0,5,17,0,0,0,7>>), %% Connack packet

% Conect Flags: UN:1, PW:1, WillRetain:1, WillQoS:2, WillFlag:1, Clear:1, 0:1
	conn_server ! {tcp, Socket,
		<<16,43,4:16,"MQTT"/utf8,5, 1:1, 1:1, 0:1, 0:2, 0:1, 1:1, 0:1, 60000:16, 
			5,17,0,0,0,7,
			11:16,"test0Client"/utf8,
			5:16,"guest"/utf8,
			5:16,"guest"/utf8
		>>},
	wait_mock_tcp("connack"),

	mock_tcp:set_expectation(
	 <<224,34,2,
		 32,  31,29:16,"Initiated by client or server"/utf8>>),
	conn_server ! {tcp, Socket, <<224,7,2,5,17,0,0,0,25>>},
	wait_mock_tcp("disconnect"),

	?passed
end}.

disconnect_test_2('5.0' = Version, {Socket, _Conn_config}) -> {"Disconnect test 2 [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->

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
	 <<224,19,130,
		 17, 31,14:16,"Protocol Error"/utf8>>),
	conn_server ! {tcp, Socket, <<224,7,2,5,17,0,0,0,25>>},
%%	gen_server:cast(conn_server, {disconnect,2,[{?Session_Expiry_Interval, 25}]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

%% Disconnect test for 3.1.1 change;
%% ====================================================================
%% Internal functions
%% ====================================================================

