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
%% @since 2023-07-05
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running tests for subscribe operation.

-module(mqtt_connect_cli_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
-include_lib("mqtt_property.hrl").
-include("test.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-import(mqtt_connection_cli_tests, [disconnect/1]).
-import(mock_tcp, [wait_mock_tcp/1, wait_no_mock_tcp/1, wait_mock_tcp/2]).

-export([]).

connect_genServer_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				 {'5.0',   fun config_setup_test/2}
				,{'3.1.1', fun restore_session_1_test/2}
				,{'5.0',   fun restore_session_1_test/2}
				,{'3.1.1', fun restore_session_2_test/2}
				,{'5.0',   fun restore_session_2_test/2}
				,{'3.1.1', fun restore_session_3_test/2}
				,{'5.0',   fun restore_session_3_test/2}
				,{'3.1.1', fun restore_session_expiration_test/2}
				,{'5.0',   fun restore_session_expiration_test/2}
				,{'3.1.1', fun disconnect_test/2}
				,{'5.0',   fun disconnect_test/2}
			]
		}
	 }
	].

do_start() ->
	application:start(mqtt_common),
	lager:start(),

	mqtt_dets_storage:start(client),
	mqtt_dets_storage:cleanup(client),
	mock_tcp:start(),
	Storage = mqtt_dets_storage,
	State = #connection_state{storage = Storage, end_type = client},
	{ok, Pid} = gen_server:start_link({local, client_gensrv}, mqtt_connection, State, [{timeout, ?MQTT_GEN_SERVER_TIMEOUT}]),
	?debug_Fmt("::test:: <<< do_start() Pid of client_gensrv = ~p~n", [Pid]),
	Pid.

do_stop(Pid) ->
	?debug_Fmt("::test:: >>> do_stop(~p) ~n", [Pid]),
% Close connection.
%%	client_gensrv ! {tcp, undefined, <<224,0>>}, %% Disconnect packet
	unregister(client_gensrv),
	mock_tcp:stop(),
	mqtt_dets_storage:cleanup(client),	
	mqtt_dets_storage:close(client).	

setup('3.1.1') ->
	#connect{client_id = <<"test0Client">>, user_name = ?TEST_USER, password = ?TEST_PASSWORD, 
					keep_alive = 60000, version = '3.1.1', conn_type = mock_tcp};
setup('5.0') ->
	#connect{client_id = <<"test0Client">>, user_name = ?TEST_USER, password = ?TEST_PASSWORD,
					keep_alive = 60000, version = '5.0', conn_type = mock_tcp}.

cleanup(X, Y) ->
%	?debug_Fmt("::test:: >>> cleanup(~p,~p) PID:~p~n", [X, Y#connect.client_id, self()]),
	mqtt_dets_storage:session(clean, <<"test0Client">>, client).

%% ====================================================================
%% API functions
%% ====================================================================

config_setup_test('5.0' = Version, Conn_config) -> {"Config setup test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect(Version, Conn_config#connect{properties = [{?Topic_Alias_Maximum, 2},{?Receive_Maximum, 10}]}),
	State = sys:get_state(client_gensrv),
	?debug_Fmt("::test:: State = ~p ~n", [State]),
	?assertEqual(11, State#connection_state.receive_max),
	?assertEqual(11, State#connection_state.send_quota),
	?assertEqual(2, proplists:get_value(?Topic_Alias_Maximum, (State#connection_state.config)#connect.properties)),

	disconnect(Version),

	?passed
end}.

restore_session_1_test('3.1.1' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=publish},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),
	?debug_Fmt("::test:: Records : ~p~n", [mqtt_dets_storage:session(get_all, <<"test0Client">>, client)]),
%%	connect(Version, Conn_config#connect{properties = []}),
	mock_tcp:set_expectation(
		<<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	mock_tcp:set_expectation(<<56,14,0,5,"Topic"/utf8,"Payload"/utf8>>),
	client_gensrv ! {tcp, get_socket(), <<32,2,1,0>>}, %% Connack packet SP=1
	wait_for_callback_event(onConnect), % may come after
	wait_mock_tcp("publish packet"), % may come before
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	?passed
end};
restore_session_1_test('5.0' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=publish},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),

%	connect(Version, Conn_config#connect{properties = [{?Topic_Alias_Maximum, 2},{?Receive_Maximum, 10}]}),
	mock_tcp:set_expectation(
		<<16,44, 4:16,"MQTT"/utf8,5,194,234,96, 6,33,10:16,34,2:16, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config#connect{properties = [{?Topic_Alias_Maximum, 2},{?Receive_Maximum, 10}]}, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	mock_tcp:set_expectation(<<56,15,0,5,"Topic"/utf8,0,"Payload"/utf8>>),
	client_gensrv ! {tcp, get_socket(), <<32,9,1,0,6,33,10:16,34,2:16>>}, %% Connack packet, % SP=1
	wait_for_callback_event(onConnect), % may come after
	wait_mock_tcp("publish packet"), % may come before
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	?passed
end}.

restore_session_2_test('3.1.1' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrel},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),

	mock_tcp:set_expectation(
		<<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	mock_tcp:set_expectation(<<98,2,0,100>>), %% pubrel packet
	client_gensrv ! {tcp, get_socket(), <<32,2,1,0>>}, %% Connack packet SP=1
	wait_for_callback_event(onConnect), % may come after
	wait_mock_tcp("pubrel packet"),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	?passed
end};
restore_session_2_test('5.0' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrel},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),

	mock_tcp:set_expectation(
		<<16,44, 4:16,"MQTT"/utf8,5,194,234,96, 6,33,10:16,34,2:16, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config#connect{properties = [{?Topic_Alias_Maximum, 2},{?Receive_Maximum, 10}]}, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	mock_tcp:set_expectation(<<98,2,0,100>>), %% pubrel packet
	client_gensrv ! {tcp, get_socket(), <<32,9,1,0,6,33,10:16,34,2:16>>}, %% Connack packet, % SP=1
	wait_for_callback_event(onConnect), % may come after
	wait_mock_tcp("pubrel packet"),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	?passed
end}.

restore_session_3_test('3.1.1' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrec},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),

	mock_tcp:set_expectation(
		<<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	mock_tcp:set_expectation(<<80,2,0,100>>), %% pubrec packet
	client_gensrv ! {tcp, get_socket(), <<32,2,1,0>>}, %% Connack packet SP=1
	wait_for_callback_event(onConnect), % may come after
	wait_mock_tcp("pubrec packet"),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	?passed
end};
restore_session_3_test('5.0' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=pubrec},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),

	mock_tcp:set_expectation(
		<<16,44, 4:16,"MQTT"/utf8,5,194,234,96, 6,33,10:16,34,2:16, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config#connect{properties = [{?Topic_Alias_Maximum, 2},{?Receive_Maximum, 10}]}, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	mock_tcp:set_expectation(<<80,2,0,100>>), %% pubrec packet
	client_gensrv ! {tcp, get_socket(), <<32,9,1,0,6,33,10:16,34,2:16>>}, %% Connack packet, % SP=1
	wait_for_callback_event(onConnect), % may come after
	wait_mock_tcp("pubrec packet"),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	?passed
end}.

restore_session_expiration_test('3.1.1' = Version, Conn_config) -> {"Restore session expiration test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=publish, expiration_time=0},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),

	mock_tcp:set_expectation(
		<<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), <<32,2,1,0>>}, %% Connack packet SP=1
	wait_for_callback_event(onConnect),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),
	wait_no_mock_tcp("no publish packet"),

	disconnect(Version),

	?passed
end};
restore_session_expiration_test('5.0' = Version, Conn_config) -> {"Restore session expiration test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	Key = #primary_key{client_id = <<"test0Client">>, packet_id = 100},
	PublishDoc = #publish{topic="Topic", qos=0, payload= <<"Payload">>, dir=out, last_sent=publish, expiration_time=0},
	mqtt_dets_storage:session(save, #storage_publish{key = Key, document = PublishDoc}, client),

	mock_tcp:set_expectation(
		<<16,44, 4:16,"MQTT"/utf8,5,194,234,96, 6,33,10:16,34,2:16, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>
	),
	gen_server:cast(client_gensrv, {connect, Conn_config#connect{properties = [{?Topic_Alias_Maximum, 2},{?Receive_Maximum, 10}]}, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), <<32,9,1,0,6,33,10:16,34,2:16>>}, %% Connack packet, % SP=1
	wait_for_callback_event(onConnect), % may come after
	wait_no_mock_tcp("no publish packet"),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	?passed
end}.

disconnect_test('3.1.1' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect(Version, Conn_config#connect{properties = []}),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	gen_server:cast(client_gensrv, {disconnect, 0, []}),
	receive
		[onError, #mqtt_error{oper= disconnect, error_msg= "Already disconnected."}] = Args ->
			?debug_Fmt("::test:: onError event callback = ~p ~n", [Args]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onClose callback from client~n", []),
			?assert(false)
	end,

	?passed
end};
disconnect_test('5.0' = Version, Conn_config) -> {"Restore session test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect(Version, Conn_config#connect{properties = [{?Topic_Alias_Maximum, 2},{?Receive_Maximum, 10}]}),
%	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	disconnect(Version),

	mock_tcp:set_expectation(<<224,0>>),
	gen_server:cast(client_gensrv, {disconnect, 0, []}),
	F = 
		fun(M) ->
			case M of
				[onError, #mqtt_error{oper= disconnect, error_msg= "Already disconnected."}] = Args ->
					?debug_Fmt("::test:: onError event callback = ~p ~n", [Args]);
				Message ->
					?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
					?assert(false)
			end
		end,
	wait_mock_tcp("disconnect | onError", F),

	?passed
end}.
%% ====================================================================
%% Internal functions
%% ====================================================================

connect('3.1.1', Conn_config) ->
	Expected_packet = <<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>, 
	Connack_packet = <<32,2,1,0>>, % SP=1
	connect(Conn_config, Expected_packet, Connack_packet);
connect('5.0', Conn_config) ->
	Expected_packet = <<16,44, 4:16,"MQTT"/utf8,5,194,234,96, 6,33,10:16,34,2:16, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>, 
	Connack_packet = <<32,9,1,0,6,33,10:16,34,2:16>>, % SP=1
	connect(Conn_config, Expected_packet, Connack_packet).

connect(Conn_config, Expected_packet, Connack_packet) ->
	mock_tcp:set_expectation(Expected_packet),
	gen_server:cast(client_gensrv, {connect, Conn_config, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), Connack_packet}, %% Connack packet
	receive
		[onConnect, _] = _Args ->
			?debug_Fmt("::test:: Message to caller process= ~p ~n", [_Args]),
			?assert(true);
		Mssg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Mssg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onConnect callback from client~n", []),
			?assert(false)
	end.

wait_for_callback_event(Event_name) ->
	receive
		[Event_name, _] = _Args ->
			?debug_Fmt("::test:: Message to caller process= ~p ~n", [_Args]),
			?assert(true);
		Mssg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Mssg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting ~p callback from client~n", [Event_name]),
			?assert(false)
	end.

get_socket() ->
	(sys:get_state(client_gensrv))#connection_state.socket.
