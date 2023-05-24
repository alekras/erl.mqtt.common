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
%% @since 2016-09-08
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_connection_cli_tests).

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
				,{'5.0',   fun reconnection_test/2}
				,{'5.0',   fun connection_props_test/2}
				,{'5.0',   fun connection_timeout_test/2}
				,{'3.1.1', fun subscribe_test/2}
				,{'5.0',   fun subscribe_test/2}
				,{'5.0',   fun subscribe_timeout_test/2}
				,{'5.0',   fun subscribe_props_test/2}
				,{'3.1.1', fun unsubscribe_test/2}
				,{'5.0',   fun unsubscribe_test/2}
				,{'5.0',   fun unsubscribe_timeout_test/2}
				,{'5.0',   fun unsubscribe_props_test/2}
				,{'3.1.1', fun publish_0_test/2}
				,{'5.0',   fun publish_0_test/2}
				,{'5.0',   fun publish_0_props_test/2}
				,{'3.1.1', fun publish_1_test/2}
				,{'5.0',   fun publish_1_test/2}
				,{'5.0',   fun publish_1_timeout_test/2}
				,{'5.0',   fun publish_1_props_test/2}
				,{'3.1.1', fun publish_2_test/2}
				,{'5.0',   fun publish_2_test/2}
				,{'5.0',   fun publish_2_timeout_test/2}
				,{'5.0',   fun publish_2_props_test/2}
				,{'3.1.1', fun receive_0_test/2}
				,{'5.0',   fun receive_0_test/2}
				,{'5.0',   fun receive_0_props_test/2}
				,{'3.1.1', fun receive_1_test/2}
				,{'5.0',   fun receive_1_test/2}
				,{'5.0',   fun receive_1_props_test/2}
				,{'3.1.1', fun receive_2_test/2}
				,{'5.0',   fun receive_2_test/2}
				,{'5.0',   fun receive_2_props_test/2}
			]
		}
	 }
	].

do_start() ->
	?debug_Fmt("::test:: >>> do_start() ~n", []),
	lager:start(),

	mqtt_dets_storage:start(client),
	mqtt_dets_storage:cleanup(client),
%	Transport = mock_tcp,
	mock_tcp:start(),
	Storage = mqtt_dets_storage,
%	Socket = undefined,
	State = #connection_state{storage = Storage, end_type = client},
	{ok, Pid} = gen_server:start_link({local, client_gensrv}, mqtt_connection, State, [{timeout, ?MQTT_GEN_SERVER_TIMEOUT}]),
	?debug_Fmt("::test:: <<< do_start() Pid of client_gensrv = ~p~n", [Pid]),
	Pid.

do_stop(Pid) ->
	?debug_Fmt("::test:: >>> do_stop(~p) ~n", [Pid]),
% Close connection.
%% 	client_gensrv ! {tcp, undefined, <<224,0>>}, %% Disconnect packet
	unregister(client_gensrv),
	mock_tcp:stop(),
	mqtt_dets_storage:cleanup(client),	
	mqtt_dets_storage:close(client).	

setup('3.1.1') ->
	?debug_Fmt("::test:: >>> setup('3.1.1') ~n", []),
	#connect{client_id = <<"test0Client">>, user_name = ?TEST_USER, password = ?TEST_PASSWORD, 
					keep_alive = 60000, version = '3.1.1', conn_type = mock_tcp};
setup('5.0') ->
	?debug_Fmt("::test:: >>> setup('5.0') ~n", []),
	#connect{client_id = <<"test0Client">>, user_name = ?TEST_USER, password = ?TEST_PASSWORD,
					keep_alive = 60000, version = '5.0', conn_type = mock_tcp}.

cleanup(X, Y) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p) PID:~p~n", [X, Y#connect.client_id, self()]).

connection_test('3.1.1'=Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),
	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),

	ping_pong(),
	disconnect(),

	?passed
end};
connection_test('5.0' = Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect_v5(Conn_config),
	ping_pong(),
	disconnect(),

	?passed
end}.

reconnection_test('5.0' = Version, Conn_config) -> {"Re-Connect test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),

	Expected_packet = <<16,38, 4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>, 
	Connack_packet = <<32,3,1,0,0>>,
	mock_tcp:set_expectation(Expected_packet),
	gen_server:cast(client_gensrv, {reconnect, self(), []}),
	wait_mock_tcp("reconnect packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), Connack_packet}, %% Connack packet
	receive
		[onConnect, _] = _Args ->
%			?debug_Fmt("::test:: Message to caller process= ~p ~n", [_Args]),
			?assert(true);
		Mssg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Mssg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onConnect callback from client~n", []),
			?assert(false)
	end,
	ping_pong(),
	disconnect(),

	?passed
end}.

connection_timeout_test('5.0' = Version, Conn_config) -> {"Connection Timeout test [" ++ atom_to_list(Version) ++ "]", timeout, 60, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),

	mock_tcp:set_expectation(<<16,38, 4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>),
	gen_server:cast(client_gensrv, {connect, Conn_config, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), <<>>}, %% Connack packet
	receive
		[onConnect, _] = _Args ->
%			?debug_Fmt("::test:: Message to caller process= ~p ~n", [_Args]),
			?assert(false);
		[onError, #mqtt_error{errno= 136, error_msg= _Err_msg}= Err] ->
			?debug_Fmt("::test:: Timeout error message arrives= ~p ~n", [Err]),
			?assert(true);
		Mssg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Mssg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onConnect callback from client~n", []),
			?assert(false)
	end,

	disconnect(),

	?passed
end}.

connection_props_test('5.0' = Version, Conn_config) -> {"Connection test with properties [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	Config = Conn_config#connect{
						will_publish = #publish{qos= 2, retain= 1, topic= "Last_msg", payload= <<"Good bye!">>,
																		properties = [{?Will_Delay_Interval, 6000}, {?Response_Topic, "AfterClose/Will"}]},
						properties = [{?Maximum_Packet_Size, 65000}, {?Session_Expiry_Interval, 16#FFFFFFFF}]
					},
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Config]),
	Expected_packet = <<16,93, 4:16,"MQTT"/utf8,5,246,234,96, 
		10, 17, 16#FFFFFFFF:32, 39, 65000:32,
		11:16,"test0Client"/utf8, 
		23, 8, 15:16,"AfterClose/Will"/utf8, 
		24, 6000:32, 8:16,"Last_msg"/utf8, 9:16,"Good bye!",
		5:16,"guest"/utf8, 5:16,"guest"/utf8>>, 
	Connack_packet = <<32,7,1,0, 4, 37,1, 36,2>>,
	connect(Config, Expected_packet, Connack_packet),
	ping_pong(),
	disconnect(),

	?passed
end}.

subscribe_test('3.1.1'=Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),
	subscribe_v3(),	
	disconnect(),

	?passed
end};
subscribe_test('5.0' = Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),
	subscribe_v5(),	
	disconnect(),

	?passed
end}.

subscribe_timeout_test('5.0' = Version, Conn_config) -> {"Subscribe Timeout test [" ++ atom_to_list(Version) ++ "]", timeout, 60, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect_v5(Conn_config),

	Expected_packet = <<130,28,0,100,17,11,233,230,10,38,0,3,75,101,121,0,5,86,97,108,117,101,0,5,84,111,112,105,99,2>>, 
	Subscribe_tuple = {subscribe, 
										[{<<"Topic">>, #subscription_options{max_qos=2, nolocal=0, retain_as_published=2}}],
										[{?User_Property, {<<"Key">>, <<"Value">>}}, {?Subscription_Identifier, 177001}]
									}, 
	mock_tcp:set_expectation(Expected_packet),
	gen_server:cast(client_gensrv, Subscribe_tuple),
	wait_mock_tcp("subscribe packet"),
%% from server:
	client_gensrv ! {tcp, get_socket(), <<>>}, %% Suback packet
	receive
		[onSubscribe, {Return_codes, Properties}] ->
			?debug_Fmt("::test:: return  codes = ~p Props = ~128p~n", [Return_codes, Properties]),
			?assert(false);
		[onError, #mqtt_error{errno= 136, error_msg= _Err_msg}= Err] ->
			?debug_Fmt("::test:: Timeout error message arrives= ~p ~n", [Err]),
			?assert(true);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting suback callback from client~n", []),
			?assert(false)
	end,

	disconnect(),

	?passed
end}.

subscribe_props_test('5.0' = Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),
	subscribe_v5_props(),	
	disconnect(),

	?passed
end}.

unsubscribe_test('3.1.1'=Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),
	subscribe_v3(),
	
	mock_tcp:set_expectation(<<162,9,0,101,0,5,84,111,112,105,99>>),
	gen_server:cast(client_gensrv, {unsubscribe, [<<"Topic">>]}),
	wait_mock_tcp("unsubscribe packet"),
%% from server:
	client_gensrv ! {tcp, get_socket(), <<176,2,0,101>>}, %% Unsuback packet
	receive
		[onUnsubscribe, {Return_codes1, _Props}] ->
			?debug_Fmt("::test:: unsuback with return codes: ~p~n", [Return_codes1]);
		Message1 -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message1]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onUnsubscribe callback from client~n", []),
			?assert(false)
	end,

	disconnect(),

	?passed
end};
unsubscribe_test('5.0' = Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),
	subscribe_v5(),

	mock_tcp:set_expectation(<<162,10,0,101,0,0,5,84,111,112,105,99>>),
	gen_server:cast(client_gensrv, {unsubscribe, [<<"Topic">>]}),
	wait_mock_tcp("unsibscribe packet"),
%% from server:
	client_gensrv ! {tcp, get_socket(), <<176,5,0,101, 0, 0, 17>>}, %% Unsuback packet
	receive
		[onUnsubscribe, {Return_codes1, _Props}] ->
			?debug_Fmt("::test:: unsuback with return codes: ~p~n", [Return_codes1]);
		Message1 ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message1]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting unsuback callback from client~n", []),
			?assert(false)
	end,

	disconnect(),

	?passed
	end}.

unsubscribe_timeout_test('5.0' = Version, Conn_config) -> {"Unsubscribe Timeout test [" ++ atom_to_list(Version) ++ "]", timeout, 60, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect_v5(Conn_config),
	subscribe_v5(),

	mock_tcp:set_expectation(<<162,10,0,101,0,0,5,84,111,112,105,99>>),
	gen_server:cast(client_gensrv, {unsubscribe, [<<"Topic">>]}),
	wait_mock_tcp("unsibscribe packet"),
%% from server:
	client_gensrv ! {tcp, get_socket(), <<>>}, %% Unsuback packet
	receive
		[onUnsubscribe, {Return_codes1, _Props}] ->
			?debug_Fmt("::test:: unsuback with return codes: ~p~n", [Return_codes1]),
			?assert(false);
		[onError, #mqtt_error{errno= 136, error_msg= _Err_msg}= Err] ->
			?debug_Fmt("::test:: Timeout error message arrives= ~p ~n", [Err]),
			?assert(true);
		Message1 ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message1]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting unsuback callback from client~n", []),
			?assert(false)
	end,

	disconnect(),

	?passed
end}.

unsubscribe_props_test('5.0' = Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),
	subscribe_v5(),
	
	mock_tcp:set_expectation(<<162,23,101:16,13, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8,5:16,84,111,112,105,99>>),
	gen_server:cast(client_gensrv, {unsubscribe, [<<"Topic">>], [{?User_Property, {"Key", "Value"}}]}),
	wait_mock_tcp("unsubscribe packet"),
%% from server:
	client_gensrv ! {tcp, get_socket(), <<176,38,101:16, 33, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 31, 17:16,"Unspecified error"/utf8, 0,17>>}, %% Unsuback packet
	receive
		[onUnsubscribe, {Return_codes1, Properties}] ->
			?debug_Fmt("::test:: unsuback with return codes: ~p props: ~128p~n", [Return_codes1, Properties]);
		Message1 ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p~n", [Message1]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting suback callback from client~n", []),
			?assert(false)
	end,

	disconnect(),

	?passed
	end}.

publish_0_test('3.1.1'=Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<48,14,0,5,84,111,112,105,99,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 0, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<0> packet"),
	
	disconnect(),

	?passed
end};
publish_0_test('5.0' = Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<48,15,0,5,84,111,112,105,99,0,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 0, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<0> packet"),
	
	disconnect(),

	?passed
	end}.

publish_0_props_test('5.0' = Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<48,25,0,5,84,111,112,105,99,10, 9,4:16,1,2,3,4, 35,300:16,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 0, topic = <<"Topic">>,
																										payload = <<"Payload">>,
																										properties=[{?Topic_Alias, 300},{?Correlation_Data, <<1,2,3,4>>}]}}),
	wait_mock_tcp("publish<0> packet"),
	
	disconnect(),

	?passed
	end}.

publish_1_test('3.1.1'=Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<50,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<1> packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), <<64,2,0,100>>}, %% Puback packet
	receive
		[onPublish, {0, []}] ->
			?debug_Fmt("::test:: Message to caller process= puback! ~n", []),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: <~p> Unexpected Message to caller process= ~p ~n", [?LINE, Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
end};
publish_1_test('5.0' = Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<50,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>}}),
	Conn_State = sys:get_state(client_gensrv),
	?debug_Fmt("::test:: State = ~p ~n", [Conn_State]),
	wait_mock_tcp("publish<1> packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), <<64,3,100:16,10>>}, %% Puback packet
	receive
		[onPublish, {RespCode, []}] ->
			?debug_Fmt("::test:: Puback message to caller process, response code = ~p ~n", [RespCode]),
			?assertEqual(10, RespCode);
		Msg -> 
			?debug_Fmt("::test:: <~p> Unexpected Message to caller process= ~p ~n", [?LINE, Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

publish_1_timeout_test('5.0' = Version, Conn_config) -> {"Publish 1 Timeout test [" ++ atom_to_list(Version) ++ "]", timeout, 60, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<50,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>}}),
	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),
	wait_mock_tcp("publish<1> packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), <<>>}, %% Puback packet
	receive
		[onPublish, _] ->
			?assert(false);
		[onError, #mqtt_error{errno= 136, error_msg= _Err_msg}= Err] ->
			?debug_Fmt("::test:: Timeout error message arrives= ~p ~n", [Err]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: <~p> Unexpected Message to caller process= ~p ~n", [?LINE, Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

publish_1_props_test('5.0' = Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<50,24,0,5,84,111,112,105,99,100:16, 7,2,120000:32,1,1,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish,
																	#publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>,
																					 properties = [{?Payload_Format_Indicator, 1},{?Message_Expiry_Interval, 120000}]}}),
	wait_mock_tcp("publish<1> packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), <<64,57,100:16,10, 53, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 31, 23:16,"No matching subscribers"/utf8>>}, %% Puback packet
	receive
		[onPublish, {RespCode, Properties}] ->
			?debug_Fmt("::test:: Puback message to caller process, response code = ~p~n      props=~128p~n", [RespCode, Properties]),
			?assertEqual(10, RespCode),
			?assertEqual([{31,<<"No matching subscribers">>},{38,{<<"Key Name">>,<<"Property Value">>}}], Properties);
		Msg -> 
			?debug_Fmt("::test:: <~p> Unexpected Message to caller process= ~p ~n", [?LINE, Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

publish_2_test('3.1.1'=Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<52,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 2, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<2> packet"),

%% from server:
	mock_tcp:set_expectation(<<98,2,0,100>>), %% expect pubrel packet from client -> server
	client_gensrv ! {tcp, get_socket(), <<80,2,0,100>>}, %% Pubrec packet from server -> client
	wait_mock_tcp("pubrel packet"),

	client_gensrv ! {tcp, get_socket(), <<112,2,0,100>>}, %% Pubcomp packet from server -> client
	receive
		[onPublish, {0, []}] ->
			?debug_Fmt("::test:: Message to caller process= pubcomp! ~n", []),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
end};
publish_2_test('5.0' = Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<52,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 2, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<2> packet"),

%% from server:
	mock_tcp:set_expectation(<<98,2,0,100>>), %% expect pubrel packet from client -> server
	client_gensrv ! {tcp, get_socket(), <<80,2,0,100>>}, %% Pubrec packet from server -> client
	wait_mock_tcp("pubrel packet"),

	client_gensrv ! {tcp, get_socket(), <<112,3,0,100,1>>}, %% Pubcomp packet from server -> client
	receive
		[onPublish, {1, []}] ->
			?debug_Fmt("::test:: Message to caller process= pubcomp! ~n", []),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

publish_2_timeout_test('5.0' = Version, Conn_config) -> {"Publish 2 Timeout test [" ++ atom_to_list(Version) ++ "]", timeout, 60, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<52,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 2, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<2> packet"),

%% from server:
	mock_tcp:set_expectation(<<98,2,0,100>>), %% expect pubrel packet from client -> server
	client_gensrv ! {tcp, get_socket(), <<80,2,0,100>>}, %% Pubrec packet from server -> client
	wait_mock_tcp("pubrel packet"),

	client_gensrv ! {tcp, get_socket(), <<>>}, %% Pubcomp packet from server -> client
	receive
		[onPublish, _] ->
			?assert(false);
		[onError, #mqtt_error{errno= 136, error_msg= _Err_msg}= Err] ->
			?debug_Fmt("::test:: Timeout error message arrives= ~p ~n", [Err]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

publish_2_props_test('5.0' = Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<52,27,0,5,84,111,112,105,99,100:16,10,9,0,4,1,2,3,4,35,1,44,80,97,121,108,111,97,100>>),
	gen_server:cast(client_gensrv, {publish, #publish{qos = 2, topic = <<"Topic">>,
																															payload = <<"Payload">>,
																															properties = [{?Topic_Alias, 300},{?Correlation_Data, <<1,2,3,4>>}]}}),
	wait_mock_tcp("publish<2> packet"),

%% from server:
	mock_tcp:set_expectation(<<98,3,0,100,16>>), %% expect pubrel packet from client -> server
	client_gensrv ! {tcp, get_socket(),
										<<80,57,0,100,16,
											53, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 
											31,23:16,"No matching subscribers"/utf8>>}, %% Pubrec packet from server -> client
	wait_mock_tcp("pubrel packet"),

	client_gensrv ! {tcp, get_socket(),
										<<112,61,0,100,1,57, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 
											31, 27:16,"Packet Identifier not found"/utf8>>}, %% Pubcomp packet from server -> client
	receive
		[onPublish, {RespCode, Properties}] ->
			?debug_Fmt("::test:: Message to caller process= pubcomp! response code = ~p~n      props=~128p~n", [RespCode, Properties]),
			?assertEqual(1, RespCode),
			?assertEqual([{31, <<"Packet Identifier not found">>}, {38, {<<"Key Name">>, <<"Property Value">>}}], Properties),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

receive_0_test('3.1.1'=Version, Conn_config) -> {"Receive 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	client_gensrv ! {tcp, get_socket(), <<48,14,0,5,84,111,112,105,99,80,97,121,108,111,97,100>>}, %% from server -> client
	
	receive
		[onReceive, {undefined, PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,
	
	disconnect(),

	?passed
end};
receive_0_test('5.0' = Version, Conn_config) -> {"Receive 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	client_gensrv ! {tcp, get_socket(), <<48,15,0,5,84,111,112,105,99,0,80,97,121,108,111,97,100>>}, %% from server -> client
	
	receive
		[onReceive, {undefined, PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

receive_0_props_test('5.0' = Version, Conn_config) -> {"Receive 0 props test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	client_gensrv ! {
		tcp,
		get_socket(),
		<<48,25,0,5,84,111,112,105,99,10, 9,4:16,1,2,3,4, 35,300:16,80,97,121,108,111,97,100>>}, %% from server -> client
	
	receive
		[onReceive, {undefined, #publish{qos = Qos, topic = Topic, payload = Payload, properties = Props} = PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assertEqual(0, Qos),
			?assertEqual("Topic", Topic),
			?assertEqual(<<"Payload">>, Payload),
			?assertEqual([{?Topic_Alias,300},{?Correlation_Data,<<1,2,3,4>>}], Props),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,
	
	disconnect(),

	?passed
	end}.

receive_1_test('3.1.1'=Version, Conn_config) -> {"Receive 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<64,2,0,100>>),
	client_gensrv ! {tcp, get_socket(), <<50,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>}, %% from server -> client

	receive
		[onReceive, {undefined, PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,

	wait_mock_tcp("publish<1> puback packet"), %% from client -> server

	disconnect(),

	?passed
end};
receive_1_test('5.0' = Version, Conn_config) -> {"Receive 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<64,2,100:16>>),
	client_gensrv ! {tcp, get_socket(), <<50,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>}, %% from server -> client

	receive
		[onReceive, {undefined, PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,
	
	wait_mock_tcp("publish<1> puback packet"), %% from client -> server

	disconnect(),

	?passed
	end}.

receive_1_props_test('5.0' = Version, Conn_config) -> {"Receive 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<64,2,100:16>>),
	client_gensrv ! {tcp, get_socket(), <<50,24,0,5,"Topic"/utf8,100:16, 7,2,120000:32,1,1, "Payload"/utf8>>}, %% from server -> client
%% #publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>,
%% 					properties = [{?Payload_Format_Indicator, 1},{?Message_Expiry_Interval, 120000}]}
	receive
		[onReceive, {undefined, #publish{qos = Qos, topic = Topic, payload = Payload, properties = Props} = PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assertEqual(1, Qos),
			?assertEqual("Topic", Topic),
			?assertEqual(<<"Payload">>, Payload),
			?assertEqual([{?Payload_Format_Indicator, 1},{?Message_Expiry_Interval, 120000}], Props),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,
	
	wait_mock_tcp("publish<1> puback packet"), %% from client -> server

	disconnect(),

	?passed
	end}.

receive_2_test('3.1.1'=Version, Conn_config) -> {"Receive 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<80,2,0,100>>), %% Pubrec packet from client -> server
	client_gensrv ! {tcp, get_socket(), <<52,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>}, %% from server -> client

	receive
		[onReceive, {undefined, PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,

	wait_mock_tcp("publish<2> pubrec packet"), %% from client -> server

	mock_tcp:set_expectation(<<112,2,0,100>>), %% expect pubcomp packet from server -> client
	client_gensrv ! {tcp, get_socket(), <<98,2,0,100>>}, %% Pubrel packet from server -> client
	wait_mock_tcp("publish<2> pubcomp packet"),

	disconnect(),

	?passed
end};
receive_2_test('5.0' = Version, Conn_config) -> {"Receive 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<80,2,0,100>>), %% Pubrec packet from client -> server
	client_gensrv ! {tcp, get_socket(), <<52,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>}, %% from server -> client

	receive
		[onReceive, {undefined, PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,

	wait_mock_tcp("publish<2> pubrec packet"), %% from client -> server

	mock_tcp:set_expectation(<<112,2,0,100>>), %% expect pubcomp packet from server -> client
	client_gensrv ! {tcp, get_socket(), <<98,2,0,100>>}, %% Pubrel packet from server -> client
	wait_mock_tcp("publish<2> pubcomp packet"),

	disconnect(),

	?passed
	end}.

receive_2_props_test('5.0' = Version, Conn_config) -> {"Receive 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<80,2,0,100>>), %% Pubrec packet from client -> server
	client_gensrv ! {tcp, get_socket(), <<52,27,0,5,84,111,112,105,99,100:16,10,9,0,4,1,2,3,4,35,1,44,80,97,121,108,111,97,100>>}, %% from server -> client

	receive
		[onReceive, {undefined, #publish{qos = Qos, topic = Topic, payload = Payload, properties = Props} = PubRecord}] ->
			?debug_Fmt("::test:: Message on Receive event = ~128p~n", [PubRecord]),
			?assertEqual(2, Qos),
			?assertEqual("Topic", Topic),
			?assertEqual(<<"Payload">>, Payload),
			?assertEqual([{?Topic_Alias,300},{?Correlation_Data,<<1,2,3,4>>}], Props),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting Message on Receive~n", []),
			?assert(false)
	end,

	wait_mock_tcp("publish<2> pubrec packet"), %% from client -> server

	mock_tcp:set_expectation(<<112,2,0,100>>), %% expect pubcomp packet from server -> client
	client_gensrv ! {tcp, get_socket(), <<98,2,0,100>>}, %% Pubrel packet from server -> client
	wait_mock_tcp("publish<2> pubcomp packet"),
	
	disconnect(),

	?passed
	end}.

%% ====================================================================
%% Internal functions
%% ====================================================================
wait_mock_tcp(R) ->
	receive
		{mock_tcp, true} ->
			?debug_Fmt("::test:: mock_tcp ~p acknowledge ~n", [R]),
			?assert(true);
		{mock_tcp, false} ->
			?assert(false);
		M ->
			?debug_Fmt("::test:: while waiting ~p mock_tcp got unexpected msg = ~p~n", [R, M]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting ~p from mock_tcp~n", [R]),
			?assert(false)
	end.

connect_v3(Conn_config) ->
	Expected_packet = <<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>, 
	Connack_packet = <<32,2,0,0>>,
	connect(Conn_config, Expected_packet, Connack_packet).

connect_v5(Conn_config) ->
	Expected_packet = <<16,38, 4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>, 
	Connack_packet = <<32,3,1,0,0>>,
	connect(Conn_config, Expected_packet, Connack_packet).

connect(Conn_config, Expected_packet, Connack_packet) ->
	mock_tcp:set_expectation(Expected_packet),
	gen_server:cast(client_gensrv, {connect, Conn_config, self(), []}),
	wait_mock_tcp("connect packet"),

%% from server:
	client_gensrv ! {tcp, get_socket(), Connack_packet}, %% Connack packet
	receive
		[onConnect, _] = _Args ->
%			?debug_Fmt("::test:: Message to caller process= ~p ~n", [_Args]),
			?assert(true);
		Mssg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Mssg]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onConnect callback from client~n", []),
			?assert(false)
	end.

ping_pong() ->
	mock_tcp:set_expectation(<<192,0>>),
	ok = gen_server:cast(client_gensrv, pingreq),
	wait_mock_tcp("ping packet"),
	client_gensrv ! {tcp, get_socket(), <<16#D0:8, 0:8>>}, %% PingResp
	receive
		[onPong, Ping_count] ->
			?debug_Fmt("::test:: onPong = ~p ~n", [Ping_count]),
			?assert(Ping_count == 0);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while onPong callback from client~n", []),
			?assert(false)
	end.

subscribe_v3() ->
	Expected_packet = <<130,10,0,100,0,5,84,111,112,105,99,2>>, 
	Subscribe_tuple = {subscribe, [{<<"Topic">>, 2}]}, 
	Suback_packet = <<144,3,0,100,2>>,
	subscribe(Expected_packet, Subscribe_tuple, Suback_packet).

subscribe_v5() ->
	Expected_packet = <<130,11,0,100,0,0,5,84,111,112,105,99,2>>, 
	Subscribe_tuple = {subscribe, [{<<"Topic">>, #subscription_options{max_qos=2, nolocal=0, retain_as_published=2}}]}, 
	Suback_packet = <<144,4, 0,100, 0, 2>>,
	subscribe(Expected_packet, Subscribe_tuple, Suback_packet).

subscribe_v5_props() ->
	Expected_packet = <<130,28,0,100,17,11,233,230,10,38,0,3,75,101,121,0,5,86,97,108,117,101,0,5,84,111,112,105,99,2>>, 
	Subscribe_tuple = {subscribe, 
										[{<<"Topic">>, #subscription_options{max_qos=2, nolocal=0, retain_as_published=2}}],
										[{?User_Property, {<<"Key">>, <<"Value">>}}, {?Subscription_Identifier, 177001}]
									}, 
	Suback_packet = <<144,37, 0,100, 33, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 31,17:16,"Unspecified error"/utf8, 0>>,
	subscribe(Expected_packet, Subscribe_tuple, Suback_packet).

subscribe(Expected_packet, Subscribe_tuple, Suback_packet) ->
	mock_tcp:set_expectation(Expected_packet),
	gen_server:cast(client_gensrv, Subscribe_tuple),
	wait_mock_tcp("subscribe packet"),
%% from server:
	client_gensrv ! {tcp, get_socket(), Suback_packet}, %% Suback packet
	receive
		[onSubscribe, {Return_codes, Properties}] ->
			?debug_Fmt("::test:: return  codes = ~p Props = ~128p~n", [Return_codes, Properties]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting suback callback from client~n", []),
			?assert(false)
	end.

disconnect() ->
	?debug_Fmt("::test:: >>> disconnect PID:~p~n", [self()]),
	mock_tcp:set_expectation(<<224,0>>),
	gen_server:cast(client_gensrv, {disconnect, 0, []}),
	wait_mock_tcp("disconnect"),
%%	from server
	client_gensrv ! {tcp, get_socket(), <<224,0>>}, %% Disconnect packet
	receive
		[onClose, {_DisconnectReasonCode, _Properties}] = Args ->
			?debug_Fmt("::test:: onClose event callback = ~p ~n", [Args]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onClose callback from client~n", []),
			?assert(false)
	end.

get_socket() ->
	(sys:get_state(client_gensrv))#connection_state.socket.
