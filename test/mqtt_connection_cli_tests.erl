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
				,{'5.0',   fun subscribe_test/2}
				,{'5.0',   fun subscribe_props_test/2}
				,{'3.1.1', fun unsubscribe_test/2}
				,{'5.0',   fun unsubscribe_test/2}
				,{'5.0',   fun unsubscribe_props_test/2}
				,{'3.1.1', fun publish_0_test/2}
				,{'5.0',   fun publish_0_test/2}
				,{'5.0',   fun publish_0_props_test/2}
				,{'3.1.1', fun publish_1_test/2}
				,{'5.0',   fun publish_1_test/2}
				,{'5.0',   fun publish_1_props_test/2}
				,{'3.1.1', fun publish_2_test/2}
				,{'5.0',   fun publish_2_test/2}
				,{'5.0',   fun publish_2_props_test/2}
			]
		}
	 }
	].

do_start() ->
	?debug_Fmt("::test:: >>> do_start() ~n", []),
	lager:start(),

	mqtt_dets_dao:start(client),
	mqtt_dets_dao:cleanup(client),
	Transport = mock_tcp,
	mock_tcp:start(),
	Storage = mqtt_dets_dao,
	Socket = undefined,
	State = #connection_state{socket = Socket, transport = Transport, storage = Storage, end_type = client},
	{ok, Pid} = gen_server:start_link({local, conn_server}, mqtt_connection, State, [{timeout, ?MQTT_GEN_SERVER_TIMEOUT}]),
	Pid.


do_stop(_Pid) ->
	?debug_Fmt("::test:: >>> do_stop(~p) ~n", [_Pid]),
% Close connection - stop the conn_server process.
	conn_server ! {tcp, undefined, <<224,0>>}, %% Disconnect packet
	unregister(conn_server),
	mock_tcp:stop(),
	mqtt_dets_dao:cleanup(client),	
	mqtt_dets_dao:close(client).	

setup('3.1.1') ->
	?debug_Fmt("::test:: >>> setup('3.1.1') ~n", []),
	#connect{client_id = <<"test0Client">>, user_name = ?TEST_USER, password = ?TEST_PASSWORD, keep_alive = 60000, version = '3.1.1'};
setup('5.0') ->
	?debug_Fmt("::test:: >>> setup('5.0') ~n", []),
		#connect{client_id = <<"test0Client">>, user_name = ?TEST_USER, password = ?TEST_PASSWORD, keep_alive = 60000, version = '5.0'}.

cleanup(X, Y) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p) ~n", [X,Y#connect.client_id]).

connection_test('3.1.1'=Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	mock_tcp:set_expectation(<<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>),
	{ok, Ref} = gen_server:call(conn_server, {connect, Conn_config}),
	wait_mock_tcp("connect packet"),
	Conn_State = sys:get_state(conn_server),
%	Transport = Conn_State#connection_state.transport,
	?debug_Fmt("::test:: State = ~p ~n", [Conn_State]),
%% from server:
	conn_server ! {tcp, undefined, <<32,2,0,0>>}, %% Connack packet
	receive
		{connack, Ref, SP, ConnRespCode, Msg, _} ->
			?debug_Fmt("::test:: Message to caller process= ~p ~n", [Msg]),
			?assertEqual(0, SP),
			?assertEqual(0, ConnRespCode);
		Msg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<192,0>>),
	ok = gen_server:call(conn_server, {pingreq, fun(_Pong) -> ok end}),
	wait_mock_tcp("ping packet"),
	Conn_State1 = sys:get_state(conn_server),
	?debug_Fmt("::test:: ping_count = ~p ~n", [Conn_State1#connection_state.ping_count]),
	?assertEqual(1, Conn_State1#connection_state.ping_count),

	conn_server ! {tcp, undefined, <<16#D0:8, 0:8>>}, %% PingResp
	Conn_State2 = sys:get_state(conn_server),
	?debug_Fmt("::test:: ping_count = ~p ~n", [Conn_State2#connection_state.ping_count]),
	?assertEqual(0, Conn_State2#connection_state.ping_count),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect, 0, []}),
	wait_mock_tcp("disconnect"),
% Close connection - stop the conn_server process.
%	conn_server ! {tcp, undefined, <<224,0>>}, 

%	timer:sleep(2000),
	?passed
end};
connection_test('5.0' = Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	mock_tcp:set_expectation(<<16,38, 4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>),
	{ok, Ref} = gen_server:call(conn_server, {connect, Conn_config}),
	wait_mock_tcp("connect packet"),
%% from server:
	conn_server ! {tcp, undefined, <<32,3,1,0,0>>}, %% Connack packet
	receive
		{connack, Ref, SP, ConnRespCode, Msg, _} ->
			?debug_Fmt("::test:: Message to caller process= ~p ~n", [Msg]),
			?assertEqual(1, SP),
			?assertEqual(0, ConnRespCode);
		Msg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<192,0>>),
	ok = gen_server:call(conn_server, {pingreq, fun(_Pong) -> ok end}),
	wait_mock_tcp("ping packet"),
	conn_server ! {tcp, undefined, <<16#D0:8, 0:8>>}, %% PingResp

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),
% Close connection - stop the conn_server process.
%	conn_server ! {tcp, undefined, <<224,2,0,0>>}, 

	?passed
end}.

connection_props_test('5.0' = Version, Conn_config) -> {"Connection test with properties [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	Config = Conn_config#connect{
						will = 1,
						will_publish = #publish{qos= 2, retain= 1, topic= "Last_msg", payload= <<"Good bye!">>,
																		properties = [{?Will_Delay_Interval, 6000}, {?Response_Topic, "AfterClose/Will"}]},
						properties = [{?Maximum_Packet_Size, 65000}, {?Session_Expiry_Interval, 16#FFFFFFFF}]
					},
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Config]),

	mock_tcp:set_expectation(<<16,93, 4:16,"MQTT"/utf8,5,246,234,96, 
		10, 17, 16#FFFFFFFF:32, 39, 65000:32,
		11:16,"test0Client"/utf8, 
		23, 8, 15:16,"AfterClose/Will"/utf8, 
		24, 6000:32, 8:16,"Last_msg"/utf8, 9:16,"Good bye!",
		5:16,"guest"/utf8, 5:16,"guest"/utf8>>),
	{ok, Ref} = gen_server:call(conn_server, {connect, Config}),
	wait_mock_tcp("connect packet"),
%% from server:
	conn_server ! {tcp, undefined, <<32,7,1,0, 4, 37,1, 36,2>>}, %% Connack packet
	receive
		{connack, Ref, SP, ConnRespCode, Msg, Props} ->
			?debug_Fmt("::test:: Message to caller process= ~128p~n         Props= ~128p", [Msg, Props]),
			?assertEqual(1, SP),
			?assertEqual(0, ConnRespCode);
		Msg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<192,0>>),
	ok = gen_server:call(conn_server, {pingreq, fun(_Pong) -> ok end}),
	wait_mock_tcp("ping packet"),
	conn_server ! {tcp, undefined, <<16#D0:8, 0:8>>}, %% PingResp

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

subscribe_test('3.1.1'=Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<130,10,0,100,0,5,84,111,112,105,99,2>>),
	{ok, Ref1} = gen_server:call(conn_server, {subscribe, [{<<"Topic">>, 2, callback}]}),
	wait_mock_tcp("subscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<144,3,0,100,2>>}, %% Suback packet
	receive
		{suback, Ref1, Return_codes, _} ->
			?debug_Fmt("::test:: return  codes = ~p ~n", [Return_codes]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),
% Close connection - stop the conn_server process.
%	conn_server ! {tcp, undefined, <<224,0>>}, 

%	timer:sleep(2000),
	?passed
end};
subscribe_test('5.0' = Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<130,11,0,100,0,0,5,84,111,112,105,99,2>>),
	{ok, Ref1} = gen_server:call(conn_server, {subscribe, [{<<"Topic">>, #subscription_options{max_qos=2, nolocal=0, retain_as_published=2}, callback}]}),
	wait_mock_tcp("subscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<144,4, 0,100, 0, 2>>}, %% Suback packet
	receive
		{suback, Ref1, Return_codes, _} ->
			?debug_Fmt("::test:: return  codes = ~p ~n", [Return_codes]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

subscribe_props_test('5.0' = Version, Conn_config) -> {"Subscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<130,28,0,100,17,11,233,230,10,38,0,3,75,101,121,0,5,86,97,108,117,101,0,5,84,111,112,105,99,2>>),
	{ok, Ref} = gen_server:call(conn_server,
																{subscribe, 
																	[{<<"Topic">>, #subscription_options{max_qos=2, nolocal=0, retain_as_published=2}, callback}],
																	[{?User_Property, {<<"Key">>, <<"Value">>}}, {?Subscription_Identifier, 177001}]
																}),
	wait_mock_tcp("subscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<144,37, 0,100, 33, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 31,17:16,"Unspecified error"/utf8, 0>>}, %% Suback packet
	receive
		{suback, Ref, Return_codes, Props} ->
			?debug_Fmt("::test:: return  codes = ~p Props = ~128p~n", [Return_codes, Props]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end}.

unsubscribe_test('3.1.1'=Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<130,10,0,100,0,5,84,111,112,105,99,2>>),
	{ok, Ref1} = gen_server:call(conn_server, {subscribe, [{<<"Topic">>, 2, callback}]}),
	wait_mock_tcp("subscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<144,3,0,100,2>>}, %% Suback packet
	receive
		{suback, Ref1, Return_codes, _} ->
			?debug_Fmt("::test:: return  codes = ~p ~n", [Return_codes]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<162,9,0,101,0,5,84,111,112,105,99>>),
	{ok, Ref2} = gen_server:call(conn_server, {unsubscribe, [<<"Topic">>]}),
	wait_mock_tcp("unsubscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<176,2,0,101>>}, %% Unsuback packet
	receive
		{unsuback, Ref2, [], _} ->
			?debug_Fmt("::test:: unsuback! ~n", []);
		Message1 -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message1]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
unsubscribe_test('5.0' = Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<130,11,0,100,0,0,5,84,111,112,105,99,2>>),
	{ok, Ref1} = gen_server:call(conn_server, {subscribe, [{<<"Topic">>, #subscription_options{max_qos=2, nolocal=0, retain_as_published=2}, callback}]}),
	wait_mock_tcp("subscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<144,4, 0,100, 0, 2>>}, %% Suback packet
	receive
		{suback, Ref1, Return_codes, _} ->
			?debug_Fmt("::test:: return  codes = ~p ~n", [Return_codes]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<162,10,0,101,0,0,5,84,111,112,105,99>>),
	{ok, Ref2} = gen_server:call(conn_server, {unsubscribe, [<<"Topic">>]}),
	wait_mock_tcp("unsibscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<176,5,0,101, 0, 0, 17>>}, %% Unsuback packet
	receive
		{unsuback, Ref2, Return_codes1, _} ->
			?debug_Fmt("::test:: unsuback with return codes: ~p ~n", [Return_codes1]);
		Message1 ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message1]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

unsubscribe_props_test('5.0' = Version, Conn_config) -> {"Unsubscribe test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<130,11,0,100,0,0,5,84,111,112,105,99,2>>),
	{ok, Ref1} = gen_server:call(conn_server, {subscribe, [{<<"Topic">>, #subscription_options{max_qos=2, nolocal=0, retain_as_published=2}, callback}]}),
	wait_mock_tcp("subscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<144,4, 0,100, 0, 2>>}, %% Suback packet
	receive
		{suback, Ref1, Return_codes, _} ->
			?debug_Fmt("::test:: suback with return codes = ~p ~n", [Return_codes]);
		Message ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<162,23,101:16,13, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8,5:16,84,111,112,105,99>>),
	{ok, Ref2} = gen_server:call(conn_server, {unsubscribe, [<<"Topic">>], [{?User_Property, {"Key", "Value"}}]}),
	wait_mock_tcp("unsubscribe packet"),
%% from server:
	conn_server ! {tcp, undefined, <<176,38,101:16, 33, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 31, 17:16,"Unspecified error"/utf8, 0,17>>}, %% Unsuback packet
	receive
		{unsuback, Ref2, Return_codes1, Properties} ->
			?debug_Fmt("::test:: unsuback with return codes: ~p props: ~128p~n", [Return_codes1, Properties]);
		Message1 ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message1]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_0_test('3.1.1'=Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<48,14,0,5,84,111,112,105,99,80,97,121,108,111,97,100>>),
	{ok, _Ref1} = gen_server:call(conn_server, {publish, #publish{qos = 0, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<0> packet"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
publish_0_test('5.0' = Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<48,15,0,5,84,111,112,105,99,0,80,97,121,108,111,97,100>>),
	{ok, _Ref1} = gen_server:call(conn_server, {publish, #publish{qos = 0, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<0> packet"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_0_props_test('5.0' = Version, Conn_config) -> {"Publish 0 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<48,25,0,5,84,111,112,105,99,10, 9,4:16,1,2,3,4, 35,300:16,80,97,121,108,111,97,100>>),
	{ok, _Ref1} = gen_server:call(conn_server, {publish, #publish{qos = 0, topic = <<"Topic">>,
																																payload = <<"Payload">>,
																																properties=[{?Topic_Alias, 300},{?Correlation_Data, <<1,2,3,4>>}]}}),
	wait_mock_tcp("publish<0> packet"),

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_1_test('3.1.1'=Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<50,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>),
	{ok, Ref} = gen_server:call(conn_server, {publish, #publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<1> packet"),

%% from server:
	conn_server ! {tcp, undefined, <<64,2,0,100>>}, %% Puback packet
	receive
		{puback, Ref, 0, _} ->
			?debug_Fmt("::test:: Message to caller process= puback! ~n", []),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: <~p> Unexpected Message to caller process= ~p ~n", [?LINE, Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
publish_1_test('5.0' = Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<50,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>),
	{ok, Ref} = gen_server:call(conn_server, {publish, #publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>}}),
	Conn_State = sys:get_state(conn_server),
	?debug_Fmt("::test:: State = ~p ~n", [Conn_State]),
	wait_mock_tcp("publish<1> packet"),

%% from server:
	conn_server ! {tcp, undefined, <<64,3,100:16,10>>}, %% Puback packet
	receive
		{puback, Ref, RespCode, _} ->
			?debug_Fmt("::test:: Puback message to caller process, response code = ~p ~n", [RespCode]),
			?assertEqual(10, RespCode);
		Msg -> 
			?debug_Fmt("::test:: <~p> Unexpected Message to caller process= ~p ~n", [?LINE, Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_1_props_test('5.0' = Version, Conn_config) -> {"Publish 1 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<50,24,0,5,84,111,112,105,99,100:16, 7,2,120000:32,1,1,80,97,121,108,111,97,100>>),
	{ok, Ref} = gen_server:call(conn_server, {publish,
																						#publish{qos = 1, topic = <<"Topic">>, payload = <<"Payload">>,
																										 properties = [{?Payload_Format_Indicator, 1},{?Message_Expiry_Interval, 120000}]}}),
	wait_mock_tcp("publish<1> packet"),

%% from server:
	conn_server ! {tcp, undefined, <<64,57,100:16,10, 53, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 31, 23:16,"No matching subscribers"/utf8>>}, %% Puback packet
	receive
		{puback, Ref, RespCode, Properties} ->
			?debug_Fmt("::test:: Puback message to caller process, response code = ~p~n      props=~128p~n", [RespCode, Properties]),
			?assertEqual(10, RespCode),
			?assertEqual([{31,<<"No matching subscribers">>},{38,{<<"Key Name">>,<<"Property Value">>}}], Properties);
		Msg -> 
			?debug_Fmt("::test:: <~p> Unexpected Message to caller process= ~p ~n", [?LINE, Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_2_test('3.1.1'=Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) PID=~p~n", [Version, Conn_config, self()]),
	connect_v3(Conn_config),

	mock_tcp:set_expectation(<<52,16,0,5,84,111,112,105,99, 100:16, 80,97,121,108,111,97,100>>),
	{ok, Ref} = gen_server:call(conn_server, {publish, #publish{qos = 2, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<2> packet"),

%% from server:
	mock_tcp:set_expectation(<<98,2,0,100>>), %% expect pubrel packet from client -> server
	conn_server ! {tcp, undefined, <<80,2,0,100>>}, %% Pubrec packet from server -> client
	wait_mock_tcp("pubrel packet"),

	conn_server ! {tcp, undefined, <<112,2,0,100>>}, %% Pubcomp packet from server -> client
	receive
		{pubcomp, Ref, 0, _} ->
			?debug_Fmt("::test:: Message to caller process= pubcomp! ~n", []),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
end};
publish_2_test('5.0' = Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<52,17,0,5,84,111,112,105,99,100:16, 0,80,97,121,108,111,97,100>>),
	{ok, Ref} = gen_server:call(conn_server, {publish, #publish{qos = 2, topic = <<"Topic">>, payload = <<"Payload">>}}),
	wait_mock_tcp("publish<2> packet"),

%% from server:
	mock_tcp:set_expectation(<<98,2,0,100>>), %% expect pubrel packet from client -> server
	conn_server ! {tcp, undefined, <<80,2,0,100>>}, %% Pubrec packet from server -> client
	wait_mock_tcp("pubrel packet"),

	conn_server ! {tcp, undefined, <<112,3,0,100,1>>}, %% Pubcomp packet from server -> client
	receive
		{pubcomp, Ref, 1, _} ->
			?debug_Fmt("::test:: Message to caller process= pubcomp! ~n", []),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,

	mock_tcp:set_expectation(<<224,0>>),
	{ok, _} = gen_server:call(conn_server, {disconnect,0,[]}),
	wait_mock_tcp("disconnect"),

	?passed
	end}.

publish_2_props_test('5.0' = Version, Conn_config) -> {"Publish 2 test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Conn_config]),
	connect_v5(Conn_config),

	mock_tcp:set_expectation(<<52,27,0,5,84,111,112,105,99,100:16,10,9,0,4,1,2,3,4,35,1,44,80,97,121,108,111,97,100>>),
	{ok, Ref} = gen_server:call(conn_server, {publish, #publish{qos = 2, topic = <<"Topic">>,
																															payload = <<"Payload">>,
																															properties = [{?Topic_Alias, 300},{?Correlation_Data, <<1,2,3,4>>}]}}),
	wait_mock_tcp("publish<2> packet"),

%% from server:
	mock_tcp:set_expectation(<<98,3,0,100,16>>), %% expect pubrel packet from client -> server
	conn_server ! {tcp, undefined, <<80,57,0,100,16,
																	 53, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 
																			 31,23:16,"No matching subscribers"/utf8>>}, %% Pubrec packet from server -> client
	wait_mock_tcp("pabrel packet"),

	conn_server ! {tcp, undefined, <<112,61,0,100,1,57, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 
																	 31, 27:16,"Packet Identifier not found"/utf8>>}, %% Pubcomp packet from server -> client
	receive
		{pubcomp, Ref, 1, Props} ->
			?debug_Fmt("::test:: Message to caller process= pubcomp! ~n       Props = ~p~n", [Props]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end,

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

connect_v3(Conn_config) ->
	mock_tcp:set_expectation(<<16,37, 4:16,"MQTT"/utf8,4,194,234,96, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>),
	{ok, Ref} = gen_server:call(conn_server, {connect, Conn_config}),
	wait_mock_tcp("connect packet"),
%% from server:
	conn_server ! {tcp, undefined, <<32,2,0,0>>}, %% Connack packet
	receive
		{connack, Ref, _SP, _ConnRespCode, _Msg, _} ->
%			?debug_Fmt("::test:: Message to caller process= ~p ~n", [Msg]),
			?assert(true);
		Msg -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end.

connect_v5(Conn_config) ->
	mock_tcp:set_expectation(<<16,38, 4:16,"MQTT"/utf8,5,194,234,96, 0, 11:16,"test0Client"/utf8, 5:16,"guest"/utf8, 5:16,"guest"/utf8>>),
	{ok, Ref} = gen_server:call(conn_server, {connect, Conn_config}),
	wait_mock_tcp("connect packet"),
%% from server:
	conn_server ! {tcp, undefined, <<32,3,1,0,0>>}, %% Connack packet
	receive
		{connack, Ref, _SP, _ConnRespCode, _Msg, _} ->
%			?debug_Fmt("::test:: Message to caller process= ~p ~n", [Msg]),
			?assert(true);
		Msg ->
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Msg]),
			?assert(false)
	end.

%% template_test(Version, Y) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 1, fun() ->
%% 	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [Version, Y]),
%% 	mock_tcp:set_expectation(<<192,0>>),
%% 	gen_server:call(conn_server, {pingreq, fun(_Pong) -> ok end}),
%% 	mock_tcp:set_expectation(<<224,0>>),
%% 	gen_server:call(conn_server, disconnect),
%% 	conn_server ! {tcp, undefined, <<224,0>>},
%% 	?passed
%% end}.
