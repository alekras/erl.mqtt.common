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

-module(mqtt_subscribe_cli_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
-include_lib("mqtt_property.hrl").
-include("test.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-import(mqtt_connection_cli_tests, [connect/2, disconnect/1]).
-import(mock_tcp, [wait_mock_tcp/1]).

-export([]).

subscribe_genServer_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				 {'3.1.1', fun storage_test/2}
				,{'5.0',   fun storage_test/2}
			]
		}
	 }
	].

do_start() ->
	application:start(mqtt_common),
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
%%	client_gensrv ! {tcp, undefined, <<224,0>>}, %% Disconnect packet
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

storage_test('3.1.1'=Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect(Version, Conn_config),
	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),
	
	subscribe(Version, 2),
%	?debug_Fmt("::test:: Records = ~p ~n", [mqtt_dets_storage:subscription(get_client_topics,<<"test0Client">>,client)]),
	[Record] = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= <<"test0Client">>, topicFilter= "Topic"}, client),
%	?debug_Fmt("::test:: Record = ~p ~n", [Record]),
	?assertEqual(2, (Record#storage_subscription.options)#subscription_options.max_qos),
	
	unsubscribe(Version),
	Record1 = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= <<"test0Client">>, topicFilter= "Topic"}, client),
	?assertEqual([], Record1),

	disconnect(Version),

	?passed
end};
storage_test('5.0' = Version, Conn_config) -> {"Connection test [" ++ atom_to_list(Version) ++ "]", timeout, 5, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) test process PID=~p~n", [Version, Conn_config, self()]),
	connect(Version, Conn_config),
	?debug_Fmt("::test:: State = ~p ~n", [sys:get_state(client_gensrv)]),
	subscribe(Version, #subscription_options{max_qos=2, nolocal=0, retain_as_published=1, retain_handling=2}),
	[Record] = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= <<"test0Client">>, topicFilter= "Topic"}, client),
	?assertEqual(#subscription_options{max_qos=2, nolocal=0, retain_as_published=1, retain_handling=2}, Record#storage_subscription.options),
	
	unsubscribe(Version),
	Record1 = mqtt_dets_storage:subscription(get, #subs_primary_key{client_id= <<"test0Client">>, topicFilter= "Topic"}, client),
	?assertEqual([], Record1),

	disconnect(Version),

	?passed
end}.

%% ====================================================================
%% Internal functions
%% ====================================================================

subscribe('3.1.1', QoS) ->
	Expected_packet = <<130,19,0,100,0,5,"Topic"/utf8,QoS,0,6,"TopicA"/utf8,QoS>>, 
	Subscribe_tuple = {subscribe, [{<<"Topic">>, QoS},{<<"TopicA">>, QoS}]}, 
	Suback_packet = <<144,4, 0,100, 2, 2>>,
	subscribe(Expected_packet, Subscribe_tuple, Suback_packet);
subscribe('5.0', #subscription_options{max_qos=MQ, nolocal=NL, retain_as_published=RAP, retain_handling=RH} = Opt) ->
	Expected_packet = <<130,20,0,100,0,0,5,"Topic"/utf8,0:2, RH:2, RAP:1, NL:1, MQ:2,0,6,"TopicA"/utf8,0:2, RH:2, RAP:1, NL:1, MQ:2>>, 
	Subscribe_tuple = {subscribe, [{<<"Topic">>, Opt},{<<"TopicA">>, Opt}]}, 
	Suback_packet = <<144,5, 0,100, 0, 2, 2>>,
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

unsubscribe('3.1.1') ->
	Expected_packet = <<162,9,0,101,0,5,"Topic"/utf8>>, 
	Unsuback_packet = <<176,2,0,101>>,
	unsubscribe(Expected_packet, Unsuback_packet);
unsubscribe('5.0') ->
	Expected_packet = <<162,10,0,101,0,0,5,"Topic"/utf8>>, 
	Unsuback_packet = <<176,5,0,101, 0, 0, 17>>,
	unsubscribe(Expected_packet, Unsuback_packet).

unsubscribe(Expected_packet, Unsuback_packet) ->
		mock_tcp:set_expectation(Expected_packet),
	gen_server:cast(client_gensrv, {unsubscribe, ["Topic"]}),
	wait_mock_tcp("unsubscribe packet"),
%% from server:
	client_gensrv ! {tcp, get_socket(), Unsuback_packet}, %% Unsuback packet
	receive
		[onUnsubscribe, {Return_codes1, _Props}] ->
			?debug_Fmt("::test:: unsuback with return codes: ~p~n", [Return_codes1]);
		Message1 -> 
			?debug_Fmt("::test:: Unexpected Message to caller process= ~p ~n", [Message1]),
			?assert(false)
	after 2000 ->
			?debug_Fmt("::test:: Timeout while waiting onUnsubscribe callback from client~n", []),
			?assert(false)
	end.

get_socket() ->
	(sys:get_state(client_gensrv))#connection_state.socket.

