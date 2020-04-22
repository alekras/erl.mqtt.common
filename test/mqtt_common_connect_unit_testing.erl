%%
%% Copyright (C) 2015-2020 by krasnop@bellsouth.net (Alexei Krasnopolski)
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
%% @since 2020-04-06
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for connect packet.

-module(mqtt_common_connect_unit_testing).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
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

unit_test_() ->
	[ 
		{"packet output", fun packet_output/0},
		{"input_parser", fun input_parser/0}
	].

packet_output() ->
	Value1_1 = mqtt_output:packet(
								connect,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									will = 0,
									will_message = <<>>,
									will_topic = [],
									clean_session = 1,
									keep_alive = 1000
								} 
	),
%	io:format(user, " value=~256p~n", [Value1_1]),
	?assertEqual(<<16,35,0,4,"MQTT"/utf8,4,194,3,232,0,9,"publisher"/utf8,0,5,"guest"/utf8,0,5,"guest">>, Value1_1),
	
	Value1_2 = mqtt_output:packet(
								connect,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									will = 1,
									will_qos = 2,
									will_retain = 1,
									will_message = <<"Good bye!">>,
									will_topic = "Last_msg",
									clean_session = 1,
									keep_alive = 1000
								} 
	),
%	io:format(user, " value=~256p~n", [Value1_2]),
	?assertEqual(<<16,56,0,4,"MQTT"/utf8,4,246,3,232,0,9,"publisher"/utf8,0,8,"Last_msg"/utf8,0,9,"Good bye!",0,5,"guest"/utf8,0,5,"guest">>, Value1_2),
	Value1_3 = mqtt_output:packet(
								connect,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									will = 0,
									will_message = <<>>,
									will_topic = [],
									clean_session = 1,
									keep_alive = 1000,
									version = '3.1'
								} 
	),
%	io:format(user, "~n value 1_3 = ~256p~n", [Value1_3]),
	?assertEqual(<<16,37,0,6,"MQIsdp"/utf8,3,194,3,232,0,9,"publisher"/utf8,0,5,"guest"/utf8,0,5,"guest">>, Value1_3),
	
	Value1_4 = mqtt_output:packet(
								connect,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									will = 1,
									will_qos = 2,
									will_retain = 1,
									will_message = <<"Good bye!">>,
									will_topic = "Last_msg",
									clean_session = 1,
									keep_alive = 1000,
									version = '3.1'
								} 
	),
%	io:format(user, "~n value 1_4= ~256p~n", [Value1_4]),
	?assertEqual(<<16,58,0,6,"MQIsdp"/utf8,3,246,3,232,0,9,"publisher"/utf8,0,8,"Last_msg"/utf8,0,9,"Good bye!",0,5,"guest"/utf8,0,5,"guest">>, Value1_4),

	?passed.

input_parser() ->
	Config = #connect{
						client_id = "publisher",
						user_name = "guest",
						password = <<"guest">>,
						will = 0,
						will_message = <<>>,
						will_topic = [],
						clean_session = 1,
						keep_alive = 1000
					}, 
	?assertEqual({connect, Config, <<7:8,7:8>>}, 
							 mqtt_input:input_parser(<<16,35,0,4,"MQTT"/utf8,4,194,3,232,0,9,"publisher"/utf8,0,5,"guest"/utf8,0,5,"guest",7:8,7:8>>)),
	?assertEqual({connect, Config#connect{version = '3.1'}, <<7:8,7:8>>}, 
							 mqtt_input:input_parser(<<16,37,0,6,"MQIsdp"/utf8,3,194,3,232,0,9,"publisher"/utf8,0,5,"guest"/utf8,0,5,"guest",7:8,7:8>>)),
	?passed.
