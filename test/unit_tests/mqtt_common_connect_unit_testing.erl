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
%% @since 2020-04-06
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for connect packet.

-module(mqtt_common_connect_unit_testing).

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

unit_test_() ->
	[ 
		{"packet output", fun() -> packet_output_zero('3.1') end},
		{"packet output", fun() -> packet_output_user('3.1') end},
		{"packet output", fun() -> packet_output_will('3.1') end},
		{"packet output", fun() -> packet_output_zero('3.1.1') end},
		{"packet output", fun() -> packet_output_user('3.1.1') end},
		{"packet output", fun() -> packet_output_will('3.1.1') end},
		{"packet output", fun() -> packet_output_zero('5.0') end},
		{"packet output", fun() -> packet_output_user('5.0') end},
		{"packet output", fun() -> packet_output_will('5.0') end},
		{"packet output", fun() -> packet_output_willProps('5.0') end},
		{"packet output", fun() -> packet_output_props('5.0') end},

		{"input_parser Zero", fun input_parser_zero/0},
		{"input_parser User", fun input_parser_user/0},
		{"input_parser Will", fun input_parser_will/0},
		{"input_parser Will properties", fun input_parser_willProps/0},
		{"input_parser properties", fun input_parser_props/0}
	].

packet_output_zero(Ver) ->
	Value = mqtt_output:packet(
								connect, Ver,
								#connect{
									client_id = "publisher",
									user_name = "",
									password = <<>>,
									clean_session = 0,
									keep_alive = 0,
									version = Ver
								},
								[]
	),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	case Ver of
		'3.1' ->
			?assertEqual(<<16,23, 6:16,"MQIsdp"/utf8, 3, 0, 0:16, 9:16,"publisher"/utf8>>, Value);
		'3.1.1' ->
			?assertEqual(<<16,21,0,4,"MQTT"/utf8,4, 0, 0:16, 9:16,"publisher"/utf8>>, Value);
		'5.0' ->
			?assertEqual(<<16,22,0,4,"MQTT"/utf8,5, 0, 0:16, 0, 9:16,"publisher"/utf8>>, Value)
	end,

	?passed.

packet_output_user(Ver) ->
	Value = mqtt_output:packet(
								connect, Ver,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									clean_session = 1,
									keep_alive = 10000,
									version = Ver
								},
								[]
	),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	case Ver of
		'3.1' ->
			?assertEqual(<<16,37, 6:16,"MQIsdp"/utf8,3,194, 10000:16, 9:16,"publisher"/utf8, 5:16,"guest"/utf8, 5:16,"guest">>, Value);
		'3.1.1' ->
			?assertEqual(<<16,35, 4:16,"MQTT"/utf8,4,194, 10000:16, 9:16,"publisher"/utf8, 5:16,"guest"/utf8, 5:16,"guest">>, Value);
		'5.0' ->
			?assertEqual(<<16,36, 4:16,"MQTT"/utf8,5,194, 10000:16, 0, 9:16,"publisher"/utf8, 5:16,"guest"/utf8, 5:16,"guest">>, Value)
	end,

	?passed.

packet_output_will(Ver) ->
	Value = mqtt_output:packet(
								connect, Ver,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									will_publish = #publish{qos= 2, retain= 1, topic= "Last_msg", payload= <<"Good bye!">>},
									clean_session = 1,
									keep_alive = 1000,
									version = Ver
								},
								[]
	),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	case Ver of
		'3.1' ->
			?assertEqual(<<16,58, 6:16,"MQIsdp"/utf8, 3, 246, 3,232, 9:16,"publisher"/utf8, 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest">>, Value);
		'3.1.1' ->
			?assertEqual(<<16,56, 4:16,"MQTT"/utf8, 4, 246, 3,232, 9:16,"publisher"/utf8, 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest">>, Value);
		'5.0' ->
			?assertEqual(<<16,58, 4:16,"MQTT"/utf8, 5, 246, 3,232, 0, 9:16,"publisher"/utf8, 0, 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest">>, Value)
	end,

	?passed.

packet_output_willProps('5.0' = Ver) ->
	Value = mqtt_output:packet(
								connect, Ver,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									will_publish = #publish{qos= 2, retain= 1, topic= "Last_msg", payload= <<"Good bye!">>,
																					properties = [{?Will_Delay_Interval, 6000}, {?Response_Topic, "AfterClose/Will"}]},
									clean_session = 1,
									keep_alive = 1000,
									version = Ver
								},
								[]
	),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<16,81, 4:16,"MQTT"/utf8, 5, 246, 3,232, 0, 9:16,"publisher"/utf8, 
								 23, 8, 15:16,"AfterClose/Will"/utf8, 24, 6000:32, 
								 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest">>, Value),

	?passed.

packet_output_props('5.0' = Ver) ->
	Value = mqtt_output:packet(
								connect, Ver,
								#connect{
									client_id = "publisher",
									user_name = "guest",
									password = <<"guest">>,
									will_publish = #publish{qos= 2, retain= 1, topic= "Last_msg", payload= <<"Good bye!">>,
																					properties = [{?Will_Delay_Interval, 6000}, {?Response_Topic, "AfterClose/Will"}]},
									clean_session = 1,
									keep_alive = 1000,
									version = Ver,
									properties = [{?Maximum_Packet_Size, 65000}, {?Session_Expiry_Interval, 16#FFFFFFFF}]
								},
								[]
	),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<16,91, 4:16,"MQTT"/utf8, 5, 246, 3,232, 
								 10, 17, 16#FFFFFFFF:32, 39, 65000:32, 
								 9:16,"publisher"/utf8, 
								 23, 8, 15:16,"AfterClose/Will"/utf8, 24, 6000:32, 
								 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest">>, Value),

	?passed.

input_parser_zero() ->
	Config = #connect{
						client_id = <<"publisher">>,
						user_name = "",
						password = <<>>,
						clean_session = 0,
						keep_alive = 0,
						version = '3.1.1',
						properties = []
					}, 
	R1 = mqtt_input:input_parser(undefined, <<16,21,0,4,"MQTT"/utf8,4,0,0,0,0,9,"publisher"/utf8,7:8,7:8>>),
%	io:format(user, "~n R1= ~256p~n", [R1]),
	?assertEqual({connect, Config, <<7:8,7:8>>}, R1),
	
	R2 = mqtt_input:input_parser(undefined, <<16,23,0,6,"MQIsdp"/utf8,3,0,0,0,0,9,"publisher"/utf8,7:8,7:8>>),
%	io:format(user, "~n R2= ~256p~n", [R2]),
	?assertEqual({connect, Config#connect{version = '3.1'}, <<7:8,7:8>>}, R2),
	
	R3 = mqtt_input:input_parser(undefined, <<16,22,0,4,"MQTT"/utf8,5, 0, 0:16, 0, 9:16,"publisher"/utf8,7:8,7:8>>),
%	io:format(user, "~n R3= ~256p~n", [R3]),
	?assertEqual({connect, Config#connect{version = '5.0'}, <<7:8,7:8>>}, R3),
	
	?passed.

input_parser_user() ->
	Config = #connect{
						client_id = <<"publisher">>,
						user_name = "guest",
						password = <<"guest">>,
						clean_session = 1,
						keep_alive = 1000,
						version = '3.1.1',
						properties = []
					}, 
	R1 = mqtt_input:input_parser(undefined, <<16,35,0,4,"MQTT"/utf8,4,194,3,232,0,9,"publisher"/utf8,0,5,"guest"/utf8,0,5,"guest",7:8,7:8>>),
%	io:format(user, "~n R1= ~256p~n", [R1]),
	?assertEqual({connect, Config, <<7:8,7:8>>}, R1),
	
	R2 = mqtt_input:input_parser(undefined, <<16,37,0,6,"MQIsdp"/utf8,3,194,3,232,0,9,"publisher"/utf8,0,5,"guest"/utf8,0,5,"guest",7:8,7:8>>),
%	io:format(user, "~n R2= ~256p~n", [R2]),
	?assertEqual({connect, Config#connect{version = '3.1'}, <<7:8,7:8>>}, R2),
	
	R4 = mqtt_input:input_parser(undefined, <<16,36, 4:16,"MQTT"/utf8,5,194, 10000:16, 0, 9:16,"publisher"/utf8, 5:16,"guest"/utf8, 5:16,"guest">>),
%	io:format(user, "~n R4= ~256p~n", [R4]),
	?assertEqual({connect, Config#connect{version = '5.0', keep_alive= 10000}, <<>>}, R4),

	?passed.

input_parser_will() ->
	Config = #connect{
						client_id = <<"publisher">>,
						user_name = "guest",
						password = <<"guest">>,
						will_publish = #publish{topic="Last_msg", qos=2, retain=1, payload= <<"Good bye!">>},
						clean_session = 1,
						keep_alive = 1000,
						version = '3.1.1'
					},

	R1 = mqtt_input:input_parser(undefined, <<16,58, 6:16,"MQIsdp"/utf8, 3, 246, 3,232, 9:16,"publisher"/utf8, 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest",7,7>>),
	io:format(user, "~n R1= ~256p~n", [R1]),
	?assertEqual({connect, Config#connect{version = '3.1'}, <<7:8,7:8>>}, R1),
	
	R2 = mqtt_input:input_parser(undefined, <<16,56, 4:16,"MQTT"/utf8, 4, 246, 3,232, 9:16,"publisher"/utf8, 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest",7,7>>),
	io:format(user, "~n R2= ~256p~n", [R2]),
	?assertEqual({connect, Config#connect{version = '3.1.1'}, <<7:8,7:8>>}, R2),
	
	R3 = mqtt_input:input_parser(undefined, <<16,58, 4:16,"MQTT"/utf8, 5, 246, 3,232, 0, 9:16,"publisher"/utf8, 0, 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest",7,7>>),
	io:format(user, "~n R3= ~256p~n", [R3]),
	?assertEqual({connect, Config#connect{version = '5.0'}, <<7:8,7:8>>}, R3),

	?passed.

input_parser_willProps() ->
	Config = #connect{
						client_id = <<"publisher">>,
						user_name = "guest",
						password = <<"guest">>,
						will_publish = #publish{topic="Last_msg", qos=2, retain=1,
																		payload= <<"Good bye!">>,
																		properties= [{?Will_Delay_Interval, 6000},{?Response_Topic, <<"AfterClose/Will">>}]},
						clean_session = 1,
						keep_alive = 1000,
						version = '5.0'
					},

	R1 = mqtt_input:input_parser(undefined, <<16,81, 4:16,"MQTT"/utf8, 5, 246, 3,232, 0, 9:16,"publisher"/utf8, 
								 23, 8, 15:16,"AfterClose/Will"/utf8, 24, 6000:32, 
								 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest",7,7>>),
	io:format(user, "~n R1= ~256p~n", [R1]),
	?assertEqual({connect, Config, <<7:8,7:8>>}, R1),
	
	?passed.

input_parser_props() ->
	Config = #connect{
						client_id = <<"publisher">>,
						user_name = "guest",
						password = <<"guest">>,
						will_publish = #publish{topic="Last_msg", qos=2, retain=1,
																		payload= <<"Good bye!">>,
																		properties= [{?Will_Delay_Interval, 6000},{?Response_Topic, <<"AfterClose/Will">>}]},
						clean_session = 1,
						keep_alive = 1000,
						properties = [{?Maximum_Packet_Size, 65000}, {?Session_Expiry_Interval, 16#FFFFFFFF}],
						version = '5.0'
					},

	R1 = mqtt_input:input_parser(undefined, <<16,91, 4:16,"MQTT"/utf8, 5, 246, 3,232, 
								 10, 17, 16#FFFFFFFF:32, 39, 65000:32, 
								 9:16,"publisher"/utf8, 
								 23, 8, 15:16,"AfterClose/Will"/utf8, 24, 6000:32, 
								 8:16,"Last_msg"/utf8, 9:16,"Good bye!", 5:16,"guest"/utf8, 5:16,"guest",7,7>>),
	io:format(user, "~n R1= ~256p~n", [R1]),
	?assertEqual({connect, Config, <<7:8,7:8>>}, R1),
	
	?passed.
