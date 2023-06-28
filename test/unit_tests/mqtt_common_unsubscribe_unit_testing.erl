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
%% @since 2020-04-10
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_common_unsubscribe_unit_testing).

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
		{"packet output 1", fun() -> packet_output('3.1.1') end},
		{"packet output 2", fun() -> packet_output('5.0') end},
		{"packet output 3", fun() -> packet_output_props() end},

		{"input_parser 1", fun input_parser/0}
	].

packet_output('3.1.1') ->
	Value = mqtt_output:packet(unsubscribe, '3.1.1', {["Test_topic_1", "Test_topic_2"], 103}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<162,30,0,103,0,12,"Test_topic_1"/utf8,0,12,"Test_topic_2"/utf8>>, Value),

	?passed;
packet_output('5.0') ->
	Value = mqtt_output:packet(unsubscribe, '5.0', {["Test_topic_1", "Test_topic_2"], 103}, []),
%	io:format(user, "~n === value=~256p~n", [Value]),
	?assertEqual(<<162,31,0,103,0, 12:16,"Test_topic_1"/utf8, 12:16,"Test_topic_2"/utf8>>, Value),

	?passed.

packet_output_props() ->
	Value = mqtt_output:packet(unsubscribe, '5.0', {["Test_topic_1", "Test_topic_2"], 103},
														 [{?User_Property, {"Key", "Value"}}]),
%	io:format(user, "~n === value=~256p~n", [Value]),
	?assertEqual(<<162,44,0,103, 13, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 12:16,"Test_topic_1"/utf8, 12:16,"Test_topic_2"/utf8>>, Value),

	?passed.

input_parser() ->
	?assertEqual({unsubscribe, 103, [<<"Test_topic_1">>,<<"Test_topic_2">>], [], <<8:8, 8:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<162,30,0,103,0,12,"Test_topic_1"/utf8,0,12,"Test_topic_2"/utf8, 8:8, 8:8>>)),
	Value = mqtt_input:input_parser('5.0', <<162,31,0,103,0, 12:16,"Test_topic_1"/utf8, 12:16,"Test_topic_2"/utf8,5,5>>),
%	io:format(user, "~n === value=~256p~n", [Value]),
	?assertEqual({unsubscribe, 103, [<<"Test_topic_1">>,<<"Test_topic_2">>], [], <<5:8, 5:8>>}, Value),
	
	Value1 = mqtt_input:input_parser('5.0', <<162,44,0,103, 13, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 12:16,"Test_topic_1"/utf8, 12:16,"Test_topic_2"/utf8,5,5>>),
%	io:format(user, "~n === value=~256p~n", [Value1]),
	?assertEqual({unsubscribe, 103, [<<"Test_topic_1">>,<<"Test_topic_2">>], 
								[{?User_Property, {<<"Key">>, <<"Value">>}}],
								<<5:8, 5:8>>}, Value1),

	?passed.
