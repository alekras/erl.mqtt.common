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
%% @since 2020-04-10
%% @copyright 2015-2022 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_common_pubrec_unit_testing).

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
		{"packet output", fun() -> packet_output('3.1.1') end},
		{"packet output", fun() -> packet_output('5.0') end},
		{"packet output", fun() -> packet_output_props() end},

		{"input_parser", fun input_parser/0}
	].

packet_output('3.1.1') ->
	Value = mqtt_output:packet(pubrec, '3.1.1', {23045,0}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<80,2,90,5>>, Value),

	?passed;
packet_output('5.0') ->
	Value = mqtt_output:packet(pubrec, '5.0', {23045,0}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<80,2,90,5>>, Value),

	Value2 = mqtt_output:packet(pubrec, '5.0', {23045,10}, []),
%	io:format(user, "~n -=- value2=~256p~n", [Value2]),
	?assertEqual(<<80,3,90,5,10>>, Value2),

	?passed.

packet_output_props() ->
	Value = mqtt_output:packet(pubrec, '5.0', {23045,16}, [{?Reason_String, "No matching subscribers"},
																													{?User_Property, {"Key Name", "Property Value"}}]),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<80,57,90,5,16,53, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 31, 23:16,"No matching subscribers"/utf8>>, Value),

	?passed.

input_parser() ->
	?assertEqual({pubrec, {101,0}, [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<16#50:8, 2:8, 101:16, 1:8, 1:8>>)),
	
	Value = mqtt_input:input_parser('5.0', <<80,2,90,5, 1,1>>),
%	io:format(user, "~n -=- value=~256p~n", [Value]),
	?assertEqual({pubrec, {23045,0}, [], <<1:8, 1:8>>}, Value),

	Value1 = mqtt_input:input_parser('5.0', <<80,3,90,5,10, 1,1>>),
%	io:format(user, "~n -=- value=~256p~n", [Value1]),
	?assertEqual({pubrec, {23045,10}, [], <<1:8, 1:8>>}, Value1),

	Value2 = mqtt_input:input_parser('5.0', <<80,57,90,5,16,53, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 31, 23:16,"No matching subscribers"/utf8, 1,1>>),
%	io:format(user, "~n -=- value=~256p~n", [Value2]),
	?assertEqual({pubrec, {23045,16}, [{?Reason_String, <<"No matching subscribers">>},
																			{?User_Property, {<<"Key Name">>, <<"Property Value">>}}],
								<<1:8, 1:8>>},
							 Value2),

	?passed.
