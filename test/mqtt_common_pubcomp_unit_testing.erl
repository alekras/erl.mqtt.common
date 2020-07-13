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
%% @since 2020-04-10
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_common_pubcomp_unit_testing).

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

packet_output() ->
	Value15 = mqtt_output:packet(pubcomp, 25046),
%	io:format(user, "~n value=~256p~n", [Value15]),
	?assertEqual(<<112,2,97,214>>, Value15),

	?passed.
packet_output('3.1.1') ->
	Value = mqtt_output:packet(pubcomp, '3.1.1', 25046, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<112,2,97,214>>, Value),

	?passed;
packet_output('5.0') ->
	Value = mqtt_output:packet(pubcomp, '5.0', {0, 25046}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<112,2,97,214>>, Value),

	Value2 = mqtt_output:packet(pubcomp, '5.0', {10, 25046}, []),
%	io:format(user, "~n -=- value2=~256p~n", [Value2]),
	?assertEqual(<<112,3,97,214,10>>, Value2),

	?passed.

packet_output_props() ->
	Value = mqtt_output:packet(pubcomp, '5.0', {146, 25046}, [{?Reason_String, "Packet Identifier not found"},
																													 {?User_Property, [{name,"Key Name"}, {value,"Property Value"}]}]),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<112,61,97,214,146,57, 38,8:16,"Key Name"/utf8, 14:16,"Property Value"/utf8, 31, 27:16,"Packet Identifier not found"/utf8>>, Value),
	
	?passed.

input_parser() ->
	?assertEqual({pubcomp, 103, [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<16#70:8, 2:8, 103:16, 1:8, 1:8>>)),

	?passed.
