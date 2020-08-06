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

-module(mqtt_common_unsuback_unit_testing).

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

		{"input_parser", fun input_parser/0}
	].

packet_output('3.1.1') ->
	Value = mqtt_output:packet(unsuback, '3.1.1', {0, 1205}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<176,2,4,181>>, Value),

	?passed;
packet_output('5.0') ->
	Value = mqtt_output:packet(unsuback, '5.0', {[0,17], 1205}, []),
%	io:format(user, "~n -=- value=~256p~n", [Value]),
	?assertEqual(<<176,5,4,181, 0, 0,17>>, Value),

	?passed.

packet_output_props() ->
	Value = mqtt_output:packet(unsuback, '5.0', {[0,17], 1205},
														 [{?Reason_String, "Unspecified error"},
															{?User_Property, {"Key", "Value"}}]),
%	io:format(user, "~n -=- value=~256p~n", [Value]),
	?assertEqual(<<176,38,4,181, 33, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 31, 17:16,"Unspecified error"/utf8, 0,17>>, Value),

	?passed.

input_parser() ->
	?assertEqual({unsuback, {103, []}, [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<16#B0:8, 2:8, 103:16, 1:8, 1:8>>)),
	Value = mqtt_input:input_parser('5.0', <<176,5,4,181, 0, 0,17, 1,1>>),
%	io:format(user, "~n -=- value=~256p~n", [Value]),
	?assertEqual({unsuback, {1205, [0,17]}, [], <<1:8, 1:8>>}, Value),
	
	Value1 = mqtt_input:input_parser('5.0', <<176,38,4,181, 33, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8, 31, 17:16,"Unspecified error"/utf8, 0,17, 1,1>>),
%	io:format(user, "~n -=- value=~256p~n", [Value1]),
	?assertEqual({unsuback, {1205, [0,17]}, [{?Reason_String, <<"Unspecified error">>},
																						{?User_Property, {<<"Key">>, <<"Value">>}}],
									<<1:8, 1:8>>},
							 Value1),

	?passed.
