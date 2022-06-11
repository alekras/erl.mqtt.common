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

-module(mqtt_common_disconnect_unit_testing).

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
	Value = mqtt_output:packet(disconnect, '3.1.1', 0, []),
%	io:format(user, "~n value=~256p~n", [Value]),
	?assertEqual(<<224,0>>, Value),

	?passed;
packet_output('5.0') ->
	Value = mqtt_output:packet(disconnect, '5.0', 0, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<224,0>>, Value),

	Value1 = mqtt_output:packet(disconnect, '5.0', 1, []),
%	io:format(user, "~n --- value=~256p~n", [Value1]),
	?assertEqual(<<224,2,1,0>>, Value1),

	?passed.

packet_output_props() ->
	Value = mqtt_output:packet(disconnect, '5.0', 128,
														 [{?Reason_String, "Unspecified error"},
															{?Session_Expiry_Interval, 3600}]),
%	io:format(user, "~n === value=~256p~n", [Value]),
	?assertEqual(<<224,27,128,25,17,0,0,14,16, 31, 17:16,"Unspecified error"/utf8>>, Value),

	?passed.

input_parser() ->
	?assertEqual({disconnect, 0, [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<224, 0, 1:8, 1:8>>)),
	Value = mqtt_input:input_parser('5.0', <<224,0, 1,1>>),
%	io:format(user, "~n === value=~256p~n", [Value]),
	?assertEqual({disconnect, 0, [], <<1:8, 1:8>>}, Value),

	Value1 = mqtt_input:input_parser('5.0', <<224,2,1,0, 1,1>>),
%	io:format(user, "~n === value=~256p~n", [Value1]),
	?assertEqual({disconnect, 1, [], <<1:8, 1:8>>}, Value1),

	Value3 = mqtt_input:input_parser('5.0', <<224,1,148, 1,1>>),
%	io:format(user, "~n === value=~256p~n", [Value1]),
	?assertEqual({disconnect, 148, [], <<1:8, 1:8>>}, Value3),

	Value2 = mqtt_input:input_parser('5.0', <<224,27,128,25,17,0,0,14,16, 31, 17:16,"Unspecified error"/utf8, 1,1>>),
%	io:format(user, "~n === value=~256p~n", [Value2]),
	?assertEqual({disconnect, 128, [{?Reason_String, <<"Unspecified error">>},
																	{?Session_Expiry_Interval, 3600}],
									<<1:8, 1:8>>},
								Value2),

	?passed.
