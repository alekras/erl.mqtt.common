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
%% @doc This module is running unit tests for some modules.

-module(mqtt_common_connack_unit_testing).

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
		{"packet output", fun() -> packet_output_props('5.0') end},

		{"input_parser", fun input_parser/0}
	].

packet_output(Version) ->
	Value = mqtt_output:packet(connack, Version, {1, 2}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	case Version of
		'3.1.1' ->
			?assertEqual(<<32,2,1,2>>, Value);
		'5.0' ->
			?assertEqual(<<32,3,1,2,0>>, Value)
	end,

	?passed.

packet_output_props('5.0' = Version) ->
	Value = mqtt_output:packet(connack, Version, {1, 24}, [{?Maximum_QoS, 2},{?Retain_Available, 1}]),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<32,7,1,24, 4, 37,1, 36,2>>, Value),

	?passed.

input_parser() ->
	?assertEqual({connack, 1, 0, "0x00 Connection Accepted", [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<16#20:8, 2:8, 1:8, 0:8, 1:8, 1:8>>)),
	?assertEqual({connack, 1, 0, "Success", [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('5.0', <<16#20:8, 3:8, 1:8, 0:8, 0, 1:8, 1:8>>)),
	Value = mqtt_input:input_parser('5.0', <<32,7,1,128, 4, 37,1, 36,2, 1, 1>>),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual({connack, 1, 128, "Unspecified error", [{?Maximum_QoS, 2},{?Retain_Available, 1}], <<1:8, 1:8>>}, 
							 Value),

	?passed.
