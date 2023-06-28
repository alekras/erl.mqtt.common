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

-module(mqtt_common_pingreq_unit_testing).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
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
		{"packet output", fun packet_output/0},
		{"input_parser", fun input_parser/0}
	].

packet_output() ->
	Value = mqtt_output:packet(pingreq, '3.1.1', 0, []),
%	io:format(user, "~n value=~256p~n", [Value]),
	?assertEqual(<<192,0>>, Value),

	Value1 = mqtt_output:packet(pingreq, '5.0', 0, []),
%	io:format(user, "~n value=~256p~n", [Value1]),
	?assertEqual(<<192,0>>, Value1),

	?passed.

input_parser() ->
	?assertEqual({pingreq, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<192,0,1:8,1:8>>)),

	?assertEqual({pingreq, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('5.0', <<192,0,1:8,1:8>>)),

	?passed.
