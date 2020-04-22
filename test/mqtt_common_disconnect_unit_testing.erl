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

-module(mqtt_common_disconnect_unit_testing).

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
	Value9 = mqtt_output:packet(disconnect, 0),
%	io:format(user, "~n value=~256p~n", [Value9]),
	?assertEqual(<<224,0>>, Value9),

	Value10 = mqtt_output:packet(pingreq, 0),
%	io:format(user, "~n value=~256p~n", [Value10]),
	?assertEqual(<<192,0>>, Value10),

	Value11 = mqtt_output:packet(pingresp, 0),
%	io:format(user, "~n value=~256p~n", [Value11]),
	?assertEqual(<<208,0>>, Value11),

	?passed.

input_parser() ->
	?assertEqual({pingreq, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser(<<192,0,1:8,1:8>>)),
	?assertEqual({pingresp, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser(<<16#D0:8, 0:8, 1:8, 1:8>>)),
	?assertEqual({disconnect, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser(<<224, 0, 1:8, 1:8>>)),
	?passed.
