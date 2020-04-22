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
%% @since 2020-04-08
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_common_publish_unit_testing).

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
	Value3 = mqtt_output:packet(publish, #publish{topic = "Topic", payload = <<"Test">>}),
%	io:format(user, " value=~256p~n", [Value3]),
	?assertEqual(<<48,11,0,5,84,111,112,105,99,84,101,115,116>>, Value3),

	Value4 = mqtt_output:packet(publish, {#publish{topic = "Topic", payload = <<"Test">>}, 100}),
%	io:format(user, " value=~256p~n", [Value4]),
	?assertEqual(<<48,13,0,5,84,111,112,105,99,0,100,84,101,115,116>>, Value4),

	?passed.

input_parser() ->
	?assertEqual({publish, #publish{topic = "Topic", payload = <<1:8, 2:8, 3:8, 4:8, 5:8, 6:8>>, dir = in}, 0, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser(<<16#30:8, 13:8, 5:16, "Topic", 1:8, 2:8, 3:8, 4:8, 5:8, 6:8, 1:8, 1:8>>)),
%					{publish, QoS, Packet_Id, Topic, Payload, Tail};
	?assertEqual({publish, #publish{qos = 1, topic = "Topic", payload = <<1:8, 2:8, 3:8, 4:8, 5:8, 6:8>>, dir = in}, 100, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser(<<16#32:8, 15:8, 5:16, "Topic", 100:16, 1:8, 2:8, 3:8, 4:8, 5:8, 6:8, 1:8, 1:8>>)),
	?assertEqual({publish, #publish{qos = 2, dup = 1, retain = 1, topic = "Топик", payload = <<1:8, 2:8, 3:8, 4:8, 5:8, 6:8>>, dir = in}, 101, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser(<<16#3D:8, 20:8, 10:16, "Топик"/utf8, 101:16, 1:8, 2:8, 3:8, 4:8, 5:8, 6:8, 1:8, 1:8>>)),

	?passed.
