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
	Value = mqtt_output:packet(publish, Version, {#publish{topic = "Topic", payload = <<"Test">>, qos = 0}, undefined}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	case Version of
		'3.1.1' ->
			?assertEqual(<<48,11, 5:16,"Topic"/utf8,"Test"/utf8>>, Value);
		'5.0' ->
			?assertEqual(<<48,12, 5:16,"Topic"/utf8, 0, "Test"/utf8>>, Value)
	end,

	Value2 = mqtt_output:packet(publish, Version, {#publish{topic = "Topic", payload = <<"Test">>, qos = 1}, 100}, []),
%	io:format(user, "~n === value=~256p~n", [Value2]),
	case Version of
		'3.1.1' ->
			?assertEqual(<<50,13, 5:16,"Topic"/utf8, 100:16, "Test"/utf8>>, Value2);
		'5.0' ->
			?assertEqual(<<50,14, 5:16,"Topic"/utf8, 100:16, 0, "Test"/utf8>>, Value2)
	end,

	?passed.

packet_output_props('5.0' = Version) ->
	Value = mqtt_output:packet(publish, Version, {#publish{topic = "Topic", payload = <<"Test">>, qos = 0}, undefined},
														 [{?Payload_Format_Indicator, 1},{?Message_Expiry_Interval, 120000}]),
	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<48,19, 5:16,"Topic"/utf8, 7,2,120000:32,1,1, "Test"/utf8>>, Value),

	Value2 = mqtt_output:packet(publish, Version, {#publish{topic = "Topic", payload = <<"Test">>, qos = 1}, 100},
															[{?Topic_Alias, 300},{?Correlation_Data, <<1,2,3,4>>}]),
	io:format(user, "~n === value=~256p~n", [Value2]),
	?assertEqual(<<50,24, 5:16,"Topic"/utf8, 100:16, 10, 9,4:16,1,2,3,4, 35,300:16, "Test"/utf8>>, Value2),

	?passed.

input_parser() ->
	?assertEqual({publish, #publish{topic = "Topic", payload = <<1:8, 2:8, 3:8, 4:8, 5:8, 6:8>>, dir = in}, 0, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<16#30:8, 13:8, 5:16, "Topic", 1:8, 2:8, 3:8, 4:8, 5:8, 6:8, 1:8, 1:8>>)),
%					{publish, QoS, Packet_Id, Topic, Payload, Tail};
	?assertEqual({publish, #publish{qos = 1, topic = "Topic", payload = <<1:8, 2:8, 3:8, 4:8, 5:8, 6:8>>, dir = in}, 100, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<16#32:8, 15:8, 5:16, "Topic", 100:16, 1:8, 2:8, 3:8, 4:8, 5:8, 6:8, 1:8, 1:8>>)),
	?assertEqual({publish, #publish{qos = 2, dup = 1, retain = 1, topic = "Топик", payload = <<1:8, 2:8, 3:8, 4:8, 5:8, 6:8>>, dir = in}, 101, <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<16#3D:8, 20:8, 10:16, "Топик"/utf8, 101:16, 1:8, 2:8, 3:8, 4:8, 5:8, 6:8, 1:8, 1:8>>)),

	?passed.
