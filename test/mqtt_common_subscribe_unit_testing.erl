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

-module(mqtt_common_subscribe_unit_testing).

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
	Value5 = mqtt_output:packet(subscribe, {[{"Topic_1", 0, 0}, {"Topic_2", 1, 0}, {"Topic_3", 2, 0}], 101}),
%	io:format(user, " value=~256p~n", [Value5]),
	?assertEqual(<<130,32,0,101,0,7,"Topic_1"/utf8,0,0,7,"Topic_2"/utf8,1,0,7,"Topic_3"/utf8,2>>, Value5),

	?passed.

input_parser() ->
	?assertEqual({subscribe, 101, [{<<"Topic_1">>,0},{<<"Topic_2">>,1},{<<"Topic_3">>,2}], <<7:8,7:8>>}, 
							 mqtt_input:input_parser(<<130,32,0,101,0,7,"Topic_1"/utf8,0,0,7,"Topic_2"/utf8,1,0,7,"Topic_3"/utf8,2,7,7>>)),

	?passed.
