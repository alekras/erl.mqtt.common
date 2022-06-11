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

-module(mqtt_common_subscribe_unit_testing).

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
	Value = mqtt_output:packet(subscribe, '3.1.1', {[{"Topic_1", 0, callback}, {"Topic_2", 1, callback}, {"Topic_3", 2, callback}], 101}, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<130,32, 101:16, 7:16,"Topic_1"/utf8,0, 7:16,"Topic_2"/utf8,1, 7:16,"Topic_3"/utf8,2>>, Value),

	?passed;
packet_output('5.0') ->
	Options = #subscription_options{nolocal = 1, retain_as_published = 1, retain_handling = 1},
	Value = mqtt_output:packet(subscribe, '5.0', {[{"Topic_1", Options, callback}, 
																									{"Topic_2", Options#subscription_options{max_qos = 1}, callback}, 
																									{"Topic_3", Options#subscription_options{max_qos = 2}, callback}],
																								101}, 
															[]),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<130,33, 101:16, 0, 7:16,"Topic_1"/utf8,28, 7:16,"Topic_2"/utf8,29, 7:16,"Topic_3"/utf8,30>>, Value),

	?passed.

packet_output_props() ->
	Options = #subscription_options{nolocal = 1, retain_as_published = 1, retain_handling = 1},
	Value = mqtt_output:packet(subscribe, '5.0', {[{"Topic_1", Options, callback}, 
																									{"Topic_2", Options#subscription_options{max_qos = 1}, callback}, 
																									{"Topic_3", Options#subscription_options{max_qos = 2}, callback}],
																								101}, 
															[{?User_Property, {"Key", "Value"}},{?Subscription_Identifier, 177001}]),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<130,50, 101:16, 17, 11,233,230,10, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8,
								 7:16,"Topic_1"/utf8,28, 7:16,"Topic_2"/utf8,29, 7:16,"Topic_3"/utf8,30>>, Value),

	?passed.

input_parser() ->
	?assertEqual({subscribe, 101, [{<<"Topic_1">>,0},{<<"Topic_2">>,1},{<<"Topic_3">>,2}], [], <<7:8,7:8>>}, 
							 mqtt_input:input_parser('3.1.1', <<130,32,0,101,0,7,"Topic_1"/utf8,0,0,7,"Topic_2"/utf8,1,0,7,"Topic_3"/utf8,2,7,7>>)),

	Value = mqtt_input:input_parser('5.0', <<130,33, 101:16, 0, 7:16,"Topic_1"/utf8,28, 7:16,"Topic_2"/utf8,29, 7:16,"Topic_3"/utf8,30, 1,7>>),
	Options = #subscription_options{nolocal = 1, retain_as_published = 1, retain_handling = 1},
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual({subscribe, 101, [{<<"Topic_1">>,Options},
																 {<<"Topic_2">>,Options#subscription_options{max_qos = 1}},
																 {<<"Topic_3">>, Options#subscription_options{max_qos = 2}}],
								[], <<1:8,7:8>>}, Value),

	Value1 = mqtt_input:input_parser('5.0', <<130,50, 101:16, 17, 11,233,230,10, 38,3:16,"Key"/utf8, 5:16,"Value"/utf8,
																						7:16,"Topic_1"/utf8,28, 7:16,"Topic_2"/utf8,29, 7:16,"Topic_3"/utf8,30, 1,7>>),
%	io:format(user, "~n --- value=~256p~n", [Value1]),
	?assertEqual({subscribe, 101, [{<<"Topic_1">>,Options},
																 {<<"Topic_2">>,Options#subscription_options{max_qos = 1}},
																 {<<"Topic_3">>, Options#subscription_options{max_qos = 2}}],
								[{?User_Property, {<<"Key">>, <<"Value">>}},
								 {?Subscription_Identifier, 177001}],
								<<1:8,7:8>>}, Value1),

	?passed.
