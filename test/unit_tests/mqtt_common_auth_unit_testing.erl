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

-module(mqtt_common_auth_unit_testing).

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
		{"packet output 1", fun() -> packet_output('5.0') end},
		{"packet output 2", fun() -> packet_output_props() end},

		{"input_parser", fun input_parser/0}
	].

packet_output('5.0') ->
	Value = mqtt_output:packet(auth, '5.0', 0, []),
%	io:format(user, "~n --- value=~256p~n", [Value]),
	?assertEqual(<<240,0>>, Value),

	Value2 = mqtt_output:packet(auth, '5.0', 24, []),
%	io:format(user, "~n --- value=~256p~n", [Value2]),
	?assertEqual(<<240,2,24,0>>, Value2),

	?passed.

packet_output_props() ->
	Value = mqtt_output:packet(auth, '5.0', 25,
														 [{?Reason_String, "Re-authenticate"},
															{?Authentication_Method, "Password"}]),
%	io:format(user, "~n === value=~256p~n", [Value]),
	?assertEqual(<<240,31,25, 29, 21, 8:16,"Password"/utf8, 31, 15:16,"Re-authenticate"/utf8>>, Value),

	?passed.

input_parser() ->
	?assertEqual({auth, 0, [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('5.0', <<240, 0, 1:8, 1:8>>)),

	?assertEqual({auth, 24, [], <<1:8, 1:8>>}, 
							 mqtt_input:input_parser('5.0', <<240, 2, 24, 0, 1:8, 1:8>>)),
	
	Value = mqtt_input:input_parser('5.0', <<240,31,25, 29, 21, 8:16,"Password"/utf8, 31, 15:16,"Re-authenticate"/utf8, 1,1>>),
%	io:format(user, "~n === value=~256p~n", [Value]),
	?assertEqual({auth, 25, [{?Reason_String, <<"Re-authenticate">>},
														{?Authentication_Method, <<"Password">>}], <<1:8, 1:8>>}, Value),
	?passed.
