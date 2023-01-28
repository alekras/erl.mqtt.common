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
%% @since 2020-04-08
%% @copyright 2015-2022 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_common_unit_testing).

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
		{"extract_variable_byte_integer", fun extract_variable_byte_integer/0},
		{"encode_variable_byte_integer", fun encode_variable_byte_integer/0},
		{"is_match", fun is_match/0},
		{"validate connect config", fun connect_validate/0},
		{"validate publish packet", fun publish_validate/0},
		{"validate hex transformation", fun hex_validate/0}
	].

extract_variable_byte_integer() ->
	?assertEqual({<<1, 1>>, 2}, mqtt_data:extract_variable_byte_integer(<<2:8, 1:8, 1:8>>)),
	?assertEqual({<<1, 1>>, 2049}, mqtt_data:extract_variable_byte_integer(<<16#81:8, 16:8, 1:8, 1:8>>)),
	?assertEqual({<<1, 1>>, 47489}, mqtt_data:extract_variable_byte_integer(<<16#81:8, 16#f3:8, 2:8, 1:8, 1:8>>)),
	?assertEqual({<<1, 1>>, 32110977}, mqtt_data:extract_variable_byte_integer(<<16#81:8, 16#f3:8, 16#A7, 15:8, 1:8, 1:8>>)),
	?assertEqual({error, 0}, mqtt_data:extract_variable_byte_integer(<<>>)),
	?assertEqual({error, 7}, mqtt_data:extract_variable_byte_integer(<<16#87:8, 16#f3:8>>)),
	?assertEqual({error, 0}, mqtt_data:extract_variable_byte_integer(<<16#81:8>>)),
	?passed.

encode_variable_byte_integer() ->
	?assertEqual(<<45>>, mqtt_data:encode_variable_byte_integer(45)),
	?assertEqual(<<161,78>>, mqtt_data:encode_variable_byte_integer(10017)),
	?assertEqual(<<142,145,82>>, mqtt_data:encode_variable_byte_integer(1345678)),
	?assertEqual(<<206,173,133,85>>, mqtt_data:encode_variable_byte_integer(178345678)),
  ?passed.

is_match() ->
	?assert(mqtt_data:is_match("Winter/Feb/23", "Winter/#")),
	?assert(mqtt_data:is_match("Winter/Feb/23", "#")),
	?assert(mqtt_data:is_match("/Winter/Feb/23", "/#")),
	?assert(mqtt_data:is_match("Winter/", "Winter/#")),
%	?assert(mqtt_data:is_match("Winter", "Winter/#")),

	?assert(mqtt_data:is_match("Winter/Feb/23", "Winter/+/23")),
	?assert(mqtt_data:is_match("Season/Spring/Month/March/25", "Season/+/Month/+/25")),
	?assert(mqtt_data:is_match("/Feb/23", "/+/23")),
	?assertNot(mqtt_data:is_match("/Feb/23", "+/23")),
	?assert(mqtt_data:is_match("Feb/23", "+/23")),
	?assert(mqtt_data:is_match("Feb/23/", "+/23/")),
	?assert(mqtt_data:is_match("Feb//23/", "+//23/")),
	?assertNot(mqtt_data:is_match("Feb Mar/23", "+/23/")),
	?assertNot(mqtt_data:is_match("Feb/23/", "+/23")),
	?assert(mqtt_data:is_match("/", "/")),
	?assertNot(mqtt_data:is_match("/Feb/23", "/February/23")),
	?assert(mqtt_data:is_match("Winter/Feb/Day/23/10pm", "Winter/+/Day/#")),
	?assert(mqtt_data:is_match("Winter/Feb", "+/+")),
	?assert(mqtt_data:is_match("/Winter", "+/+")),
	?assert(mqtt_data:is_match("Winter", "+")),
	?assertNot(mqtt_data:is_match("/Winter", "+")),
	?assertNot(mqtt_data:is_match("Winter/", "+")),
	?assert(mqtt_data:is_match("Winter/", "+/")),
	?assert(mqtt_data:is_match("/Winter", "/+")),
	?assert(mqtt_data:is_match("//Winter", "//+")),
%“/finance” matches “+/+” and “/+”, but not “+”

	?assertEqual({true, ["","+"]}, mqtt_data:is_topicFilter_valid("+")),
	?assertEqual({true, ["","/+"]}, mqtt_data:is_topicFilter_valid("/+")),
	?assertEqual({true, ["","+/+"]}, mqtt_data:is_topicFilter_valid("+/+")),
	?assertEqual({true, ["","+/"]}, mqtt_data:is_topicFilter_valid("+/")),
	?assertEqual(false, mqtt_data:is_topicFilter_valid("/Winter/++/")),
	?assertEqual(false, mqtt_data:is_topicFilter_valid("/Winter/#/a")),

	?assertEqual({true, ["","/Winter"]}, mqtt_data:is_topicFilter_valid("/Winter")),
	?assertEqual({true, ["","Winter"]}, mqtt_data:is_topicFilter_valid("Winter")),
	?assertEqual({true, ["","Winter/+"]}, mqtt_data:is_topicFilter_valid("Winter/+")),
	?assertEqual({true, ["","/Winter/+"]}, mqtt_data:is_topicFilter_valid("/Winter/+")),
	?assertEqual({true, ["","Winter/+/season/"]}, mqtt_data:is_topicFilter_valid("Winter/+/season/")),
	?assertEqual({true, ["","/Winter/+/season/+"]}, mqtt_data:is_topicFilter_valid("/Winter/+/season/+")),
	?assertEqual({true, ["","/Winter/+/season/+/#"]}, mqtt_data:is_topicFilter_valid("/Winter/+/season/+/#")),
	?assertEqual({true, ["","Winter/+/season/+/#"]}, mqtt_data:is_topicFilter_valid("Winter/+/season/+/#")),
	?assertEqual({true, ["SHARED 1","//Winter/+/season/+/#"]}, mqtt_data:is_topicFilter_valid("$share/SHARED 1///Winter/+/season/+/#")),
	?assertEqual(false, mqtt_data:is_topicFilter_valid("$share/SHARED+1/Winter/+/season/+/#")),
	
	?passed.

connect_validate() ->
	?assertNotException(throw, true, mqtt_data:validate_config(#connect{client_id= "testClient1", version= '5.0'})),
	?assertException(throw, #mqtt_client_error{type= utf8, message="Client Id"}, mqtt_data:validate_config(#connect{client_id= ["testClient1",16#d801], version= '5.0'})),
	?assertException(throw, #mqtt_client_error{type= name, message="Client Id"}, mqtt_data:validate_config(#connect{client_id= "testClient-1", version= '5.0'})),

	?assertNotException(throw, true, mqtt_data:validate_config(#connect{client_id= "testClient1", user_name= "alexei", version= '5.0'})),
	?assertException(throw, #mqtt_client_error{type= utf8, message="User name"}, mqtt_data:validate_config(#connect{client_id= "testClient1", user_name= ["alex",16#d801,"ei"], version= '5.0'})),

	?assertNotException(throw, true, mqtt_data:validate_config(#connect{client_id= "c", user_name= "u", properties= [{?Topic_Alias_Maximum, 7}], version= '5.0'})),
	?assertException(throw, #mqtt_client_error{type= property, message="Connect Properties"}, mqtt_data:validate_config(#connect{client_id= "c", user_name= "u", properties= [{?Topic_Alias, 7}], version= '5.0'})),

	?assertNotException(throw, true, 
											mqtt_data:validate_config(#connect{client_id= "c", user_name= "u", properties= [{?Topic_Alias_Maximum, 7}], version= '5.0',
																									will= 1,
																									will_publish = #publish{topic= "Topic", payload= <<"Will Message">>, properties= [{?Payload_Format_Indicator, 1}]}})),
	?assertException(throw, #mqtt_client_error{type= topic, message="Will Topic"},
											mqtt_data:validate_config(#connect{client_id= "c", user_name= "u", properties= [{?Topic_Alias_Maximum, 7}], version= '5.0',
																									will= 1,
																									will_publish = #publish{topic= "Topic/#/a", payload= <<"Will Message">>, properties= [{?Payload_Format_Indicator, 1}]}})),
	?assertException(throw, #mqtt_client_error{type= utf8, message="Will Topic"},
											mqtt_data:validate_config(#connect{client_id= "c", user_name= "u", properties= [{?Topic_Alias_Maximum, 7}], version= '5.0',
																									will= 1,
																									will_publish = #publish{topic= ["Topic",16#d801,"/#/a"], payload= <<"Will Message">>, properties= [{?Payload_Format_Indicator, 1}]}})),
	?assertException(throw, #mqtt_client_error{type= utf8, message="Will Payload"},
											mqtt_data:validate_config(#connect{client_id= "c", user_name= "u", properties= [{?Topic_Alias_Maximum, 7}], version= '5.0',
																									will= 1,
																									will_publish = #publish{topic= "Topic", payload= <<"Will ", 16#d801:16,"Message">>, properties= [{?Payload_Format_Indicator, 1}]}})),
	?assertException(throw, #mqtt_client_error{type= will_property, message="Will Properties"},
											mqtt_data:validate_config(#connect{client_id= "c", user_name= "u", properties= [{?Topic_Alias_Maximum, 7}], version= '5.0',
																									will= 1,
																									will_publish = #publish{topic= "Topic", payload= <<"Will Message">>, properties= [{?Subscription_Identifier_Available, 1}]}})),

	?passed.

publish_validate() ->
	?assertNotException(throw, true, mqtt_data:validate_publish('3.1.1',#publish{topic= "Topic/a"})),
	?assertNotException(throw, true, mqtt_data:validate_publish('5.0',#publish{topic= "Topic/a"})),
	?assertException(throw, #mqtt_client_error{type= utf8, message="Publish Topic"}, mqtt_data:validate_publish('3.1.1',#publish{topic= <<"Topic/",16#d801:16,"a">>})),
	
	?passed.

hex_validate() ->
	?assertEqual("0111ff", mqtt_data:binary_to_hex(<<1,17,255>>)),
	?assertEqual(<<1,17,255>>, mqtt_data:hex_to_binary("0111ff")),
	?assertEqual("011147503a4b5c6d7e8fff", mqtt_data:binary_to_hex(mqtt_data:hex_to_binary("011147503a4b5C6d7e8fff"))).

