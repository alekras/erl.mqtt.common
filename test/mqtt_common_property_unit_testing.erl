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
%% @since 2020-04-13
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for connect packet.

-module(mqtt_common_property_unit_testing).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
-include("mqtt_property.hrl").
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
		{"property parser", fun prop_parse/0},
		{"property to binary", fun prop_2_bin/0},
		{"properties validate ", fun validate_props/0}
	].

prop_parse() ->
	PropName_One_byte_Value_List = [
		{?Payload_Format_Indicator, <<2,1,7>>},
		{?Request_Problem_Information,<<2,23,7>>},
		{?Request_Response_Information,<<2,25,7>>},
		{?Maximum_QoS,<<2,36,7>>},
		{?Retain_Available,<<2,37,7>>},
		{?Wildcard_Subscription_Available,<<2,40,7>>},
		{?Subscription_Identifier_Available,<<2,41,7>>},
		{?Shared_Subscription_Available,<<2,42,7>>}
	],
	[
	 begin 
		{Prop, _} = mqtt_property:parse(Bin),
		io:format(user, "~n Props = ~256p", [Prop]),
		?assertEqual([{Prop_Name,7}], Prop)
	 end || {Prop_Name, Bin} <- PropName_One_byte_Value_List],

	PropName_Two_byte_Value_List = [
		{?Server_Keep_Alive,<<3,19,4,1>>},
		{?Receive_Maximum,<<3,33,4,1>>},
		{?Topic_Alias_Maximum,<<3,34,4,1>>},
		{?Topic_Alias,<<3,35,4,1>>}
	],
	[
	 begin 
		{Prop, _} = mqtt_property:parse(Bin),
		io:format(user, "~n Props = ~256p", [Prop]),
		?assertEqual([{Prop_Name,1025}], Prop)
	 end || {Prop_Name, Bin} <- PropName_Two_byte_Value_List],

	PropName_Four_byte_Value_List = [
		{?Message_Expiry_Interval,<<5,2,0,109,254,227>>},
		{?Session_Expiry_Interval,<<5,17,0,109,254,227>>},
		{?Will_Delay_Interval,<<5,24,0,109,254,227>>},
		{?Maximum_Packet_Size,<<5,39,0,109,254,227>>}
	],
	[
	 begin 
		{Prop, _} = mqtt_property:parse(Bin),
		io:format(user, "~n Props = ~256p", [Prop]),
		?assertEqual([{Prop_Name,7208675}], Prop)
	 end || {Prop_Name, Bin} <- PropName_Four_byte_Value_List],

	PropName_UTF_8_Value_List = [
		{?Content_Type,<<55,3,0,52,84,101,115,116,32,112,114,111,112,101,114,116,121,32,115,116,114,105,
											110,103,46,32,208,162,208,181,209,129,209,130,208,190,208,178,208,176,209,143,32,
											209,129,209,130,209,128,208,190,208,186,208,176,46>>},
		{?Response_Topic,<<55,8,0,52,84,101,115,116,32,112,114,111,112,101,114,116,121,32,115,116,114,105,
											110,103,46,32,208,162,208,181,209,129,209,130,208,190,208,178,208,176,209,143,32,
											209,129,209,130,209,128,208,190,208,186,208,176,46>>},
		{?Assigned_Client_Identifier,<<55,18,0,52,84,101,115,116,32,112,114,111,112,101,114,116,121,32,115,116,114,105,
											110,103,46,32,208,162,208,181,209,129,209,130,208,190,208,178,208,176,209,143,32,
											209,129,209,130,209,128,208,190,208,186,208,176,46>>},
		{?Authentication_Method,<<55,21,0,52,84,101,115,116,32,112,114,111,112,101,114,116,121,32,115,116,114,105,
											110,103,46,32,208,162,208,181,209,129,209,130,208,190,208,178,208,176,209,143,32,
											209,129,209,130,209,128,208,190,208,186,208,176,46>>},
		{?Response_Information,<<55,26,0,52,84,101,115,116,32,112,114,111,112,101,114,116,121,32,115,116,114,105,
											110,103,46,32,208,162,208,181,209,129,209,130,208,190,208,178,208,176,209,143,32,
											209,129,209,130,209,128,208,190,208,186,208,176,46>>},
		{?Server_Reference,<<55,28,0,52,84,101,115,116,32,112,114,111,112,101,114,116,121,32,115,116,114,105,
											110,103,46,32,208,162,208,181,209,129,209,130,208,190,208,178,208,176,209,143,32,
											209,129,209,130,209,128,208,190,208,186,208,176,46>>},
		{?Reason_String,<<55,31,0,52,84,101,115,116,32,112,114,111,112,101,114,116,121,32,115,116,114,105,
											110,103,46,32,208,162,208,181,209,129,209,130,208,190,208,178,208,176,209,143,32,
											209,129,209,130,209,128,208,190,208,186,208,176,46>>}
	],
	[
	 begin 
		{Prop, _} = mqtt_property:parse(Bin),
		[{Prop_Name, String}] = Prop,
		io:format(user, "~n Props = [{~p,\"~ts\"}]", [Prop_Name, String]),
		?assertEqual([{Prop_Name,<<"Test property string. Тестовая строка."/utf8>>}], Prop)
	 end || {Prop_Name, Bin} <- PropName_UTF_8_Value_List],

	PropName_Binary_Value_List = [
		{?Correlation_Data,<<8,9,0,5,1,2,3,4,5>>},
		{?Authentication_Data,<<8,22,0,5,1,2,3,4,5>>}
	],
	[
	 begin 
		{Prop, _} = mqtt_property:parse(Bin),
		io:format(user, "~n Props = ~256p", [Prop]),
		?assertEqual([{Prop_Name,<<1,2,3,4,5>>}], Prop)
	 end || {Prop_Name, Bin} <- PropName_Binary_Value_List],

	{Prop, _} = mqtt_property:parse(<<5,11,227,253,183,3>>),
	io:format(user, "~n Props = ~256p", [Prop]),
	?assertEqual([{?Subscription_Identifier,7208675}], Prop),

	{Prop_1, _} = mqtt_property:parse(<<27,38,0,8,75,101,121,32,78,97,109,101,0,14,80,114,111,112,101,114,116,121,32,86,97,108,117,101>>),
	io:format(user, "~n Props = ~256p", [Prop_1]),
	?assertEqual([{?User_Property, {<<"Key Name">>,<<"Property Value">>}}], Prop_1),

	{Prop_2, _} = mqtt_property:parse(<<0>>),
	io:format(user, "~n Empty properties = ~256p", [Prop_2]),
	?assertEqual([], Prop_2),

	?passed.

prop_2_bin() ->
	PropName_One_byte_Value_List = [
		?Payload_Format_Indicator,
		?Request_Problem_Information,
		?Request_Response_Information,
		?Maximum_QoS,
		?Retain_Available,
		?Wildcard_Subscription_Available,
		?Subscription_Identifier_Available,
		?Shared_Subscription_Available
	],
	[
	 begin 
		Prop = [{Prop_Name, 7}],
		Bin = mqtt_property:to_binary(Prop),
		io:format(user, "~n Binary = ~256p", [Bin]),
		?assertEqual(<<2,Prop_Name,7>>, Bin)
	 end || Prop_Name <- PropName_One_byte_Value_List],

	PropName_Two_byte_Value_List = [
		?Server_Keep_Alive,
		?Receive_Maximum,
		?Topic_Alias_Maximum,
		?Topic_Alias
	],
	[
	 begin 
		Prop = [{Prop_Name, 1025}],
		Bin = mqtt_property:to_binary(Prop),
		io:format(user, "~n 2bytes Binary = ~256p", [Bin]),
		?assertEqual(<<3,Prop_Name,4,1>>, Bin)
	 end || Prop_Name <- PropName_Two_byte_Value_List],

	PropName_Four_byte_Value_List = [
		?Message_Expiry_Interval,
		?Session_Expiry_Interval,
		?Will_Delay_Interval,
		?Maximum_Packet_Size
	],
	[
	 begin 
		Prop = [{Prop_Name, 7208675}],
		Bin = mqtt_property:to_binary(Prop),
		io:format(user, "~n 4bytes Binary = ~256p", [Bin]),
		?assertEqual(<<5,Prop_Name,0,109,254,227>>, Bin)
	 end || Prop_Name <- PropName_Four_byte_Value_List],

	PropName_UTF_8_Value_List = [
		?Content_Type,
		?Response_Topic,
		?Assigned_Client_Identifier,
		?Authentication_Method,
		?Response_Information,
		?Server_Reference,
		?Reason_String
	],
	[
	 begin 
		Prop = [{Prop_Name, "Test property string. Тестовая строка."}],
		Bin = mqtt_property:to_binary(Prop),
		<<L:8,PN:8,SL:16,String/binary>> = Bin,
		io:format(user, "~n UTF-8 = ~p,~p,~p,~ts, = ~256p", [L,PN,SL,unicode:characters_to_list(String, utf8),Bin]),
		?assertEqual(<<55,Prop_Name,0,52,"Test property string. Тестовая строка."/utf8>>, Bin)
	 end || Prop_Name <- PropName_UTF_8_Value_List],

	PropName_Binary_Value_List = [
		?Correlation_Data,
		?Authentication_Data
	],
	[
	 begin 
		Prop = [{Prop_Name, <<1,2,3,4,5>>}],
		Bin = mqtt_property:to_binary(Prop),
		io:format(user, "~n Binary = ~256p", [Bin]),
		?assertEqual(<<8,Prop_Name,0,5, (<<1,2,3,4,5>>)/binary>>, Bin)
	 end || Prop_Name <- PropName_Binary_Value_List],

	Prop = [{?Subscription_Identifier, 7208675}],
	Bin = mqtt_property:to_binary(Prop),
	io:format(user, "~n Variable byte Integer = ~256p", [Bin]),
	?assertEqual(<<5,?Subscription_Identifier,227,253,183,3>>, Bin),

	Prop_1 = [{?User_Property, {"Key Name", "Property Value"}}],
	Bin_1 = mqtt_property:to_binary(Prop_1),
	io:format(user, "~n UTF-8 Pair Type = ~256p", [Bin_1]),
	?assertEqual(<<27,?User_Property,0,8,"Key Name", 0,14,"Property Value">>, Bin_1),

	Prop_2 = [],
	Bin_2 = mqtt_property:to_binary(Prop_2),
	io:format(user, "~n Empty properties = ~256p", [Bin_2]),
	?assertEqual(<<0>>, Bin_2),

	?passed.

validate_props() ->
	?assert(mqtt_property:validate(connect, [{?Session_Expiry_Interval, 1000},
																					 {?Authentication_Method, "AM"},
																					 {?Authentication_Data, <<0,1>>}]
																)),
	?assertNot(mqtt_property:validate(connect, [{?Session_Expiry_Interval, 1000},
																							{?Authentication_Method, "AM"},
																							{?User_Property, {"A", "aaa"}},
																							{?Authentication_Method, "AM1"},
																							{?Authentication_Data, <<0,1>>}]
																)),
	?assert(mqtt_property:validate(connect, [{?Session_Expiry_Interval, 1000},
																					 {?Authentication_Method, "AM"},
																					 {?User_Property, {"A", "aaa"}},
																					 {?User_Property, {"B", "bbb"}},
																					 {?Authentication_Data, <<0,1>>}]
																)),
	?passed.