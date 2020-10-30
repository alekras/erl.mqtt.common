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

%% @since 2015-12-25
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_client_input.

-module(mqtt_input).

%%
%% Include files
%%
-include("mqtt.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	input_parser/2
]).

-ifdef(TEST).
-export([
]).
-endif.

input_parser(_, <<?CONNECT_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	case RestBin of
		<<4:16, "MQTT", 5:8, RestBin0/binary>> ->
			parse_connect_packet('5.0', Length - 10, RestBin0);
		<<4:16, "MQTT", 4:8, RestBin0/binary>> ->
			parse_connect_packet('3.1.1', Length - 10, RestBin0);
		<<6:16, "MQIsdp", 3:8, RestBin0/binary>> ->
			parse_connect_packet('3.1', Length - 12, RestBin0);
		_ ->
			{connect, undefined, Bin}
	end;

input_parser('5.0', <<?CONNACK_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	L = (Length - 2),
	<<0:7, SP:1, Connect_Return_Code:8, RestBin_1:L/binary, Tail/binary>> = RestBin,
	{Properties, _RestBin_2} = mqtt_property:parse(RestBin_1), % _RestBin_2 has to be empty
	{connack, SP, Connect_Return_Code, connect_reason_code(Connect_Return_Code), Properties, Tail};
input_parser(_, <<?CONNACK_PACK_TYPE, 2:8, 0:7, SP:1, Connect_Return_Code:8, Tail/binary>>) ->
	{connack, SP, Connect_Return_Code, return_code_response(Connect_Return_Code), [], Tail};

input_parser(Version, <<?PUBLISH_PACK_TYPE, DUP:1, QoS:2, RETAIN:1, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	<<L:16, TopicBin:L/binary, RestBin1/binary>> = RestBin,
	Topic = unicode:characters_to_list(TopicBin, utf8),
	case QoS of
		0 ->
			RL = Length - L - 2,
			Packet_Id = 0,
			Tail = RestBin1;
		_ when (QoS =:= 1) orelse (QoS =:= 2) ->
			RL = Length - L - 4,
			<<Packet_Id:16, Tail/binary>> = RestBin1
	end,
	case Version of
		'5.0' -> 
			{Properties, Tail_1} = mqtt_property:parse(Tail),
			PL = byte_size(Tail) - byte_size(Tail_1);
		_ ->
			{Properties, Tail_1} = {[], Tail},
			PL = 0
	end,
	PLL = RL - PL,
	<<Payload:PLL/binary, New_Tail/binary>> = Tail_1,
	{publish, #publish{topic = Topic, dup = DUP, qos = QoS, retain = RETAIN, payload = Payload, dir = in, properties = Properties}, Packet_Id, New_Tail};

input_parser('5.0', <<?PUBACK_PACK_TYPE, Bin/binary>>) -> 
	parse_pub_response(puback, Bin); %% {PacketCode, {Packet_Id, Reason_Code}, Properties, Tail}
input_parser(_, <<?PUBACK_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {puback, {Packet_Id, 0}, [], Tail};

input_parser('5.0', <<?PUBREC_PACK_TYPE, Bin/binary>>) -> 
	parse_pub_response(pubrec, Bin);
input_parser(_, <<?PUBREC_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {pubrec, {Packet_Id, 0}, [], Tail};

input_parser(_, <<16#60:8, 2:8, Packet_Id:16, Tail/binary>>) -> {pubrel, {Packet_Id, 0}, [], Tail}; %% @todo issue with websocket client from HiveMQ

input_parser('5.0', <<?PUBREL_PACK_TYPE, Bin/binary>>) -> 
	parse_pub_response(pubrel, Bin);
input_parser(_, <<?PUBREL_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {pubrel, {Packet_Id, 0}, [], Tail};

input_parser('5.0', <<?PUBCOMP_PACK_TYPE, Bin/binary>>) ->
	parse_pub_response(pubcomp, Bin);
input_parser(_, <<?PUBCOMP_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {pubcomp, {Packet_Id, 0}, [], Tail};

input_parser(Version, <<?SUBSCRIBE_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	L = Length - 2,
	<<Packet_Id:16, RestBin1:L/binary, Tail/binary>> = RestBin,
	{Properties, RestBin2} =
		case Version of
			'5.0' -> mqtt_property:parse(RestBin1);
			_ -> {[], RestBin1}
		end,
	{subscribe, Packet_Id, parse_subscription(Version, RestBin2, []), Properties, Tail};

input_parser(Version, <<?SUBACK_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	L = Length - 2,
	<<Packet_Id:16, RestBin1:L/binary, Tail/binary>> = RestBin,
	{Properties, Return_codes} =
		case Version of
			'5.0' -> mqtt_property:parse(RestBin1);
			_ -> {[], RestBin1}
		end,
	{suback, Packet_Id, binary_to_list(Return_codes), Properties, Tail};

input_parser(Version, <<?UNSUBSCRIBE_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	L = Length - 2,
	<<Packet_Id:16, RestBin1:L/binary, Tail/binary>> = RestBin,
	{Properties, RestBin2} =
		case Version of
			'5.0' -> mqtt_property:parse(RestBin1);
			_ -> {[], RestBin1}
		end,
	{unsubscribe, Packet_Id, parse_unsubscription(RestBin2, []), Properties, Tail};

input_parser(Version, <<?UNSUBACK_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	L = Length - 2,
	<<Packet_Id:16, RestBin1:L/binary, Tail/binary>> = RestBin,
	case Version of
		'5.0' -> 
			{Properties, ReasonCodeList} = mqtt_property:parse(RestBin1),
			{unsuback, {Packet_Id, binary_to_list(ReasonCodeList)}, Properties, Tail};
		_ ->
			{unsuback, {Packet_Id, []}, [], Tail}
	end;

input_parser(_, <<?PING_PACK_TYPE, 0:8, Tail/binary>>) -> {pingreq, Tail};

input_parser(_, <<?PINGRESP_PACK_TYPE, 0:8, Tail/binary>>) -> {pingresp, Tail};

input_parser('5.0', <<?DISCONNECT_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	if Length == 0 -> {disconnect, 0, [], RestBin};
		 Length == 1 ->
			<<DisconnectReasonCode:8, Tail/binary>> = RestBin,
			{disconnect, DisconnectReasonCode, [], Tail};
		 true ->
			L = Length - 1,
			<<DisconnectReasonCode:8, RestBin1:L/binary, Tail/binary>> = RestBin,
			{Properties, _} = mqtt_property:parse(RestBin1),
			{disconnect, DisconnectReasonCode, Properties, Tail}
	end;
input_parser(_, <<?DISCONNECT_PACK_TYPE, 0:8, Tail/binary>>) -> {disconnect, 0, [], Tail};

input_parser('5.0', <<?AUTH_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	if Length == 0 -> {auth, 0, [], RestBin};
		 true ->
			L = Length - 1,
			<<DisconnectReasonCode:8, RestBin1:L/binary, Tail/binary>> = RestBin,
			{Properties, _} = mqtt_property:parse(RestBin1),
			{auth, DisconnectReasonCode, Properties, Tail}
	end;

input_parser(_, Binary) ->
	lager:error("Unknown Binary received ~p~n", [Binary]),
	{unparsed, Binary}.

%% ====================================================================
%% Internal functions
%% ====================================================================
% For 3.1.1 version:
return_code_response(0) -> "0x00 Connection Accepted";
return_code_response(1) -> "0x01 Connection Refused, unacceptable protocol version";
return_code_response(2) -> "0x02 Connection Refused, identifier rejected";
return_code_response(3) -> "0x03 Connection Refused, Server unavailable";
return_code_response(4) -> "0x04 Connection Refused, bad user name or password";
return_code_response(5) -> "0x05 Connection Refused, not authorized";
return_code_response(_) -> "Return Code is not recognizable".
% For 5.0 version:
connect_reason_code(0) -> "Success";
connect_reason_code(128) -> "Unspecified error";
connect_reason_code(129) -> "Malformed Packet";
connect_reason_code(130) -> "Protocol Error";
connect_reason_code(131) -> "Implementation specific error";
connect_reason_code(132) -> "Unsupported Protocol Version";
connect_reason_code(133) -> "Client Identifier not valid";
connect_reason_code(134) -> "Bad User Name or Password";
connect_reason_code(135) -> "Not authorized";
connect_reason_code(136) -> "Server unavailable";
connect_reason_code(137) -> "Server busy";
connect_reason_code(138) -> "Banned";
connect_reason_code(140) -> "Bad authentication method";
connect_reason_code(144) -> "Topic Name invalid";
connect_reason_code(149) -> "Packet too large";
connect_reason_code(151) -> "Quota exceeded";
connect_reason_code(153) -> "Payload format invalid";
connect_reason_code(154) -> "Retain not supported";
connect_reason_code(155) -> "QoS not supported";
connect_reason_code(156) -> "Use another server";
connect_reason_code(157) -> "Server moved";
connect_reason_code(159) -> "Connection rate exceeded";
connect_reason_code(_) -> "Return Code is not recognizable".

parse_connect_packet(MQTT_Version, Length, Binary) ->
%% Retrieve Connect Flags and KeepAlive 
	<<User:1, Password:1, Will_retain:1, Will_QoS:2, Will:1, Clean_Session:1, _:1, 
		Keep_Alive:16, RestBin0:Length/binary, Tail/binary>> = Binary,
%% Properties Field processing
	if MQTT_Version == '5.0' ->
			{Properties, RestBin1} = mqtt_property:parse(RestBin0);
		true ->
			Properties = [],
			RestBin1 = RestBin0
	end,

%% Retrieve Payload:
%% Step 1: retrieve Client_id
	{RestBin2, Client_id} = mqtt_data:extract_binary_field(RestBin1),
%% Step 2, 3: retrieve Will_Topic and Will_Message
	case Will of
		0 ->
			WillTopic = <<>>,
			WillMessage = <<>>,
			WillProperties = [],
			RestBin3 = RestBin2;
		1 -> 
			if MQTT_Version == '5.0' ->
					{WillProperties, RestBin2_1} = mqtt_property:parse(RestBin2);
				true ->
					WillProperties = [],
					RestBin2_1 = RestBin2
			end,
			<<SizeT:16, WillTopic:SizeT/binary, SizeM:16, WillMessage:SizeM/binary, RestBin3/binary>> = RestBin2_1
	end,

%% Step 4: retrieve User_Name
	case User of
		0 ->
			UserName = <<>>,
			RestBin4 = RestBin3;
		1 -> 
			{RestBin4, UserName} = mqtt_data:extract_binary_field(RestBin3)
	end,

%% Step 5: retrieve Password
	case Password of
		0 ->
			Password_bin = <<>>;
		1 -> 
			{_, Password_bin} = mqtt_data:extract_binary_field(RestBin4)
	end,
	
	Config1 = #connect{
		client_id = binary_to_list(Client_id), %% @todo utf8 unicode:characters_to_list(Client_id, utf8),???
		user_name = binary_to_list(UserName), %% @todo do we need a list?
		password = Password_bin,
		will = Will,
		clean_session = Clean_Session,
		keep_alive = Keep_Alive,
		version = MQTT_Version,
		properties = Properties
	},
	Config =
	if Will == 0 ->
			 Config1;
		 Will == 1 ->
			 Config1#connect{
%% 				will_qos = Will_QoS,
%% 				will_retain = Will_retain,
%% 				will_topic = binary_to_list(WillTopic), %% @todo do we need a list?
%% 				will_message = WillMessage,
%% 				will_properties = WillProperties,
				will_publish = #publish{
					qos = Will_QoS,
					retain = Will_retain,
					topic = binary_to_list(WillTopic),
					payload = WillMessage,
					properties = WillProperties
			}}
	end,
	{connect, Config, Tail}.

parse_subscription(_, <<>>, Subscriptions) -> lists:reverse(Subscriptions);
parse_subscription('5.0', InputBinary, Subscriptions) ->
	{<<0:2, RetainHandling:2, RetainAsPub:1, NoLocal:1, MaxQos:2, BinaryRest/binary>>, Topic} = mqtt_data:extract_utf8_binary(InputBinary),
	parse_subscription('5.0', BinaryRest,
										 [{Topic, 
											 #subscription_options{max_qos = MaxQos,
																						 nolocal = NoLocal,
																						 retain_as_published = RetainAsPub,
																						 retain_handling = RetainHandling}
											} | Subscriptions]);
parse_subscription(Vrs, InputBinary, Subscriptions) ->
	{<<QoS:8, BinaryRest/binary>>, Topic} = mqtt_data:extract_utf8_binary(InputBinary),
	parse_subscription(Vrs, BinaryRest, [{Topic, QoS} | Subscriptions]).

parse_unsubscription(<<>>, Topics) -> lists:reverse(Topics);
parse_unsubscription(InputBinary, Topics) ->
	{BinaryRest, Topic} = mqtt_data:extract_utf8_binary(InputBinary),
	parse_unsubscription(BinaryRest, [Topic | Topics]).

parse_pub_response(PacketCode, Bin) ->
	{RestBin, Length} = mqtt_data:extract_variable_byte_integer(Bin),
	if Length == 2 -> 
				<<Packet_Id:16, Tail/binary>> = RestBin,
				{PacketCode, {Packet_Id, 0}, [], Tail};
		 Length == 3 ->
				<<Packet_Id:16, Reason_Code:8, Tail/binary>> = RestBin,
				{PacketCode, {Packet_Id, Reason_Code}, [], Tail};
		 true -> 
				<<Packet_Id:16, Reason_Code:8, Tail/binary>> = RestBin,
				{Properties, New_Tail} = mqtt_property:parse(Tail),
				{PacketCode, {Packet_Id, Reason_Code}, Properties, New_Tail}
	end.
