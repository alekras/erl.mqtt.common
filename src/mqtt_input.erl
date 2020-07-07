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
	extract_variable_byte_integer/1,
	extract_utf8_binary/1,
	extract_utf8_list/1
]).
-endif.

input_parser(_, <<?CONNECT_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = extract_variable_byte_integer(Bin),
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
	{RestBin, Length} = extract_variable_byte_integer(Bin),
	<<0:7, SP:1, Connect_Return_Code:8, RestBin_1:Length/binary, Tail/binary>> = RestBin,
	{Properties, _RestBin_2} = mqtt_property:parse(RestBin_1), % _RestBin_2 has to be empty
	{connack, SP, Connect_Return_Code, connect_reason_code(Connect_Return_Code), Properties, Tail};
input_parser(_, <<?CONNACK_PACK_TYPE, 2:8, 0:7, SP:1, Connect_Return_Code:8, Tail/binary>>) ->
	{connack, SP, Connect_Return_Code, return_code_response(Connect_Return_Code), [], Tail};

input_parser(Version, <<?PUBLISH_PACK_TYPE, DUP:1, QoS:2, RETAIN:1, Bin/binary>>) ->
	{RestBin, Length} = extract_variable_byte_integer(Bin),
	<<L:16, TopicBin:L/binary, RestBin1/binary>> = RestBin,
	Topic = unicode:characters_to_list(TopicBin, utf8),
	case QoS of
		0 ->
			PL = Length - L - 2,
			Packet_Id = 0,
			Tail = RestBin1;
		_ when (QoS =:= 1) orelse (QoS =:= 2) ->
			PL = Length - L - 4,
			<<Packet_Id:16, Tail/binary>> = RestBin1
	end,
	<<Payload:PL/binary, Tail_1/binary>> = Tail,
	case Version of
		'5.0' -> 
			{Properties, New_Tail} = mqtt_property:parse(Tail_1);
		_ ->
			{Properties, New_Tail} = {[], Tail_1}
	end,
	{publish, #publish{topic = Topic, dup = DUP, qos = QoS, retain = RETAIN, payload = Payload, dir = in, properties = Properties}, Packet_Id, New_Tail};

%input_parser('5.0', <<?PUBACK_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> 
input_parser('5.0', <<?PUBACK_PACK_TYPE, Bin/binary>>) -> 
	{RestBin, Length} = extract_variable_byte_integer(Bin),
	if Length == 2 -> 
				<<Packet_Id:16, Tail/binary>> = RestBin,
				{puback, Packet_Id, [], Tail};
		 true -> 
				<<Packet_Id:16, Reason_Code:8, Tail/binary>> = RestBin,
				{Properties, New_Tail} = mqtt_property:parse(Tail),
				{puback, Packet_Id, Properties, New_Tail}
	end;
input_parser(_, <<?PUBACK_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {puback, Packet_Id, [], Tail};

input_parser('5.0', <<?PUBREC_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> 
	{Properties, New_Tail} = mqtt_property:parse(Tail),
	{pubrec, Packet_Id, Properties, New_Tail};
input_parser(_, <<?PUBREC_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {pubrec, Packet_Id, [], Tail};

input_parser('5.0', <<16#60:8, 2:8, Packet_Id:16, Tail/binary>>) -> 
	{Properties, New_Tail} = mqtt_property:parse(Tail),
	{pubrel, Packet_Id, Properties, New_Tail}; %% @todo issue with websocket client from HiveMQ
input_parser(_, <<16#60:8, 2:8, Packet_Id:16, Tail/binary>>) -> {pubrel, Packet_Id, [], Tail}; %% @todo issue with websocket client from HiveMQ

input_parser('5.0', <<?PUBREL_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> 
	{Properties, New_Tail} = mqtt_property:parse(Tail),
	{pubrel, Packet_Id, Properties, New_Tail};
input_parser(_, <<?PUBREL_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {pubrel, Packet_Id, [], Tail};

input_parser('5.0', <<?PUBCOMP_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) ->
	{Properties, New_Tail} = mqtt_property:parse(Tail),
	{pubcomp, Packet_Id, Properties, New_Tail};
input_parser(_, <<?PUBCOMP_PACK_TYPE, 2:8, Packet_Id:16, Tail/binary>>) -> {pubcomp, Packet_Id, [], Tail};

input_parser(Version, <<?SUBSCRIBE_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = extract_variable_byte_integer(Bin),
	L = Length - 2,
	<<Packet_Id:16, RestBin1:L/binary, Tail/binary>> = RestBin,
	case Version of
		'5.0' -> 
			{Properties, New_Tail} = mqtt_property:parse(Tail);
		_ ->
			{Properties, New_Tail} = {[], Tail}
	end,
	{subscribe, Packet_Id, parse_subscription(RestBin1, []), Properties, New_Tail};

input_parser(Version, <<?SUBACK_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = extract_variable_byte_integer(Bin),
	L = Length - 2,
	<<Packet_Id:16, Return_codes:L/binary, Tail/binary>> = RestBin,
	case Version of
		'5.0' -> 
			{Properties, New_Tail} = mqtt_property:parse(Tail);
		_ ->
			{Properties, New_Tail} = {[], Tail}
	end,
	{suback, Packet_Id, binary_to_list(Return_codes), Properties, New_Tail};

input_parser(Version, <<?UNSUBSCRIBE_PACK_TYPE, Bin/binary>>) ->
	{RestBin, Length} = extract_variable_byte_integer(Bin),
	L = Length - 2,
	<<Packet_Id:16, RestBin1:L/binary, Tail/binary>> = RestBin,
	case Version of
		'5.0' -> 
			{Properties, New_Tail} = mqtt_property:parse(Tail);
		_ ->
			{Properties, New_Tail} = {[], Tail}
	end,
	{unsubscribe, Packet_Id, parse_unsubscription(RestBin1, []), Properties, New_Tail};

input_parser(Version, <<?UNSUBACK_PACK_TYPE, Bin/binary>>) ->
	{RestBin, _Length} = extract_variable_byte_integer(Bin),
	<<Packet_Id:16, Tail/binary>> = RestBin,
	case Version of
		'5.0' -> 
			{Properties, New_Tail} = mqtt_property:parse(Tail);
		_ ->
			{Properties, New_Tail} = {[], Tail}
	end,
	{unsuback, Packet_Id, Properties, New_Tail};

input_parser(_, <<?PING_PACK_TYPE, 0:8, Tail/binary>>) -> {pingreq, Tail};

input_parser(_, <<?PINGRESP_PACK_TYPE, 0:8, Tail/binary>>) -> {pingresp, Tail};

input_parser('5.0', <<?DISCONNECT_PACK_TYPE, 0:8, Tail/binary>>) ->
	{Properties, New_Tail} = mqtt_property:parse(Tail),
	{disconnect, Properties, New_Tail};
input_parser(_, <<?DISCONNECT_PACK_TYPE, 0:8, Tail/binary>>) -> {disconnect, [], Tail};

input_parser('5.0', <<?AUTH_PACK_TYPE, 0:8, Tail/binary>>) ->
	{Properties, New_Tail} = mqtt_property:parse(Tail),
	{auth, Properties, New_Tail};

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
connect_reason_code(159) -> "Connection rate exceeded".

extract_variable_byte_integer(Binary) ->
	decode_rl(Binary, 1, 0).

decode_rl(_, MP, L) when MP > (128 * 128 * 128) -> {error, L};
decode_rl(<<0:1, EncodedByte:7, Binary/binary>>, MP, L) ->
	NewL = L + EncodedByte * MP,
	{Binary, NewL};
decode_rl(<<1:1, EncodedByte:7, Binary/binary>>, MP, L) ->
	NewL = L + EncodedByte * MP,
	decode_rl(Binary, MP * 128, NewL).

extract_utf8_binary(Binary) ->
	<<Size:16, UTF8_binary:Size/binary, Tail/binary>> = Binary,
	UTF8_string = 
		case unicode:characters_to_binary(UTF8_binary, utf8) of
			{error, _, _} -> error;
			{incomplete, _, _} -> error;
			R -> R
		end,
	{Tail, UTF8_string}.

extract_utf8_list(Binary) ->
	<<Size:16, UTF8_binary:Size/binary, Tail/binary>> = Binary,
	UTF8_list = 
		case unicode:characters_to_list(UTF8_binary, utf8) of
			{error, _, _} -> error;
			{incomplete, _, _} -> error;
			R -> R
		end,
	{Tail, UTF8_list}.

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
	<<Client_id_bin_size:16, Client_id:Client_id_bin_size/binary, RestBin2/binary>> = RestBin1,

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
			<<SizeU:16, UserName:SizeU/binary, RestBin4/binary>> = RestBin3
	end,

%% Step 5: retrieve Password
	case Password of
		0 ->
			Password_bin = <<>>;
		1 -> 
			<<SizeP:16, Password_bin:SizeP/binary>> = RestBin4
	end,
	
	Config = #connect{
		client_id = binary_to_list(Client_id), %% @todo utf8 unicode:characters_to_list(Client_id, utf8),???
		user_name = binary_to_list(UserName),
		password = Password_bin,
		will = Will,
		will_qos = Will_QoS,
		will_retain = Will_retain,
		will_topic = binary_to_list(WillTopic),
		will_message = WillMessage,
		will_properties = WillProperties,
		clean_session = Clean_Session,
		keep_alive = Keep_Alive,
		version = MQTT_Version,
		properties = Properties
	},
	{connect, Config, Tail}.

parse_subscription(<<>>, Subscriptions) -> lists:reverse(Subscriptions);
parse_subscription(<<Size:16, Topic:Size/binary, QoS:8, BinartRest/binary>>, Subscriptions) ->
	parse_subscription(BinartRest, [{Topic, QoS} | Subscriptions]).

parse_unsubscription(<<>>, Topics) -> lists:reverse(Topics);
parse_unsubscription(<<Size:16, Topic:Size/binary, BinaryRest/binary>>, Topics) ->
	parse_unsubscription(BinaryRest, [Topic | Topics]).
