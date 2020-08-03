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
%% @doc @todo Add description to mqtt_output.


-module(mqtt_output).

%%
%% Include files
%%
-include("mqtt.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	packet/4
]).

-ifdef(TEST).
-export([
	variable_header/2,
	payload/2
]).
-endif.

packet(connect, _, #connect{version = '5.0', properties = Properties} = Conn_config, []) -> %% @todo remove arg [] !
	Remaining_packet = <<(variable_header(connect, Conn_config))/binary,
											 (mqtt_property:to_binary(Properties))/binary,
											 (payload(connect, Conn_config))/binary>>,
	<<(fixed_header(connect, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;
packet(connect, _, Conn_config, _) ->
	Remaining_packet = <<(variable_header(connect, Conn_config))/binary, (payload(connect, Conn_config))/binary>>,
	<<(fixed_header(connect, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;

packet(connack, '5.0', {SP, Connect_Reason_Code}, Properties) ->
	Props_Bin = mqtt_property:to_binary(Properties),
	Remaining_Length = byte_size(Props_Bin) + 2,
	<<(fixed_header(connack, 0, Remaining_Length))/binary, 0:7, SP:1, Connect_Reason_Code:8, Props_Bin/binary>>;
packet(connack, _, {SP, Connect_Reason_Code}, _) ->
	<<(fixed_header(connack, 0, 2))/binary, 0:7, SP:1, Connect_Reason_Code:8>>;

packet(publish, MQTT_Version, {#publish{payload = Payload, properties = Properties} = Params, Packet_Id}, []) ->
	Props_Bin = case MQTT_Version of
								'5.0' -> mqtt_property:to_binary(Properties);
								_ -> <<>>
							end,
	Remaining_packet = <<(variable_header(publish, {Params#publish.qos, Params#publish.topic, Packet_Id}))/binary,
												Props_Bin/binary,
												(payload(publish, Payload))/binary>>,
	<<(fixed_header(publish, 
									{Params#publish.dup, Params#publish.qos, Params#publish.retain}, 
									byte_size(Remaining_packet))
		)/binary, 
		Remaining_packet/binary>>;

packet(subscribe, '5.0', {Subscriptions, Packet_Id}, Properties) ->
	Remaining_packet = <<(variable_header(subscribe, Packet_Id))/binary,
											 (mqtt_property:to_binary(Properties))/binary,
											 (payload(subscribe, Subscriptions))/binary>>,
	<<(fixed_header(subscribe, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;
packet(subscribe, _, {Subscriptions, Packet_Id}, _) ->
	Remaining_packet = <<(variable_header(subscribe, Packet_Id))/binary, (payload(subscribe, Subscriptions))/binary>>,
	<<(fixed_header(subscribe, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;

packet(suback, '5.0', {Return_Codes, Packet_Id}, Properties) ->
	Remaining_packet = <<(variable_header(suback, Packet_Id))/binary, (mqtt_property:to_binary(Properties))/binary, (payload(suback, Return_Codes))/binary>>,
	<<(fixed_header(suback, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;
packet(suback, _, {Return_Codes, Packet_Id}, _) ->
	Remaining_packet = <<(variable_header(suback, Packet_Id))/binary, (payload(suback, Return_Codes))/binary>>,
	<<(fixed_header(suback, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;

packet(unsubscribe, '5.0', {Topics, Packet_Id}, Properties) ->
	Remaining_packet = <<(variable_header(unsubscribe, Packet_Id))/binary, (mqtt_property:to_binary(Properties))/binary, (payload(unsubscribe, Topics))/binary>>,
	<<(fixed_header(unsubscribe, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;
packet(unsubscribe, _, {Topics, Packet_Id}, _) ->
	Remaining_packet = <<(variable_header(unsubscribe, Packet_Id))/binary, (payload(unsubscribe, Topics))/binary>>,
	<<(fixed_header(unsubscribe, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;

packet(unsuback, '5.0', {ReasonCodeList, Packet_Id}, Properties) ->
	Remaining_packet = <<(variable_header(unsuback, Packet_Id))/binary, (mqtt_property:to_binary(Properties))/binary, (payload(unsuback, ReasonCodeList))/binary>>,
	<<(fixed_header(unsuback, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;
packet(unsuback, _, {_, Packet_Id}, _) ->
	<<(fixed_header(unsuback, 0, 2))/binary, Packet_Id:16>>;

packet(disconnect, '5.0', 0, []) ->
	<<(fixed_header(disconnect, 0, 0))/binary>>;
packet(disconnect, '5.0', DisconnectReasonCode, Properties) ->
	Remaining_packet = <<DisconnectReasonCode:8, (mqtt_property:to_binary(Properties))/binary>>,
	<<(fixed_header(disconnect, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;
packet(disconnect, _, _, _) ->
	<<(fixed_header(disconnect, 0, 0))/binary>>;

packet(pingreq, _MQTT_Version, _, _) ->
	<<(fixed_header(pingreq, 0, 0))/binary>>;

packet(pingresp, _MQTT_Version, _, _) ->
	<<(fixed_header(pingresp, 0, 0))/binary>>;

packet(auth, '5.0', AuthenticateReasonCode, Properties) ->
	Remaining_packet = 
	if (AuthenticateReasonCode == 0) and (Properties == []) -> <<>>;
			true -> <<AuthenticateReasonCode:8, (mqtt_property:to_binary(Properties))/binary>>
	end,
	<<(fixed_header(auth, 0, byte_size(Remaining_packet)))/binary, Remaining_packet/binary>>;

packet(puback, '5.0', Param, Properties) ->  %% {Packet_Id, Reason_Code} = Param
	pub_response(puback, Param, Properties);
packet(puback, _, {Packet_Id,0}, _) ->
	<<(fixed_header(puback, 0, 2))/binary, Packet_Id:16>>;

packet(pubrec, '5.0', Param, Properties) ->
	pub_response(pubrec, Param, Properties);
packet(pubrec, _, {Packet_Id,0}, _) ->
	<<(fixed_header(pubrec, 0, 2))/binary, Packet_Id:16>>;

packet(pubrel, '5.0', Param, Properties) ->
	pub_response(pubrel, Param, Properties);
packet(pubrel, _, {Packet_Id,0}, _) ->
	<<(fixed_header(pubrel, 0, 2))/binary, Packet_Id:16>>;

packet(pubcomp, '5.0', Param, Properties) ->
	pub_response(pubcomp, Param, Properties);
packet(pubcomp, _, {Packet_Id,0}, _) ->
	<<(fixed_header(pubcomp, 0, 2))/binary, Packet_Id:16>>.

fixed_header(connect, _Flags, Length) ->
	<<?CONNECT_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(connack, _Flags, Length) ->
	<<?CONNACK_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(publish, {Dup, QoS, Retain}, Length) ->
	<<?PUBLISH_PACK_TYPE, Dup:1, QoS:2, Retain:1, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(subscribe, _Flags, Length) ->
	<<?SUBSCRIBE_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(suback, _Flags, Length) ->
	<<?SUBACK_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(unsubscribe, _Flags, Length) ->
	<<?UNSUBSCRIBE_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(unsuback, _Flags, Length) ->
	<<?UNSUBACK_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(puback, _Flags, Length) ->
	<<?PUBACK_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(pubrec, _Flags, Length) ->
	<<?PUBREC_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(pubrel, _Flags, Length) ->
	<<?PUBREL_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(pubcomp, _Flags, Length) ->
	<<?PUBCOMP_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(pingreq, _Flags, _Length) ->
	<<?PING_PACK_TYPE, 0:8>>;
fixed_header(pingresp, _Flags, _Length) ->
	<<?PINGRESP_PACK_TYPE, 0:8>>;
fixed_header(disconnect, _Flags, Length) ->
	<<?DISCONNECT_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>;
fixed_header(auth, _Flags, Length) ->
	<<?AUTH_PACK_TYPE, (mqtt_data:encode_variable_byte_integer(Length))/binary>>.

variable_header(connect, Config) ->
	User = case mqtt_data:fieldSize(Config#connect.user_name) of 0 -> 0; _ -> 1 end,
	Password = case mqtt_data:fieldSize(Config#connect.password) of 0 -> 0; _ -> 1 end,
	case mqtt_data:fieldSize(Config#connect.will_topic) of
		0 ->
			Will_retain = 0,
			Will_QoS = 0,
			Will = 0;
		_ ->
			Will_retain = Config#connect.will_retain,
			Will_QoS = Config#connect.will_qos,
			Will = Config#connect.will
	end,
	Clean_Session = Config#connect.clean_session,
	Keep_alive = Config#connect.keep_alive,
	case Config#connect.version of
		'5.0' ->
			<<4:16, "MQTT", 5:8, User:1, Password:1, Will_retain:1, Will_QoS:2, Will:1, Clean_Session:1, 0:1, Keep_alive:16>>;
		'3.1.1' -> 
			<<4:16, "MQTT", 4:8, User:1, Password:1, Will_retain:1, Will_QoS:2, Will:1, Clean_Session:1, 0:1, Keep_alive:16>>;
		'3.1' ->
			<<6:16, "MQIsdp", 3:8, User:1, Password:1, Will_retain:1, Will_QoS:2, Will:1, Clean_Session:1, 0:1, Keep_alive:16>>
	end;
variable_header(publish, {0, Topic, _}) ->
	mqtt_data:encode_utf8_string(Topic);
variable_header(publish, {_, Topic, Packet_Id}) ->
	<<(mqtt_data:encode_utf8_string(Topic))/binary, Packet_Id:16>>;
variable_header(subscribe, Packet_Id) ->
	<<Packet_Id:16>>;
variable_header(suback, Packet_Id) ->
	<<Packet_Id:16>>;
variable_header(unsubscribe, Packet_Id) ->
	<<Packet_Id:16>>;
variable_header(unsuback, Packet_Id) ->
	<<Packet_Id:16>>.

payload(connect, Config) ->
	Will_bin =
	if Config#connect.will == 0 ->
				<<>>;
			true ->
				WP =
				if Config#connect.version == '5.0' ->
					mqtt_property:to_binary(Config#connect.will_properties);
				true -> 
					<<>>
				end,
				<<WP/binary, 
					(mqtt_data:encode_utf8_string(Config#connect.will_topic))/binary, 
					(mqtt_data:encode_binary_field(Config#connect.will_message))/binary>>
	end,
	Username_bin =
	case mqtt_data:fieldSize(Config#connect.user_name) of
		0 ->
			<<>>;
		_ ->
			mqtt_data:encode_utf8_string(Config#connect.user_name)
	end,
	Password_bin =
	case mqtt_data:fieldSize(Config#connect.password) of
		0 ->
			<<>>;
		_ ->
			mqtt_data:encode_utf8_string(Config#connect.password)
	end,
	<<(mqtt_data:encode_utf8_string(Config#connect.client_id))/binary, 
		Will_bin/binary, 
		Username_bin/binary,
		Password_bin/binary>>;
payload(publish, Payload) ->
	Payload;

payload(subscribe, []) -> <<>>;
payload(subscribe, [{Topic, #subscription_options{max_qos = MaxQos, nolocal = NoLocal, retain_as_published = RetainAsPub, retain_handling = RetainHandling}, _Callback} | Subscriptions]) ->
	<<(mqtt_data:encode_utf8_string(Topic))/binary, 0:2, RetainHandling:2, RetainAsPub:1, NoLocal:1, MaxQos:2, (payload(subscribe, Subscriptions))/binary>>;
payload(subscribe, [{Topic, QoS, _Callback} | Subscriptions]) ->
	<<(mqtt_data:encode_utf8_string(Topic))/binary, QoS:8, (payload(subscribe, Subscriptions))/binary>>;

payload(suback, []) -> <<>>;
payload(suback, [#subscription_options{max_qos = MaxQos, nolocal = NoLocal, retain_as_published = RetainAsPub, retain_handling = RetainHandling} | Return_Codes]) ->
	<<0:2, RetainHandling:2, RetainAsPub:1, NoLocal:1, MaxQos:2, (payload(suback, Return_Codes))/binary>>;
payload(suback, [Return_Code | Return_Codes]) ->
	<<Return_Code:8, (payload(suback, Return_Codes))/binary>>;

payload(unsubscribe, []) -> <<>>;
payload(unsubscribe, [Topic | Topics]) ->
	<<(mqtt_data:encode_utf8_string(Topic))/binary, (payload(unsubscribe, Topics))/binary>>;

payload(unsuback, []) -> <<>>;
payload(unsuback, [ReasonCode | ReasonCodeList]) ->
	<<ReasonCode:8, (payload(unsuback, ReasonCodeList))/binary>>.

%% ====================================================================
%% Internal functions
%% ====================================================================

pub_response(PacketCode, {Packet_Id, 0}, []) ->
	<<(fixed_header(PacketCode, 0, 2))/binary, Packet_Id:16>>;
pub_response(PacketCode, {Packet_Id, Reason_Code}, []) ->
	<<(fixed_header(PacketCode, 0, 3))/binary, Packet_Id:16, Reason_Code:8>>;
pub_response(PacketCode, {Packet_Id, Reason_Code}, Properties) ->
	Props_Bin = mqtt_property:to_binary(Properties),
	<<(fixed_header(PacketCode, 0, byte_size(Props_Bin) + 3))/binary, Packet_Id:16, Reason_Code:8, Props_Bin/binary>>.
