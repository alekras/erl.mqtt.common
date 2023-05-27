%%
%% Copyright (C) 2015-2023 by krasnop@bellsouth.net (Alexei Krasnopolski)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%		 http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License. 
%%

%% @since 2017-08-06
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc The module implements processing input stream of packets.


-module(mqtt_socket_stream).

%%
%% Include files
%%
-include("mqtt.hrl").
-include("mqtt_property.hrl").
-include("mqtt_macros.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	process/2
]).

-import(mqtt_output, [packet/4]).
-import(mqtt_input, [input_parser/2]).
-import(mqtt_publish, [do_callback/2]).

process(State, <<>>) -> 
	State#connection_state{tail = <<>>};
process(State, <<_PacketType:8, 0:8>> = Binary) ->
	process_internal(State, Binary);
process(State, <<_PacketType:8, 2:8, Bin/binary>> = Binary) ->
	if size(Bin) < 2 -> State#connection_state{tail = Binary};
		 true -> process_internal(State, Binary)
	end;
process(State, <<_PacketType:8, Bin/binary>> = Binary) ->
	case mqtt_data:extract_variable_byte_integer(Bin) of
		{error, _} -> State#connection_state{tail = Binary};
		{RestBin, Length} ->
			if size(RestBin) < Length -> State#connection_state{tail = Binary};
				 true -> process_internal(State, Binary)
			end
	end.

process_internal(State, Binary) ->
% Common values:
	Client_Id = (State#connection_state.config)#connect.client_id,
	Version = State#connection_state.config#connect.version,
	Processes = State#connection_state.processes,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	case input_parser(Version, Binary) of

%% server side only
		{connect, Config, Tail} ->
			process(mqtt_connect:connect(State, Config), Tail);
%% client side only
		{connack, SP, CRC, Msg, Properties, Tail} ->
			process(mqtt_connect:connack(State, SP, CRC, Msg, Properties), Tail);

%% Server side only
		{pingreq, Tail} ->
			lager:info([{endtype, State#connection_state.end_type}], "Ping received from client ~p~n", [Client_Id]),
			Transport:send(Socket, packet(pingresp, Version, undefined, [])),
			process(State, Tail);

%% Client side only
		{pingresp, Tail} -> 
			case maps:get(pingreq, Processes, undefined) of
				undefined -> ok;
				Timeout_ref ->
				if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
		 			?ELSE -> ok
				end
			end,
			lager:info([{endtype, State#connection_state.end_type}], "Pong received from server ~p~n", [Client_Id]),
			Ping_count = State#connection_state.ping_count - 1,
			do_callback(State#connection_state.event_callback, [onPong, Ping_count]),
			process(
				State#connection_state{
						processes = maps:remove(pingreq, Processes), 
						ping_count = Ping_count},
				Tail);

%% Server side only
		{subscribe, Packet_Id, Subscriptions, Properties, Tail} ->
			process(mqtt_subscribe:subscribe(State, Packet_Id, Subscriptions, Properties), Tail);
%% Client side only::
		{suback, Packet_Id, Return_codes, Properties, Tail} ->
			process(mqtt_subscribe:suback(State, Packet_Id, Return_codes, Properties), Tail);
%% Server side only
		{unsubscribe, Packet_Id, Topics, _Properties, Tail} ->
			process(mqtt_subscribe:unsubscribe(State, Packet_Id, Topics, _Properties), Tail);
%% Client side only::
		{unsuback, {Packet_Id, ReturnCodes}, Properties, Tail} ->
			process(mqtt_subscribe:unsuback(State, {Packet_Id, ReturnCodes}, Properties), Tail);

		?test_fragment_skip_rcv_publish

		{publish, #publish{} = PubRec, Packet_Id, Tail} ->
			process(mqtt_publish:publish(State, PubRec, Packet_Id), Tail);

		?test_fragment_skip_rcv_puback

		{puback, {Packet_Id, ReasonCode}, Properties, Tail} ->
			process(mqtt_publish:puback(State, {Packet_Id, ReasonCode}, Properties), Tail);

		?test_fragment_skip_rcv_pubrec
		
		{pubrec, {Packet_Id, ResponseCode}, Properties, Tail} ->
			process(mqtt_publish:pubrec(State, {Packet_Id, ResponseCode}, Properties), Tail);

		?test_fragment_skip_rcv_pubrel
		
		{pubrel, {Packet_Id, ReasonCode}, Properties, Tail} ->
			process(mqtt_publish:pubrel(State, {Packet_Id, ReasonCode}, Properties), Tail);

		?test_fragment_skip_rcv_pubcomp

		{pubcomp, {Packet_Id, ReasonCode}, Properties, Tail} ->
			process(mqtt_publish:pubcomp(State, {Packet_Id, ReasonCode}, Properties), Tail);

		{disconnect, DisconnectReasonCode, Properties, Tail} ->
			process(mqtt_connect:disconnect(State, DisconnectReasonCode, Properties), Tail);

		M ->
			lager:error([{endtype, State#connection_state.end_type}], "unparsed message: ~p, input binary: ~p Current State: ~p~n", [M, Binary, State]),
			gen_server:cast(self(), {disconnect, 16#82, [{?Reason_String, "Protocol Error"}]}),
			process(State, <<>>)
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================
