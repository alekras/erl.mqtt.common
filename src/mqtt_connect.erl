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

%% @since 2023-03-29
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc Module implements connect, connack and disconnect functionality.


-module(mqtt_connect).

%%
%% Include files
%%
-include("mqtt.hrl").
-include("mqtt_property.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	connect/2,
	connack/5,
	disconnect/3
]).

-import(mqtt_output, [packet/4]).
-import(mqtt_publish, [do_callback/2]).

%% server side only
connect(State, undefined) ->
	lager:alert([{endtype, State#connection_state.end_type}], "Connection packet cannot be parsed.~n", []),
	self() ! disconnect,
	State;
connect(State, Config) ->
% Common values:
	Client_Id = Config#connect.client_id,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Storage = State#connection_state.storage,
	ClientPid = Storage:connect_pid(get, Client_Id, server),
	try 
%% validate connect config.
		mqtt_data:validate_config(Config),
%% check credentials 
		Encrypted_password_db =
			case Storage:user(get, Config#connect.user_name) of
				undefined -> <<>>;
				#{password := Password_db} -> Password_db
			end,
		Encrypted_password_cli = list_to_binary(mqtt_data:binary_to_hex(crypto:hash(md5, Config#connect.password))),
		lager:debug([{endtype, server}], "Previous Client PID = ~p~n", [ClientPid]),
		ConnVersion = Config#connect.version,
		if ClientPid =:= undefined -> ok;
			 is_pid(ClientPid) -> %%% @TODO: Check this !!!
%					throw(#mqtt_error{oper = session, error_msg = lists:concat(["Process with Client Id: '", Client_Id, "' already exists."])});
					try 
						gen_server:cast(ClientPid, {disconnect, 16#8e, [{?Reason_String, "Session taken over"}]})
					catch _:_ -> ok
					end;
			 true -> ok
		end,
		Resp_code =
			if Encrypted_password_db =/= Encrypted_password_cli -> 5;
				 Encrypted_password_db == <<>> -> 5;
				 true -> 0
			end,
		if Resp_code =:= 0 ->
				New_State = State#connection_state{config = Config, topic_alias_in_map = #{}, topic_alias_out_map = #{}},
				New_State_2 =
				case Config#connect.clean_session of
					1 -> 
						Storage:cleanup(Config#connect.client_id, server),
						New_State#connection_state{session_present = 0};
					0 ->	 
						mqtt_connection:restore_session(New_State) 
				end,
				New_Client_Id = Config#connect.client_id,
				Storage:connect_pid(save, #storage_connectpid{client_id = New_Client_Id, pid = self()}, server),
				Storage:session_state(save, #session_state{client_id = New_Client_Id,
						session_expiry_interval = proplists:get_value(?Session_Expiry_Interval, Config#connect.properties, 0),
						will_publish = Config#connect.will_publish}),
				New_State_3 = receive_max_set_handle(ConnVersion, New_State_2),
				Packet = packet(connack, ConnVersion, {New_State_3#connection_state.session_present, Resp_code}, Config#connect.properties), %% now just return connect properties TODO
				Transport:send(Socket, Packet),
				lager:info([{endtype, server}], "Connection to client ~p is established~n", [New_Client_Id]),
				New_State_3#connection_state{connected = 1};
			true ->
				Packet = packet(connack, ConnVersion, {0, Resp_code}, []),
				Transport:send(Socket, Packet),
				lager:warning([{endtype, server}], "Connection to client ~p is closed by reason: ~p~n", [Config#connect.client_id, Resp_code]),
				gen_server:cast(self(), {disconnect, 16#82, [{?Reason_String, "Error reason: " ++ Resp_code}]}),
				State
		end
	catch
		throw:#mqtt_error{error_msg = Msg} -> 
			gen_server:cast(self(), {disconnect, 16#82, [{?Reason_String, "Protocol Error: " ++ Msg}]}),
			State
	end.

%% client side only
connack(State, SP, CRC, Msg, Properties) ->
	Timeout_ref = State#connection_state.timeout_ref,
	if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
		 ?ELSE -> ok
	end,
% Common values:
	Client_Id = (State#connection_state.config)#connect.client_id,
	Version = State#connection_state.config#connect.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Storage = State#connection_state.storage,
	{Host, Port} = get_peername(Transport, Socket),
	lager:debug([{endtype, client}], "SessionPresent=~p, CRC=~p, Msg=~p, Properties=~128p", [SP, CRC, Msg, Properties]),
	if SP == 0 -> Storage:cleanup(Client_Id, client);
		 ?ELSE -> ok
	end,
	IsConnected = if CRC == 0 -> %% TODO process all codes for v5.0
			lager:info([{endtype, client}], "Client ~p is successfuly connected to ~p:~p, version=~p", [Client_Id, Host, Port, Version]),
			1;
		?ELSE ->
			lager:info([{endtype, client}], "Client ~p is failed to connect to ~p:~p, version=~p, reason=~p", [Client_Id, Host, Port, Version, Msg]),
			0
		end,
	do_callback(State#connection_state.event_callback, [onConnect, {CRC, Msg, Properties}]),
	NewState = handle_conack_properties(Version, State, Properties),
	NewState#connection_state{session_present = SP,
														connected = IsConnected,
														timeout_ref = undefined}.

disconnect(#connection_state{end_type = client} = State, DisconnectReasonCode, Properties) ->
% Common values:
	Client_Id = (State#connection_state.config)#connect.client_id,
%%			Storage:connection_state(remove, Client_Id),
	gen_server:cast(self(), disconnect),
	do_callback(State#connection_state.event_callback, [onClose, {DisconnectReasonCode, Properties}]),
	lager:info([{endtype, State#connection_state.end_type}],
		"Client ~p is disconnecting with reason ~p and Props=~p~n",
		[Client_Id, DisconnectReasonCode, Properties]
	),
	State#connection_state{connected = 0};
disconnect(#connection_state{end_type = server} = State, DisconnectReasonCode, Properties) ->
% Common values:
	Client_Id = (State#connection_state.config)#connect.client_id,
	Version = State#connection_state.config#connect.version,
	ConfProps = (State#connection_state.config)#connect.properties,
	if Version == '5.0' ->
			SessExpCnfg = proplists:get_value(?Session_Expiry_Interval, ConfProps, 0),
			SessExpDscn = proplists:get_value(?Session_Expiry_Interval, Properties, 0),
			if (SessExpCnfg == 0) and (SessExpDscn > 0) ->
						gen_server:cast(self(), {disconnect, 16#82, [{?Reason_String, "Protocol Error"}]});
					?ELSE ->
						gen_server:cast(self(), {disconnect, DisconnectReasonCode, [{?Reason_String, "Initiated by client or server"}]})
			end;
		?ELSE ->
			gen_server:cast(self(), {disconnect, DisconnectReasonCode, []})
	end,
%%			Storage:connection_state(remove, Client_Id),
	lager:info([{endtype, State#connection_state.end_type}], "Process ~p is disconnecting with reason ~p and Props=~p~n", [Client_Id, DisconnectReasonCode, Properties]),
	State#connection_state{connected = 0}.

%% ====================================================================
%% Internal functions
%% ====================================================================

handle_conack_properties('5.0', #connection_state{config = Config} = State, Properties) ->
	NewState =
	case proplists:get_value(?Topic_Alias_Maximum, Properties, undefined) of
		undefined -> State;
		TAMaximum ->
			ConfProps = lists:keystore(?Topic_Alias_Maximum, 1, Config#connect.properties, {?Topic_Alias_Maximum, TAMaximum}),
			State#connection_state{config = Config#connect{properties = ConfProps}}
	end,
	case proplists:get_value(?Receive_Maximum, Properties, -1) of
		-1 -> NewState#connection_state{receive_max = 65535, send_quota = 65535};
		 0 -> NewState; %% TODO protocol_error;
		 N -> NewState#connection_state{receive_max = N + 1, send_quota = N + 1} %% it allows server side to treat the number publish proceses 
	end;
handle_conack_properties(_, State, _) ->
	State.

receive_max_set_handle('5.0', #connection_state{config = #connect{properties = Props}} = State) ->
	case proplists:get_value(?Receive_Maximum, Props, -1) of
		-1 -> State#connection_state{receive_max = 65535, send_quota = 65535};
		 0 -> State; %% TODO protocol_error;
		 N -> State#connection_state{receive_max = N, send_quota = N}
	end;
receive_max_set_handle(_, State) ->
	State.

get_peername(ssl, Socket) ->
	case ssl:peername(Socket) of
		{ok, {Host, Port}} -> {Host, Port};
		_ -> {undefined, ""}
	end;
get_peername(gen_tcp, Socket) ->
	case inet:peername(Socket) of
		{ok, {Host, Port}} -> {Host, Port};
		_ -> {undefined, ""}
	end;
get_peername(mqtt_ws_client_handler, Socket) ->
	case mqtt_ws_client_handler:peername(Socket) of
		{ok, {Host, Port}} -> {Host, Port};
		_ -> {undefined, ""}
	end;
get_peername(mqtt_ws_handler, Socket) ->
	case mqtt_ws_handler:peername(Socket) of
		{ok, {Host, Port}} -> {Host, Port};
		_ -> {undefined, ""}
	end;
get_peername(_, _Socket) ->
	{"test mock host", "0"}.
