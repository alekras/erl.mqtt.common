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
	ConnVersion = Config#connect.version,
	State1 = State#connection_state{
		client_id = Client_Id,
		version = ConnVersion,
		keep_alive = Config#connect.keep_alive,
		properties = Config#connect.properties
	},
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
		Resp_code =
			if (Encrypted_password_db =/= Encrypted_password_cli) or (Encrypted_password_db == <<>>) ->
						case ConnVersion of
							'5.0' -> 135;
							_ -> 5
						end;
				 true -> 0
			end,
		if Resp_code =:= 0 ->
%% check if client Id already taken
				ClientPid = Storage:connect_pid(get, Client_Id, server),
				lager:debug([{endtype, server}],
										?LOGGING_FORMAT ++ " previous Client PID = ~p~n",
										[Client_Id, none, connect, ConnVersion, ClientPid]),
				if ClientPid =:= undefined -> ok;
					 is_pid(ClientPid) -> %%% @TODO: Check this !!!
%					throw(#mqtt_error{oper = session, error_msg = lists:concat(["Process with Client Id: '", Client_Id, "' already exists."])});
							try 
								gen_server:cast(ClientPid, {disconnect, 16#8e, [{?Reason_String, "Session taken over"}]})
							catch _:_ -> ok
							end;
					 ?ELSE -> ok
				end,

				New_State =
					case Config#connect.clean_session of
						1 -> 
							Storage:cleanup(Client_Id, server),
							State1#connection_state{session_present = 0};
						0 ->	 
							restore_session(State1) 
					end,
				Storage:connect_pid(save, #storage_connectpid{client_id = Client_Id, pid = self()}, server),
				Session_expiry_interval = proplists:get_value(?Session_Expiry_Interval, Config#connect.properties, 0),
				Will_publish = Config#connect.will_publish,
				Session_state_new =
				case Storage:session_state(get, Client_Id) of
					undefined ->
						#session_state{
							client_id = Client_Id,
							will_publish = Will_publish};
					Session_state when Will_publish == undefined -> Session_state;
					Session_state -> Session_state#session_state{will_publish = Will_publish}
				end,
				Storage:session_state(save, Session_state_new#session_state{session_expiry_interval = Session_expiry_interval}),
				New_State_1 = receive_max_set_handle(ConnVersion, New_State),
				Packet = packet(connack, ConnVersion, {New_State_1#connection_state.session_present, Resp_code}, Config#connect.properties), %% now just return connect properties TODO
				Transport:send(Socket, Packet),
				lager:info([{endtype, server}],
									?LOGGING_FORMAT++" connection to client is established~n",
									[Client_Id, none, connect, ConnVersion]),
				New_State_1#connection_state{topic_alias_in_map = #{}, topic_alias_out_map = #{}, event_callback = self(), connected = 1};
			?ELSE ->
				Packet = packet(connack, ConnVersion, {0, Resp_code}, []),
				Transport:send(Socket, Packet),
				lager:error([{endtype, server}],
										?LOGGING_FORMAT ++ " connection to client is closed by reason: ~p~n",
										[Client_Id, none, connect, ConnVersion, Resp_code]),
				gen_server:cast(self(), {disconnect, Resp_code, [{?Reason_String, "Error reason: " ++ integer_to_list(Resp_code)}]}),
				State1
		end
	catch
		throw:#mqtt_error{error_msg = Msg} -> 
			gen_server:cast(self(), {disconnect, 16#82, [{?Reason_String, "Protocol Error: " ++ Msg}]}),
			State1
	end.

%% client side only
connack(State, SessionPresent, CRC, Msg, Properties) ->
	Processes = State#connection_state.processes,
	Timeout_ref = maps:get(connect, Processes, undefined),
	if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
		 ?ELSE -> ok
	end,
% Common values:
	Client_Id = State#connection_state.client_id,
	Version = State#connection_state.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Storage = State#connection_state.storage,
	{Host, Port} = get_peername(Transport, Socket),
	lager:debug([{endtype, client}],
							?LOGGING_FORMAT ++ " connection acknowledged with SessionPresent=~p, CRC=~p, Msg=~p, Properties=~128p", 
							[Client_Id, none, connack, Version, SessionPresent, CRC, Msg, Properties]),
	if SessionPresent == 0 -> Storage:cleanup(Client_Id, client);
		 ?ELSE -> ok
	end,
	IsConnected = if CRC == 0 -> %% TODO process all codes for v5.0
			lager:info([{endtype, client}],
								?LOGGING_FORMAT ++ " client is successfuly connected to ~p:~p~n",
								[Client_Id, none, connack, Version, Host, Port]),
			do_callback(State#connection_state.event_callback, [onConnect, {CRC, Msg, Properties}]),
			1;
		?ELSE ->
			lager:info([{endtype, client}],
								 ?LOGGING_FORMAT ++ " client is failed to connect to ~p:~p, reason=~p~n",
								 [Client_Id, none, connack, Version, Host, Port, Msg]),
			do_callback(State#connection_state.event_callback, [onError, #mqtt_error{oper= connect, errno= CRC, error_msg= Msg}]),
			0
		end,
	NewState =
		case SessionPresent of
			0 ->
				Storage:cleanup(Client_Id, State#connection_state.end_type),
				State;
			1 ->
				restore_session(State) 
		end,
	NewState1 = handle_conack_properties(Version, NewState, Properties),
	NewState1#connection_state{session_present = SessionPresent,
														connected = IsConnected,
														processes = maps:remove(connect, Processes)}.

disconnect(#connection_state{client_id = Client_Id, version = Version, end_type = client} = State, DisconnectReasonCode, Properties) ->
	Processes = State#connection_state.processes,
	Timeout_ref = maps:get(disconnect, Processes, undefined),
	if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
		 ?ELSE -> ok
	end,
%%			Storage:connection_state(remove, Client_Id),
	gen_server:cast(self(), disconnect),
	do_callback(State#connection_state.event_callback, [onClose, {DisconnectReasonCode, Properties}]),
	lager:info([{endtype, client}],
		?LOGGING_FORMAT ++ " process receives disconnect packet with reason ~p and Props=~p~n",
		[Client_Id, none, disconnect, Version, DisconnectReasonCode, Properties]
	),
	State#connection_state{connected = 0, processes = maps:remove(disconnect, Processes)};
disconnect(#connection_state{end_type = server} = State, DisconnectReasonCode, Properties) ->
% Common values:
	Client_Id = State#connection_state.client_id,
	Version = State#connection_state.version,
	ConfProps = State#connection_state.properties,
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
	lager:info([{endtype, server}],
						 ?LOGGING_FORMAT ++ " process receives disconnect packet with reason ~p and Props=~p~n",
						 [Client_Id, none, disconnect, Version, DisconnectReasonCode, Properties]),
	State#connection_state{connected = 0}.

%% ====================================================================
%% Internal functions
%% ====================================================================

restore_session(#connection_state{client_id = Client_Id, version= '5.0', storage = Storage, end_type = server} = State) ->
	case Storage:session_state(get, Client_Id) of
		undefined ->
			State#connection_state{session_present= 0};
		_ ->
			run_restore_session(State)
	end;
restore_session(State) ->
	run_restore_session(State).

run_restore_session(#connection_state{client_id = Client_Id, version = Version, storage = Storage, end_type = EndType} = State) ->
	Records = Storage:session(get_all, Client_Id, EndType),
	MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
	lager:debug([{endtype, EndType}],
							?LOGGING_FORMAT ++ " in restore session, message list = ~128p~n",
							[Client_Id, none, restore_session, Version, MessageList]),
	Storage:session(clean, Client_Id, EndType),
	lists:foldl(fun restore_state/2, State#connection_state{session_present= 1}, MessageList).

restore_state({Packet_Id, Params}, #connection_state{client_id = Client_id, version = Version} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " in restore session, publish msg ~p.~n",
							[Client_id, Packet_Id, restore_session, Version, Params]),
	spawn(gen_server, cast, [self(), {republish, Params, Packet_Id}]),
	New_processes = (State#connection_state.processes)#{Packet_Id => Params},
	State#connection_state{processes = New_processes, packet_id= mqtt_connection:next(Packet_Id, State)}.

handle_conack_properties('5.0', #connection_state{properties = Props} = State, Properties) ->
	NewState =
	case proplists:get_value(?Topic_Alias_Maximum, Properties, undefined) of
		undefined -> State;
		TAMaximum ->
			ConfProps1 = lists:keystore(?Topic_Alias_Maximum, 1, Props, {?Topic_Alias_Maximum, TAMaximum}),
			State#connection_state{properties = ConfProps1}
	end,
	case proplists:get_value(?Receive_Maximum, Properties, -1) of
		-1 -> NewState#connection_state{receive_max = 65535, send_quota = 65535};
		 0 -> NewState; %% TODO protocol_error;
		 N ->
			ConfProps2 = proplists:delete(?Receive_Maximum, Props),
			NewState#connection_state{
				receive_max = N + 1, send_quota = N + 1, %% it allows server side to treat the number publish proceses
				properties = ConfProps2
			}
	end;
handle_conack_properties(_, State, _) ->
	State.

receive_max_set_handle('5.0', #connection_state{properties = Props} = State) ->
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
