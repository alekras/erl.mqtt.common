%%
%% Copyright (C) 2015-2020 by krasnop@bellsouth.net (Alexei Krasnopolski)
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
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_socket_stream.


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
	process/2,
	server_send_publish/2
]).

-import(mqtt_output, [packet/4]).
-import(mqtt_input, [input_parser/2]).

process(State, <<>>) -> 
	State;
process(State, Binary) ->
% Common values:
	Client_Id = (State#connection_state.config)#connect.client_id,
	Version = State#connection_state.config#connect.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,
	case input_parser(Version, Binary) of

		{connect, undefined, Tail} ->
			lager:alert([{endtype, State#connection_state.end_type}], "Connection packet cannot be parsed: ~p~n", [Tail]),
			self() ! disconnect,
			process(State, <<>>);

		{connect, Config, Tail} ->
			%% check credentials 
			Encrypted_password_db = Storage:get(server, {user_id, Config#connect.user_name}),
			Encrypted_password_cli = crypto:hash(md5, Config#connect.password),
			ClientPid = Storage:get(server, {client_id, Config#connect.client_id}),
			lager:debug([{endtype, server}], "Client PID = ~p~n", [ClientPid]),
			ConnVersion = Config#connect.version,
			if ClientPid =:= undefined -> ok;
				 is_pid(ClientPid) -> try gen_server:cast(ClientPid, {disconnect, 16#8e, [{?Reason_String, "Session taken over"}]}) catch _:_ -> ok end; %% @todo maybe just ClientPid ! disconnect ?
				 true -> ok
			end,
			Resp_code =
			if Encrypted_password_db =/= Encrypted_password_cli -> 5;
				 true -> 0
			end,
			Packet_Id = State#connection_state.packet_id,
			SP = if Config#connect.clean_session =:= 0 -> 1; true -> 0 end,
			if Resp_code =:= 0 ->
					New_State = State#connection_state{config = Config, session_present = SP, topic_alias_in_map = #{}, topic_alias_out_map = #{}},
					New_State_2 =
					case Config#connect.clean_session of
						1 -> 
							Storage:cleanup(State#connection_state.end_type, Config#connect.client_id),
							New_State;
						0 ->	 
							mqtt_connection:restore_session(New_State) 
					end,
					New_Client_Id = Config#connect.client_id,
					Storage:save(State#connection_state.end_type, #storage_connectpid{client_id = New_Client_Id, pid = self()}),
					Packet = packet(connack, ConnVersion, {SP, Resp_code}, Config#connect.properties), %% now just return connect properties @todo
					Transport:send(Socket, Packet),
					lager:info([{endtype, State#connection_state.end_type}], "Connection to client ~p is established~n", [New_Client_Id]),
					process(New_State_2#connection_state{packet_id = mqtt_connection:next(Packet_Id, New_State_2), connected = 1}, Tail);
				true ->
					Packet = packet(connack, ConnVersion, {SP, Resp_code}, []),
					Transport:send(Socket, Packet),
					lager:warning([{endtype, State#connection_state.end_type}], "Connection to client ~p is broken by reason: ~p~n", [Config#connect.client_id, Resp_code]),
					self() ! disconnect,
					process(State, Tail)
			end;

		{connack, SP, CRC, Msg, Properties, Tail} ->
			case maps:get(connect, Processes, undefined) of
				{Pid, Ref} ->
					Pid ! {connack, Ref, SP, CRC, Msg, Properties},
					{Host, Port} = get_peername(Transport, Socket),
					lager:debug([{endtype, client}], "SessionPresent=~p, CRC=~p, Msg=~p, Properties=~128p", [SP, CRC, Msg, Properties]),
					IsConnected =
					if CRC == 0 -> %% TODO process all codes for v5.0
							lager:info([{endtype, client}], "Client ~p is successfuly connected to ~p:~p, version=~p", [Client_Id, Host, Port, Version]),
							1;
						true ->
							lager:info([{endtype, client}], "Client ~p is disconnected to ~p:~p, version=~p, reason=~p", [Client_Id, Host, Port, Version, Msg]),
							0
					end,
					NewState = handle_conack_properties(Version, State, Properties),
					process(
						NewState#connection_state{processes = maps:remove(connect, Processes), 
																		session_present = SP,
																		connected = IsConnected},
						Tail);
				undefined ->
					process(State, Tail)
			end;

		{pingreq, Tail} ->
			lager:info([{endtype, State#connection_state.end_type}], "Ping received from client ~p~n", [Client_Id]),
			Packet = packet(pingresp, Version, undefined, []),
			Transport:send(Socket, Packet),
			process(State, Tail);

		{pingresp, Tail} -> 
			lager:info([{endtype, State#connection_state.end_type}], "Pong received to client ~p~n", [Client_Id]),
			case maps:get(pingreq, Processes, undefined) of
				{M, F} ->
					spawn(M, F, [pong]);
				F when is_function(F)->
					spawn(fun() -> apply(F, [pong]) end);
				_ -> true
			end,
			process(
				State#connection_state{processes = maps:remove(pingreq, Processes), 
																ping_count = State#connection_state.ping_count - 1},
				Tail);
%% Server side::
		{subscribe, Packet_Id, Subscriptions, Properties, Tail} ->
%% store session subscriptions
			Return_Codes = 
			[ begin %% Topic, QoS - new subscriptions
					Sub_Options = 
						if Version == '5.0' -> Options;
							 true ->
								#subscription_options{max_qos = Options}
						end,
					{ShareName, TopicFilter} =
					case mqtt_data:is_topicFilter_valid(Topic) of
						{true, [SN, TF]} -> {if SN == "" -> undefined; true -> SN end, TF};
						false -> {undefined, ""}		%% @todo process the error!
			 		end,
					Key = #subs_primary_key{topicFilter = TopicFilter, shareName = ShareName, client_id = Client_Id},
					handle_retain_msg_after_subscribe(Version, State, Sub_Options, Key),
					Storage:save(State#connection_state.end_type,
													#storage_subscription{key = Key,
																								options = Sub_Options, 
																								callback = not_defined_yet}
											),
					Sub_Options#subscription_options.max_qos
				end || {Topic, Options} <- Subscriptions],
			Packet = packet(suback, Version, {Return_Codes, Packet_Id}, Properties), %% @todo now just return subscribe properties @todo generate return codes
			lager:info([{endtype, State#connection_state.end_type}], "Subscription(s) ~p is completed for client: ~p~n", [Subscriptions, Client_Id]),
			Transport:send(Socket, Packet),
			process(State, Tail);
%% Client side::
		{suback, Packet_Id, Return_codes, Properties, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Subscriptions} when is_list(Subscriptions) ->
%% store session subscriptions
					[ begin 
							Storage:save(State#connection_state.end_type, #storage_subscription{key = #subs_primary_key{topicFilter = Topic, client_id = Client_Id}, options = Options, callback = Callback})
						end || {Topic, Options, Callback} <- Subscriptions], %% @todo check clean_session flag
					Pid ! {suback, Ref, Return_codes, Properties},
					lager:info([{endtype, State#connection_state.end_type}], "Client ~p is subscribed to topics ~p with return codes: ~p~n", [Client_Id, Subscriptions, Return_codes]),
					process(
						State#connection_state{
							processes = maps:remove(Packet_Id, Processes)
						},
						Tail);
				undefined ->
					process(State, Tail)
			end;
		
		{unsubscribe, Packet_Id, Topics, Properties, Tail} ->
%% discard session subscriptions
			ReasonCodeList =
			[ begin 
					Storage:remove(State#connection_state.end_type, #subs_primary_key{topicFilter = Topic, client_id = Client_Id}),
					0 %% @todo add reason code list
				end || Topic <- Topics],
			Packet = packet(unsuback, Version, {ReasonCodeList, Packet_Id}, Properties),
			Transport:send(Socket, Packet),
			lager:info([{endtype, State#connection_state.end_type}], "Unsubscription(s) ~p is completed for client: ~p~n", [Topics, Client_Id]),
			process(State, Tail);
		
		{unsuback, {Packet_Id, ReturnCodes}, Properties, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Topics} ->
					Pid ! {unsuback, Ref, ReturnCodes, Properties},
%% discard session subscriptions
					[ begin 
							Storage:remove(State#connection_state.end_type, #subs_primary_key{topicFilter = Topic, client_id = Client_Id})
						end || Topic <- Topics], %% @todo check clean_session flag
					lager:info([{endtype, State#connection_state.end_type}], "Client ~p is unsubscribed from topics ~p~n", [Client_Id, Topics]),
					process(
						State#connection_state{
							processes = maps:remove(Packet_Id, Processes)
						}, 
						Tail);
				undefined ->
					process(State, Tail)
			end;
		?test_fragment_skip_rcv_publish
		{publish, #publish{qos = QoS, topic = Topic, dup = Dup, properties = Props} = Record, Packet_Id, Tail} ->
			lager:debug([{endtype, State#connection_state.end_type}], " >>> publish comes PI = ~p, Record = ~p Prosess List = ~p~n", [Packet_Id, Record, State#connection_state.processes]),
			lager:info([{endtype, State#connection_state.end_type}], "Published message for client ~p received [topic ~p:~p]~n", [Client_Id, Topic, QoS]),
			case mqtt_connection:topic_alias_handle(Version, Record, State) of
				{#mqtt_client_error{errno = ErrNo, message = Msg}, NewState} ->
					gen_server:cast(self(), {disconnect, ErrNo, [{?Reason_String, Msg}]}),
					process(NewState, Tail);
				{NewRecord, NewState} -> 
lager:debug([{endtype, State#connection_state.end_type}], " >>> NewRecord = ~p NewState = ~p~n", [NewRecord, NewState]),
			case QoS of
				0 -> 	
					delivery_to_application(NewState, NewRecord),
					process(NewState, Tail);
				1 ->
					delivery_to_application(NewState, NewRecord),  %% TODO check for successful delivery
					Packet = if NewState#connection_state.test_flag =:= skip_send_puback -> <<>>; true -> packet(puback, Version, {Packet_Id, 0}, []) end, %% TODO properties?
					case Transport:send(Socket, Packet) of
						ok -> ok;
						{error, _Reason} -> ok
					end,
					process(NewState, Tail);
				2 ->
					NewState1 = 
						case maps:is_key(Packet_Id, Processes) of
							true when Dup =:= 0 -> 
								lager:warning([{endtype, NewState#connection_state.end_type}], " >>> incoming PI = ~p, already exists Record = ~p Prosess List = ~p~n", [Packet_Id, NewRecord, NewState#connection_state.processes]),
								NewState;
							_ ->
								case NewState#connection_state.end_type of 
									client -> 
										delivery_to_application(NewState, NewRecord);
									server -> none
								end,
%% store PI after receiving message
								Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
								Storage:save(NewState#connection_state.end_type, #storage_publish{key = Prim_key, document = NewRecord#publish{last_sent = pubrec}}),
								Packet = 
								if NewState#connection_state.test_flag =:= skip_send_pubrec -> <<>>;
									true -> packet(pubrec, Version, {Packet_Id, 0}, []) %% TODO fill out properties with ReasonString Or/And UserProperty 
								end,
								case Transport:send(Socket, Packet) of
									ok -> 
										New_processes = Processes#{Packet_Id => {{undefined, undefined}, #publish{topic = Topic, qos = QoS, last_sent = pubrec}}},
										NewState#connection_state{processes = New_processes};
									{error, _Reason} -> NewState
								end
						end,
					process(NewState1, Tail);
				_ -> process(State, Tail)
			end
			end;

		?test_fragment_skip_rcv_puback
		{puback, {Packet_Id, ReasonCode}, Properties, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, _Params} ->
%% discard message after pub ack
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:remove(State#connection_state.end_type, Prim_key),
					Pid ! {puback, Ref, ReasonCode, Properties},
					process(
						State#connection_state{processes = maps:remove(Packet_Id, Processes)},
						Tail);
				undefined ->
					process(State, Tail)
			end;

		?test_fragment_skip_rcv_pubrec
%%		?test_fragment_skip_send_pubrel
		{pubrec, {Packet_Id, ResponseCode}, _Properties, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{From, Params} ->
%% store message before pubrel
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = #publish{last_sent = pubrel}}),
					Packet =
					if State#connection_state.test_flag =:= skip_send_pubrel -> <<>>;
							true -> packet(pubrel, Version, {Packet_Id, ResponseCode}, [])  %% TODO fill out properties with ReasonString Or/And UserProperty 
					end,
					New_State =
					case Transport:send(Socket, Packet) of
						ok -> 
							New_processes = Processes#{Packet_Id => {From, Params#publish{last_sent = pubrel}}},
							State#connection_state{processes = New_processes}; 
						{error, _Reason} -> State
					end,
					process(New_State, Tail);
				undefined ->
					process(State, Tail)
			end;
		?test_fragment_skip_rcv_pubrel
		{pubrel, {Packet_Id, _ReasonCode}, Properties, Tail} ->
			lager:debug([{endtype, State#connection_state.end_type}], " >>> pubrel arrived PI: ~p	~p reason Code=~p, Props=~p~n", [Packet_Id, Processes, _ReasonCode, Properties]),
			case maps:get(Packet_Id, Processes, undefined) of
				{_From, _Params} ->
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					if
						State#connection_state.end_type =:= server ->
							case Storage:get(State#connection_state.end_type, Prim_key) of
								#storage_publish{document = Record} ->
									delivery_to_application(State, Record);
								_ -> none
							end;
						true -> none
					end,
%% discard PI before pubcomp send
					Storage:remove(State#connection_state.end_type, Prim_key),
					Packet =
					if State#connection_state.test_flag =:= skip_send_pubcomp -> <<>>; 
							true -> packet(pubcomp, Version, {Packet_Id, 0}, []) %% TODO fill out properties with ReasonString or/and UserProperty
					end,
					New_State =
					case Transport:send(Socket, Packet) of
						ok ->
							New_processes = maps:remove(Packet_Id, Processes),
							State#connection_state{processes = New_processes};
						{error, _Reason} -> State
					end,
					process(New_State, Tail);
				undefined ->
					process(State, Tail)
			end;
		?test_fragment_skip_rcv_pubcomp
		{pubcomp, {Packet_Id, ReasonCode}, Properties, Tail} ->
%			lager:debug([{endtype, State#connection_state.end_type}], " >>> pubcomp arrived PI: ~p. Processes-~p~n", [Packet_Id, Processes]),
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, _Params} ->
					case Pid of
						undefined -> none;
						_ -> 	Pid ! {pubcomp, Ref, ReasonCode, Properties}
					end,
%% discard message after pub comp
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:remove(State#connection_state.end_type, Prim_key),
					process(State#connection_state{processes = maps:remove(Packet_Id, Processes)}, Tail);
				undefined ->
					process(State, Tail)
			end;

		{disconnect, DisconnectReasonCode, Properties, Tail} ->
%%			Storage:remove(State#connection_state.end_type, {client_id, Client_Id}),
			self() ! disconnect, %% TODO stop the process, close the socket !!!
			lager:info([{endtype, State#connection_state.end_type}], "Client ~p disconnected with reason ~p and Props=~p~n", [Client_Id, DisconnectReasonCode, Properties]),
			%% TODO on client side: callback with reason and properties
			if State#connection_state.end_type =:= client ->
					do_callback(State#connection_state.default_callback, [{DisconnectReasonCode, Properties}]);
				true -> ok
			end,
			process(State#connection_state{connected = 0}, Tail);

		_ ->
			lager:error([{endtype, State#connection_state.end_type}], "unparsed message: ~p state:~p~n", [Binary, State]),
			self() ! disconnect,
			process(State, <<>>)
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

handle_conack_properties('5.0', #connection_state{config = Config} = State, Properties) ->
	case proplists:get_value(?Topic_Alias_Maximum, Properties, undefined) of
		undefined -> State;
		TAMaximum ->
			ConfProps = lists:keystore(?Topic_Alias_Maximum, 1, Config#connect.properties, {?Topic_Alias_Maximum, TAMaximum}),
			State#connection_state{config = Config#connect{properties = ConfProps}}
	end;
handle_conack_properties(_, State, _) ->
	State.

get_topic_attributes(#connection_state{storage = Storage} = State, Topic) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
	Topic_List = Storage:get_matched_topics(client, #subs_primary_key{topicFilter = Topic, client_id = Client_Id}),
	[{Options, Callback} || #storage_subscription{options = Options, callback = Callback} <- Topic_List].

delivery_to_application(#connection_state{end_type = client, default_callback = Default_Callback} = State,
												#publish{qos = QoS, dup = Dup, retain = Retain} = PubRecord) ->
	Topic = handle_get_topic_from_alias(State#connection_state.config#connect.version, PubRecord, State),
%%	NewPubRecord = PubRecord#publish{topic = Topic},
	case get_topic_attributes(State, Topic) of
		[] -> do_callback(Default_Callback, [{undefined, PubRecord}]);
		List ->
			[
				case do_callback(Callback, [{SubsOption, PubRecord}]) of
					false -> do_callback(Default_Callback, [{SubsOption, PubRecord}]);
					_ -> ok
				end
				|| {SubsOption, Callback} <- List
			]
	end,
	lager:info([{endtype, State#connection_state.end_type}], 
						 "Published message for client ~p delivered [topic ~p:~p, dup=~p, retain=~p]~n", 
						 [(State#connection_state.config)#connect.client_id, Topic, QoS, Dup, Retain]);

delivery_to_application(#connection_state{end_type = server, storage = Storage} = State, 
												#publish{payload = <<>>, retain = 1} = PubParam) ->
	PublishTopic = handle_get_topic_from_alias(State#connection_state.config#connect.version, PubParam, State),
	Storage:remove(server, {topic, PublishTopic});
delivery_to_application(#connection_state{end_type = server} = State, 
												#publish{} = PubParam) ->
	PublishTopic = handle_get_topic_from_alias(State#connection_state.config#connect.version, PubParam, State),
	handle_retain_msg_during_publish(State#connection_state.config#connect.version, State, PubParam, PublishTopic),
	handle_server_publish(State#connection_state.config#connect.version, State, PubParam, PublishTopic).

do_callback(Callback, Args) ->
	case Callback of
		{M, F} -> spawn(M, F, Args);
		F when is_function(F) -> spawn(fun() -> apply(F, Args) end);
		_ -> false
	end.

server_send_publish(Pid, Params) -> 
	lager:debug([{endtype, server}], "Pid=~p Params=~128p~n", [Pid, Params]),
%% TODO process look up topic alias for the Pid client/session and update #publish record
	R =
	case gen_server:call(Pid, {publish, Params#publish{dir=out}}, ?MQTT_GEN_SERVER_TIMEOUT) of
		{ok, Ref} -> 
			case Params#publish.qos of
				0 -> ok;
				1 ->
					receive
						{puback, Ref, _ReasonCode, _Properties} ->  %% TODO just get properties for now
lager:debug([{endtype, server}], "Received puback. Reason code=~p, props=~128p~n", [_ReasonCode, _Properties]),
							ok
					after ?MQTT_GEN_SERVER_TIMEOUT ->
						#mqtt_client_error{type = publish, source = "mqtt_connection:server_send_publish/2", message = "puback timeout"}
					end;
				2 ->
					receive
						{pubcomp, Ref, _ReasonCode,_Properties} -> 
lager:debug([{endtype, server}], "Received pubcomp. Reason code=~p, props=~128p~n", [_ReasonCode, _Properties]),
							ok
					after ?MQTT_GEN_SERVER_TIMEOUT ->
						#mqtt_client_error{type = publish, source = "mqtt_connection:server_send_publish/2", message = "pubcomp timeout"}
					end
			end;
		{error, Reason} ->
				#mqtt_client_error{type = publish, source = "mqtt_connection:server_send_publish/2", message = Reason}
	end,
	case R of
		ok -> lager:debug([{endtype, server}], "Server successfuly have published message to subscriber.~n", []);
		_	-> lager:error([{endtype, server}], "~128p~n", [R])
	end.

handle_retain_msg_after_subscribe('5.0', _State, #subscription_options{retain_handling = 2} = _Options, _Key) ->
	ok;
handle_retain_msg_after_subscribe('5.0', #connection_state{storage = Storage} = State, 
																	Options, 
																	#subs_primary_key{topicFilter = TopicFilter, shareName = undefined} = Key) ->
	Retain_Messages = Storage:get(State#connection_state.end_type, {topic, TopicFilter}),
	lager:debug([{endtype, State#connection_state.end_type}], "Retain messages=~p~n", [Retain_Messages]),
	QoS = Options#subscription_options.max_qos,
	Exist = Storage:get(State#connection_state.end_type, Key),
	Retain_handling = Options#subscription_options.retain_handling,
	if (Retain_handling == 0) or ((Retain_handling == 1) and (Exist == undefined)) ->
			Retain_as_published = Options#subscription_options.retain_as_published,
			[ begin
					QoS_4_Retain = if Params_QoS > QoS -> QoS; true -> Params_QoS end,
					Retain = if Retain_as_published == 0 -> 0; true -> Params#publish.retain end,
					erlang:spawn(?MODULE, 
												server_send_publish, 
												[self(), 
												Params#publish{qos = QoS_4_Retain, retain = Retain}])
				end || #publish{qos = Params_QoS} = Params <- Retain_Messages];
		 true -> ok
	end;
handle_retain_msg_after_subscribe('5.0', _State, _Options, _Key) ->
	ok;
handle_retain_msg_after_subscribe(_, #connection_state{storage = Storage} = State, 
																	Options,
																	#subs_primary_key{topicFilter = TopicFilter} = _Key) ->
	Retain_Messages = Storage:get(State#connection_state.end_type, {topic, TopicFilter}),
	lager:debug([{endtype, State#connection_state.end_type}], "Retain messages=~p~n", [Retain_Messages]),
	QoS = Options#subscription_options.max_qos,
	[ begin
			QoS_4_Retain = if Params_QoS > QoS -> QoS; true -> Params_QoS end,
			erlang:spawn(?MODULE, server_send_publish, [self(), Params#publish{qos = QoS_4_Retain}])
		end || #publish{qos = Params_QoS} = Params <- Retain_Messages].

handle_retain_msg_during_publish('5.0',
																	#connection_state{storage = Storage} = _State, 
																	#publish{qos = Params_QoS, payload = _Payload, retain = Retain, dup = _Dup} = Param, Params_Topic) ->
	if (Retain =:= 1) and (Params_QoS =:= 0) ->
				Storage:remove(server, {topic, Params_Topic}),
				Storage:save(server, Param);
			(Retain =:= 1) ->
				Storage:save(server, Param);
			true -> ok
	end;
handle_retain_msg_during_publish(_,
																	#connection_state{storage = Storage} = _State,
																	#publish{qos = Params_QoS, retain = Retain} = Param, Params_Topic) ->
	if (Retain =:= 1) and (Params_QoS =:= 0) ->
				Storage:remove(server, {topic, Params_Topic}),
				Storage:save(server, Param);
			(Retain =:= 1) ->
				Storage:save(server, Param); %% It is protocol extension: we have to remove all previously retained messages by MQTT protocol.
			true -> ok
	end.

handle_get_topic_from_alias('5.0', #publish{topic = Prms_Topic} = PubParam, State) ->
	if Prms_Topic =:= "" ->
				TopicAlias = proplists:get_value(?Topic_Alias, PubParam#publish.properties, 0),
				if TopicAlias == 0 -> error;
					 true ->
							maps:get(TopicAlias, State#connection_state.topic_alias_in_map, error)
				end;
		 true -> Prms_Topic
	end;
handle_get_topic_from_alias(_, #publish{topic = Prms_Topic}, _) ->
	Prms_Topic.

handle_server_publish('5.0',
												#connection_state{storage = Storage} = State,
												#publish{qos = Params_QoS, payload = Payload, retain = Retain, dup = Dup} = Param, PubTopic) ->
	case Storage:get_matched_topics(server, PubTopic) of
		[] when Retain =:= 1 -> ok;
		[] ->
			lager:notice([{endtype, server}], "There is no the topic in DB. Publish came: Topic=~p QoS=~p Payload=~p~n", [PubTopic, Params_QoS, Payload]);
		List ->
			lager:debug([{endtype, server}], "Topic list=~128p~n", [List]),
			%% TODO if Client_Id topic matches multi subscriptions then create one message with multiple subscription identifier.
			[
				case Storage:get(server, {client_id, Client_Id}) of
					undefined -> 
						lager:debug([{endtype, server}], "Cannot find connection PID for client id=~p~n", [Client_Id]);
					Pid ->
						NoLocal = Options#subscription_options.nolocal,
						ProcessCliD = State#connection_state.config#connect.client_id,
						if (NoLocal =:= 1) and (ProcessCliD =:= Client_Id) -> ok;
							 true ->
								TopicQoS = Options#subscription_options.max_qos,
								QoS = if Params_QoS > TopicQoS -> TopicQoS; true -> Params_QoS end,
								erlang:spawn(?MODULE, server_send_publish, [Pid, Param#publish{qos = QoS, retain = 0}])
						end
				end
				|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, options = Options} <- List
			]
	end,
	%%handle shared subscriptions :
	case Storage:get_matched_shared_topics(server, PubTopic) of
		[] -> ok;
		ShSubsList -> 
			F = fun(Subs, ShNamesMap) ->
						ShareName = Subs#storage_subscription.key#subs_primary_key.shareName,
						GroupList =
						try
							maps:get(ShareName, ShNamesMap)
						catch
							error:{badkey, _} -> []
						end,
						maps:put(ShareName, [Subs | GroupList], ShNamesMap)
					end,
			ShNamesMap = lists:foldl(F, #{}, ShSubsList),
			[ begin
					GroupSize = length(GroupList),
					N = rand:uniform(GroupSize),
					lager:debug([{endtype, server}], "Shared subscription:: GroupList=~p~n     Random N=~p~n", [GroupList, N]),
					#storage_subscription{key = #subs_primary_key{client_id = CliId}, options = Opts} = lists:nth(N, GroupList),
					case Storage:get(server, {client_id, CliId}) of
						undefined ->
							lager:debug([{endtype, server}], "Cannot find connection PID for client id=~p~n", [CliId]);
						Pid ->
							ShTopicQoS = Opts#subscription_options.max_qos,
							QoS = if Params_QoS > ShTopicQoS -> ShTopicQoS; true -> Params_QoS end,
							erlang:spawn(?MODULE, server_send_publish, [Pid, Param#publish{qos = QoS, retain = 0}])
					end
				end
				|| {_ShareName, GroupList} <- maps:to_list(ShNamesMap)]

	end,
	lager:info([{endtype, server}], 
						 "Published message for client ~p delivered [topic ~p:~p, dup=~p, retain=~p]~n", 
						 [(State#connection_state.config)#connect.client_id, PubTopic, Params_QoS, Dup, Retain]);
handle_server_publish(_,
												#connection_state{storage = Storage} = State,
												#publish{qos = Params_QoS, payload = Payload, retain = Retain, dup = Dup} = Param, PubTopic) ->
	case Storage:get_matched_topics(server, PubTopic) of
		[] when Retain =:= 1 -> ok;
		[] ->
			lager:notice([{endtype, server}], "There is no the topic in DB. Publish came: Topic=~p QoS=~p Payload=~p~n", [PubTopic, Params_QoS, Payload]);
		List ->
			lager:debug([{endtype, server}], "Topic list=~128p~n", [List]),
			[
				case Storage:get(server, {client_id, Client_Id}) of
					undefined -> 
						lager:debug([{endtype, server}], "Cannot find connection PID for client id=~p~n", [Client_Id]);
					Pid ->
						TopicQoS = Options#subscription_options.max_qos,
						QoS = if Params_QoS > TopicQoS -> TopicQoS; true -> Params_QoS end,
						erlang:spawn(?MODULE, server_send_publish, [Pid, Param#publish{qos = QoS, retain = 0}])
				end
				|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, options = Options} <- List
			]
	end,
	lager:info([{endtype, server}], 
						 "Published message for client ~p delivered [topic ~p:~p, dup=~p, retain=~p]~n", 
						 [(State#connection_state.config)#connect.client_id, PubTopic, Params_QoS, Dup, Retain]).

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
get_peername(mqtt_ws_handler, Socket) ->
	case mqtt_ws_handler:peername(Socket) of
		{ok, {Host, Port}} -> {Host, Port};
		_ -> {undefined, ""}
	end;
get_peername(_, _Socket) ->
	{"test mock host", "0"}.
