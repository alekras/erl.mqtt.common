%%
%% Copyright (C) 2015-2017 by krasnop@bellsouth.net (Alexei Krasnopolski)
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
%% @copyright 2015-2017 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_socket_stream.


-module(mqtt_socket_stream).

%%
%% Include files
%%
-include("mqtt.hrl").
-include("mqtt_macros.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	process/2,
	is_match/2,
	topic_regexp/1,
	server_publish/2
]).

-import(mqtt_output, [packet/2]).
-import(mqtt_input, [input_parser/1]).

process(State, <<>>) -> 
	State;
process(State, Binary) ->
% Common values:
	Client_Id = (State#connection_state.config)#connect.client_id,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,
	case input_parser(Binary) of

		{connect, undefined, Tail} ->
			lager:alert([{endtype, State#connection_state.end_type}], "Connection packet connot be parsed: ~p~n", [Tail]),
			self() ! disconnect,
			process(State, <<>>);

		{connect, Config, Tail} ->
			%% check credentials 
			Encrypted_password_db = Storage:get(server, {user_id, Config#connect.user_name}),
			Encrypted_password_cli = crypto:hash(md5, Config#connect.password),
			ClientPid = Storage:get(server, {client_id, Config#connect.client_id}),
			if ClientPid =:= undefined -> ok;
				 is_pid(ClientPid) -> try gen_server:cast(ClientPid, disconnect) catch _:_ -> ok end;
				 true -> ok
			end,
			Resp_code =
			if Encrypted_password_db =/= Encrypted_password_cli -> 5;
				 true -> 0
			end,
			Packet_Id = State#connection_state.packet_id,
			SP = if Config#connect.clean_session =:= 0 -> 1; true -> 0 end,
			if Resp_code =:= 0 ->
					New_State = State#connection_state{config = Config, session_present = SP},
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
					Packet = packet(connack, {SP, Resp_code}),
					Transport:send(Socket, Packet),
					lager:info([{endtype, State#connection_state.end_type}], "Connection to client ~p is established~n", [New_Client_Id]),
					process(New_State_2#connection_state{packet_id = mqtt_connection:next(Packet_Id, New_State_2), connected = 1}, Tail);
				true ->
					Packet = packet(connack, {SP, Resp_code}),
					Transport:send(Socket, Packet),
					lager:warning([{endtype, State#connection_state.end_type}], "Connection to client ~p is broken by reason: ~p~n", [Config#connect.client_id, Resp_code]),
					self() ! disconnect,
					process(State, Tail)
			end;

		{connack, SP, CRC, Msg, Tail} ->
			case maps:get(connect, Processes, undefined) of
				{Pid, Ref} ->
					Pid ! {connack, Ref, SP, CRC, Msg},
					{Host, Port} = get_peername(Transport, Socket),
					lager:info([{endtype, client}], "Client ~p is successfuly connected to ~p:~p", [Client_Id, Host, Port]),
					process(
						State#connection_state{processes = maps:remove(connect, Processes), 
																		session_present = SP,
																		connected = 1},
						Tail);
				undefined ->
					process(State, Tail)
			end;

		{pingreq, Tail} ->
			lager:info([{endtype, State#connection_state.end_type}], "Ping received to client ~p~n", [Client_Id]),
			Packet = packet(pingresp, true),
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

		{subscribe, Packet_Id, Subscriptions, Tail} ->
			Return_Codes = [ QoS || {_, QoS} <- Subscriptions],
%% store session subscriptions

			[ begin %% Topic, QoS - new subscriptions
					Storage:save(State#connection_state.end_type,
													#storage_subscription{key = #subs_primary_key{topic = Topic, client_id = Client_Id},
																																				qos = QoS, 
																																				callback = not_defined_yet}
												),
					lager:debug([{endtype, State#connection_state.end_type}], "Retain messages=~p~n", [Storage:get(State#connection_state.end_type, {topic, Topic})]),
					[ begin
							QoS_4_Retain = if Params_QoS > QoS -> QoS; true -> Params_QoS end,
							erlang:spawn(?MODULE, server_publish, [self(), Params#publish{qos = QoS_4_Retain}])
						end || #publish{qos = Params_QoS} = Params <- Storage:get(State#connection_state.end_type, {topic, Topic})]
				end || {Topic, QoS} <- Subscriptions],
			Packet = packet(suback, {Return_Codes, Packet_Id}),
			lager:info([{endtype, State#connection_state.end_type}], "Subscription(s) ~p is completed for client: ~p~n", [Subscriptions, Client_Id]),
			Transport:send(Socket, Packet),
			process(State, Tail);

		{suback, Packet_Id, Return_codes, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Subscriptions} when is_list(Subscriptions) ->
%% store session subscriptions
					[ begin 
							Storage:save(State#connection_state.end_type, #storage_subscription{key = #subs_primary_key{topic = Topic, client_id = Client_Id}, qos = QoS, callback = Callback})
						end || {Topic, QoS, Callback} <- Subscriptions], %% @todo check clean_session flag
					Pid ! {suback, Ref, Return_codes},
					lager:info([{endtype, State#connection_state.end_type}], "Client ~p is subscribed to topics ~p with return codes: ~p~n", [Client_Id, Subscriptions, Return_codes]),
					process(
						State#connection_state{
							processes = maps:remove(Packet_Id, Processes)
						},
						Tail);
				undefined ->
					process(State, Tail)
			end;
		
		{unsubscribe, Packet_Id, Topics, Tail} ->
%% discard session subscriptions
			[ begin 
					Storage:remove(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id})
				end || Topic <- Topics],
			Packet = packet(unsuback, Packet_Id),
			Transport:send(Socket, Packet),
			lager:info([{endtype, State#connection_state.end_type}], "Unsubscription(s) ~p is completed for client: ~p~n", [Topics, Client_Id]),
			process(State, Tail);
		
		{unsuback, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Topics} ->
					Pid ! {unsuback, Ref},
%% discard session subscriptions
					[ begin 
							Storage:remove(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id})
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
		{publish, #publish{qos = QoS, topic = Topic, dup = Dup} = Record, Packet_Id, Tail} ->
%			lager:debug([{endtype, State#connection_state.end_type}], " >>> publish comes PI = ~p, Record = ~p Prosess List = ~p~n", [Packet_Id, Record, State#connection_state.processes]),
			lager:info([{endtype, State#connection_state.end_type}], "Published message for client ~p received [topic ~p:~p]~n", [Client_Id, Topic, QoS]),
			case QoS of
				0 -> 	
					delivery_to_application(State, Record),
					process(State, Tail);
				1 ->
					delivery_to_application(State, Record),
					Packet = if State#connection_state.test_flag =:= skip_send_puback -> <<>>; true -> packet(puback, Packet_Id) end,
					case Transport:send(Socket, Packet) of
						ok -> ok;
						{error, _Reason} -> ok
					end,
					process(State, Tail);
				2 ->
					New_State = 
						case maps:is_key(Packet_Id, Processes) of
							true when Dup =:= 0 -> 
								lager:warning([{endtype, State#connection_state.end_type}], " >>> incoming PI = ~p, already exists Record = ~p Prosess List = ~p~n", [Packet_Id, Record, State#connection_state.processes]),
								State;
							_ ->
								case State#connection_state.end_type of 
										client -> 
											delivery_to_application(State, Record);
										server -> none
								end,
%% store PI after receiving message
								Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
								Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = Record#publish{last_sent = pubrec}}),
								Packet = if State#connection_state.test_flag =:= skip_send_pubrec -> <<>>; true -> packet(pubrec, Packet_Id) end,
								case Transport:send(Socket, Packet) of
									ok -> 
										New_processes = Processes#{Packet_Id => {{undefined, undefined}, #publish{topic = Topic, qos = QoS, last_sent = pubrec}}},
										State#connection_state{processes = New_processes};
									{error, _Reason} -> State
								end
						end,
					process(New_State, Tail);
				_ -> process(State, Tail)
			end;

		?test_fragment_skip_rcv_puback
		{puback, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, _Params} ->
%% discard message after pub ack
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:remove(State#connection_state.end_type, Prim_key),
					Pid ! {puback, Ref},
					process(
						State#connection_state{processes = maps:remove(Packet_Id, Processes)},
						Tail);
				undefined ->
					process(State, Tail)
			end;

		?test_fragment_skip_rcv_pubrec
%%		?test_fragment_skip_send_pubrel
		{pubrec, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{From, Params} ->
%% store message before pubrel
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = #publish{last_sent = pubrel}}),
					Packet = if State#connection_state.test_flag =:= skip_send_pubrel -> <<>>; true -> packet(pubrel, Packet_Id) end,
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
		{pubrel, Packet_Id, Tail} ->
%			lager:debug([{endtype, State#connection_state.end_type}], " >>> pubrel arrived PI: ~p	~p.~n", [Packet_Id, Processes]),
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
					Packet = if State#connection_state.test_flag =:= skip_send_pubcomp -> <<>>; true -> packet(pubcomp, Packet_Id) end,
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
		{pubcomp, Packet_Id, Tail} ->
%			lager:debug([{endtype, State#connection_state.end_type}], " >>> pubcomp arrived PI: ~p. Processes-~p~n", [Packet_Id, Processes]),
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, _Params} ->
					case Pid of
						undefined -> none;
						_ -> 	Pid ! {pubcomp, Ref}
					end,
%% discard message after pub comp
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:remove(State#connection_state.end_type, Prim_key),
					process(State#connection_state{processes = maps:remove(Packet_Id, Processes)}, Tail);
				undefined ->
					process(State, Tail)
			end;

		{disconnect, Tail} ->
			Storage:remove(State#connection_state.end_type, {client_id, Client_Id}),
			self() ! disconnect, %% @todo stop the process, close the socket !!!
			lager:info([{endtype, State#connection_state.end_type}], "Client ~p disconnected~n", [Client_Id]),
			process(State#connection_state{connected = 0}, Tail);

		_ ->
			lager:error([{endtype, State#connection_state.end_type}], "unparsed message: ~p state:~p~n", [Binary, State]),
			self() ! disconnect,
			process(State, <<>>)
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

get_topic_attributes(#connection_state{storage = Storage} = State, Topic) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
	Topic_List = Storage:get_matched_topics(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id}),
	[{QoS, Callback} || {_TopicFilter, QoS, Callback} <- Topic_List].

delivery_to_application(#connection_state{end_type = client, default_callback = Default_Callback} = State,
												#publish{topic = Topic, qos = QoS, dup = Dup, retain = Retain, payload = Payload}) ->
	case get_topic_attributes(State, Topic) of
		[] -> do_callback(Default_Callback, [{{Topic, undefined}, QoS, Dup, Retain, Payload}]);
		List ->
			[
				case do_callback(Callback, [{{Topic, TopicQoS}, QoS, Dup, Retain, Payload}]) of
					false -> do_callback(Default_Callback, [{{Topic, TopicQoS}, QoS, Dup, Retain, Payload}]);
					_ -> ok
				end
				|| {TopicQoS, Callback} <- List
			]
	end,
	lager:info([{endtype, State#connection_state.end_type}], 
						 "Published message for client ~p delivered [topic ~p:~p, dup=~p, retain=~p]~n", 
						 [(State#connection_state.config)#connect.client_id, Topic, QoS, Dup, Retain]);

delivery_to_application(#connection_state{end_type = server, storage = Storage} = State, 
												#publish{topic = Params_Topic, qos = Params_QoS, payload = Payload, retain = Retain, dup = Dup} = Params) ->
	if (Retain =:= 1) and (Payload =:= <<>>) ->
				Storage:remove(server, {topic, Params_Topic});
			(Retain =:= 1) and (Params_QoS =:= 0) ->
				Storage:remove(server, {topic, Params_Topic}),
				Storage:save(server, Params);
			(Retain =:= 1) ->
				Storage:save(server, Params);
			true -> ok
	end,

	case Storage:get_matched_topics(server, Params_Topic) of
		[] when Retain =:= 1 -> ok;
		[] ->
			lager:notice([{endtype, server}], "There is no the topic in DB. Publish came: Topic=~p QoS=~p Payload=~p~n", [Params_Topic, Params_QoS, Payload]);
		List ->
			lager:debug([{endtype, server}], "Topic list=~128p~n", [List]),
			[
				case Storage:get(server, {client_id, Client_Id}) of
					undefined -> 
						lager:debug([{endtype, server}], "Cannot find connection PID for client id=~p~n", [Client_Id]);
					Pid ->
						QoS = if Params_QoS > TopicQoS -> TopicQoS; true -> Params_QoS end,
						erlang:spawn(?MODULE, server_publish, [Pid, Params#publish{qos = QoS, retain = 0}])
				end
				|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, qos = TopicQoS} <- List
			]
	end,
	lager:info([{endtype, State#connection_state.end_type}], 
						 "Published message for client ~p delivered [topic ~p:~p, dup=~p, retain=~p]~n", 
						 [(State#connection_state.config)#connect.client_id, Params_Topic, Params_QoS, Dup, Retain]).

do_callback(Callback, Args) ->
	case Callback of
		{M, F} -> spawn(M, F, Args);
		F when is_function(F) -> spawn(fun() -> apply(F, Args) end);
		_ -> false
	end.

server_publish(Pid, Params) -> 
	lager:debug([{endtype, server}], "Pid=~p Params=~128p~n", [Pid, Params]),
	R =
	case gen_server:call(Pid, {publish, Params}, ?MQTT_GEN_SERVER_TIMEOUT) of
		{ok, Ref} -> 
			case Params#publish.qos of
				0 -> ok;
				1 ->
					receive
						{puback, Ref} -> 
							ok
					after ?MQTT_GEN_SERVER_TIMEOUT ->
						#mqtt_client_error{type = publish, source = "mqtt_connection:server_publish/2", message = "puback timeout"}
					end;
				2 ->
					receive
						{pubcomp, Ref} -> 
							ok
					after ?MQTT_GEN_SERVER_TIMEOUT ->
						#mqtt_client_error{type = publish, source = "mqtt_connection:server_publish/2", message = "pubcomp timeout"}
					end
			end;
		{error, Reason} ->
				#mqtt_client_error{type = publish, source = "mqtt_connection:server_publish/2", message = Reason}
	end,
	case R of
		ok -> ok;
		_	-> lager:error([{endtype, server}], "~128p~n", [R])
	end.

is_match(Topic, TopicFilter) ->
	{ok, Pattern} = re:compile(topic_regexp(TopicFilter)),
	case re:run(Topic, Pattern, [global, {capture, [1], list}]) of
		{match, _R} -> true;
		_E ->		false
	end.

topic_regexp(TopicFilter) ->
	R1 = re:replace(TopicFilter, "\\+", "([^/]*)", [global, {return, list}]),
	R2 = re:replace(R1, "#", "(.*)", [global, {return, list}]),
	"^" ++ R2 ++ "$".

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
	end.
