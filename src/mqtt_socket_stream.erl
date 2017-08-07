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

		{connect, Config, Tail} ->
			lager:debug([{endtype, State#connection_state.end_type}], "connect: ~p~n", [Config]),
			%% check credentials 
			Encrypted_password_db = Storage:get(server, {user_id, Config#connect.user_name}),
			Encrypted_password_cli = crypto:hash(md5, Config#connect.password),
			ClientPid = Storage:get(server, {client_id, Config#connect.client_id}),
%			lager:debug([{endtype, State#connection_state.end_type}], "Client pid: ~p~n", [ClientPid]),
			if ClientPid =:= undefined -> ok;
				 is_pid(ClientPid) -> try gen_server:call(ClientPid, disconnect) catch _:_ -> ok end;
				 true -> ok
			end,
			Resp_code =
			if Encrypted_password_db =/= Encrypted_password_cli -> 5;
				 true -> 0
			end,
			Packet_Id = State#connection_state.packet_id,
			SP = 
			if Config#connect.clean_session =:= 0 -> 1; true -> 0 end, %% @todo check session in DB
			Packet = packet(connack, {SP, Resp_code}),
			Transport:send(Socket, Packet),
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
					process(New_State_2#connection_state{packet_id = mqtt_connection:next(Packet_Id, New_State_2)}, Tail);
				true ->
					Transport:close(Socket),
					process(State, Tail)
			end;

		{connack, SP, CRC, Msg, Tail} ->
			case maps:get(connect, Processes, undefined) of
				{Pid, Ref} ->
					Pid ! {connack, Ref, SP, CRC, Msg},
					process(
						State#connection_state{processes = maps:remove(connect, Processes), 
																		session_present = SP},
						Tail);
				undefined ->
					process(State, Tail)
			end;

		{pingreq, Tail} ->
			%% @todo keep-alive concern.
			Packet = packet(pingresp, true),
			Transport:send(Socket, Packet),
			process(State, Tail);

		{pingresp, Tail} -> 
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
			lager:debug([{endtype, State#connection_state.end_type}], "store session subscriptions:~p from client:~p~n", [Subscriptions, Client_Id]),
			[ begin 
					lager:debug([{endtype, State#connection_state.end_type}], "save subscribtion: Topic=~p QoS=~p~n", [Topic, QoS]),
					Storage:save(State#connection_state.end_type, #storage_subscription{key = #subs_primary_key{topic = Topic, client_id = Client_Id}, qos = QoS, callback = not_defined_yet})
				end || {Topic, QoS} <- Subscriptions],
			Packet = packet(suback, {Return_Codes, Packet_Id}),
			Transport:send(Socket, Packet),
			lager:debug([{endtype, State#connection_state.end_type}], "subscribe completed: packet:~p~n", [Packet]),
			process(State, Tail);

		{suback, Packet_Id, Return_codes, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Subscriptions} when is_list(Subscriptions) ->
%% store session subscriptions
					[ begin 
							Storage:save(State#connection_state.end_type, #storage_subscription{key = #subs_primary_key{topic = Topic, client_id = Client_Id}, qos = QoS, callback = Callback})
						end || {Topic, QoS, Callback} <- Subscriptions], %% @todo check clean_session flag
					Pid ! {suback, Ref, Return_codes},
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
			lager:debug([{endtype, State#connection_state.end_type}], " unsubscribe completed for ~p. unsuback packet:~p~n", [Topics, Packet]),
			process(State, Tail);
		
		{unsuback, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Topics} ->
					Pid ! {unsuback, Ref},
%% discard session subscriptions
					[ begin 
							Storage:remove(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id})
						end || Topic <- Topics], %% @todo check clean_session flag
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
			lager:debug([{endtype, State#connection_state.end_type}], " >>> publish comes PI = ~p, Record = ~p Prosess List = ~p~n", [Packet_Id, Record, State#connection_state.processes]),
			case QoS of
				0 -> 	
					delivery_to_application(State, Record),
					process(State, Tail);
				?test_fragment_skip_send_puback
				1 ->
					delivery_to_application(State, Record),
					case Transport:send(Socket, packet(puback, Packet_Id)) of
						ok -> ok;
						{error, _Reason} -> ok
					end,
					process(State, Tail);
				?test_fragment_skip_send_pubrec
				2 ->
					New_State = 
						case maps:is_key(Packet_Id, Processes) of
							true when Dup =:= 0 -> 
								lager:warning([{endtype, State#connection_state.end_type}], " >>> incoming PI = ~p, already exists Record = ~p Prosess List = ~p~n", [Packet_Id, Record, State#connection_state.processes]),
								State;
							_ ->
								case State#connection_state.end_type of 
										client -> delivery_to_application(State, Record);
										server -> none
								end,
%% store PI after receiving message
								Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
								Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = Record#publish{last_sent = pubrec}}),
								case Transport:send(Socket, packet(pubrec, Packet_Id)) of
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
					Pid ! {puback, Ref},
%% discard message after pub ack
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:remove(State#connection_state.end_type, Prim_key),
					process(
						State#connection_state{processes = maps:remove(Packet_Id, Processes)},
						Tail);
				undefined ->
					process(State, Tail)
			end;

		?test_fragment_skip_rcv_pubrec
		?test_fragment_skip_send_pubrel
		{pubrec, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{From, Params} ->
%% store message before pubrel
					Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
					Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = #publish{last_sent = pubrel}}),
					New_State =
					case Transport:send(Socket, packet(pubrel, Packet_Id)) of
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
		?test_fragment_skip_send_pubcomp
		{pubrel, Packet_Id, Tail} ->
			lager:debug([{endtype, State#connection_state.end_type}], " >>> pubrel arrived PI: ~p	~p.~n", [Packet_Id, Processes]),
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
					New_State =
					case Transport:send(Socket, packet(pubcomp, Packet_Id)) of
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
			lager:debug([{endtype, State#connection_state.end_type}], " >>> pubcomp arrived PI: ~p.~n", [Packet_Id]),
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
			%% @todo stop the process, close the socket !!!
			process(State, Tail);

		_ ->
			lager:debug([{endtype, State#connection_state.end_type}], "unparsed message: ~p state:~p~n", [Binary, State]),
			State#connection_state{tail = Binary}
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

get_topic_attributes(#connection_state{storage = Storage} = State, Topic) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
	Topic_List = Storage:get_matched_topics(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id}),
	[{QoS, Callback} || {_TopicFilter, QoS, Callback} <- Topic_List].

delivery_to_application(#connection_state{end_type = client} = State, #publish{topic = Topic, qos = QoS, payload = Payload}) ->
	case get_topic_attributes(State, Topic) of
		[] -> do_callback(State#connection_state.default_callback, [{{Topic, QoS}, QoS, Payload}]);
		List ->
			[
				case do_callback(Callback, [{{Topic, TopicQoS}, QoS, Payload}]) of
					false -> do_callback(State#connection_state.default_callback, [{{Topic, QoS}, QoS, Payload}]);
					_ -> ok
				end
				|| {TopicQoS, Callback} <- List
			]
	end;
delivery_to_application(#connection_state{end_type = server, storage = Storage}, #publish{topic = Topic_Params, qos = QoS_Params, payload = Payload} = Params) ->
%	Topic_List = Storage:get_matched_topics(State#connection_state.end_type, Topic),
%	[{QoS, Callback} || {_TopicFilter, QoS, Callback} <- Topic_List].
	case Storage:get_matched_topics(server, Topic_Params) of
		[] ->
			lager:warning([{endtype, server}], "There is no the topic in DB. Publish came: Topic=~p QoS=~p Payload=~p~n", [Topic_Params, QoS_Params, Payload]);
		List ->
			lager:debug([{endtype, server}], "Topic list=~128p~n", [List]),
			[
				case Storage:get(server, {client_id, Client_Id}) of
					undefined -> 
						lager:debug([{endtype, server}], "Cannot find connection PID for client id=~p~n", [Client_Id]);
					Pid ->
						if QoS_Params > TopicQoS -> 
								erlang:spawn(?MODULE, server_publish, [Pid, Params#publish{qos = TopicQoS}]);
							true ->
								erlang:spawn(?MODULE, server_publish, [Pid, Params])
						end
				end
				|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, qos = TopicQoS} <- List
			]
	end.

do_callback(Callback, Args) ->
	lager:debug([{endtype, client}], "Client invokes callback function ~p with Args:~p~n", [Callback, Args]),
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
		{match, _R} -> 
%			io:format(user, " match: ~p ~n", [_R]),
			true;
		_E ->
%			io:format(user, " NO match: ~p ~n", [_E]),
			false
	end.

topic_regexp(TopicFilter) ->
	R1 = re:replace(TopicFilter, "\\+", "([^/]*)", [global, {return, list}]),
%	io:format(user, " after + replacement: ~p ~n", [R1]),
	R2 = re:replace(R1, "#", "(.*)", [global, {return, list}]),
%	io:format(user, " after # replacement: ~p ~n", [R2]),
	"^" ++ R2 ++ "$".

