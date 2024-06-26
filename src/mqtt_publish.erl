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

%% @since 2023-04-03
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc Module implements publish, puback, pubrec, pubrel and pubcomp functionality.


-module(mqtt_publish).

%%
%% Include files
%%
-include("mqtt.hrl").
-include("mqtt_property.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	publish/3,
	puback/3,
	pubrec/3,
	pubrel/3,
	pubcomp/3,
	server_send_publish/2,
	decr_send_quote_handle/2,
	do_callback/2
]).

-import(mqtt_output, [packet/4]).

publish(State, #publish{qos = QoS, topic = Topic, dup = Dup, properties = _Props} = PubRec, Packet_Id) ->
% Common values:
	Client_Id = State#connection_state.client_id,
	Version = State#connection_state.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	ProcessesExt = State#connection_state.processes_ext,
	Storage = State#connection_state.storage,

	Record = msg_experation_handle(Version, PubRec),
	lager:debug([{endtype, State#connection_state.end_type}],
						 ?LOGGING_FORMAT ++ " process receives publish packet from network [topic ~p:~p]~n",
						 [Client_Id, Packet_Id, publish, Version, Topic, QoS]),
	case mqtt_connection:topic_alias_handle(Version, Record, State) of
		{#mqtt_error{oper = publish, errno = ErrNo, error_msg = Msg}, NewState} ->
			if State#connection_state.end_type == server ->
					gen_server:cast(self(), {disconnect, ErrNo, [{?Reason_String, Msg}]});
				?ELSE ->
					gen_server:cast(self(), disconnect)
			end,
			NewState;
		{NewRecord, NewState} -> 
			%%lager:debug([{endtype, State#connection_state.end_type}], " >>> NewRecord = ~p NewState = ~p~n", [NewRecord, NewState]),
			case QoS of
				0 -> 	
					delivery_to_application(NewState, NewRecord),
					NewState;
				1 ->
					case decr_send_quote_handle(Version, NewState) of %% TODO do we need it for Qos=1 ?
						{error, VeryNewState} ->
							gen_server:cast(self(), {disconnect, 16#93, [{?Reason_String, "Receive Maximum exceeded"}]}),
							VeryNewState;
						{ok, VeryNewState} ->
							delivery_to_application(VeryNewState, NewRecord),  %% TODO check for successful delivery
							Packet = 
								if VeryNewState#connection_state.test_flag =:= skip_send_puback -> <<>>; 
									 ?ELSE -> packet(puback, Version, {Packet_Id, 0}, [])  %% TODO properties?
								end,
							case Transport:send(Socket, Packet) of
								ok -> ok;
								{error, _Reason} -> ok %% TODO : process error
							end,
							VeryNewState_1 = inc_send_quote_handle(Version, VeryNewState), %% TODO do we need it for Qos=1 ?
							VeryNewState_1
					end;
				2 ->
					case decr_send_quote_handle(Version, NewState) of
						{error, VeryNewState} ->
							gen_server:cast(self(), {disconnect, 16#93, [{?Reason_String, "Receive Maximum exceeded"}]}),
							VeryNewState;
						{ok, VeryNewState} ->
							NewState1 = 
							case maps:is_key(Packet_Id, ProcessesExt) of
								true when Dup =:= 0 -> 
									lager:warning([{endtype, VeryNewState#connection_state.end_type}],
																?LOGGING_FORMAT ++ " process received message with PacketId that already exists. ~s",
																[Client_Id, Packet_Id, publish, Version, mqtt_data:state_to_string(VeryNewState)]),
									VeryNewState;
								_ ->
									case VeryNewState#connection_state.end_type of 
										client -> 
											delivery_to_application(VeryNewState, NewRecord);
										server -> none
									end,
%% store PI after receiving message
									Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
									Storage:session(save, #storage_publish{key = Prim_key, document = NewRecord#publish{last_sent = pubrec}}, VeryNewState#connection_state.end_type),
									Packet = 
									if VeryNewState#connection_state.test_flag =:= skip_send_pubrec -> <<>>;
										?ELSE -> packet(pubrec, Version, {Packet_Id, 0}, []) %% TODO fill out properties with ReasonString Or/And UserProperty 
									end,
									case Transport:send(Socket, Packet) of
										ok -> 
											New_processes = ProcessesExt#{Packet_Id => {undefined, #publish{topic = Topic, qos = QoS, last_sent = pubrec}}},
											VeryNewState#connection_state{processes_ext = New_processes};
										{error, _Reason} -> VeryNewState
									end
							end,
							NewState1
					end;
				_ -> State
			end
	end.

puback(State, {Packet_Id, ReasonCode}, Properties) ->
	Client_Id = State#connection_state.client_id,
	Version = State#connection_state.version,
	Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,
	lager:debug([{endtype, State#connection_state.end_type}],
						 ?LOGGING_FORMAT ++ " process receives puback packet from network~n",
						 [Client_Id, Packet_Id, puback, Version]),
	case maps:get(Packet_Id, Processes, undefined) of
		undefined -> State;
		{Timeout_ref, _Params} ->
			if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
				 ?ELSE -> ok
			end,
%% discard message<QoS=1> after pub ack
			NewState = inc_send_quote_handle(Version, State),
			Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id}, 
			Storage:session(remove, Prim_key, NewState#connection_state.end_type),
			do_callback(State#connection_state.event_callback, [onPublish, {ReasonCode, Properties}]),
			NewState#connection_state{processes = maps:remove(Packet_Id, Processes)}
	end.

pubrec(State, {Packet_Id, ResponseCode}, _Properties) ->
	Client_Id = State#connection_state.client_id,
	Version = State#connection_state.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,
	lager:debug([{endtype, State#connection_state.end_type}],
						 ?LOGGING_FORMAT ++ " process receives pubrec packet from network~n",
						 [Client_Id, Packet_Id, pubrec, Version]),
	case maps:get(Packet_Id, Processes, undefined) of
		undefined -> State;
		{Timeout_ref, Params} ->
%% TODO Check ResponseCode > 0x80 for NewState = inc_send_quote_handle(Version, State)
%% store message before pubrel
			Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
			Storage:session(save, #storage_publish{key = Prim_key, document = #publish{last_sent = pubrel}}, State#connection_state.end_type),
			Packet =
			if State#connection_state.test_flag =:= skip_send_pubrel -> <<>>;
					?ELSE -> packet(pubrel, Version, {Packet_Id, ResponseCode}, [])  %% TODO fill out properties with ReasonString Or/And UserProperty 
			end,
			New_State =
			case Transport:send(Socket, Packet) of
				ok -> 
					New_processes = Processes#{Packet_Id => {Timeout_ref, Params#publish{last_sent = pubrel}}},
					State#connection_state{processes = New_processes}; 
				{error, _Reason} -> State
			end,
			New_State
	end.

pubrel(State, {Packet_Id, _ReasonCode}, Properties) ->
	Client_Id = State#connection_state.client_id,
	Version = State#connection_state.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Processes = State#connection_state.processes,
	ProcessesExt = State#connection_state.processes_ext,
	Storage = State#connection_state.storage,
	lager:debug([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " process receives pubrel packet from network Pr:~p PrExt:~p reason Code=~p, Props=~p~n",
							[Client_Id, Packet_Id, pubrel, Version, Processes, ProcessesExt, _ReasonCode, Properties]),
%	case maps:get(Packet_Id, ProcessesExt, undefined) of
%		undefined -> State;
%		{_, _Params} ->
			Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
			if
				State#connection_state.end_type =:= server ->
					case Storage:session(get, Prim_key, server) of
						#storage_publish{document = Record} ->
							delivery_to_application(State, Record);
						_ -> ok
					end;
				?ELSE -> ok
			end,
%% discard PI before pubcomp send
			Storage:session(remove, Prim_key, State#connection_state.end_type),
			Packet =
			if State#connection_state.test_flag =:= skip_send_pubcomp -> <<>>; 
					?ELSE -> packet(pubcomp, Version, {Packet_Id, 0}, []) %% TODO fill out properties with ReasonString or/and UserProperty
			end,
			New_State =
			case Transport:send(Socket, Packet) of
				ok ->
					New_processes = maps:remove(Packet_Id, ProcessesExt),
					VeryNewState = State#connection_state{processes_ext = New_processes},
					inc_send_quote_handle(Version, VeryNewState);
				{error, _Reason} -> State
			end,
			New_State.
%	end.

pubcomp(State, {Packet_Id, ReasonCode}, Properties) ->
	Client_Id = State#connection_state.client_id,
	Version = State#connection_state.version,
	Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,
	lager:debug([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " process receives pubcomp packet from network. Processes-~p~n",
							[Client_Id, Packet_Id, pubcomp, Version, Processes]),
	case maps:get(Packet_Id, Processes, undefined) of
		undefined -> State;
		{Timeout_ref, _Params} ->
			if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
				 ?ELSE -> ok
			end,
			NewState = inc_send_quote_handle(Version, State),
			do_callback(State#connection_state.event_callback, [onPublish, {ReasonCode, Properties}]),
%% discard message after pub comp
			Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
			Storage:session(remove, Prim_key, NewState#connection_state.end_type),
			NewState#connection_state{processes = maps:remove(Packet_Id, Processes)}
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

get_topic_attributes(#connection_state{storage = Storage} = State, Topic) ->
	Client_Id = State#connection_state.client_id,
	Topic_List = Storage:subscription(get_matched_topics, #subs_primary_key{topicFilter = Topic, client_id = Client_Id}, client),
	[Options || #storage_subscription{options = Options} <- Topic_List].

delivery_to_application(#connection_state{end_type = client, event_callback = Callback, version = Vrsn} = State,
												#publish{qos = QoS, dup = Dup, retain = Retain} = PubRecord) ->
	Topic = handle_get_topic_from_alias(Vrsn, PubRecord, State),
%%	NewPubRecord = PubRecord#publish{topic = Topic},
	case get_topic_attributes(State, Topic) of
		[] -> do_callback(Callback, [onReceive, {undefined, PubRecord}]);
		List ->
			case Vrsn of
				'5.0' -> [do_callback(Callback, [onReceive, {SubsOption, PubRecord}]) || SubsOption <- List];
				_     -> [do_callback(Callback, [onReceive, {SubsOption#subscription_options.max_qos, PubRecord}]) || SubsOption <- List]
			end
	end,
	lager:info([{endtype, client}], 
						 ?LOGGING_FORMAT ++ " process send publish message to client application [topic ~p:~p, dup=~p, retain=~p]~n", 
						 [State#connection_state.client_id, none, publish, Vrsn, Topic, QoS, Dup, Retain]);

delivery_to_application(#connection_state{end_type = server, storage = Storage} = State, 
												#publish{payload = <<>>, retain = 1} = PubParam) ->
	PublishTopic = handle_get_topic_from_alias(State#connection_state.version, PubParam, State),
	Storage:retain(remove, PublishTopic);
delivery_to_application(#connection_state{end_type = server} = State, 
												#publish{} = PubParam) ->
	PublishTopic = handle_get_topic_from_alias(State#connection_state.version, PubParam, State),
	handle_retain_msg_during_publish(State#connection_state.version, State, PubParam, PublishTopic),
	handle_server_publish(State#connection_state.version, State, PubParam, PublishTopic).

do_callback(Callback, Args) ->
	case Callback of
		{M, F} -> spawn(M, F, Args);
		F when is_function(F) -> spawn(fun() -> apply(F, Args) end);
		Pid when is_pid(Pid) -> Pid ! Args;
		_ -> false
	end.

handle_retain_msg_during_publish('5.0',
																	#connection_state{storage = Storage} = _State, 
																	#publish{qos = Params_QoS, payload = _Payload, retain = Retain, dup = _Dup} = Param, Params_Topic) ->
	if (Retain =:= 1) and (Params_QoS =:= 0) ->
				Storage:retain(remove, Params_Topic),
				Storage:retain(save, Param);
			(Retain =:= 1) ->
				Storage:retain(save, Param);
			true -> ok
	end;
handle_retain_msg_during_publish(_,
																	#connection_state{storage = Storage} = _State,
																	#publish{qos = Params_QoS, retain = Retain} = Param, Params_Topic) ->
	if (Retain =:= 1) and (Params_QoS =:= 0) ->
				Storage:retain(remove, Params_Topic),
				Storage:retain(save, Param);
			(Retain =:= 1) ->
				Storage:retain(save, Param); %% It is protocol extension: we have to remove all previously retained messages by MQTT protocol.
			true -> ok
	end.

handle_server_publish(
		'5.0',
		#connection_state{storage = Storage, client_id = State_client_id} = State,
		#publish{qos = Params_QoS, expiration_time = ExpT, retain = Retain, dup = Dup} = Param,
		PubTopic) ->
	RemainedTime =
		case ExpT of
			infinity -> 1;
			_ -> ExpT - erlang:system_time(millisecond)
		end,
	if RemainedTime > 0 ->
			case Storage:subscription(get_matched_topics, PubTopic, server) of
				[] when Retain =:= 1 -> ok;
				[] ->
					lager:notice([{endtype, server}],
											 ?LOGGING_FORMAT ++ " process received publish message with topic ~p that does not exist in storage.~n",
											 [State_client_id, none, publish, '5.0', PubTopic]);
				List ->
					lager:debug([{endtype, server}], 
											?LOGGING_FORMAT ++ " topic list=~128p~n", 
											[State_client_id, none, publish, '5.0', List]),
					%% TODO if Client_Id topic matches multi subscriptions then create one message with multiple subscription identifier.
					[
						case Storage:connect_pid(get, Client_Id, server) of
							undefined -> 
								lager:warning([{endtype, server}],
															?LOGGING_FORMAT ++ " process cannot find connection PID for client id=~p~n",
															[State_client_id, none, publish, '5.0', Client_Id]);
							Pid ->
								NoLocal = Options#subscription_options.nolocal,
								ProcessCliD = State#connection_state.client_id,
								if (NoLocal =:= 1) and (ProcessCliD =:= Client_Id) -> ok;
									 ?ELSE ->
										TopicQoS = Options#subscription_options.max_qos,
										QoS = if Params_QoS > TopicQoS -> TopicQoS; true -> Params_QoS end,
										Retain_as_published = Options#subscription_options.retain_as_published,
										Retain1 = if Retain_as_published == 0 -> 0; true -> Param#publish.retain end,
										SubId = Options#subscription_options.identifier,
										NewPubProps = 
											if SubId == 0 -> Param#publish.properties;
												 true -> lists:keystore(?Subscription_Identifier, 1, Param#publish.properties, {?Subscription_Identifier, SubId})
											end,
										erlang:spawn(?MODULE, server_send_publish, [Pid, Param#publish{qos = QoS, properties = NewPubProps, retain = Retain1}])
								end
						end
						|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, options = Options} <- List
					]
			end,
			%%handle shared subscriptions :
			case Storage:subscription(get_matched_shared_topics, PubTopic, server) of
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
							lager:debug([{endtype, server}],
													?LOGGING_FORMAT ++ " shared subscription:: GroupList=~128p Random N=~p~n",
													[State_client_id, none, publish_shared, '5.0', GroupList, N]),
							#storage_subscription{key = #subs_primary_key{client_id = CliId}, options = Opts} = lists:nth(N, GroupList),
							case Storage:connect_pid(get, CliId, server) of
								undefined ->
								lager:warning([{endtype, server}],
															?LOGGING_FORMAT ++ " process cannot find connection PID for client id=~p~n",
															[State_client_id, none, publish_shared, '5.0', CliId]);
								Pid ->
%							NoLocal = Opts#subscription_options.nolocal,
%							ProcessCliD = State#connection_state.client_id,
%							if (NoLocal =:= 1) and (ProcessCliD =:= CliId) -> ok;
%								 true ->
											ShTopicQoS = Opts#subscription_options.max_qos,
											QoS = if Params_QoS > ShTopicQoS -> ShTopicQoS; true -> Params_QoS end,
											Retain_as_published = Opts#subscription_options.retain_as_published,
											Retain1 = if Retain_as_published == 0 -> 0; true -> Param#publish.retain end,
											erlang:spawn(?MODULE, server_send_publish, [Pid, Param#publish{qos = QoS, retain = Retain1}])
%							end
							end
						end
						|| {_ShareName, GroupList} <- maps:to_list(ShNamesMap)]
			end,
			lager:info([{endtype, server}], 
								 ?LOGGING_FORMAT ++ " process sent publish message to corresponded topics [topic ~p:~p, dup=~p, retain=~p]~n", 
								 [State_client_id, none, publish, '5.0', PubTopic, Params_QoS, Dup, Retain]);
		?ELSE ->
			lager:info([{endtype, server}], 
								 " process did not send message because it is expired [topic ~p:~p, dup=~p, retain=~p]~n", 
								 [State_client_id, none, publish, '5.0', PubTopic, Params_QoS, Dup, Retain])
	end;
handle_server_publish(
		Version, %% versions 3.1 and 3.1.1
		#connection_state{storage = Storage, client_id = State_client_id},
		#publish{qos = Params_QoS, retain = Retain, dup = Dup} = Param, PubTopic) ->
	case Storage:subscription(get_matched_topics, PubTopic, server) of
		[] when Retain =:= 1 -> ok;
		[] ->
			lager:notice([{endtype, server}],
									 ?LOGGING_FORMAT ++ " process received publish message with topic ~p that does not exist in storage.~n",
									 [State_client_id, none, publish, Version, PubTopic]);
		List ->
					lager:debug([{endtype, server}], 
											?LOGGING_FORMAT ++ " topic list=~128p~n", 
											[State_client_id, none, publish, Version, List]),
			[
				case Storage:connect_pid(get, Client_Id, server) of
					undefined -> 
						lager:warning([{endtype, server}],
													?LOGGING_FORMAT ++ " process cannot find connection PID for client id=~p~n",
													[State_client_id, none, publish, Version, Client_Id]);
					Pid ->
						TopicQoS = Options#subscription_options.max_qos,
						QoS = if Params_QoS > TopicQoS -> TopicQoS; true -> Params_QoS end,
						erlang:spawn(?MODULE, server_send_publish, [Pid, Param#publish{qos = QoS, retain = 0}])
				end
				|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, options = Options} <- List
			]
	end,
		lager:info([{endtype, server}], 
							 ?LOGGING_FORMAT ++ " process sent publish message to corresponded topics [topic ~p:~p, dup=~p, retain=~p]~n", 
							 [State_client_id, none, publish, Version, PubTopic, Params_QoS, Dup, Retain]).

server_send_publish(Pid, Params) -> 
%% TODO process look up topic alias for the Pid client/session and update #publish record
	gen_server:cast(Pid, {publish, Params#publish{dir=out}}), %% @todo callback for timeout or error in self process connection_state
	lager:info([{endtype, server}],
						 " process has successfuly published message to subscriber with PID:~p.~n",
						 [Pid]).

msg_experation_handle('5.0', #publish{properties = Props} = PubRec) ->
	Msg_Exp_Interval = proplists:get_value(?Message_Expiry_Interval, Props, infinity),
	if (Msg_Exp_Interval == 0) or (Msg_Exp_Interval == infinity) -> PubRec#publish{expiration_time= infinity};
		 ?ELSE -> PubRec#publish{expiration_time= (erlang:system_time(millisecond) + Msg_Exp_Interval * 1000)}
	end;
msg_experation_handle(_, PubRec) ->
PubRec.

decr_send_quote_handle('5.0', State) ->
	Send_Quote = State#connection_state.send_quota - 1,
	if Send_Quote =< 0 -> {error, State};
		 ?ELSE -> {ok, State#connection_state{send_quota = Send_Quote}}
	end;
decr_send_quote_handle(_, State) ->
	{ok, State}.

inc_send_quote_handle('5.0', State) ->
	Send_Quote = State#connection_state.send_quota + 1,
	Rec_Max = State#connection_state.receive_max,
	if Send_Quote > Rec_Max -> State;
		 ?ELSE -> State#connection_state{send_quota = Send_Quote}
	end;
inc_send_quote_handle(_, State) ->
	State.

handle_get_topic_from_alias('5.0', #publish{topic = Prms_Topic} = PubParam, State) ->
	if Prms_Topic =:= "" ->
				TopicAlias = proplists:get_value(?Topic_Alias, PubParam#publish.properties, 0),
				if TopicAlias == 0 -> error;
					 ?ELSE ->
							maps:get(TopicAlias, State#connection_state.topic_alias_in_map, error)
				end;
		 true -> Prms_Topic
	end;
handle_get_topic_from_alias(_, #publish{topic = Prms_Topic}, _) ->
	Prms_Topic.
