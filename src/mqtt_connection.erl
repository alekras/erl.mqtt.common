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

%% @since 2015-12-25
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_connection.

-module(mqtt_connection).
-behaviour(gen_server).

%%
%% Include files
%%
-include("mqtt.hrl").
-include("mqtt_property.hrl").
-include("mqtt_macros.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% ====================================================================
%% API functions
%% ====================================================================

-export([	
	next/2,
	restore_session/1,
	topic_alias_handle/3,
	session_expire/2
]).

-import(mqtt_output, [packet/4]).

%% ====================================================================
%% Behavioural functions
%% ====================================================================

%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
%% @private
-spec init(Args :: term()) -> Result when
	Result :: {ok, State}
			| {ok, State, Timeout}
			| {ok, State, hibernate}
			| {stop, Reason :: term()}
			| ignore,
	State :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init(#connection_state{end_type = server} = State) ->
	lager:debug([{endtype, server}], "init mqtt connection with state: ~120p~n", [State]),
	gen_server:enter_loop(?MODULE, [], State, ?MQTT_GEN_SERVER_TIMEOUT);
init(#connection_state{end_type = client} = State) ->
	lager:debug([{endtype, client}], "init mqtt connection with state: ~120p~n", [State]),
	{ok, State};
init(#mqtt_client_error{} = Error) ->
	lager:error("init mqtt connection stops with error: ~120p~n", [Error]),
	{stop, Error}.

%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
%% @private
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
	Result :: {reply, Reply, NewState}
			| {reply, Reply, NewState, Timeout}
			| {reply, Reply, NewState, hibernate}
			| {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason, Reply, NewState}
			| {stop, Reason, NewState},
	Reply :: term(),
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity,
	Reason :: term().
%% ====================================================================
handle_call({connect, Conn_config}, From, #connection_state{end_type = client} = State) ->
	handle_call({connect, Conn_config, undefined}, From, State);

handle_call({connect, Conn_config, Callback},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport, storage = Storage, end_type = client} = State) ->
	try 
		mqtt_data:validate_config(Conn_config),
		case Transport:send(Socket, packet(connect, undefined, Conn_config, [])) of
			ok -> 
				New_processes = (State#connection_state.processes)#{connect => From},
				New_State = State#connection_state{config = Conn_config, default_callback = Callback, processes = New_processes, topic_alias_in_map = #{}, topic_alias_out_map = #{}},
				New_State_2 =
				case Conn_config#connect.clean_session of
					1 -> 
						Storage:cleanup(State#connection_state.end_type, Conn_config#connect.client_id),
						New_State;
					0 ->	 
						restore_session(New_State) 
				end,
				{reply, {ok, Ref}, New_State_2};
			{error, Reason} -> {reply, {error, Reason}, State}
		end
	catch throw:E -> {reply, {error, E}, State}
	end;

?test_fragment_set_test_flag

handle_call(status, _From, #connection_state{storage = Storage} = State) ->	
	{reply, [{session_present, State#connection_state.session_present}, {subscriptions, Storage:get_all(State#connection_state.end_type, topic)}], State};

?test_fragment_break_connection

handle_call({publish, #publish{qos = 0} = PubRec}, 
						{_, Ref}, 
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	try 
		mqtt_data:validate_publish(Config#connect.version, PubRec),
		case topic_alias_handle(Config#connect.version, PubRec#publish{dir=out}, State) of
			{#mqtt_client_error{} = Response, NewState} ->
				{reply, {error, Response, Ref}, NewState};
			{Params, NewState} ->
				Transport:send(Socket, packet(publish, Config#connect.version, {Params#publish{dup = 0}, 0}, [])), %% qos=0 and dup=0
				lager:info([{endtype, NewState#connection_state.end_type}], "Client ~p published message to topic=~p:0~n", [Config#connect.client_id, Params#publish.topic]),
				{reply, {ok, Ref}, NewState}
		end
	catch throw:E -> {reply, {error, E, Ref}, State}
	end;
%%?test_fragment_skip_send_publish

handle_call({publish, #publish{qos = QoS} = PubRec}, 
						{_, Ref} = From, 
						#connection_state{socket = Socket, transport = Transport, packet_id = Packet_Id, storage = Storage, config = Config} = State) when (QoS =:= 1) orelse (QoS =:= 2) ->
	try
		mqtt_data:validate_publish(Config#connect.version, PubRec),
		case mqtt_socket_stream:decr_send_quote_handle(Config#connect.version, State) of
			{error, NewState} ->
				gen_server:cast(self(), {disconnect, 16#93, [{?Reason_String, "Receive Maximum exceeded"}]}),
				{reply, {error, #mqtt_client_error{type= protocol, errno= 16#93, message= "Receive Maximum exceeded"}, Ref}, NewState};
			{ok, NewState} ->
				case topic_alias_handle(Config#connect.version, PubRec#publish{dir=out}, NewState) of
					{#mqtt_client_error{} = Response, VeryNewState} ->
						{reply, {error, Response, Ref}, VeryNewState};
					{Params, VeryNewState} ->
						Packet =
							if VeryNewState#connection_state.test_flag =:= skip_send_publish -> <<>>;
								 ?ELSE -> packet(publish, Config#connect.version, {Params#publish{dup = 0}, Packet_Id}, [])
							end,
%% store message before sending
						Params2Save = Params#publish{dir = out, last_sent = publish}, %% for sure
						Prim_key = #primary_key{client_id = (VeryNewState#connection_state.config)#connect.client_id, packet_id = Packet_Id},
						Storage:save(VeryNewState#connection_state.end_type, #storage_publish{key = Prim_key, document = Params2Save}),
						case Transport:send(Socket, Packet) of
							ok -> 
								New_processes = (VeryNewState#connection_state.processes)#{Packet_Id => {From, Params2Save}},
								lager:info([{endtype, VeryNewState#connection_state.end_type}], "Client ~p published message to topic=~p:~p <PktId=~p>~n", [Config#connect.client_id, Params#publish.topic, QoS, Packet_Id]),
								{reply, {ok, Ref}, VeryNewState#connection_state{packet_id = next(Packet_Id, VeryNewState), processes = New_processes}};
							{error, Reason} -> {reply, {error, Reason, Ref}, NewState} %% do not update State!
						end
				end
		end
	catch throw:E -> {reply, {error, E, Ref}, State}
	end;

handle_call({republish, #publish{last_sent = pubrel}, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> re-publish request last_sent = pubrel, PI: ~p.~n", [Packet_Id]),
	Packet = packet(pubrel, State#connection_state.config#connect.version, {Packet_Id, 0}, []),
	case Transport:send(Socket, Packet) of
		ok ->
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, #publish{last_sent = pubrel}}},
			{reply, {ok, Ref}, State#connection_state{processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;
handle_call({republish, #publish{last_sent = pubrec}, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> re-publish request last_sent = pubrec, PI: ~p.~n", [Packet_Id]),
	Packet = packet(pubrec, State#connection_state.config#connect.version, {Packet_Id, 0}, []),
	case Transport:send(Socket, Packet) of
		ok ->
			New_processes = (State#connection_state.processes_ext)#{Packet_Id => {From, #publish{last_sent = pubrec}}},
			{reply, {ok, Ref}, State#connection_state{processes_ext = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;
handle_call({republish, #publish{last_sent = publish, expiration_time= ExpT} = Params, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> re-publish request ~p, PI: ~p.~n", [Params, Packet_Id]),
	RemainedTime =
	case ExpT of
		infinity -> 1;
		_ -> ExpT - erlang:system_time(millisecond)
	end,
	if RemainedTime > 0 ->
			Packet = packet(publish, State#connection_state.config#connect.version, {Params#publish{dup = 1}, Packet_Id}, []),
			case Transport:send(Socket, Packet) of
				ok -> 
					New_processes = (State#connection_state.processes)#{Packet_Id => {From, Params}},
					{reply, {ok, Ref}, State#connection_state{processes = New_processes}};
				{error, Reason} -> {reply, {error, Reason}, State}
			end;
		 ?ELSE -> 
			lager:debug([{endtype, State#connection_state.end_type}], " >>> re-publish Message is expired ~p, PI: ~p.~n", [Params, Packet_Id]),
			{reply, {ok, Ref}, State}
	end;

handle_call({subscribe, Subscriptions}, From, State) ->
	handle_call({subscribe, Subscriptions, []}, From, State);
handle_call({subscribe, Subscriptions, Properties},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	case Transport:send(Socket, packet(subscribe, State#connection_state.config#connect.version, {Subscriptions, Packet_Id}, Properties)) of
		ok ->
			Subscriptions2 =
			[case mqtt_data:is_topicFilter_valid(Topic) of
				{true, [_, TopicFilter]} -> {TopicFilter, Options, Callback};
				false -> {"", Options, Callback}		%% TODO process the error!
			 end || {Topic, Options, Callback} <- Subscriptions], %% TODO check for proper record #subs_options for v5 (and v3.1.1 ?)
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Subscriptions2}},
			{reply, {ok, Ref}, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;

handle_call({unsubscribe, Topics}, From, State) ->
	handle_call({unsubscribe, Topics, []}, From, State);
handle_call({unsubscribe, Topics, Properties},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	Packet = packet(unsubscribe, State#connection_state.config#connect.version, {Topics, Packet_Id}, Properties),
	case Transport:send(Socket, Packet) of
		ok ->
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Topics}},
			{reply, {ok, Ref}, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;

handle_call({disconnect, ReasonCode, Properties},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	case Transport:send(Socket, packet(disconnect, Config#connect.version, ReasonCode, Properties)) of
		ok -> 
			New_processes = (State#connection_state.processes)#{disconnect => From},
			lager:info([{endtype, State#connection_state.end_type}], "<handle_call> Client ~p sent disconnect request.", [Config#connect.client_id]),
			{reply, {ok, Ref}, State#connection_state{connected = 0, processes = New_processes, packet_id = 100}};
		{error, closed} -> {stop, shutdown, State};
		{error, Reason} -> {stop, {shutdown, Reason}, State}
	end;

handle_call({pingreq, Callback},
						_From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	case Transport:send(Socket, packet(pingreq, State#connection_state.config#connect.version, undefined, [])) of
		ok ->
			New_processes = (State#connection_state.processes)#{pingreq => Callback},
			{reply, ok, State#connection_state{
																				processes = New_processes, 
																				ping_count = State#connection_state.ping_count + 1}
			};
		{error, Reason} -> {reply, {error, Reason}, State}
	end.

%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
%% @private
-spec handle_cast(Request :: term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast({disconnect, ReasonCode, Properties}, %% TODO add Reason code !!!
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	case Transport:send(Socket, packet(disconnect, Config#connect.version, ReasonCode, Properties)) of
		ok -> 
			lager:info([{endtype, State#connection_state.end_type}], "<handle_cast> Client ~p sent disconnect request.", [Config#connect.client_id]),
			{stop, shutdown, State#connection_state{connected = 0}};
		{error, closed} -> {stop, shutdown, State};
		{error, Reason} -> {stop, {shutdown, Reason}, State}
	end;

handle_cast(_Msg, State) ->
		{noreply, State}.

%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
%% @private
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info({tcp, Socket, Binary}, #connection_state{socket = Socket, end_type = server} = State) ->
	Timer_Ref = keep_alive_timer((State#connection_state.config)#connect.keep_alive, State#connection_state.timer_ref),
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State#connection_state{timer_ref = Timer_Ref}};
handle_info({tcp, Socket, Binary}, #connection_state{socket = Socket, end_type = client} = State) ->
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State};

handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, connected = 0} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info tcp closed while disconnected, state:~p~n", [State]),
	{stop, normal, State};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, connected = 1} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info tcp closed while connected, state:~p~n", [State]),
	{stop, shutdown, State};

handle_info({ssl, Socket, Binary}, #connection_state{socket = Socket} = State) ->
	Timer_Ref = keep_alive_timer((State#connection_state.config)#connect.keep_alive, State#connection_state.timer_ref),
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State#connection_state{timer_ref = Timer_Ref}};

handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, connected = 0} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info ssl closed while disconnected, state:~p~n", [State]),
	{stop, normal, State};
handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, connected = 1} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info ssl closed while connected, state:~p~n", [State]),
	{stop, shutdown, State};

handle_info(disconnect, State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info DISCONNECT message, state:~p~n", [State]),
	{stop, normal, State};
handle_info({timeout, _TimerRef, _Msg} = Info, State) ->
	lager:debug([{endtype, State#connection_state.end_type}], "handle_info timeout message: ~p state:~p~n", [Info, State]),
	{stop, normal, State};
handle_info(Info, State) ->
	lager:warning([{endtype, State#connection_state.end_type}], "handle_info unknown message: ~p state:~p~n", [Info, State]),
	{noreply, State}.

%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
%% @private
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(Reason, #connection_state{socket = Socket, transport = Transport, processes = Processes, end_type = client} = State) ->
	lager:notice([{endtype, client}], "TERMINATE, reason:~p, state:~p~n", [Reason, State]),
	Transport:close(Socket), %% we do not need if stop after tcp closed
	case maps:get(disconnect, Processes, undefined) of
		{Pid, Ref} ->
			Pid ! {disconnected, Ref};
		undefined ->
			ok
	end;
terminate(Reason, #connection_state{config = Config, socket = Socket, transport = Transport, storage = Storage, end_type = server} = State) ->
	lager:notice([{endtype, server}], "TERMINATE, reason:~p, state:~p~n", [Reason, State]),
	if (Config#connect.will =:= 1) and (Reason =:= shutdown) ->
			will_publish_handle(Storage, Config);
		 ?ELSE -> ok
	end,
	session_end_handle(Storage, Config),
	Storage:remove(server, {client_id, Config#connect.client_id}),
	Transport:close(Socket),
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

next(Packet_Id, #connection_state{storage = Storage} = State) ->
	PI =
		if Packet_Id == 16#FFFF -> 0;
			true -> Packet_Id + 1 
		end,
	Prim_key = #primary_key{client_id = (State#connection_state.config)#connect.client_id, packet_id = PI},
	case Storage:exist(State#connection_state.end_type, Prim_key) of
		false -> PI;
		true -> next(PI, State)
	end.

restore_session(#connection_state{config = #connect{client_id = Client_Id, version= '5.0'}, storage = Storage, end_type = server} = State) ->
	case Storage:get(server, {session_client_id, Client_Id}) of
		undefined ->
			State#connection_state{session_present= 0};
		_ ->
			Records = Storage:get_all(server, {session, Client_Id}),
			MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
			lager:debug([{endtype, server}], "In restore session: MessageList = ~128p~n", [MessageList]),
			lists:foldl(fun restore_state/2, State#connection_state{session_present= 1}, MessageList)
	end;
restore_session(#connection_state{config = #connect{client_id = Client_Id, version= '5.0'}, storage = Storage, end_type = client} = State) ->
	Records = Storage:get_all(client, {session, Client_Id}),
	MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
	lager:debug([{endtype, client}], "In restore session: MessageList = ~128p~n", [MessageList]),
	lists:foldl(fun restore_state/2, State#connection_state{session_present= 1}, MessageList);
restore_session(#connection_state{config = #connect{client_id = Client_Id}, storage = Storage, end_type = EndType} = State) ->
	Records = Storage:get_all(EndType, {session, Client_Id}),
	MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
	lager:debug([{endtype, EndType}], "In restore session: MessageList = ~128p~n", [MessageList]),
	lists:foldl(fun restore_state/2, State#connection_state{session_present= 1}, MessageList).

restore_state({Packet_Id, Params}, State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> restore_prosess_list request ~p, PI: ~p.~n", [Params, Packet_Id]),
	Pid = spawn(gen_server, call, [self(), {republish, Params, Packet_Id}, ?MQTT_GEN_SERVER_TIMEOUT]),
	New_processes = (State#connection_state.processes)#{Packet_Id => {{Pid, undefined}, Params}},
	State#connection_state{processes = New_processes, packet_id= next(Packet_Id, State)}.

keep_alive_timer(undefined, undefined) -> undefined;
keep_alive_timer(Keep_Alive_Time, undefined) ->
	erlang:start_timer(Keep_Alive_Time * 1000, self(), keep_alive_timeout);
keep_alive_timer(Keep_Alive_Time, Timer_Ref) ->
	erlang:cancel_timer(Timer_Ref),
	erlang:start_timer(Keep_Alive_Time * 1000, self(), keep_alive_timeout).

%% This is out of publish (client send publish-pack or server send publish-pack to topic):
topic_alias_handle('5.0', 
									 #publish{topic = Topic, properties = Props, dir = out} = PubRec, 
									 #connection_state{topic_alias_out_map = TopicAliasOUTMap, config = #connect{properties = ConnectProps}} = State) ->
	Alias = proplists:get_value(?Topic_Alias, Props, -1),
	AliasMax = proplists:get_value(?Topic_Alias_Maximum, ConnectProps, 16#ffff),
	SkipCheck = State#connection_state.test_flag =:= skip_alias_max_check,
	%% TODO client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
	if ((Alias == 0) orelse (Alias > AliasMax)) and (not SkipCheck) ->
			{#mqtt_client_error{type=protocol, errno=16#94, source="topic_alias_handle/3:1", message="Topic Alias invalid"}, State};
		 SkipCheck ->
			{PubRec, State};
		 true ->
			case {Topic, Alias} of
				%% TODO client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
				{"", -1} -> {#mqtt_client_error{type=protocol, errno=16#82, source="topic_alias_handle/3:2", message="Protocol Error"}, State}; 
				{"", N} ->
					if State#connection_state.end_type == client ->
							case maps:get(N, TopicAliasOUTMap, undefined) of
								undefined -> {#mqtt_client_error{type=protocol, errno=16#94, source="topic_alias_handle/3:3", message="Topic Alias invalid"}, State}; 
								_ -> {PubRec, State}
							end;
						 true ->
							 {#mqtt_client_error{type=protocol, errno=16#94, source="topic_alias_handle/3:4", message="Topic Alias invalid"}, State}
					end;
				{T, -1} -> %% if server publish msg to a client that already has topic alias map but server has no knowledge about this.
					if State#connection_state.end_type == server ->
							Map1 = maps:filter(fun(_K,V) when V == T -> true; (_K,_) -> false end, TopicAliasOUTMap),
							MapSize = maps:size(Map1),
							if MapSize > 0 ->
										Als = hd(maps:keys(Map1)),
										{PubRec#publish{topic = "", properties = [{?Topic_Alias, Als} | Props]}, State};
								 true -> {PubRec, State}
							end;
						 true -> {PubRec, State}
					end;
				{T, N} ->
					{PubRec, State#connection_state{topic_alias_out_map = maps:put(N, T, TopicAliasOUTMap)}}
			end
	end;
%% This is in of publish:
topic_alias_handle('5.0',
									 #publish{topic = Topic, properties = Props, dir=in} = PubRec,
									 #connection_state{topic_alias_in_map = TopicAliasINMap, config = #connect{properties = ConnectProps}} = State) ->
	Alias = proplists:get_value(?Topic_Alias, Props, -1),
	AliasMax = proplists:get_value(?Topic_Alias_Maximum, ConnectProps, 16#ffff),
	%% TODO client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
	if (Alias == 0) orelse (Alias > AliasMax) -> {#mqtt_client_error{type=protocol, errno=16#94, source="topic_alias_handle/3:a", message="Topic Alias invalid"}, State};
		 true ->
%% TODO client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
			case {Topic, Alias} of
				%% TODO client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
				{"", -1} -> {#mqtt_client_error{type=protocol, errno=16#82, source="topic_alias_handle/3:b", message="Protocol Error"}, State};
				{"", N} ->
					case maps:get(N, TopicAliasINMap, undefined) of
%% TODO client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
						undefined -> {#mqtt_client_error{type=protocol, errno=16#94, source="topic_alias_handle/3:c", message="Topic Alias invalid"}, State};
						AliasTopic -> {PubRec#publish{topic = AliasTopic, properties = proplists:delete(?Topic_Alias, Props)}, State}
					end;
				{_T, -1} -> {PubRec, State};
				{T, N} ->
					{PubRec, State#connection_state{topic_alias_in_map = maps:put(N, T, TopicAliasINMap)}}
			end
	end;
topic_alias_handle(_, PubRec, State) ->
	{PubRec, State}.

will_publish_handle(Storage, Config) ->
	Sess_Client_Id = Config#connect.client_id,
	PubRec = Config#connect.will_publish,
	List = Storage:get_matched_topics(server, PubRec#publish.topic),
	lager:debug([{endtype, server}], "Will matched Topic list = ~128p~n", [List]),
	{WillDelayInt, Properties} =
	case lists:keytake(?Will_Delay_Interval, 1, PubRec#publish.properties) of
		false -> {0, PubRec#publish.properties};
		{value, {?Will_Delay_Interval, WillDelay}, Props} -> {WillDelay, Props}
	end,
	Params = PubRec#publish{properties = Properties},
	[
		case Storage:get(server, {client_id, Client_Id}) of
			undefined -> 
				lager:warning([{endtype, server}], "Cannot find connection PID for client id=~p~n", [Client_Id]);
			Pid ->
				TopicQoS = TopicOptions#subscription_options.max_qos,
				QoS = if PubRec#publish.qos > TopicQoS -> TopicQoS; true -> PubRec#publish.qos end, 
				{ok, _} = timer:apply_after(WillDelayInt * 1000,
																		mqtt_socket_stream,
																		server_send_publish,
																		[Pid, Params#publish{qos = QoS}])
%%					erlang:spawn(mqtt_socket_stream, server_send_publish, [Pid, Params])
		end
		|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, options = TopicOptions} <- List
	],

	case Storage:get(server, {session_client_id, Sess_Client_Id}) of
		undefined -> skip;
		Record -> Storage:save(server, Record#session_state{will_publish = undefined})
	end,

	if (PubRec#publish.retain =:= 1) ->
			Storage:save(server, Params); %% TODO avoid duplicates ???
		 true -> ok
	end.

session_expire(Storage, Config) ->
	Sess_Client_Id = Config#connect.client_id,
	SessionState = Storage:get(server, {session_client_id, Sess_Client_Id}),
	Will_PubRec = SessionState#session_state.will_publish,
	if Will_PubRec == undefined -> ok;
		 ?ELSE ->
			 will_publish_handle(Storage, Config)
	end,
	Storage:cleanup(server, Sess_Client_Id).
	
session_end_handle(Storage, #connect{version= '5.0'} = Config) ->
	Sess_Client_Id = Config#connect.client_id,
	SSt = Storage:get(server, {session_client_id, Sess_Client_Id}),
	lager:debug([{endtype, server}], ">>> session_end_handle: Client=~p SessionState=~p~n", [Sess_Client_Id,SSt]),
	case SSt of
		undefined -> ok;
		SessionState ->
			Exp_Interval = SessionState#session_state.session_expiry_interval,
			if Exp_Interval == 16#FFFFFFFF -> ok;
				 Exp_Interval == 0 ->
					Storage:cleanup(server, Sess_Client_Id);
				 ?ELSE ->
					{ok, _} = timer:apply_after(Exp_Interval * 1000,
																			?MODULE,
																			session_expire,
																			[Storage, Config])
			end
	end;
session_end_handle(_Storage, _Config) ->
	ok.
