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
	restore_session/1
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
	case Transport:send(Socket, packet(connect, undefined, Conn_config, [])) of
		ok -> 
			New_processes = (State#connection_state.processes)#{connect => From},
			New_State = State#connection_state{config = Conn_config, default_callback = Callback, processes = New_processes},
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
	end;

?test_fragment_set_test_flag

handle_call(status, _From, #connection_state{storage = Storage} = State) ->	
	{reply, [{session_present, State#connection_state.session_present}, {subscriptions, Storage:get_all(State#connection_state.end_type, topic)}], State};

?test_fragment_break_connection

handle_call({publish, #publish{qos = 0} = PubRec}, 
						{_, Ref}, 
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	{Params, NewState} = topic_alias_handle(Config#connect.version, PubRec, State),
	Transport:send(Socket, packet(publish, Config#connect.version, {Params#publish{dup = 0}, 0}, [])), %% qos=0 and dup=0
	lager:info([{endtype, NewState#connection_state.end_type}], "Client ~p published message to topic=~p:0~n", [Config#connect.client_id, Params#publish.topic]),
	{reply, {ok, Ref}, NewState};

%%?test_fragment_skip_send_publish

handle_call({publish, #publish{qos = QoS} = PubRec}, 
						{_, Ref} = From, 
						#connection_state{socket = Socket, transport = Transport, packet_id = Packet_Id, storage = Storage, config = Config} = State) when (QoS =:= 1) orelse (QoS =:= 2) ->
	{Params, NewState} = topic_alias_handle(Config#connect.version, PubRec, State),
	Packet = if NewState#connection_state.test_flag =:= skip_send_publish -> <<>>; true -> packet(publish, Config#connect.version, {Params#publish{dup = 0}, Packet_Id}, []) end,
%% store message before sending
	Params2Save = Params#publish{dir = out, last_sent = publish}, %% for sure
	Prim_key = #primary_key{client_id = (NewState#connection_state.config)#connect.client_id, packet_id = Packet_Id},
	Storage:save(NewState#connection_state.end_type, #storage_publish{key = Prim_key, document = Params2Save}),
	case Transport:send(Socket, Packet) of
		ok -> 
			New_processes = (NewState#connection_state.processes)#{Packet_Id => {From, Params2Save}},
			lager:info([{endtype, NewState#connection_state.end_type}], "Client ~p published message to topic=~p:~p <PktId=~p>~n", [Config#connect.client_id, Params#publish.topic, QoS, Packet_Id]),
		{reply, {ok, Ref}, NewState#connection_state{packet_id = next(Packet_Id, NewState), processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State} %% do not update State!
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
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, #publish{last_sent = pubrec}}},
			{reply, {ok, Ref}, State#connection_state{processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;
handle_call({republish, #publish{last_sent = publish} = Params, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> re-publish request ~p, PI: ~p.~n", [Params, Packet_Id]),
	Packet = packet(publish, State#connection_state.config#connect.version, {Params#publish{dup = 1}, Packet_Id}, []),
	case Transport:send(Socket, Packet) of
		ok -> 
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Params}},
			{reply, {ok, Ref}, State#connection_state{processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
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
				false -> {"", Options, Callback}		%% @todo process the error!
			 end || {Topic, Options, Callback} <- Subscriptions], %% @todo check for proper record #subs_options for v5 (and v3.1.1 ?)
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

handle_call(disconnect, %% @todo add Disconnect ReasonCode
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	case Transport:send(Socket, packet(disconnect, Config#connect.version, 0, [])) of
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
handle_cast(disconnect, %% @todo add Reason code !!!
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	case Transport:send(Socket, packet(disconnect, Config#connect.version, 0, [])) of
		ok -> 
			lager:info([{endtype, State#connection_state.end_type}], "Client ~p sent disconnect request.", [Config#connect.client_id]),
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
				List = Storage:get_matched_topics(server, Config#connect.will_topic),
				lager:debug([{endtype, server}], "Will Topic list = ~128p~n", [List]),
				[
					case Storage:get(server, {client_id, Client_Id}) of
						undefined -> 
							lager:warning([{endtype, server}], "Cannot find connection PID for client id=~p~n", [Client_Id]);
						Pid ->
							TopicQoS = TopicOptions#subscription_options.max_qos,
							QoS = if Config#connect.will_qos > TopicQoS -> TopicQoS; true -> Config#connect.will_qos end, 
							Params = #publish{topic = Topic, 
																qos = QoS, 
																retain = Config#connect.will_retain, 
																payload = Config#connect.will_message},
							erlang:spawn(mqtt_socket_stream, server_publish, [Pid, Params])
					end
					|| #storage_subscription{key = #subs_primary_key{topicFilter = Topic, client_id = Client_Id}, options = TopicOptions} <- List
				],
				if (Config#connect.will_retain =:= 1) ->
						Params1 = #publish{topic = Config#connect.will_topic, 
															qos = Config#connect.will_qos, 
															retain = Config#connect.will_retain, 
															payload = Config#connect.will_message},
						Storage:save(server, Params1); %% @todo avoid duplocates ???
					true -> ok
				end;
			true -> ok
	end,
	Storage:remove(State#connection_state.end_type, {client_id, Config#connect.client_id}),
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

restore_session(#connection_state{config = #connect{client_id = Client_Id}, storage = Storage, end_type = End_Type} = State) ->
	Records = Storage:get_all(End_Type, {session, Client_Id}),
	MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
	lager:debug([{endtype, End_Type}], "In restore session: MessageList = ~128p~n", [MessageList]),
	New_State = lists:foldl(fun restore_state/2, State, MessageList),
	New_State.

restore_state({Packet_Id, Params}, State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> restore_prosess_list request ~p, PI: ~p.~n", [Params, Packet_Id]),
	Pid = spawn(gen_server, call, [self(), {republish, Params, Packet_Id}, ?MQTT_GEN_SERVER_TIMEOUT]),
	New_processes = (State#connection_state.processes)#{Packet_Id => {{Pid, undefined}, Params}},
	State#connection_state{processes = New_processes}.

keep_alive_timer(undefined, undefined) -> undefined;
keep_alive_timer(Keep_Alive_Time, undefined) ->
	erlang:start_timer(Keep_Alive_Time * 1000, self(), keep_alive_timeout);
keep_alive_timer(Keep_Alive_Time, Timer_Ref) ->
	erlang:cancel_timer(Timer_Ref),
	erlang:start_timer(Keep_Alive_Time * 1000, self(), keep_alive_timeout).

topic_alias_handle('5.0', #publish{qos = QoS, topic = Topic, properties = Props} = PubRec, #connection_state{topic_alias_map = TopicAliasMap} = State) ->
	Alias = proplists:get_value(?Topic_Alias, Props, 0),
	case {Topic, Alias} of
		{"", 0} -> {error, State};
		{"", N} ->
			case maps:get(N, TopicAliasMap, error) of
				error -> {error, State};
				_ -> {PubRec, State}
			end;
		{T, 0} -> 
			Map1 = maps:filter(fun(_K,V) when V == T -> true; (_K,_) -> false end, TopicAliasMap),
			MapSize = maps:size(Map1),
			if MapSize > 0 ->
						Als = hd(maps:keys(Map1)),
						{PubRec#publish{topic = "", properties = [{?Topic_Alias, Als} | Props]}, State};
				 true -> {PubRec, State}
			end;
		{T, N} ->
			{PubRec, State#connection_state{topic_alias_map = maps:put(N, T, TopicAliasMap)}}
	end;
topic_alias_handle(_, PubRec, State) ->
	{PubRec, State}.
