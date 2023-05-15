%%
%% Copyright (C) 2015-2022 by krasnop@bellsouth.net (Alexei Krasnopolski)
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
%% @copyright 2015-2022 Alexei Krasnopolski
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
	session_expire/3
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
	lager:debug([{endtype, client}], "init mqtt client process.~n", []),
	{ok, State};
init(#mqtt_error{} = Error) ->
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

?test_fragment_set_test_flag

handle_call(status, _From, #connection_state{storage = Storage} = State) ->	
	{reply,
		[
			{connected, State#connection_state.connected},
			{session_present, State#connection_state.session_present},
			{subscriptions, Storage:subscription(get_all, topic, State#connection_state.end_type)} %% only client side?
		], State};

?test_fragment_break_connection

handle_call({publish, #publish{qos = 0} = PubRec}, 
						{_, Ref}, 
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	try 
		mqtt_data:validate_publish(Config#connect.version, PubRec),
		case topic_alias_handle(Config#connect.version, PubRec#publish{dir=out}, State) of
			{#mqtt_error{} = Response, NewState} ->
				{reply, {error, Response, Ref}, NewState};
			{Params, NewState} ->
				Transport:send(Socket, packet(publish, Config#connect.version, {Params#publish{dup = 0}, 0}, [])), %% qos=0 and dup=0
				lager:info([{endtype, NewState#connection_state.end_type}], "Process with ClientId= ~p published message to topic=~p:0~n", [Config#connect.client_id, Params#publish.topic]),
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
		case mqtt_publish:decr_send_quote_handle(Config#connect.version, State) of
			{error, NewState} ->
				gen_server:cast(self(), {disconnect, 16#93, [{?Reason_String, "Receive Maximum exceeded"}]}),
				{reply, {error, #mqtt_error{oper = publish, errno= 16#93, error_msg = "Receive Maximum exceeded"}, Ref}, NewState};
			{ok, NewState} ->
				case topic_alias_handle(Config#connect.version, PubRec#publish{dir=out}, NewState) of
					{#mqtt_error{} = Response, VeryNewState} ->
						{reply, {error, Response, Ref}, VeryNewState};
					{Params, VeryNewState} ->
						Packet =
							if VeryNewState#connection_state.test_flag =:= skip_send_publish -> <<>>;
								 ?ELSE -> packet(publish, Config#connect.version, {Params#publish{dup = 0}, Packet_Id}, [])
							end,
%% store message before sending
						Params2Save = Params#publish{dir = out, last_sent = publish}, %% for sure
						Prim_key = #primary_key{client_id = (VeryNewState#connection_state.config)#connect.client_id, packet_id = Packet_Id},
						Storage:session(save, #storage_publish{key = Prim_key, document = Params2Save}, VeryNewState#connection_state.end_type),
						case Transport:send(Socket, Packet) of
							ok -> 
								New_processes = (VeryNewState#connection_state.processes)#{Packet_Id => {From, Params2Save}},
								lager:info([{endtype, VeryNewState#connection_state.end_type}], "Client ~p published message to topic=~p:~p <PktId=~p>, s_q:~p~n",
													 [Config#connect.client_id, Params#publish.topic, QoS, Packet_Id, State#connection_state.send_quota]),
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
%% Client only site.
handle_cast({connect, Conn_config, Callback, Socket_options},
						#connection_state{storage = Storage, end_type = client} = State) ->
	{Transport, Sock_Opts} =
	case Conn_config#connect.conn_type of 
		CT when CT == ssl; CT == tls -> {ssl, [
				{verify, verify_none},
				{depth, 2},
				{server_name_indication, disable} | Socket_options]
			};
		clear -> {gen_tcp, Socket_options};
		web_socket -> {mqtt_ws_client_handler, [{conn_type, web_socket} | Socket_options]};
		web_sec_socket -> {mqtt_ws_client_handler, [{conn_type, web_sec_socket} | Socket_options]};
		T -> {T, Socket_options}
	end,

	Socket = open_socket(
			Transport,
			Conn_config#connect.host,
			Conn_config#connect.port,
			Sock_Opts),

%%	New_processes = (State#connection_state.processes)#{connect => From},
	New_State = State#connection_state{
		config = Conn_config,
		transport = Transport,
		socket = Socket,
		event_callback = Callback,
%%		processes = New_processes,
		topic_alias_in_map = #{},
		topic_alias_out_map = #{}},

	try 
		mqtt_data:validate_config(Conn_config),
		if is_pid(Socket); is_port(Socket); is_record(Socket, sslsocket) ->
			case Transport:send(Socket, packet(connect, undefined, Conn_config, [])) of
				ok -> 
					New_State_2 =
					case Conn_config#connect.clean_session of
						1 -> 
							Storage:cleanup(Conn_config#connect.client_id, State#connection_state.end_type),
							New_State;
						0 ->	 
							restore_session(New_State) 
					end,
					{noreply, New_State_2};
				{error, _Reason} -> {noreply, New_State};
			Exit -> 
				lager:debug([{endtype, client}], "EXIT while send message~p~n", [Exit]),
				{noreply, New_State}
		end;
			?ELSE ->
				{noreply, New_State}
		end
	catch _:_E -> {noreply, New_State}
	end;

handle_cast({reconnect, _},
						#connection_state{config = #connect{client_id = undefined}, end_type = client} = State) ->
	{reply, #mqtt_error{oper = reconnect, error_msg = "Try reconnect while connect config not defined"}, State};
handle_cast({reconnect, Socket_options},
						#connection_state{transport = Transport, config = Conn_config, end_type = client} = State) ->
	Sock_Opts =
	case Conn_config#connect.conn_type of 
		CT when CT == ssl; CT == tls ->
			[
				{verify, verify_none},
				{depth, 2},
				{server_name_indication, disable} | Socket_options];
		clear -> Socket_options;
		web_socket -> [{conn_type, web_socket} | Socket_options];
		web_sec_socket -> [{conn_type, web_sec_socket} | Socket_options];
		_ -> Socket_options
	end,

	Socket = open_socket(
			Transport,
			Conn_config#connect.host,
			Conn_config#connect.port,
			Sock_Opts),

%%	New_processes = (State#connection_state.processes)#{connect => From},
	New_State = State#connection_state{
		socket = Socket
%%		processes = New_processes
	},

	try 
		if is_pid(Socket); is_port(Socket); is_record(Socket, sslsocket) ->
			case Transport:send(Socket, packet(connect, undefined, Conn_config, [])) of
				ok -> 
					New_State_2 = restore_session(New_State),
					{noreply, New_State_2};
				{error, _Reason} -> {noreply, New_State};
			Exit -> 
				lager:debug([{endtype, client}], "EXIT while send message~p~n", [Exit]),
				{noreply, New_State}
		end;
			?ELSE ->
				{noreply, New_State} %% Socket = #mqtt_error{}
		end
	catch _:_E -> {noreply, New_State}
	end;

%% Client side:
handle_cast({subscribe, Subscriptions}, State) ->
	handle_cast({subscribe, Subscriptions, []}, State);
handle_cast({subscribe, Subscriptions, Properties},
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	case Transport:send(Socket, packet(subscribe, State#connection_state.config#connect.version, {Subscriptions, Packet_Id}, Properties)) of
		ok ->
			Subscriptions2 =
			[case mqtt_data:is_topicFilter_valid(Topic) of
				{true, [_, TopicFilter]} -> {TopicFilter, Options, Callback};
				false -> {"", Options, Callback}		%% TODO process the error!
			 end || {Topic, Options, Callback} <- Subscriptions], %% TODO check for proper record #subs_options for v5 (and v3.1.1 ?)
			New_processes = (State#connection_state.processes)#{Packet_Id => {{self(), 0}, Subscriptions2}}, %% @todo remove {self(),0} !
			{noreply, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, _Reason} -> {noreply, State}
	end;

handle_cast({unsubscribe, Topics}, State) ->
	handle_cast({unsubscribe, Topics, []}, State);
handle_cast({unsubscribe, Topics, Properties},
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	Packet = packet(unsubscribe, State#connection_state.config#connect.version, {Topics, Packet_Id}, Properties),
	case Transport:send(Socket, Packet) of
		ok ->
			New_processes = (State#connection_state.processes)#{Packet_Id => {{self(), 0}, Topics}}, %% @todo remove {self(),0} !
			{noreply, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, _Reason} -> {noreply, State}
	end;

%% Client side:
handle_cast(pingreq,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	case Transport:send(Socket, packet(pingreq, any, undefined, [])) of
		ok ->
			{noreply, 
				State#connection_state{
					ping_count = State#connection_state.ping_count + 1
				}
			};
		{error, _Reason} -> {noreply, State}
	end;

%% Client side:
handle_cast({disconnect, ReasonCode, Properties},
						#connection_state{socket = Socket, transport = Transport, config = Config, end_type = client} = State) when is_pid(Socket); is_port(Socket); is_record(Socket, sslsocket) ->
	case Transport:send(Socket, packet(disconnect, Config#connect.version, ReasonCode, Properties)) of
		ok -> 
			lager:info([{endtype, client}], 
								"Process ~p sent disconnect request to server with reason code:~p, and properties:~p.",
								[Config#connect.client_id, ReasonCode, Properties]
						),
			{noreply, State#connection_state{packet_id = 100}};
		{error, _Reason} -> 
			close_socket(State),
			{noreply, State#connection_state{connected = 0}}
	end;
handle_cast({disconnect, _, _}, #connection_state{end_type = client} = State) ->
	close_socket(State),
	{noreply, State#connection_state{connected = 0, packet_id = 100}};
handle_cast(disconnect, #connection_state{end_type = client} = State) ->
	close_socket(State),
	{noreply, State#connection_state{connected = 0, packet_id = 100}};

%% Server side:
handle_cast({disconnect, ReasonCode, Properties}, %% TODO add Reason code !!!
						#connection_state{socket = Socket, transport = Transport, config = Config, end_type = server} = State) ->
	case Transport:send(Socket, packet(disconnect, Config#connect.version, ReasonCode, Properties)) of
		ok -> 
			lager:info([{endtype, server}],
						"Process ~p sent disconnect response to client with reason code:~p, and properties:~p.",
						[Config#connect.client_id, ReasonCode, Properties]
					),
%			close_socket(State),
			{stop, shutdown, State#connection_state{connected = 0}};
		{error, _Reason} -> 
%			close_socket(State),
			{stop, shutdown, State#connection_state{connected = 0}}
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

handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, connected = 0, end_type = client} = State) ->
	lager:notice([{endtype, client}], "handle_info tcp closed while disconnected, state:~p~n", [State]),
	gen_server:cast(self(), {disconnect, 0, [{?Reason_String, "TCP socket closed event"}]}),
	{noreply, State};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, connected = 1, end_type = client} = State) ->
	lager:notice([{endtype, client}], "handle_info tcp closed while connected, state:~p~n", [State]),
	gen_server:cast(self(), {disconnect, 0, [{?Reason_String, "TCP socket closed event"}]}),
	{noreply, State#connection_state{connected = 0}};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, connected = 0, end_type = server} = State) ->
	lager:notice([{endtype, server}], "handle_info tcp closed while disconnected, state:~p~n", [State]),
	{stop, normal, State};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, connected = 1, end_type = server} = State) ->
	lager:notice([{endtype, server}], "handle_info tcp closed while connected, state:~p~n", [State]),
	{stop, shutdown, State#connection_state{connected = 0}};

handle_info({ssl, Socket, Binary}, #connection_state{socket = Socket, end_type = server} = State) ->
	Timer_Ref = keep_alive_timer((State#connection_state.config)#connect.keep_alive, State#connection_state.timer_ref),
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State#connection_state{timer_ref = Timer_Ref}};
handle_info({ssl, Socket, Binary}, #connection_state{socket = Socket, end_type = client} = State) ->
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State};

handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, connected = 0, end_type = client} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info ssl closed while disconnected, state:~p~n", [State]),
	gen_server:cast(self(), {disconnect, 0, [{?Reason_String, "TLS socket closed event"}]}),
	{noreply, State};
handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, connected = 1, end_type = client} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info ssl closed while connected, state:~p~n", [State]),
	gen_server:cast(self(), {disconnect, 0, [{?Reason_String, "TLS socket closed event"}]}),
	{noreply, State#connection_state{connected = 0}};
handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, connected = 0, end_type = server} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info ssl closed while disconnected, state:~p~n", [State]),
	{stop, normal, State};
handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, connected = 1, end_type = server} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], "handle_info ssl closed while connected, state:~p~n", [State]),
	{stop, shutdown, State};

%% handle_info(disconnect, #connection_state{processes = Processes, end_type = client} = State) ->
%% 	lager:notice([{endtype, client}], "Client handle_info DISCONNECT message, state:~p~n", [State]),
%% 	close_socket(State),
%% 	case maps:get(disconnect, Processes, undefined) of
%% 		{Pid, Ref} ->
%% 			Pid ! {disconnected, Ref};
%% 		undefined ->
%% 			ok
%% 	end,
%% 	{noreply, State#connection_state{connected = 0}};
%% handle_info(disconnect, #connection_state{end_type = server} = State) ->
%% 	lager:notice([{endtype, server}], "Server handle_info DISCONNECT message, state:~p~n", [State]),
%% 	{stop, normal, State#connection_state{connected = 0}};
handle_info({timeout, TimerRef, keep_alive_timeout} = Info, #connection_state{timer_ref = TimerRef} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}], "handle_info keep alive timeout message: ~p state:~p~n", [Info, State]),
	{stop, timeout, State};
handle_info(Info, State) ->
	lager:warning([{endtype, State#connection_state.end_type}], "handle_info get unexpected message: ~p state:~p~n", [Info, State]),
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
terminate(Reason, #connection_state{end_type = client} = State) ->
	lager:notice([{endtype, client}], "Client TERMINATED, reason:~p, state:~p~n", [Reason, State]),
	ok;
terminate(Reason, #connection_state{config = Config, socket = Socket, transport = Transport, storage = Storage, end_type = server} = State) ->
	lager:notice([{endtype, server}], "Server process TERMINATED, reason:~p, state:~p~n", [Reason, State]),
	if is_record(Config#connect.will_publish, publish) and (Reason =:= shutdown) ->
			will_publish_handle(Storage, Config);
		 ?ELSE -> ok
	end,
	session_end_handle(Storage, Config),
	MySelf = self(),
	case Storage:connect_pid(get, Config#connect.client_id, server) of
		MySelf ->
			Storage:connect_pid(remove, Config#connect.client_id, server);
		_ ->
			ok
	end,
	Transport:close(Socket),
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

open_socket(mqtt_ws_client_handler = Transport, Host, Port, Options) ->
	case 
		try
			mqtt_ws_client_handler:start_link(Host, Port, Options)
		catch
			_:_Err -> {error, _Err}
		end
	of
		{ok, WS_handler_Pid} -> 
			ok = Transport:controlling_process(WS_handler_Pid, self()),
			WS_handler_Pid;
		{error, Reason} -> 
			#mqtt_error{oper = open_socket, source = {?MODULE, ?FUNCTION_NAME, ?LINE}, error_msg = Reason}
	end;
open_socket(Transport, Host, Port, Options) ->
	case 
		try
			Transport:connect(
				Host, 
				Port, 
				[
					binary, %% @todo check and add options from Argument _Options
					{active, true}, 
					{packet, 0}, 
					{recbuf, ?SOC_BUFFER_SIZE}, 
					{sndbuf, ?SOC_BUFFER_SIZE}, 
					{send_timeout, ?SOC_SEND_TIMEOUT} | Options
				], 
				?SOC_CONN_TIMEOUT
			)
		catch
			_:_Err -> {error, _Err}
		end
	of
		{ok, Socket} -> 
			ok = Transport:controlling_process(Socket, self()),
			Socket;
		{error, Reason} -> 
			#mqtt_error{oper = Transport, source = {?MODULE, ?FUNCTION_NAME, ?LINE}, error_msg = Reason}
	end.	

close_socket(#connection_state{socket = Socket, transport = Transport, end_type = client}) when is_pid(Socket); is_port(Socket); is_record(Socket, sslsocket) ->
	Transport:close(Socket);
close_socket(#connection_state{end_type = client}) ->
	ok;
close_socket(#connection_state{socket = Socket, transport = Transport, end_type = server}) ->
	Transport:close(Socket).

next(Packet_Id, #connection_state{storage = Storage} = State) ->
	PI =
		if Packet_Id == 16#FFFF -> 0;
			true -> Packet_Id + 1 
		end,
	Prim_key = #primary_key{client_id = (State#connection_state.config)#connect.client_id, packet_id = PI},
	case Storage:session(exist, Prim_key, State#connection_state.end_type) of
		false -> PI;
		true -> next(PI, State)
	end.

restore_session(#connection_state{config = #connect{client_id = Client_Id, version= '5.0'}, storage = Storage, end_type = server} = State) ->
	case Storage:session_state(get, Client_Id) of
		undefined ->
			State#connection_state{session_present= 0};
		_ ->
			Records = Storage:session(get_all, Client_Id, server),
			MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
			lager:debug([{endtype, server}], "In restore session: MessageList = ~128p~n", [MessageList]),
			lists:foldl(fun restore_state/2, State#connection_state{session_present= 1}, MessageList)
	end;
restore_session(#connection_state{config = #connect{client_id = Client_Id, version= '5.0'}, storage = Storage, end_type = client} = State) ->
	Records = Storage:session(get_all, Client_Id, client),
	MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
	lager:debug([{endtype, client}], "In restore session: MessageList = ~128p~n", [MessageList]),
	lists:foldl(fun restore_state/2, State#connection_state{session_present= 1}, MessageList);
restore_session(#connection_state{config = #connect{client_id = Client_Id}, storage = Storage, end_type = EndType} = State) ->
	Records = Storage:session(get_all, Client_Id, EndType),
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

%% This is dir=out of publish (client send publish-pack or server send publish-pack to topic):
topic_alias_handle('5.0', 
									 #publish{topic = Topic, properties = Props, dir = out} = PubRec, 
									 #connection_state{topic_alias_out_map = TopicAliasOUTMap, config = #connect{properties = ConnectProps}} = State) ->
	Alias = proplists:get_value(?Topic_Alias, Props, -1),
	AliasMax = proplists:get_value(?Topic_Alias_Maximum, ConnectProps, 16#ffff),
	SkipCheck = State#connection_state.test_flag =:= skip_alias_max_check,
	%% TODO client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
	if ((Alias == 0) orelse (Alias > AliasMax)) and (not SkipCheck) ->
			{#mqtt_error{oper = protocol, errno = 16#94, source = {?MODULE, ?FUNCTION_NAME, ?LINE}, error_msg = "Topic Alias invalid"}, State};
		 SkipCheck ->
			{PubRec, State};
		 true ->
			case {Topic, Alias} of
				%% TODO client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
				{"", -1} -> {#mqtt_error{oper = protocol, errno = 16#82, error_msg = "Protocol Error: topic alias invalid"}, State}; 
				{"", N} ->
					if State#connection_state.end_type == client ->
							case maps:get(N, TopicAliasOUTMap, undefined) of
								undefined -> {#mqtt_error{oper = protocol, errno = 16#94, error_msg = "Topic Alias invalid"}, State}; 
								_ -> {PubRec, State}
							end;
						 true ->
							 {#mqtt_error{oper = protocol, errno = 16#94, error_msg = "Topic Alias invalid"}, State}
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
%% This is dir=in of publish:
topic_alias_handle('5.0',
									 #publish{topic = Topic, properties = Props, dir = in} = PubRec,
									 #connection_state{topic_alias_in_map = TopicAliasINMap, config = #connect{properties = ConnectProps}} = State) ->
	Alias = proplists:get_value(?Topic_Alias, Props, -1),
	AliasMax = proplists:get_value(?Topic_Alias_Maximum, ConnectProps, 16#ffff),
	%% TODO client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
	if (Alias == 0) orelse (Alias > AliasMax) -> {#mqtt_error{oper = protocol, errno=16#94, error_msg = "Topic Alias invalid"}, State};
		 true ->
%% TODO client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
			case {Topic, Alias} of
				%% TODO client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
				{"", -1} -> {#mqtt_error{oper = protocol, errno = 16#82, error_msg = "Protocol Error"}, State};
				{"", N} ->
					case maps:get(N, TopicAliasINMap, undefined) of
%% TODO client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
						undefined -> {#mqtt_error{oper = protocol, errno = 16#94, error_msg = "Topic Alias invalid"}, State};
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
	List = Storage:subscription(get_matched_topics, PubRec#publish.topic, server),
	lager:debug([{endtype, server}], "Will matched Topic list = ~128p~n", [List]),
	{WillDelayInt, Properties} =
	case lists:keytake(?Will_Delay_Interval, 1, PubRec#publish.properties) of
		false -> {0, PubRec#publish.properties};
		{value, {?Will_Delay_Interval, WillDelay}, Props} -> {WillDelay, Props}
	end,
	Params = PubRec#publish{properties = Properties},
	[
		case Storage:connect_pid(get, Client_Id, server) of
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

	case Storage:session_state(get, Sess_Client_Id) of
		undefined -> skip;
		Record -> Storage:session_state(save, Record#session_state{will_publish = undefined})
	end,

	if (PubRec#publish.retain =:= 1) ->
			Storage:retain(save, Params); %% TODO avoid duplicates ???
		 true -> ok
	end.

session_expire(Storage, SessionState, Config) ->
	lager:debug([{endtype, server}], ">>> session_expire: SessionState=~p Config=~p~n", [SessionState,Config]),
		Will_PubRec = SessionState#session_state.will_publish,
		if Will_PubRec == undefined -> ok;
			 ?ELSE ->
				 will_publish_handle(Storage, Config)
		end,
		Storage:cleanup(Config#connect.client_id, server).

session_end_handle(Storage, #connect{version= '5.0'} = Config) ->
	Sess_Client_Id = Config#connect.client_id,
	SSt = Storage:session_state(get, Sess_Client_Id),
	lager:debug([{endtype, server}], ">>> session_end_handle: Client=~p SessionState=~p~n", [Sess_Client_Id,SSt]),
	case SSt of
		undefined -> ok;
		SessionState ->
			Exp_Interval = SessionState#session_state.session_expiry_interval,
			if Exp_Interval == 16#FFFFFFFF -> ok;
				 Exp_Interval == 0 ->
					Storage:cleanup(Sess_Client_Id, server);
				 ?ELSE ->
					{ok, _} = timer:apply_after(Exp_Interval * 1000,
																			?MODULE,
																			session_expire,
																			[Storage, SessionState, Config])
			end
	end;
session_end_handle(_Storage, _Config) ->
	ok.
