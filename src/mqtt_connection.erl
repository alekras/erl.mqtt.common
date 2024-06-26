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

%% @since 2015-12-25
%% @copyright 2015-2023 Alexei Krasnopolski
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
	topic_alias_handle/3,
	session_expire/2
]).

-import(mqtt_output, [packet/4]).
-import(mqtt_publish, [do_callback/2]).

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
init(#connection_state{end_type = server, packet_id = PkId, client_id = ClId, version = Vrs} = State) ->
	lager:debug([{endtype, server}], ?LOGGING_FORMAT ++ " init connection process with state: ~120p~n", [ClId, PkId, init, Vrs, State]),
	Timeout = application:get_env(mqtt_common, timeout, ?MQTT_GEN_SERVER_TIMEOUT),
	gen_server:enter_loop(?MODULE, [], State#connection_state{timeout = Timeout}, Timeout);
init(#connection_state{end_type = client, packet_id = PkId, client_id = ClId, version = Vrs} = State) ->
	lager:debug([{endtype, client}], ?LOGGING_FORMAT ++ " init mqtt connection process with state: ~120p~n", [ClId, PkId, init, Vrs, State]),
	Timeout = application:get_env(mqtt_common, timeout, ?MQTT_GEN_SERVER_TIMEOUT),
	{ok, State#connection_state{timeout = Timeout}};
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
		], State}.

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
handle_cast({connect, _, Callback, _}, #connection_state{connected = 1, end_type = client} = State) ->
	do_callback(Callback, [onError, #mqtt_error{oper= connect, error_msg= "Already connected."}]),
	{noreply, State};
handle_cast({connect, Conn_config, Callback, Socket_options},
						#connection_state{processes = Processes, end_type = client} = State) ->
	case maps:get(connect, Processes, undefined) of
		undefined ->
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
		
			Normalized_Config = mqtt_data:normalize_config(Conn_config),
			New_State = State#connection_state{
				client_id = Normalized_Config#connect.client_id,
				keep_alive = Normalized_Config#connect.keep_alive,
				version = Normalized_Config#connect.version,
				properties = Normalized_Config#connect.properties,
				transport = Transport,
				socket = Socket,
				event_callback = Callback,
				topic_alias_in_map = #{},
				topic_alias_out_map = #{}},
		
			try 
				mqtt_data:validate_config(Normalized_Config),
				if is_pid(Socket); is_port(Socket); is_record(Socket, sslsocket) ->
					case Transport:send(Socket, packet(connect, undefined, Normalized_Config, [])) of
						ok -> 
							clean_up_timeout(Processes),
							Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, connect}),
							New_processes = #{connect => Timeout_ref},
							{noreply, New_State#connection_state{processes = New_processes}};
						{error, Reason} ->
							do_callback(Callback, [onError, #mqtt_error{oper= connect, error_msg= Reason}]),
							{noreply, New_State};
						Exit -> 
							do_callback(Callback, [onError, #mqtt_error{oper= connect, error_msg= Exit}]),
							{noreply, New_State}
					end;
					?ELSE ->
						do_callback(Callback, [onError, Socket]),
						{noreply, New_State}
				end
			catch _:E -> 
				do_callback(Callback, [onError, #mqtt_error{oper= connect, error_msg= E}]),
				{noreply, New_State}
			end;
		_ ->
			do_callback(Callback, [onError, #mqtt_error{oper= connect, error_msg= "Already received connection request."}]),
			{noreply, State}
	end;

?test_fragment_break_connection

handle_cast({publish, #publish{qos = 0} = PubRec}, 
			#connection_state{
				socket = Socket,
				transport = Transport,
				event_callback = Callback,
				version = Version, client_id = Client_id} = State) ->
	try 
		mqtt_data:validate_publish(Version, PubRec),
		case topic_alias_handle(Version, PubRec#publish{dir=out}, State) of
			{#mqtt_error{} = Response, NewState} ->
				do_callback(Callback, [onError, Response#mqtt_error{oper= publish}]),
				{noreply, NewState};
			{Params, NewState} ->
				Transport:send(Socket, packet(publish, Version, {Params#publish{dup = 0}, 0}, [])), %% qos=0 and dup=0
				lager:info([{endtype, NewState#connection_state.end_type}],
									?LOGGING_FORMAT ++ "Process published message to topic=~p:0~n", 
									[Client_id, none, publish, Version, Params#publish.topic]),
				{noreply, NewState}
		end
	catch throw:E ->
		case E of
			#mqtt_error{} = Mqtt_error ->
				do_callback(Callback, [onError, Mqtt_error#mqtt_error{oper= publish}]);
			_ ->
				do_callback(Callback, [onError, #mqtt_error{oper= publish, error_msg= E}])
		end,
		{noreply, State}
	end;

handle_cast({publish, #publish{qos = QoS} = PubRec}, 
						#connection_state{
								socket = Socket,
								transport = Transport,
								packet_id = Packet_Id,
								storage = Storage,
								client_id = Client_id,
								version = Version} = State) when (QoS =:= 1) orelse (QoS =:= 2) ->
	try
		mqtt_data:validate_publish(Version, PubRec),
		case mqtt_publish:decr_send_quote_handle(Version, State) of
			{error, NewState} ->
				gen_server:cast(self(), {disconnect, 16#93, [{?Reason_String, "Receive Maximum exceeded"}]}),
%%				{reply, {error, #mqtt_error{oper = publish, errno= 16#93, error_msg = "Receive Maximum exceeded"}, Ref}, NewState};
				{noreply, NewState}; %% @todo process error
			{ok, NewState} ->
				case topic_alias_handle(Version, PubRec#publish{dir=out}, NewState) of
					{#mqtt_error{} = Response, VeryNewState} ->
						do_callback(State#connection_state.event_callback, [onError, Response#mqtt_error{oper= publish}]),
						{noreply, VeryNewState};
					{Params, VeryNewState} ->
						Packet =
							if VeryNewState#connection_state.test_flag =:= skip_send_publish -> <<>>;
								 ?ELSE -> packet(publish, Version, {Params#publish{dup = 0}, Packet_Id}, [])
							end,
%% store message before sending
						Params2Save = Params#publish{dir = out, last_sent = publish}, %% for sure
						Prim_key = #primary_key{client_id = VeryNewState#connection_state.client_id, packet_id = Packet_Id},
						Storage:session(save, #storage_publish{key = Prim_key, document = Params2Save}, VeryNewState#connection_state.end_type),
						case Transport:send(Socket, Packet) of
							ok -> 
								Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, publish}),
								New_processes = (VeryNewState#connection_state.processes)#{Packet_Id => {Timeout_ref, Params2Save}},
								lager:info([{endtype, VeryNewState#connection_state.end_type}],
													 ?LOGGING_FORMAT ++ " process sent publish message to network for topic=~p:~p send_quota:~p~n",
													 [Client_id, Packet_Id, publish, Version, Params#publish.topic, QoS, State#connection_state.send_quota]),
								{noreply, VeryNewState#connection_state{packet_id = next(Packet_Id, VeryNewState), processes = New_processes}};
							{error, _Reason} -> {noreply, NewState} %% @todo: process error, do not update State!
						end
				end
		end
	catch throw:_E ->
		{noreply, State} %% @todo process error
	end;

handle_cast({republish, #publish{last_sent = pubrel}, Packet_Id},
						#connection_state{socket = Socket, transport = Transport, client_id = Client_id, version = Version} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}], 
							?LOGGING_FORMAT ++ " process republished message(last_sent = pubrel).~n", 
							[Client_id, Packet_Id, republish, Version]),
	Packet = packet(pubrel, Version, {Packet_Id, 0}, []),
	case Transport:send(Socket, Packet) of
		ok ->
			Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, publish}),
			New_processes = (State#connection_state.processes)#{Packet_Id => {Timeout_ref, #publish{last_sent = pubrel}}},
			{noreply, State#connection_state{processes = New_processes}};
		{error, _Reason} -> {noreply, State} %% @todo error process
	end;
handle_cast({republish, #publish{last_sent = pubrec}, Packet_Id},
						#connection_state{socket = Socket, transport = Transport, client_id = Client_id, version = Version} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " process republished message (last_sent = pubrec).~n",
							[Client_id, Packet_Id, republish, Version]),
	Packet = packet(pubrec, Version, {Packet_Id, 0}, []),
	case Transport:send(Socket, Packet) of
		ok ->
			New_processes = (State#connection_state.processes_ext)#{Packet_Id => {undefined, #publish{last_sent = pubrec}}},
			{noreply, State#connection_state{processes_ext = New_processes}};
		{error, _Reason} -> {noreply, State} %% @todo process error
	end;
handle_cast({republish, #publish{last_sent = publish, expiration_time= ExpT} = Params, Packet_Id},
						#connection_state{socket = Socket, transport = Transport, processes = Processes, version = Version, client_id = Client_id} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " process republish message (last_sent = publish).~n",
							[Client_id, Packet_Id, republish, Version]),
	RemainedTime =
	case ExpT of
		infinity -> 1;
		_ -> ExpT - erlang:system_time(millisecond)
	end,
	if RemainedTime > 0 ->
			Packet = packet(publish, State#connection_state.version, {Params#publish{dup = 1}, Packet_Id}, []),
			case Transport:send(Socket, Packet) of
				ok -> 
					Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, publish}),
					New_processes = Processes#{Packet_Id => {Timeout_ref, Params}},
					{noreply, State#connection_state{processes = New_processes}};
				{error, _Reason} -> {noreply, State} %% @todo process error
			end;
		?ELSE -> 
			lager:debug([{endtype, State#connection_state.end_type}],
									?LOGGING_FORMAT ++ " republishing message is expired.~n",
									[Client_id, Packet_Id, republish, Version]),
			{noreply, State#connection_state{processes = maps:remove(Packet_Id, Processes)}}
	end;

%% Client side:
handle_cast({subscribe, Subscriptions}, State) ->
	handle_cast({subscribe, Subscriptions, []}, State);
handle_cast({subscribe, Subscriptions, Properties},
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	case Transport:send(Socket, packet(subscribe, State#connection_state.version, {Subscriptions, Packet_Id}, Properties)) of
		ok ->
			Subscriptions2 =
			[case mqtt_data:is_topicFilter_valid(Topic) of
				{true, [_, TopicFilter]} -> 
					if is_integer(Options) -> {TopicFilter, #subscription_options{max_qos = Options}};
						 is_record(Options, subscription_options) -> {TopicFilter, Options};
						 ?ELSE -> {"", Options}		%% @todo process the error!
					end;
				false -> {"", Options}		%% @todo process the error!
			 end || {Topic, Options} <- Subscriptions], %% @todo check for proper record #subs_options for v5 (and v3.1.1 ?)
			Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, subscribe}),
			New_processes = (State#connection_state.processes)#{Packet_Id => {Timeout_ref, Subscriptions2}},
			{noreply, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, _Reason} -> {noreply, State}
	end;

handle_cast({unsubscribe, Topics}, State) ->
	handle_cast({unsubscribe, Topics, []}, State);
handle_cast({unsubscribe, Topics, Properties},
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	Packet = packet(unsubscribe, State#connection_state.version, {Topics, Packet_Id}, Properties),
	case Transport:send(Socket, Packet) of
		ok ->
			Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, unsubscribe}),
			New_processes = (State#connection_state.processes)#{Packet_Id => {Timeout_ref, Topics}},
			{noreply, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, _Reason} -> {noreply, State}
	end;

%% Client side:
handle_cast(pingreq,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	case Transport:send(Socket, packet(pingreq, any, undefined, [])) of
		ok ->
			Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, ping}),
			New_processes = (State#connection_state.processes)#{pingreq => Timeout_ref}, %% @todo check if pingreq already exists, if so then reject it
			{noreply, 
				State#connection_state{
					processes = New_processes,
					ping_count = State#connection_state.ping_count + 1}
			};
		{error, _Reason} -> {noreply, State}
	end;

%% Client side:
handle_cast({disconnect, ReasonCode, Properties},
						#connection_state{socket = Socket,
															transport = Transport,
															processes = Processes,
															version = Version,
															client_id = Client_id,
															end_type = client,
															event_callback = Callback,
															connected = 1} = State)
						when is_pid(Socket); is_port(Socket); is_record(Socket, sslsocket) ->
	case maps:get(disconnect, Processes, undefined) of
		undefined ->
			case Transport:send(Socket, packet(disconnect, Version, ReasonCode, Properties)) of
				ok -> 
					lager:info([{endtype, client}], 
										?LOGGING_FORMAT ++ " process sent disconnect request to server with reason code:~p, and properties:~p.~n",
										[Client_id, none, disconnect, Version, ReasonCode, Properties]
								),
					if Version == '5.0' ->
							Timeout_ref = erlang:start_timer(State#connection_state.timeout, self(), {operation_timeout, disconnect}),
							New_processes = Processes#{disconnect => Timeout_ref},
							{noreply, State#connection_state{packet_id = 100, processes = New_processes}};
						?ELSE -> %% if version = 3.1 or 3.1.1
							close_socket(State),
							do_callback(Callback, [onClose, {ReasonCode, Properties}]),
							{noreply, State#connection_state{packet_id = 100, connected = 0}}
					end;
				{error, _Reason} -> 
					close_socket(State),
					{noreply, State#connection_state{packet_id = 100, connected = 0}} %% @todo process error
			end;
		_ ->
			do_callback(Callback, [onError, #mqtt_error{oper= disconnect, error_msg= "Disconnect request is already sent."}]),
			{noreply, State}
	end;
handle_cast({disconnect, _, _}, #connection_state{event_callback = Callback, connected = 0, end_type = client} = State) ->
	do_callback(Callback, [onError, #mqtt_error{oper= disconnect, error_msg= "Already disconnected."}]),
	{noreply, State};
handle_cast({disconnect, _, _}, #connection_state{end_type = client} = State) ->
	close_socket(State),
	{noreply, State#connection_state{connected = 0, packet_id = 100}};

handle_cast(disconnect, #connection_state{event_callback = _Callback, end_type = client} = State) ->
	Processes = State#connection_state.processes,
	Timeout_ref = maps:get(disconnect, Processes, undefined),
	if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
		 ?ELSE -> ok
	end,
	close_socket(State),
%	do_callback(_Callback, [onClose, {128, [{?Reason_String, "Unspecified error"}]}]),
	{noreply, State#connection_state{connected = 0, packet_id = 100, processes = maps:remove(disconnect, Processes)}};

%% Server side:
handle_cast({disconnect, ReasonCode, Properties}, %% @todo add Reason code !!!
						#connection_state{socket = Socket, transport = Transport, client_id = Client_id, version = '5.0', end_type = server} = State) ->
	lager:debug([{endtype, server}],
							?LOGGING_FORMAT ++ " process enter disconnect flow with State: ~p.~n",
							[Client_id, none, disconnect, '5.0', State]),
	case Transport:send(Socket, packet(disconnect, '5.0', ReasonCode, Properties)) of
		ok -> 
			lager:info([{endtype, server}],
						?LOGGING_FORMAT ++ " process sent disconnect response to client with reason code:~p, and properties:~p.~n",
						[Client_id, none, disconnect, '5.0', ReasonCode, Properties]
					),
%			close_socket(State),
			{stop, normal, State#connection_state{connected = 0}};
		{error, _Reason} -> 
%			close_socket(State),
			{stop, shutdown, State#connection_state{connected = 0}}
	end;
handle_cast({disconnect, _ReasonCode, _Properties},
						#connection_state{client_id = Client_id, version = Version, end_type = server} = State) ->
	lager:debug([{endtype, server}],
							?LOGGING_FORMAT ++ " process enter disconnect flow with State: ~p.~n",
							[Client_id, none, disconnect, Version, State]),
	{stop, normal, State#connection_state{connected = 0}};

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
	Timer_Ref = keep_alive_timer(State#connection_state.keep_alive, State#connection_state.timer_ref),
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State#connection_state{timer_ref = Timer_Ref}};
handle_info({tcp, Socket, Binary}, #connection_state{socket = Socket, end_type = client} = State) ->
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State};

handle_info({ssl, Socket, Binary}, #connection_state{socket = Socket, end_type = server} = State) ->
	Timer_Ref = keep_alive_timer(State#connection_state.keep_alive, State#connection_state.timer_ref),
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State#connection_state{timer_ref = Timer_Ref}};
handle_info({ssl, Socket, Binary}, #connection_state{socket = Socket, end_type = client} = State) ->
	New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
	{noreply, New_State};

handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, client_id = Client_id, version = Ver, end_type = client, event_callback = _Callback} = State) ->
	lager:notice([{endtype, client}],
							 ?LOGGING_FORMAT ++ " process receives tcp_closed:~s",
							 [Client_id, none, tcp_closed, Ver, mqtt_data:state_to_string(State)]),
	gen_server:cast(self(), disconnect),
	do_callback(_Callback, [onClose, {128, [{?Reason_String, "Unspecified error"}]}]),
	{noreply, State#connection_state{connected = 0}};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, client_id = Client_id, version = Ver, connected = 0, end_type = server} = State) ->
	lager:notice([{endtype, server}],
							 ?LOGGING_FORMAT ++ " process receives tcp_closed while state:~s",
							 [Client_id, none, tcp_closed, Ver, mqtt_data:state_to_string(State)]),
	{stop, normal, State};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, client_id = Client_id, version = Ver, connected = 1, end_type = server} = State) ->
	lager:notice([{endtype, server}],
							 ?LOGGING_FORMAT ++ " process receives tcp_closed while state:~s",
							 [Client_id, none, tcp_closed, Ver, mqtt_data:state_to_string(State)]),
	{stop, shutdown, State};

handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, client_id = Client_id, version = Ver, end_type = client} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}], 
							 ?LOGGING_FORMAT ++ " process receives ssl_closed while state:~s",
							 [Client_id, none, ssl_closed, Ver, mqtt_data:state_to_string(State)]),
	gen_server:cast(self(), {disconnect, 0, [{?Reason_String, "TLS socket closed event"}]}),
	{noreply, State#connection_state{connected = 0}};
handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, client_id = Client_id, version = Ver, connected = 0, end_type = server} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}],
							 ?LOGGING_FORMAT ++ " process receives ssl_closed while state:~s",
							 [Client_id, none, ssl_closed, Ver, mqtt_data:state_to_string(State)]),
	{stop, normal, State};
handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, client_id = Client_id, version = Ver, connected = 1, end_type = server} = State) ->
	lager:notice([{endtype, State#connection_state.end_type}],
							 ?LOGGING_FORMAT ++ " process receives ssl_closed while state:~s",
							 [Client_id, none, ssl_closed, Ver, mqtt_data:state_to_string(State)]),
	{stop, shutdown, State};

handle_info({timeout, TimerRef, keep_alive_timeout} = Info, #connection_state{timer_ref = TimerRef, client_id = Client_id, version = Ver, end_type = server} = State) ->
	lager:notice([{endtype, server}],
							?LOGGING_FORMAT ++ " process receives KEEP_ALIVE timeout message: ~p state:~s",
							[Client_id, none, keep_alive_timeout, Ver, Info, mqtt_data:state_to_string(State)]),
	{stop, timeout, State};
%% @todo for server side separate:
handle_info({timeout, _TimerRef, {operation_timeout, Operation}} = Info, #connection_state{processes = Processes, client_id = Client_id, version = Ver} = State) ->
	lager:error([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " process receives OPERATION timeout message: ~p state:~s",
							[Client_id, none, operation_timeout, Ver, Info, mqtt_data:state_to_string(State)]),
	do_callback(State#connection_state.event_callback, [onError, #mqtt_error{oper= Operation, errno= 136, error_msg= "Server unavailable. Operation timeout"}]),
	Pr1 = maps:remove(connect, Processes),
	Pr2 = maps:remove(disconnect, Pr1),
	{noreply, State#connection_state{processes = Pr2}};

handle_info([Event, Args], #connection_state{client_id = Client_id, version = Ver, end_type = server} = State) ->
	lager:debug([{endtype, server}],
							?LOGGING_FORMAT ++ " process receives callback event: ~p args: ~p state:~s",
							[Client_id, none, callback_event, Ver, Event, Args, mqtt_data:state_to_string(State)]),
	{noreply, State};
handle_info(Info, #connection_state{client_id = Client_id, version = Ver} = State) ->
	lager:error([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " process receives unexpected message: ~p state:~s",
							[Client_id, none, operation_timeout, Ver, Info, mqtt_data:state_to_string(State)]),
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
terminate(Reason, #connection_state{client_id = Client_id, version = Ver, end_type = client} = State) ->
	lager:notice([{endtype, client}],
							 ?LOGGING_FORMAT ++ "client process TERMINATED with reason:~p, state:~p~n",
							 [Client_id, none, termination, Ver, Reason, State]),
	ok;
terminate(Reason, #connection_state{client_id = Client_id, version = Ver, 
					socket = Socket, transport = Transport, storage = Storage, end_type = server} = State) ->
	lager:notice([{endtype, server}],
							 ?LOGGING_FORMAT ++ " server process TERMINATED with reason:~p, state:~p~n",
							 [Client_id, none, termination, Ver, Reason, State]),
	Session_state = Storage:session_state(get, Client_id),
	if Reason =:= shutdown ->
			will_publish_handle(Storage, Session_state);
		 ?ELSE -> ok
	end,
	session_end_handle(Storage, Session_state, State),
	MySelf = self(),
	case Storage:connect_pid(get, Client_id, server) of
		MySelf ->
			Storage:connect_pid(remove, Client_id, server);
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
					binary,
					{active, true}, 
					{packet, 0}, 
					{recbuf, ?SOC_BUFFER_SIZE}, 
					{sndbuf, ?SOC_BUFFER_SIZE}, 
					{send_timeout, ?SOC_SEND_TIMEOUT} | Options %% @todo check and add options from Argument _Options
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
			#mqtt_error{oper = Transport, source = {?MODULE, ?FUNCTION_NAME, ?LINE}, error_msg = Reason} %% @todo process error
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
	Prim_key = #primary_key{client_id = State#connection_state.client_id, packet_id = PI},
	case Storage:session(exist, Prim_key, State#connection_state.end_type) of
		false -> PI;
		true -> next(PI, State)
	end.

keep_alive_timer(undefined, undefined) -> undefined;
keep_alive_timer(Keep_Alive_Time, undefined) ->
	erlang:start_timer(Keep_Alive_Time * 1000, self(), keep_alive_timeout);
keep_alive_timer(Keep_Alive_Time, Timer_Ref) ->
	erlang:cancel_timer(Timer_Ref),
	erlang:start_timer(Keep_Alive_Time * 1000, self(), keep_alive_timeout).

%% This is dir=out of publish (client send publish-pack or server send publish-pack to topic):
topic_alias_handle('5.0', 
									 #publish{topic = Topic, properties = Props, dir = out} = PubRec, 
									 #connection_state{topic_alias_out_map = TopicAliasOUTMap, properties = ConnectProps} = State) ->
	Alias = proplists:get_value(?Topic_Alias, Props, -1),
	AliasMax = proplists:get_value(?Topic_Alias_Maximum, ConnectProps, 16#ffff),
	SkipCheck = State#connection_state.test_flag =:= skip_alias_max_check,
	%% @todo client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
	if ((Alias == 0) orelse (Alias > AliasMax)) and (not SkipCheck) ->
			{#mqtt_error{errno = 16#94, source = {?MODULE, ?FUNCTION_NAME, ?LINE}, error_msg = "Topic Alias invalid"}, State};
		 SkipCheck ->
			{PubRec, State};
		 ?ELSE ->
			case {Topic, Alias} of
				%% @todo client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
				{"", -1} -> {#mqtt_error{errno = 16#82, error_msg = "Protocol Error: topic alias invalid"}, State}; 
				{"", N} ->
					if State#connection_state.end_type == client ->
							case maps:get(N, TopicAliasOUTMap, undefined) of
								undefined -> {#mqtt_error{errno = 16#94, error_msg = "Topic Alias invalid"}, State}; 
								_ -> {PubRec, State}
							end;
						 ?ELSE ->
							 {#mqtt_error{errno = 16#94, error_msg = "Topic Alias invalid"}, State}
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
									 #connection_state{topic_alias_in_map = TopicAliasINMap, properties = ConnectProps} = State) ->
	Alias = proplists:get_value(?Topic_Alias, Props, -1),
	AliasMax = proplists:get_value(?Topic_Alias_Maximum, ConnectProps, 16#ffff),
	%% @todo client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
	if (Alias == 0) orelse (Alias > AliasMax) -> {#mqtt_error{oper = protocol, errno=16#94, error_msg = "Topic Alias invalid"}, State};
		 ?ELSE ->
%% @todo client side: error; server side: Alias == 0 or > maxAlias -> DISCONNECT(0x94,"Topic Alias invalid")
			case {Topic, Alias} of
				%% @todo client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
				{"", -1} -> {#mqtt_error{errno = 16#82, error_msg = "Protocol Error"}, State};
				{"", N} ->
					case maps:get(N, TopicAliasINMap, undefined) of
%% @todo client side: catch error and return from call; server side: DISCONNECT(0x82, "Protocol Error"))
						undefined -> {#mqtt_error{errno = 16#94, error_msg = "Topic Alias invalid"}, State};
						AliasTopic -> {PubRec#publish{topic = AliasTopic, properties = proplists:delete(?Topic_Alias, Props)}, State}
					end;
				{_T, -1} -> {PubRec, State};
				{T, N} ->
					{PubRec, State#connection_state{topic_alias_in_map = maps:put(N, T, TopicAliasINMap)}}
			end
	end;
topic_alias_handle(_, PubRec, State) ->
	{PubRec, State}.

will_publish_handle(Storage, #session_state{client_id = ClId, will_publish = #publish{}} = Session_state) ->
	PubRec = Session_state#session_state.will_publish,
	List = Storage:subscription(get_matched_topics, PubRec#publish.topic, server),
	lager:debug([{endtype, server}],
							?LOGGING_FORMAT ++ " will has matched topics list = ~128p~n", 
							[ClId, none, will_publish, '5.0', List]),

	{WillDelayInt, Properties} =
		case lists:keytake(?Will_Delay_Interval, 1, PubRec#publish.properties) of
			false -> {0, PubRec#publish.properties};
			{value, {?Will_Delay_Interval, WillDelay}, Props} -> {WillDelay, Props}
		end,
	Params = PubRec#publish{properties = Properties},

	[
		case Storage:connect_pid(get, Client_Id, server) of
			undefined -> 
				lager:warning([{endtype, server}],
											?LOGGING_FORMAT ++ "Cannot find connection PID for client id=~p~n",
											[ClId, none, will_publish, '5.0', Client_Id]);
			Pid ->
				TopicQoS = TopicOptions#subscription_options.max_qos,
				QoS = if PubRec#publish.qos > TopicQoS -> TopicQoS; true -> PubRec#publish.qos end, 
				{ok, _} = timer:apply_after(WillDelayInt * 1000,
																		mqtt_publish,
																		server_send_publish,
																		[Pid, Params#publish{qos = QoS}])
		end
		|| #storage_subscription{key = #subs_primary_key{client_id = Client_Id}, options = TopicOptions} <- List
	],

	case Session_state of
		undefined -> skip;
		Record -> Storage:session_state(save, Record#session_state{will_publish = undefined})
	end,

	if (PubRec#publish.retain =:= 1) ->
			Storage:retain(save, Params); %% @todo avoid duplicates ???
 		true -> ok
	end;
will_publish_handle(_, _) ->
	ok.

session_expire(Storage, #session_state{client_id = Client_id} = SessionState) ->
	lager:debug([{endtype, server}],
							?LOGGING_FORMAT ++ " process catch session_expire: SessionState=~p.~n",
							[Client_id, none, session_expire, '5.0', SessionState]),
	will_publish_handle(Storage, SessionState),
	Storage:cleanup(Client_id, server).

session_end_handle(Storage, Session_state, #connection_state{version= '5.0', client_id = Sess_Client_Id}) ->
	lager:debug([{endtype, server}],
							?LOGGING_FORMAT ++ " process enter session_end_handle: SessionState= ~p~n",
							[Sess_Client_Id, none, session_end, '5.0', Session_state]),
	case Session_state of
		undefined -> ok;
		#session_state{session_expiry_interval = Exp_Interval} ->
			case Exp_Interval of 
				16#FFFFFFFF -> ok;
				0 ->
					Storage:cleanup(Sess_Client_Id, server);
				_ ->
					{ok, _} = timer:apply_after(Exp_Interval * 1000,
																			?MODULE,
																			session_expire,
																			[Storage, Session_state])
			end
	end;
session_end_handle(_Storage, _Session_state, _State) ->
	ok.

clean_up_timeout(Processes) ->
	Func = 
		fun(_Key, {Timeout_ref, _}) -> 
			if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
				 ?ELSE -> ok
			end
		end,
	maps:foreach(Func, Processes).
