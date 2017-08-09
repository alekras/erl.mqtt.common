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

%% @since 2015-12-25
%% @copyright 2015-2017 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_connection.

-module(mqtt_connection).
-behaviour(gen_server).

%%
%% Include files
%%
-include("mqtt.hrl").
-include("mqtt_macros.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

-export([	
	next/2,
	restore_session/1
]).

-import(mqtt_output, [packet/2]).

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
	case Transport:send(Socket, packet(connect, Conn_config)) of
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

handle_call({publish, #publish{qos = 0} = Params}, 
						{_, Ref}, 
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Transport:send(Socket, packet(publish, Params)),
	{reply, {ok, Ref}, State};

%%?test_fragment_skip_send_publish

handle_call({publish, #publish{qos = QoS} = Params}, 
						{_, Ref} = From, 
						#connection_state{socket = Socket, transport = Transport, packet_id = Packet_Id, storage = Storage} = State) when (QoS =:= 1) orelse (QoS =:= 2) ->
%	Packet = packet(publish, {Params, Packet_Id}),
	Packet = if State#connection_state.test_flag =:= skip_send_publish -> <<>>; true -> packet(publish, {Params, Packet_Id}) end,
%% store message before sending
	Params2Save = Params#publish{dir = out, last_sent = publish}, %% for sure
	Prim_key = #primary_key{client_id = (State#connection_state.config)#connect.client_id, packet_id = Packet_Id},
	Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = Params2Save}),
	case Transport:send(Socket, Packet) of
		ok -> 
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Params2Save}},
		{reply, {ok, Ref}, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;

handle_call({republish, #publish{last_sent = pubrel}, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> re-publish request last_sent = pubrel, PI: ~p.~n", [Packet_Id]),
	Packet = packet(pubrel, Packet_Id),
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
	Packet = packet(pubrec, Packet_Id),
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
	Packet = packet(publish, {Params#publish{dup = 1}, Packet_Id}),
	case Transport:send(Socket, Packet) of
		ok -> 
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Params}},
			{reply, {ok, Ref}, State#connection_state{processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;

handle_call({subscribe, Subscriptions},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	case Transport:send(Socket, packet(subscribe, {Subscriptions, Packet_Id})) of
		ok ->
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Subscriptions}},
			{reply, {ok, Ref}, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;

handle_call({unsubscribe, Topics},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	Packet_Id = State#connection_state.packet_id,
	case Transport:send(Socket, packet(unsubscribe, {Topics, Packet_Id})) of
		ok ->
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Topics}},
			{reply, {ok, Ref}, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;

handle_call(disconnect,
						_From,
						#connection_state{socket = Socket, transport = Transport, config = Config} = State) ->
	case Transport:send(Socket, packet(disconnect, false)) of
		ok -> 
			lager:info([{endtype, State#connection_state.end_type}], "Client ~p is disconnected.", [Config#connect.client_id]),
			{stop, normal, State};
		{error, closed} -> {stop, normal, State};
		{error, Reason} -> {reply, {error, Reason}, State}
	end;

handle_call({pingreq, Callback},
						_From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
	case Transport:send(Socket, packet(pingreq, false)) of
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
handle_info({tcp, Socket, Binary}, #connection_state{socket = Socket} = State) ->
			New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
			{noreply, New_State};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, transport = Transport} = State) ->
			lager:warning([{endtype, State#connection_state.end_type}], "handle_info tcp closed, state:~p~n", [State]),
			Transport:close(Socket),
			{stop, normal, State};
handle_info({ssl, Socket, Binary}, #connection_state{socket = Socket} = State) ->
			New_State = mqtt_socket_stream:process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
			{noreply, New_State};
handle_info({ssl_closed, Socket}, #connection_state{socket = Socket, transport = Transport} = State) ->
			lager:warning([{endtype, State#connection_state.end_type}], "handle_info ssl closed, state:~p~n", [State]),
			Transport:close(Socket),
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
terminate(_Reason, _State) ->
%	io:format(user, " >>> terminate ~p~n~p~n", [_Reason, _State]),
	ok.

%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
%% @private
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
		{ok, State}.

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
	lager:debug([{endtype, End_Type}], "Enter restore session: Client Id = ~p Prosess List = ~128p~n", [Client_Id, State#connection_state.processes]),
	Records = Storage:get_all(End_Type, {session, Client_Id}),
	MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
	lager:debug([{endtype, End_Type}], "In restore session: MessageList = ~128p~n", [MessageList]),
	New_State = lists:foldl(fun restore_state/2, State, MessageList),
%	[spawn(gen_server, call, [self(), {republish, Params, PI}, ?MQTT_GEN_SERVER_TIMEOUT])	|| {PI, Params} <- MessageList],
	lager:debug([{endtype, End_Type}], "Exit restore session: Client Id = ~p Prosess List = ~128p~n", [Client_Id, New_State#connection_state.processes]),
	New_State.

%% restore_prosess_list({Packet_Id, undefined}, State) ->
%% 	lager:debug([{endtype, State#connection_state.end_type}], " >>> restore_prosess_list(undefined, PI: ~p).~n", [Packet_Id]),
%% 	New_processes = (State#connection_state.processes)#{Packet_Id => {{self(), undefined}, #publish{acknowleged = pubrec}}},
%% 	State#connection_state{processes = New_processes};
%% restore_prosess_list({Packet_Id, #publish{topic = undefined, acknowleged = pubrec}}, State) ->
%% 	lager:debug([{endtype, State#connection_state.end_type}], " >>> restore_prosess_list #publish{topic = undefined, acknowleged = pubrec}, PI: ~p.~n", [Packet_Id]),
%% 	New_processes = (State#connection_state.processes)#{Packet_Id => {{self(), undefined}, #publish{acknowleged = pubrec}}},
%% 	State#connection_state{processes = New_processes};
restore_state({Packet_Id, Params}, State) ->
	lager:debug([{endtype, State#connection_state.end_type}], " >>> restore_prosess_list request ~p, PI: ~p.~n", [Params, Packet_Id]),
	Pid = spawn(gen_server, call, [self(), {republish, Params, Packet_Id}, ?MQTT_GEN_SERVER_TIMEOUT]),
	New_processes = (State#connection_state.processes)#{Packet_Id => {{Pid, undefined}, Params}},
	State#connection_state{processes = New_processes}.


