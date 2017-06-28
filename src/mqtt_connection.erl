%%
%% Copyright (C) 2015-2017 by krasnop@bellsouth.net (Alexei Krasnopolski)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
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
	is_match/2,
	topic_regexp/1,
	server_publish/2
]).

-import(mqtt_output, [packet/2]).
-import(mqtt_input, [input_parser/1]).

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
			case Conn_config#connect.clean_session of
				1 -> 
					Storage:cleanup(State#connection_state.end_type, Conn_config#connect.client_id);
				0 ->	 
					restore_session(New_State) 
	    end,
			{reply, {ok, Ref}, New_State};
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

?test_fragment_skip_send_publish

handle_call({publish, #publish{qos = QoS} = Params}, 
						{_, Ref} = From, 
						#connection_state{socket = Socket, transport = Transport, packet_id = Packet_Id, storage = Storage} = State) when (QoS =:= 1) orelse (QoS =:= 2) ->
	Packet = packet(publish, {Params, Packet_Id}),
%% store message before sending
  Prim_key = #primary_key{client_id = (State#connection_state.config)#connect.client_id, packet_id = Packet_Id},
	Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = Params}),
	case Transport:send(Socket, Packet) of
		ok -> 
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, Params}},
		{reply, {ok, Ref}, State#connection_state{packet_id = next(Packet_Id, State), processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
  end;

handle_call({republish, undefined, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
%	io:format(user, " >>> re-publish request undefined, PI: ~p.~n", [Packet_Id]),
	Packet = packet(pubrel, Packet_Id),
	case Transport:send(Socket, Packet) of
		ok ->
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, #publish{acknowleged = pubrec}}},
	    {reply, {ok, Ref}, State#connection_state{processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
  end;
handle_call({republish, #publish{topic = undefined, acknowleged = pubrec}, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
%	io:format(user, " >>> re-publish request #publish{topic = undefined, acknowleged = pubrec}, PI: ~p.~n", [Packet_Id]),
	Packet = packet(pubrec, Packet_Id),
	case Transport:send(Socket, Packet) of
		ok ->
			New_processes = (State#connection_state.processes)#{Packet_Id => {From, #publish{acknowleged = pubrec}}},
	    {reply, {ok, Ref}, State#connection_state{processes = New_processes}};
		{error, Reason} -> {reply, {error, Reason}, State}
  end;
handle_call({republish, Params, Packet_Id},
						{_, Ref} = From,
						#connection_state{socket = Socket, transport = Transport} = State) ->
%	io:format(user, " >>> re-publish request ~p, PI: ~p.~n", [Params, Packet_Id]),
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
			New_State = socket_stream_process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
			{noreply, New_State};
handle_info({tcp_closed, Socket}, #connection_state{socket = Socket, transport = Transport} = State) ->
			lager:warning([{endtype, State#connection_state.end_type}], "handle_info tcp closed, state:~p~n", [State]),
			Transport:close(Socket),
			{stop, normal, State};
handle_info({ssl, Socket, Binary}, #connection_state{socket = Socket} = State) ->
			New_State = socket_stream_process(State,<<(State#connection_state.tail)/binary, Binary/binary>>),
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

socket_stream_process(State, <<>>) -> 
	State;
socket_stream_process(State, Binary) ->
% Common values:
	Client_Id = (State#connection_state.config)#connect.client_id,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
  Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,
	case input_parser(Binary) of

		{connect, Config, Tail} ->
			lager:debug([{endtype, State#connection_state.end_type}], "connect: ~p~n", [Config]),
			%% @todo check credentials here
			Packet_Id = State#connection_state.packet_id,
			SP = if Config#connect.clean_session =:= 0 -> 1; true -> 0 end, %% @todo check session in DB
			Packet = packet(connack, {SP, 0}),
			Transport:send(Socket, Packet),
			New_State = State#connection_state{config = Config, session_present = SP},
			case Config#connect.clean_session of
				1 -> 
					Storage:cleanup(State#connection_state.end_type, Config#connect.client_id);
				0 ->	 
					restore_session(New_State) 
	    end,
			New_Client_Id = Config#connect.client_id,
			Storage:save(State#connection_state.end_type, #storage_connectpid{client_id = New_Client_Id, pid = self()}),
			socket_stream_process(New_State#connection_state{packet_id = next(Packet_Id, New_State)}, Tail);

		{connack, SP, CRC, Msg, Tail} ->
			case maps:get(connect, Processes, undefined) of
				{Pid, Ref} ->
					Pid ! {connack, Ref, SP, CRC, Msg},
					socket_stream_process(
						State#connection_state{processes = maps:remove(connect, Processes), 
																		session_present = SP},
						Tail);
				undefined ->
					socket_stream_process(State, Tail)
			end;

		{pingreq, Tail} ->
			%% @todo keep-alive concern.
			Packet = packet(pingresp, true),
			Transport:send(Socket, Packet),
			socket_stream_process(State, Tail);

		{pingresp, Tail} -> 
			case maps:get(pingreq, Processes, undefined) of
				{M, F} ->
					spawn(M, F, [pong]);
				F when is_function(F)->
					spawn(fun() -> apply(F, [pong]) end);
				_ -> true
			end,
			socket_stream_process(
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
			socket_stream_process(State, Tail);

		{suback, Packet_Id, Return_codes, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Subscriptions} when is_list(Subscriptions) ->
%% store session subscriptions
					[ begin 
							Storage:save(State#connection_state.end_type, #storage_subscription{key = #subs_primary_key{topic = Topic, client_id = Client_Id}, qos = QoS, callback = Callback})
						end || {Topic, QoS, Callback} <- Subscriptions], %% @todo check clean_session flag
					Pid ! {suback, Ref, Return_codes},
					socket_stream_process(
						State#connection_state{
							processes = maps:remove(Packet_Id, Processes)
						},
						Tail);
				undefined ->
					socket_stream_process(State, Tail)
			end;
		
		{unsubscribe, Packet_Id, Topics, Tail} ->
%% discard session subscriptions
			[ begin 
					Storage:remove(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id})
				end || Topic <- Topics],
			Packet = packet(unsuback, Packet_Id),
			Transport:send(Socket, Packet),
			lager:debug([{endtype, State#connection_state.end_type}], " unsubscribe completed for ~p. unsuback packet:~p~n", [Topics, Packet]),
			socket_stream_process(State, Tail);
		
		{unsuback, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, Topics} ->
					Pid ! {unsuback, Ref},
%% discard session subscriptions
					[ begin 
							Storage:remove(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id})
						end || Topic <- Topics], %% @todo check clean_session flag
					socket_stream_process(
						State#connection_state{
							processes = maps:remove(Packet_Id, Processes)
						}, 
						Tail);
				undefined ->
					socket_stream_process(State, Tail)
			end;
		?test_fragment_skip_rcv_publish
		{publish, #publish{qos = QoS, topic = Topic} = Record, Packet_Id, Tail} ->
			case QoS of
				0 -> 	
					delivery_to_application(State, Record),
					socket_stream_process(State, Tail);
				?test_fragment_skip_send_puback
				1 ->
					delivery_to_application(State, Record),
					case Transport:send(Socket, packet(puback, Packet_Id)) of
						ok -> ok;
						{error, _Reason} -> ok
					end,
					socket_stream_process(State, Tail);
				?test_fragment_skip_send_pubrec
				2 ->
					New_State = 
						case	maps:is_key(Packet_Id, Processes) of
							true -> State;
							false -> 
					      delivery_to_application(State, Record),
%% store PI after receiving message
                Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
                Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = #publish{acknowleged = pubrec}}),
					      case Transport:send(Socket, packet(pubrec, Packet_Id)) of
					        ok -> 
        				    New_processes = Processes#{Packet_Id => {undefined, #publish{topic = Topic, qos = QoS, acknowleged = pubrec}}},
				        	  State#connection_state{processes = New_processes};
						      {error, _Reason} -> State
					      end
					  end,
					socket_stream_process(New_State, Tail);
				_ -> socket_stream_process(State, Tail)
			end;
		?test_fragment_skip_rcv_puback
		{puback, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, _Params} ->
					Pid ! {puback, Ref},
%% discard message after pub ack
          Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
	        Storage:remove(State#connection_state.end_type, Prim_key),
					socket_stream_process(
						State#connection_state{processes = maps:remove(Packet_Id, Processes)},
						Tail);
				undefined ->
					socket_stream_process(State, Tail)
			end;
		?test_fragment_skip_rcv_pubrec
		?test_fragment_skip_send_pubrel
		{pubrec, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{From, Params} ->
%% store message before pubrel
          Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
          Storage:save(State#connection_state.end_type, #storage_publish{key = Prim_key, document = undefined}),
					New_processes = Processes#{Packet_Id => {From, Params#publish{acknowleged = pubrec}}},
					case Transport:send(Socket, packet(pubrel, Packet_Id)) of
						ok -> ok; 
						{error, _Reason} -> ok
					end,
					socket_stream_process(State#connection_state{processes = New_processes}, Tail);
				undefined ->
					socket_stream_process(State, Tail)
			end;
		?test_fragment_skip_rcv_pubrel
		?test_fragment_skip_send_pubcomp
		{pubrel, Packet_Id, Tail} ->
			case maps:get(Packet_Id, Processes, undefined) of
				{_From, _Params} ->
%% discard PI before pubcomp send
          Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
          Storage:remove(State#connection_state.end_type, Prim_key),
					New_processes = maps:remove(Packet_Id, Processes),
					case Transport:send(Socket, packet(pubcomp, Packet_Id)) of
						ok -> ok;
						{error, _Reason} -> ok
					end,
					socket_stream_process(State#connection_state{processes = New_processes}, Tail);
				undefined ->
					socket_stream_process(State, Tail)
			end;
		?test_fragment_skip_rcv_pubcomp
		{pubcomp, Packet_Id, Tail} ->
%			io:format(user, " >>> handle_info pubcomp: Pk Id=~p state=~p~n", [Packet_Id, State]),
			case maps:get(Packet_Id, Processes, undefined) of
				{{Pid, Ref}, _Params} ->
					Pid ! {pubcomp, Ref},
%% discard message after pub comp
          Prim_key = #primary_key{client_id = Client_Id, packet_id = Packet_Id},
	        Storage:remove(State#connection_state.end_type, Prim_key),
					socket_stream_process(State#connection_state{processes = maps:remove(Packet_Id, Processes)}, Tail);
				undefined ->
					socket_stream_process(State, Tail)
			end;

		{disconnect, Tail} ->
			Storage:remove(State#connection_state.end_type, {client_id, Client_Id}),
			%% @todo stop the process, close the socket !!!
			socket_stream_process(State, Tail);

		_ ->
			lager:debug([{endtype, State#connection_state.end_type}], "unparsed message: ~p state:~p~n", [Binary, State]),
			State#connection_state{tail = Binary}
	end.

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

get_topic_attributes(#connection_state{storage = Storage} = State, Topic) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
 	Topic_List = Storage:get_matched_topics(State#connection_state.end_type, #subs_primary_key{topic = Topic, client_id = Client_Id}),
	[{QoS, Callback} || {_TopicFilter, QoS, Callback} <- Topic_List].

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
delivery_to_application(#connection_state{end_type = server, storage = Storage} = State, #publish{topic = Topic_Params, qos = QoS_Params, payload = Payload} = Params) ->
%	Topic_List = Storage:get_matched_topics(State#connection_state.end_type, Topic),
%	[{QoS, Callback} || {_TopicFilter, QoS, Callback} <- Topic_List].
	case Storage:get_matched_topics(server, Topic_Params) of
    [] ->
			lager:debug([{endtype, server}], "There is no the topic in DB. Publish came: Topic=~p QoS=~p Payload=~p~n", [Topic_Params, QoS_Params, Payload]);
		List ->
			lager:debug([{endtype, server}], "Topic list=~128p~n", [List]),
			[
			  case Storage:get(server, {client_id, Client_Id}) of
					undefined -> 
						lager:debug([{endtype, server}], "Cannot find connection PID for client id=~p~n", [Client_Id]);
					Pid ->
						erlang:spawn(?MODULE, server_publish, [Pid, Params])
			  end
				|| #storage_subscription{key = #subs_primary_key{topic = Topic, client_id = Client_Id}, qos = TopicQoS, callback = Callback} <- List
			]
	end.

do_callback(Callback, Args) ->
  case Callback of
	  {M, F} -> spawn(M, F, Args);
	  F when is_function(F) -> spawn(fun() -> apply(F, Args) end);
		_ -> false
  end.

server_publish(Pid, Params) -> 
	lager:debug([{endtype, server}], "Pid=~p Params=~128p~n", [Pid, Params]),
	case gen_server:call(Pid, {publish, Params}, ?MQTT_GEN_SERVER_TIMEOUT) of
		{ok, Ref} -> 
			case Params#publish.qos of
				0 -> ok;
				1 ->
					receive
						{puback, Ref} -> 
							ok
					after ?MQTT_GEN_SERVER_TIMEOUT ->
						#mqtt_client_error{type = publish, source = "mqtt_client:publish/2", message = "puback timeout"}
					end;
				2 ->
					receive
						{pubcomp, Ref} -> 
							ok
					after ?MQTT_GEN_SERVER_TIMEOUT ->
						#mqtt_client_error{type = publish, source = "mqtt_client:publish/2", message = "pubcomp timeout"}
					end
			end;
		{error, Reason} ->
				#mqtt_client_error{type = publish, source = "mqtt_client:publish/2", message = Reason}
  end.

restore_session(#connection_state{config = #connect{client_id = Client_Id}, storage = Storage, end_type = End_Type}) ->
 	Records = Storage:get_all(End_Type, {session, Client_Id}),
	MessageList = [{PI, Doc} || #storage_publish{key = #primary_key{packet_id = PI}, document = Doc} <- Records],
  [spawn(gen_server, call, [self(), {republish, Params, PI}, ?MQTT_GEN_SERVER_TIMEOUT])	|| {PI, Params} <- MessageList].
