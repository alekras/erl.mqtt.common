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

%% @since 2023-04-01
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc Module implements subscribe and unsubscribe functionality.


-module(mqtt_subscribe).

%%
%% Include files
%%
-include("mqtt.hrl").
-include("mqtt_property.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	subscribe/4,
	suback/4,
	unsubscribe/4,
	unsuback/3
]).

-import(mqtt_output, [packet/4]).
-import(mqtt_publish, [do_callback/2]).

%% server side only
subscribe(State, Packet_Id, Subscriptions, Properties) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
	Version = State#connection_state.config#connect.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Storage = State#connection_state.storage,
%% store session subscriptions
	SubId = proplists:get_value(?Subscription_Identifier, Properties, 0),
	Return_Codes = 
	[ begin %% Topic, QoS - new subscriptions
			Sub_Options = 
				if Version == '5.0' -> 
							if SubId == 0 -> Options;
								 ?ELSE -> Options#subscription_options{identifier = SubId}
							end;
					 ?ELSE ->
							#subscription_options{max_qos = Options}
				end,

			case mqtt_data:is_topicFilter_valid(Topic) of
				{true, Return} ->
					{ShareName, TopicFilter} =
					case Return of
						["", TF] -> {undefined, TF};
						[SN, TF] -> {SN, TF}
					end,
					Key = #subs_primary_key{topicFilter = TopicFilter, shareName = ShareName, client_id = Client_Id},
					handle_retain_msg_after_subscribe(Version, State, Sub_Options, Key),
					Storage:subscription(
						save,
						#storage_subscription{key = Key,
																	options = Sub_Options},
						server
					),
					Sub_Options#subscription_options.max_qos;
				false -> 128 %% 0x80 Unspecified error (page 78)
			end
		end || {Topic, Options} <- Subscriptions],
	Packet = packet(suback, Version, {Return_Codes, Packet_Id}, []), %% TODO now just return empty properties
	case Transport:send(Socket, Packet) of
		ok -> 
			lager:info([{endtype, server}],
								 ?LOGGING_FORMAT ++ " process completed subscriptions: ~128p Return codes: ~p.~n",
								 [Client_Id, Packet_Id, subscribe, Version, Subscriptions, Return_Codes]);
		{error, Reason} -> 
			lager:error([{endtype, server}], 
									?LOGGING_FORMAT ++ " process failed to send suback packet with reason: ~p~n",
									[Client_Id, Packet_Id, subscribe, Version, Reason])
	end,
	State.

%% client side only
suback(State, Packet_Id, Return_codes, Properties) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
	Ver = (State#connection_state.config)#connect.version,
	Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,

	case maps:get(Packet_Id, Processes, undefined) of
		undefined ->
			lager:error([{endtype, client}],
									?LOGGING_FORMAT ++ " process is failed during suback. ~s",
									[Client_Id, Packet_Id, suback, Ver, mqtt_data:state_to_string(State)]),
			State;
		{Timeout_ref, Subscriptions} when is_list(Subscriptions) ->
			if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
				 ?ELSE -> ok
			end,
%% store session subscriptions
			[ if (Return_code >= 0) and (Return_code =< 2) ->
					Storage:subscription(
						save,
						#storage_subscription{
							key = #subs_primary_key{topicFilter = Topic, client_id = Client_Id},
							options = Options},
						client);
					?ELSE -> ok %% @todo process error ???
				end
				|| {{Topic, Options}, Return_code} <- lists:zip(Subscriptions, Return_codes)], %% @TODO check clean_session flag
			do_callback(State#connection_state.event_callback, [onSubscribe, {Return_codes, Properties}]),
			lager:info([{endtype, client}],
								 ?LOGGING_FORMAT ++ " process subscribed to topics ~p with return codes: ~p~n",
								 [Client_Id, Packet_Id, suback, Ver, Subscriptions, Return_codes]),
			State#connection_state{
				processes = maps:remove(Packet_Id, Processes)
			}
	end.

%% server side only
unsubscribe(State, Packet_Id, Topics, _Properties) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
	Version = State#connection_state.config#connect.version,
	Socket = State#connection_state.socket,
	Transport = State#connection_state.transport,
	Storage = State#connection_state.storage,
%% discard session subscriptions
	ReasonCodeList =
	[ begin 
			Storage:subscription(
				remove, 
				#subs_primary_key{topicFilter = binary_to_list(Topic), shareName = '_', client_id = Client_Id}, 
				server
			),
			0 %% @TODO add reason code list
		end || Topic <- Topics],
	Packet = packet(unsuback, Version, {ReasonCodeList, Packet_Id}, []), %% @TODO now just return empty properties
	Transport:send(Socket, Packet),
	lager:info([{endtype, server}],
						 ?LOGGING_FORMAT ++ " process unsubscribed from topics: ~p~n", 
						 [Client_Id, Packet_Id, unsubscribe, Version, Topics]),
	State.

%% client side only
unsuback(State, {Packet_Id, Return_codes}, Properties) ->
	Client_Id = (State#connection_state.config)#connect.client_id,
	Version = (State#connection_state.config)#connect.version,
	Processes = State#connection_state.processes,
	Storage = State#connection_state.storage,
	case maps:get(Packet_Id, Processes, undefined) of
		undefined -> 
			lager:error([{endtype, client}],
									?LOGGING_FORMAT ++ " process is failed during unsuback. ~s",
									[Client_Id, Packet_Id, unsuback, Version, mqtt_data:state_to_string(State)]),
			State;
		{Timeout_ref, Topics} when is_list(Topics) ->
			if is_reference(Timeout_ref) -> erlang:cancel_timer(Timeout_ref);
				 ?ELSE -> ok
			end,
%% discard session subscriptions
			[ begin
					Storage:subscription(remove, #subs_primary_key{topicFilter = Topic, client_id = Client_Id}, client)
				end || Topic <- Topics], %% TODO check clean_session flag
			lager:info([{endtype, client}],
								 ?LOGGING_FORMAT ++ " process unsubscribed from topics ~p with return codes: ~p~n",
								 [Client_Id, Packet_Id, unsuback, Version, Topics, Return_codes]),
			do_callback(State#connection_state.event_callback, [onUnsubscribe, {Return_codes, Properties}]),
			State#connection_state{
				processes = maps:remove(Packet_Id, Processes)
			}
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% retain_handling
%% 0 - send retain msg
%% 1 - send retain msg if subscription is new
%% 2 - do not send retain msg
%% shared - do not send retain msg

handle_retain_msg_after_subscribe('5.0', _State, #subscription_options{retain_handling = 2}, _Key) ->
	ok;
handle_retain_msg_after_subscribe('5.0', #connection_state{storage = Storage, config = #connect{client_id = Client_id}} = State, 
																	Options, 
																	#subs_primary_key{topicFilter = TopicFilter, shareName = undefined} = Key) ->
	Retain_Messages = Storage:retain(get, TopicFilter),
	Exist = Storage:subscription(exist, Key, server),
	lager:debug([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " retain messages=~128p subscription exists=~p~n",
							[Client_id, none, retain_publish, '5.0', Retain_Messages, Exist]),
	QoS = Options#subscription_options.max_qos,
	Retain_handling = Options#subscription_options.retain_handling,
	if (Retain_handling == 0) or ((Retain_handling == 1) and (not Exist)) ->
			[ begin
					QoS_4_Retain = if Params_QoS > QoS -> QoS; true -> Params_QoS end,
					erlang:spawn(mqtt_publish, 
												server_send_publish, 
												[self(), Params#publish{qos = QoS_4_Retain}])
				end || #publish{qos = Params_QoS} = Params <- Retain_Messages];
		 true -> ok
	end;
handle_retain_msg_after_subscribe('5.0', _State, _Options, _Key) ->
	ok;
handle_retain_msg_after_subscribe(_, #connection_state{storage = Storage, config = #connect{client_id = Client_id, version = Version}} = State, 
																	Options,
																	#subs_primary_key{topicFilter = TopicFilter} = _Key) ->
	Retain_Messages = Storage:retain(get, TopicFilter),
	lager:debug([{endtype, State#connection_state.end_type}],
							?LOGGING_FORMAT ++ " retain messages=~128p~n",
							[Client_id, none, retain_publish, Version, Retain_Messages]),
	QoS = Options#subscription_options.max_qos,
	[ begin
			QoS_4_Retain = if Params_QoS > QoS -> QoS; true -> Params_QoS end,
			erlang:spawn(mqtt_publish,
									 server_send_publish,
									 [self(), Params#publish{qos = QoS_4_Retain}])
		end || #publish{qos = Params_QoS} = Params <- Retain_Messages].
