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


-ifdef(TEST).

-define(test_fragment_set_test_flag, 
handle_call({set_test_flag, Flag}, _From, State) ->	
	{reply, ok, State#connection_state{test_flag = Flag}};
).

-define(test_fragment_break_connection, 
handle_call({publish, _}, _, #connection_state{test_flag = break_connection} = State) ->
%	Transport:close(State#connection_state.socket),
	{stop, shutdown, State};
).

-define(test_fragment_skip_rcv_publish, 
		{publish, _QoS, _Packet_Id, _Topic, _Payload, Tail} when State#connection_state.test_flag =:= skip_rcv_publish ->
			process(State, Tail);
).

-define(test_fragment_skip_rcv_puback, 
		{puback, _Packet_Id, Tail} when State#connection_state.test_flag =:= skip_rcv_puback ->
			process(State, Tail);
).

-define(test_fragment_skip_rcv_pubrec, 
		{pubrec, _Packet_Id, Tail} when State#connection_state.test_flag =:= skip_rcv_pubrec ->
			process(State, Tail);
).

-define(test_fragment_skip_rcv_pubrel, 
		{pubrel, _Packet_Id, Tail} when State#connection_state.test_flag =:= skip_rcv_pubrel ->
			process(State, Tail);
).

-define(test_fragment_skip_rcv_pubcomp, 
		{pubcomp, _Packet_Id, Tail} when State#connection_state.test_flag =:= skip_rcv_pubcomp ->
			process(State, Tail);
).

-else.
-define(test_fragment_set_test_flag, ).
-define(test_fragment_break_connection, ).
-define(test_fragment_skip_rcv_publish, ).
-define(test_fragment_skip_rcv_puback, ).
-define(test_fragment_skip_rcv_pubrec, ).
-define(test_fragment_skip_rcv_pubrel, ).
-define(test_fragment_skip_rcv_pubcomp, ).
-endif.
