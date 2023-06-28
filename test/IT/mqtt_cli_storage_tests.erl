%%
%% Copyright (C) 2015-2023 by krasnop@bellsouth.net (Alexei Krasnopolski)
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

%% @hidden
%% @since 2016-09-08
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_cli_storage_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
-include("test.hrl").

%%
%% Import modules
%%

%%
%% Exported Functions
%%
-export([
]).

%%
%% API Functions
%%

dets_dao_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				{dets, fun create/2},
				{mysql, fun create/2},
 				{dets, fun create_srv/2},
 				{mysql, fun create_srv/2},
				{dets, fun read/2},
				{mysql, fun read/2},
 				{dets, fun read_srv/2},
 				{mysql, fun read_srv/2},
				{dets, fun extract_topic/2},
				{mysql, fun extract_topic/2},
				{dets, fun extract_matched_topic/2},
				{mysql, fun extract_matched_topic/2},
				{dets, fun read_all/2},
				{mysql, fun read_all/2},
				{dets, fun update/2},
				{mysql, fun update/2},
				{dets, fun delete/2},
				{mysql, fun delete/2}
			]
		}
	 }
	].

do_start() ->
	lager:start(),
	mqtt_mysql_storage:start(client),
	mqtt_mysql_storage:cleanup(client),
	mqtt_mysql_storage:start(server),
	mqtt_mysql_storage:cleanup(server),
	mqtt_mysql_storage:user(clean, undefined),

	mqtt_dets_storage:start(client),
	mqtt_dets_storage:cleanup(client),
	mqtt_dets_storage:start(server),
	mqtt_dets_storage:cleanup(server),
	mqtt_dets_storage:user(clean, undefined).

do_stop(_X) ->
%	mqtt_mysql_storage:cleanup(client),
	mqtt_mysql_storage:close(client),

%	mqtt_dets_storage:cleanup(client),	
%	mqtt_dets_storage:cleanup(server),	
	mqtt_dets_storage:close(client).	

setup(dets) ->
	mqtt_dets_storage;
setup(mysql) ->
	mqtt_mysql_storage.

cleanup(_, _) ->
	ok.

create(X, Storage) -> {"create [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload lemon 1">>}}, client),
 	Storage:session(save, #storage_publish{key = #primary_key{client_id = "orange", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload orange 1">>}}, client),
 	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 10101}, document = #publish{topic = "AK", payload = <<"Payload 2">>}}, client),
 	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = #publish{topic = "AK", payload = <<"Payload 3">>}}, client),
 
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}}, client),
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=1}}, client),
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "apple"}, options = #subscription_options{max_qos=2}}, client),
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/#", client_id = "pear"}, options = #subscription_options{max_qos=1}}, client),
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+/2", client_id = "plum"}, options = #subscription_options{max_qos=2}}, client),
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "/+/December/+", client_id = "orange"}, options = #subscription_options{max_qos=2}}, client),
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "orange"}, options = #subscription_options{max_qos=0}}, client),
 	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "A", client_id = "apple"}, options = #subscription_options{max_qos=0}}, client),
 
 	Storage:connect_pid(save, #storage_connectpid{client_id = "lemon", pid = list_to_pid("<0.4.1>")}, client),
 	Storage:connect_pid(save, #storage_connectpid{client_id = "orange", pid = list_to_pid("<0.4.2>")}, client),
 	Storage:connect_pid(save, #storage_connectpid{client_id = "apple", pid = list_to_pid("<0.4.3>")}, client),

	R = Storage:session(get_all, "lemon", client),
%	?debug_Fmt("::test:: after create session ~p", [R]),	
	?assertEqual(3, length(R)),
	R1 = Storage:session(get_all, "orange", client),
%	?debug_Fmt("::test:: after create session ~p", [R1]),	
	?assertEqual(1, length(R1)),
	R2 = Storage:session(get_all, all, client),
%	?debug_Fmt("::test:: after create session ~p", [R2]),	
	?assertEqual(4, length(R2)),
	R3 = Storage:subscription(get_all, topic, client),
%	?debug_Fmt("::test:: after create topic ~p", [R3]),	
	?assertEqual(8, length(R3)),

	R4 = Storage:connect_pid(get_all, undefined, client),
%% %	?debug_Fmt("::test:: after create ~p", [R4]),	
 	?assertEqual(3, length(R4)),
	?passed
end}.

create_srv(X, Storage) -> {"create srv [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:session_state(save, #session_state{client_id = "lemon", session_expiry_interval = 10, end_time = 1000, will_publish = #publish{}}),
	Storage:session_state(save, #session_state{client_id = "orange", session_expiry_interval = 10, end_time = 1000, will_publish = #publish{}}),
	Storage:session_state(save, #session_state{client_id = "apple", session_expiry_interval = 10, end_time = 1000, will_publish = #publish{}}),
	Storage:session_state(save, #session_state{client_id = "pear", session_expiry_interval = 10, end_time = 1000, will_publish = #publish{}}),
	R = Storage:session_state(get_all, undefined),
%	?debug_Fmt("::test:: after create session ~p", [R]),	
	?assertEqual(4, length(R)),
	
	Storage:retain(save, #publish{topic = "AKtest", dup=0, qos=2, payload= <<"Payload">>, dir=out, last_sent=publish, expiration_time=1009}),
	Storage:retain(save, #publish{topic = "Winter/+", dup=1, qos=0, payload= <<"Payload">>, dir=in, last_sent=publish, expiration_time=1109}),
	Storage:retain(save, #publish{topic = "+/December", dup=0, qos=1, payload= <<"Payload">>, dir=in, last_sent=pubrec, expiration_time=1029}),
	Storage:retain(save, #publish{topic = "Winter/#", dup=1, qos=1, payload= <<"Payload">>, dir=out, last_sent=pubrel, expiration_time=10099}),
	Storage:retain(save, #publish{topic = "Winter/+/2", dup=0, qos=0, payload= <<"Payload">>, dir=out, last_sent=publish, expiration_time=1909}),
	Storage:retain(save, #publish{topic = "/+/December/+", dup=1, qos=0, payload= <<"Payload">>, dir=in, last_sent=pubrel, expiration_time=11009}),
	Storage:retain(save, #publish{topic = "+/December", dup=1, qos=1, payload= <<"Payload">>, dir=out, last_sent=pubcomp, expiration_time=10095}),
	Storage:retain(save, #publish{topic = "+/December/+", dup=0, qos=1, payload= <<"Payload">>, dir=out, last_sent=publish, expiration_time=10069}),
	R1 = Storage:retain(get_all, undefined),
%	?debug_Fmt("::test:: after create session ~p", [R1]),	
	?assertEqual(8, length(R1)),
	
	Storage:user(save, #user{user_id="guest", password= <<"guest">>, roles= ["USER"]}),
	Storage:user(save, #user{user_id="alex", password= <<"e3fs4578">>, roles= ["USER", "ADMIN"]}),
	Storage:user(save, #user{user_id="tom", password= <<"e3fs4578">>, roles= ["USER", "ADMIN"]}),
	Storage:user(save, #user{user_id="sam", password= <<"e3fs4578">>, roles= ["USER", "ADMIN"]}),
	Storage:user(save, #user{user_id="john", password= <<"e3fs4578">>, roles= ["USER", "ADMIN"]}),
	R2 = Storage:user(get_all, undefined),
	?debug_Fmt("::test:: after create users ~p", [R2]),	
	?assertEqual(5, length(R2)),

	?passed
end}.

read(X, Storage) -> {"read [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 101}, client),
%	?debug_Fmt("::test:: read returns R ~120p", [R]),	
 	?assertEqual(#publish{topic = "AK",payload = <<"Payload lemon 1">>}, R#storage_publish.document),
	Ra = Storage:session(get, #primary_key{client_id = "plum", packet_id = 101}, client),
%	?debug_Fmt("::test:: read returns Ra ~120p", [Ra]),	
 	?assertEqual(undefined, Ra),
 	[R1] = Storage:subscription(get, #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, client),
%	?debug_Fmt("::test:: read returns R1 ~120p", [R1]),	
 	?assertEqual(#storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}}, R1),
 	R1a = Storage:subscription(get, #subs_primary_key{topicFilter = "AK_Test", client_id = "lemon"}, client),
%	?debug_Fmt("::test:: read returns R1a ~120p", [R1a]),	
 	?assertEqual([], R1a),
 	R2 = Storage:connect_pid(get, "apple", client),
%	?debug_Fmt("::test:: read returns R2 ~120p", [R2]),	
 	?assertEqual(list_to_pid("<0.4.3>"), R2),
 	R2a = Storage:connect_pid(get, "plum", client),
%	?debug_Fmt("::test:: read returns R2a ~120p", [R2a]),	
 	?assertEqual(undefined, R2a),
	?passed
end}.

read_srv(X, Storage) -> {"read srv [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:session_state(get, "lemon"),
%	?debug_Fmt("::test:: read all returns R ~120p", [R]),	
	?assertEqual(#session_state{client_id = "lemon", session_expiry_interval = 10, end_time = 1000, will_publish = #publish{}}, R),

	?passed
end}.

extract_topic(X, Storage) -> {"extract topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:subscription(get_client_topics, "orange", client),
	?debug_Fmt("::test:: read returns ~120p", [R]),
	?assertEqual(3, length(R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},
                                             {subscription_options,0,0,0,0,0}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"/+/December/+",undefined,"orange"},
                                             {subscription_options,2,0,0,0,0}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},
                                             {subscription_options,1,0,0,0,0}}, R)),
	?passed
end}.
	
extract_matched_topic(X, Storage) -> {"extract matched topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:subscription(get_matched_topics, #subs_primary_key{topicFilter = "Winter/December", client_id = "orange"}, client),
	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(2, length(R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},
                                             {subscription_options,1,0,0,0,0}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},
                                             {subscription_options,0,0,0,0,0}}, R)),

	R1 = Storage:subscription(get_matched_topics, "Winter/December", client),
	?debug_Fmt("::test:: read returns ~120p", [R1]),	
	?assertEqual(4, length(R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"apple"},#subscription_options{max_qos=2}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},#subscription_options{max_qos=0}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/#",undefined,"pear"},#subscription_options{max_qos=1}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},#subscription_options{max_qos=1}}, R1)),
	?passed
end}.

read_all(X, Storage) -> {"read all [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:session(get_all, "lemon", client),
%	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(3, length(R)),
	?passed
end}.
	
update(X, Storage) -> {"update [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "", payload = <<>>}}, client),
	R = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 101}, client),
%	?debug_Fmt("::test:: read returns ~120p", [R]),
	?assertEqual(#publish{topic = "",payload = <<>>}, R#storage_publish.document),
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = undefined}, client),
	R1 = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 201}, client),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),
	?assertEqual(undefined, R1#storage_publish.document),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=2}}, client),
	[R2] = Storage:subscription(get, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, client),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),
	?assertEqual(2, R2#storage_subscription.options#subscription_options.max_qos),
	?passed
end}.
	
delete(X, Storage) -> {"delete [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:session(remove, #primary_key{client_id = "lemon", packet_id = 101}, client),
	R = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 101}, client),
%	?debug_Fmt("::test:: after delete ~p", [R]),	
	?assertEqual(undefined, R),
	
	Storage:subscription(remove, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, client),
	R1 = Storage:subscription(get, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, client),
%	?debug_Fmt("::test:: after delete ~p", [R1]),	
	?assertEqual([], R1),
	R2 = Storage:subscription(get_all, undefined, client),	
%	?debug_Fmt("::test:: read returns ~120p", [R2]),	
	?assertEqual(7, length(R2)),
	
	Storage:subscription(remove, #subs_primary_key{client_id = "apple", _='_'}, client),
	R3 = Storage:subscription(get, #subs_primary_key{topicFilter = "+/December", client_id = "apple"}, client),
%	?debug_Fmt("::test:: after delete ~p", [R3]),
	?assertEqual([], R3),
	R4 = Storage:subscription(get_all, undefined, client),	
%	?debug_Fmt("::test:: read returns ~120p", [R4]),	
	?assertEqual(5, length(R4)),

	Storage:cleanup("orange", client),
	R5 = Storage:subscription(get_all, undefined, client),
	?debug_Fmt("::test:: after cleanup ~p", [R5]),	
%%	?assertEqual(undefined, R5),
	?assertEqual(3, length(R5)),
	R6 = Storage:session(get_all, "orange", client),	
	?debug_Fmt("::test:: read returns ~120p", [R6]),	
	?assertEqual(0, length(R6)),

	?passed
end}.
