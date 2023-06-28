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

-module(mqtt_srv_storage_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").
-include_lib("mqtt.hrl").
-include("test.hrl").

%%
%% Import modules
%%
%-import(helper_common, []).

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
				{dets, fun read/2},
				{mysql, fun read/2},
				{dets, fun extract_topic/2},
				{mysql, fun extract_topic/2},
				{dets, fun extract_matched_topic/2},
				{mysql, fun extract_matched_topic/2},
				{dets, fun extract_matched_shared_topic/2},
				{mysql, fun extract_matched_shared_topic/2},
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
	mqtt_mysql_storage:start(server),
	mqtt_mysql_storage:cleanup(server),
	mqtt_mysql_storage:user(clean, undefined),

	mqtt_dets_storage:start(server),
	mqtt_dets_storage:cleanup(server),
	mqtt_mysql_storage:user(clean, undefined).

do_stop(_X) ->
%	mqtt_mysql_storage:cleanup(server),
	mqtt_mysql_storage:close(server),

%	mqtt_dets_storage:cleanup(server),	
	mqtt_dets_storage:close(server).	

setup(dets) ->
	mqtt_dets_storage;
setup(mysql) ->
	mqtt_mysql_storage.

cleanup(_, _) ->
	ok.

create(X, Storage) -> {"create [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload lemon 1">>}}, server),
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "orange", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload orange 1">>}}, server),
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 10101}, document = #publish{topic = "AK", payload = <<"Payload 2">>}}, server),
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = #publish{topic = "AK", payload = <<"Payload 3">>}}, server),

	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=1}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "apple"}, options = #subscription_options{max_qos=2}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/#", client_id = "pear"}, options = #subscription_options{max_qos=1}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+/2", client_id = "plum"}, options = #subscription_options{max_qos=2}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "/+/December/+", client_id = "orange"}, options = #subscription_options{max_qos=2}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "orange"}, options = #subscription_options{max_qos=0}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", client_id = "apple"}, options = #subscription_options{max_qos=0}}, server),

	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "A", client_id = "apple"}, options = #subscription_options{max_qos=0}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "A", client_id = "orange"}, options = #subscription_options{max_qos=0}}, server),
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "B", client_id = "lemon"}, options = #subscription_options{max_qos=0}}, server),

 	Storage:connect_pid(save, #storage_connectpid{client_id = "lemon", pid = list_to_pid("<0.4.1>")}, server),
 	Storage:connect_pid(save, #storage_connectpid{client_id = "orange", pid = list_to_pid("<0.4.2>")}, server),
 	Storage:connect_pid(save, #storage_connectpid{client_id = "apple", pid = list_to_pid("<0.4.3>")}, server),

	Storage:user(save, #user{user_id = "guest", password = <<"guest">>, roles = ["USER", "ADMIN"]}),
	Storage:user(save, #user{user_id = "alex", password = <<"aaaaaaa">>, roles = ["USER", "ADMIN", "OWNER"]}),
	Storage:user(save, #user{user_id = "fedor", password = <<"fffffff">>, roles = []}),

	Storage:retain(save, #publish{topic = "AK", payload = <<"Payload A">>}),
	Storage:retain(save, #publish{topic = "AK/Test", payload = <<"Payload B">>}),
	Storage:retain(save, #publish{topic = "AK/Do", payload = <<"Payload C">>}),
	Storage:retain(save, #publish{topic = "/Season/December", payload = <<"Payload D">>}),
	Storage:retain(save, #publish{topic = "/Season/December", payload = <<"Payload DD">>}),
	Storage:retain(save, #publish{topic = "Season/December/01", payload = <<"Payload E">>}),
	Storage:retain(save, #publish{topic = "Season/December/02", payload = <<"Payload F">>}),
	Storage:retain(save, #publish{topic = "Season/May/21", payload = <<"Payload G">>}),
	
	Storage:session_state(save, #session_state{client_id = "lemon", end_time = 10, will_publish = #publish{}}),
	Storage:session_state(save, #session_state{client_id = "orange", end_time = 20, will_publish = #publish{}}),
	Storage:session_state(save, #session_state{client_id = "apple", end_time = 30, will_publish = #publish{}}),

	R = Storage:session(get_all, "lemon", server),
%	?debug_Fmt("::test:: after create session ~p", [R]),	
	?assertEqual(3, length(R)),
	R1 = Storage:session(get_all, "orange", server),
%	?debug_Fmt("::test:: after create session ~p", [R1]),	
	?assertEqual(1, length(R1)),
	R2 = Storage:subscription(get_all, topic, server),
%	?debug_Fmt("::test:: after create topic ~p", [R2]),	
	?assertEqual(11, length(R2)),
	R3 = Storage:session_state(get_all, server),
%	?debug_Fmt("::test:: after create topic ~p", [R3]),	
	?assertEqual(3, length(R3)),

	R4 = Storage:connect_pid(get_all, undefined, server),
%% %	?debug_Fmt("::test:: after create ~p", [R4]),	
	?assertEqual(3, length(R4)),

	?passed
end}.

read(X, Storage) -> {"read [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 101}, server),
%	?debug_Fmt("::test:: read returns R ~120p", [R]),	
 	?assertEqual(#publish{topic = "AK",payload = <<"Payload lemon 1">>}, R#storage_publish.document),
	Ra = Storage:session(get, #primary_key{client_id = "plum", packet_id = 101}, server),
%	?debug_Fmt("::test:: read returns Ra ~120p", [Ra]),	
 	?assertEqual(undefined, Ra),
 	R1 = Storage:subscription(get, #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, server),
%	?debug_Fmt("::test:: read returns R1 ~120p", [R1]),	
 	?assertEqual([#storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}}], R1),
 	R1a = Storage:subscription(get, #subs_primary_key{topicFilter = "AK_Test", client_id = "lemon"}, server),
%	?debug_Fmt("::test:: read returns R1a ~120p", [R1a]),	
 	?assertEqual([], R1a),
 	R2 = Storage:connect_pid(get, "apple", server),
%	?debug_Fmt("::test:: read returns R2 ~120p", [R2]),	
 	?assertEqual(list_to_pid("<0.4.3>"), R2),
 	R2a = Storage:connect_pid(get, <<"apple">>, server),
%	?debug_Fmt("::test:: read returns R2 ~120p", [R2a]),	
 	?assertEqual(list_to_pid("<0.4.3>"), R2a),
 	R2b = Storage:connect_pid(get, "plum", server),
%	?debug_Fmt("::test:: read returns R2a ~120p", [R2b]),	
 	?assertEqual(undefined, R2b),
	R3 = Storage:user(get, "alex"),
	?debug_Fmt("::test:: read get User Info returns R3 ~120p", [R3]),	
	?assertEqual(#{password => list_to_binary(mqtt_data:binary_to_hex(crypto:hash(md5, <<"aaaaaaa">>))), roles => ["USER", "ADMIN", "OWNER"]}, R3),
	R3a = Storage:user(get, <<"alex">>),
	?debug_Fmt("::test:: read get User Info returns R3a ~120p", [R3a]),	
	?assertEqual(#{password => list_to_binary(mqtt_data:binary_to_hex(crypto:hash(md5, <<"aaaaaaa">>))), roles => ["USER", "ADMIN", "OWNER"]}, R3a),
	R4a = Storage:retain(get, "AK"),
%	?debug_Fmt("::test:: read returns R4a ~120p", [R4a]),	
	?assertEqual([#publish{topic = "AK", payload = <<"Payload A">>}], R4a),
	R4b = Storage:retain(get, "AK/#"),
	?debug_Fmt("::test:: read returns R4b ~120p", [R4b]),	
	?assertEqual(2, length(R4b)),
	?assert(lists:member(#publish{topic="AK/Test",payload= <<"Payload B">>}, R4b)),
	?assert(lists:member(#publish{topic="AK/Do",payload= <<"Payload C">>}, R4b)),
	R4c = Storage:retain(get, "Season/+/#"),
%	?debug_Fmt("::test:: read returns R4c ~120p", [R4c]),	
	?assertEqual(3, length(R4c)),
	?assert(lists:member(#publish{topic = "Season/December/01", payload = <<"Payload E">>}, R4c)),
	?assert(lists:member(#publish{topic = "Season/December/02", payload = <<"Payload F">>}, R4c)),
	?assert(lists:member(#publish{topic = "Season/May/21", payload = <<"Payload G">>}, R4c)),
	R4d = Storage:retain(get, "/Season/December"),
%	?debug_Fmt("::test:: read returns R4d ~120p", [R4d]),	
	?assertEqual(2, length(R4d)),
	?assert(lists:member(#publish{topic = "/Season/December", payload = <<"Payload D">>}, R4d)),
	?assert(lists:member(#publish{topic = "/Season/December", payload = <<"Payload DD">>}, R4d)),
	
	R4f = Storage:session_state(get, "lemon"),
	?assertMatch(#session_state{client_id="lemon", end_time=10}, R4f),
	?passed
end}.

extract_topic(X, Storage) -> {"extract topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:subscription(get_client_topics, "orange", server),
	?debug_Fmt("::test:: extract_topic returns ~120p", [R]),
	?assertEqual(4, length(R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="+/December",client_id="orange"},options=#subscription_options{max_qos=0}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="+/December/+",shareName="A",client_id="orange"},options=#subscription_options{max_qos=0}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="/+/December/+",client_id="orange"},options=#subscription_options{max_qos=2}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="Winter/+",client_id="orange"},options=#subscription_options{max_qos=1}}, R)),
	?passed
end}.

extract_matched_topic(X, Storage) -> {"extract matched topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:subscription(get_matched_topics, #subs_primary_key{topicFilter = "Winter/December", client_id = "orange"}, server),
	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(2, length(R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="+/December",client_id="orange"},options=#subscription_options{max_qos=0}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="Winter/+",client_id="orange"},options=#subscription_options{max_qos=1}}, R)),

	R1 = Storage:subscription(get_matched_topics, "Winter/December", server),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),
	?assertEqual(4, length(R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"apple"},#subscription_options{max_qos=2}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},#subscription_options{max_qos=0}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/#",undefined,"pear"},#subscription_options{max_qos=1}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},#subscription_options{max_qos=1}}, R1)),
	?passed
end}.

extract_matched_shared_topic(X, Storage) -> {"extract matched shared topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:subscription(get_matched_shared_topics, "Year/December/12", server),
%	?debug_Fmt("::test:: extract_matched_shared_topic returns ~120p", [R]),	
	?assertEqual(3, length(R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December/+","B","lemon"},#subscription_options{max_qos=0}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December/+","A","orange"},#subscription_options{max_qos=0}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December/+","A","apple"},#subscription_options{max_qos=0}}, R)),
	?passed
end}.

read_all(X, Storage) -> {"read all [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:session(get_all, "lemon", server),
%	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(3, length(R)),
	R1 = Storage:session_state(get_all, server),
	?assertEqual(3, length(R1)),
	?passed
end}.
	
update(X, Storage) -> {"update [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "", payload = <<>>}}, server),
	R = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 101}, server),
%	?debug_Fmt("::test:: read returns ~120p", [R]),
	?assertEqual(#publish{topic = "",payload = <<>>}, R#storage_publish.document),
	
	Storage:session(save, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = undefined}, server),
	R1 = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 201}, server),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),
	?assertEqual(undefined, R1#storage_publish.document),
	
	Storage:subscription(save, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=2}}, server),
	[R2] = Storage:subscription(get, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, server),
%	?debug_Fmt("::test:: read returns ~120p", [R2]),
	?assertEqual(2, R2#storage_subscription.options#subscription_options.max_qos),
	
	Storage:session_state(save, #session_state{client_id = "apple", end_time = 300, will_publish = #publish{topic="Will Topic"}}),
	R3 = Storage:session_state(get, "apple"),
	?assertMatch(#session_state{client_id = "apple", end_time = 300, will_publish = #publish{topic="Will Topic"}}, R3),
	?passed
end}.
	
delete(X, Storage) -> {"delete [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:session(remove, #primary_key{client_id = "lemon", packet_id = 101}, server),
	R = Storage:session(get, #primary_key{client_id = "lemon", packet_id = 101}, server),
%	?debug_Fmt("::test:: after delete ~p", [R]),	
	?assertEqual(undefined, R),

	Storage:subscription(remove, #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, server),
	Storage:subscription(remove, #subs_primary_key{topicFilter = "+/December/+", shareName = "A", client_id = "apple"}, server),
	R0 = Storage:subscription(get_all, topic, server),
	?assertEqual(9, length(R0)),

	Storage:user(remove, "fedor"),
	R1 = Storage:user(get, "fedor"),
%	?debug_Fmt("::test:: after delete ~p", [R1]),	
	?assertEqual(undefined, R1),
	
	Storage:retain(remove, "/Season/December"),
	R2 = Storage:retain(get, "/Season/December"),
%	?debug_Fmt("::test:: after delete ~p", [R2]),	
	?assertEqual([], R2),

	Storage:session_state(remove, "lemon"),
	R3 = Storage:session_state(get, "lemon"),
%	?debug_Fmt("::test:: after delete ~p", [R3]),	
	?assertEqual(undefined, R3),

	?passed
end}.
