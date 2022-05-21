%%
%% Copyright (C) 2015-2020 by krasnop@bellsouth.net (Alexei Krasnopolski)
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
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc This module is running unit tests for some modules.

-module(mqtt_srv_dao_tests).

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
	mqtt_mysql_dao:start(server),
	mqtt_mysql_dao:cleanup(server),
	mqtt_mysql_dao:cleanup_users(),

	mqtt_dets_dao:start(server),
	mqtt_dets_dao:cleanup(server),
	mqtt_dets_dao:cleanup_users().

do_stop(_X) ->
%	mqtt_mysql_dao:cleanup(server),
	mqtt_mysql_dao:close(server),

%	mqtt_dets_dao:cleanup(server),	
	mqtt_dets_dao:close(server).	

setup(dets) ->
	mqtt_dets_dao;
setup(mysql) ->
	mqtt_mysql_dao.

cleanup(_, _) ->
	ok.

create(X, Storage) -> {"create [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:save(server, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload lemon 1">>}}),
 	Storage:save(server, #storage_publish{key = #primary_key{client_id = "orange", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload orange 1">>}}),
 	Storage:save(server, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 10101}, document = #publish{topic = "AK", payload = <<"Payload 2">>}}),
 	Storage:save(server, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = #publish{topic = "AK", payload = <<"Payload 3">>}}),
 
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}, callback = {erlang, timestamp}}),
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=1}, callback = {mqtt_client_test, callback}}),
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "apple"}, options = #subscription_options{max_qos=2}, callback = {length}}),
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/#", client_id = "pear"}, options = #subscription_options{max_qos=1}, callback = {length}}),
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+/2", client_id = "plum"}, options = #subscription_options{max_qos=2}, callback = {length}}),
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "/+/December/+", client_id = "orange"}, options = #subscription_options{max_qos=2}, callback = {length}}),
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "orange"}, options = #subscription_options{max_qos=0}, callback = {size}}),
 	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", client_id = "apple"}, options = #subscription_options{max_qos=0}, callback = {length}}),

	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "A", client_id = "apple"}, options = #subscription_options{max_qos=0}, callback = {length}}),
	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "A", client_id = "orange"}, options = #subscription_options{max_qos=0}, callback = {length}}),
	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "B", client_id = "lemon"}, options = #subscription_options{max_qos=0}, callback = {length}}),

 	Storage:save(server, #storage_connectpid{client_id = "lemon", pid = list_to_pid("<0.4.1>")}),
 	Storage:save(server, #storage_connectpid{client_id = "orange", pid = list_to_pid("<0.4.2>")}),
 	Storage:save(server, #storage_connectpid{client_id = "apple", pid = list_to_pid("<0.4.3>")}),

	Storage:save(server, #user{user_id = "guest", password = <<"guest">>, roles = ["USER", "ADMIN"]}),
	Storage:save(server, #user{user_id = "alex", password = <<"aaaaaaa">>, roles = ["USER", "ADMIN", "OWNER"]}),
	Storage:save(server, #user{user_id = "fedor", password = <<"fffffff">>, roles = []}),

	Storage:save(server, #publish{topic = "AK", payload = <<"Payload A">>}),
	Storage:save(server, #publish{topic = "AK/Test", payload = <<"Payload B">>}),
	Storage:save(server, #publish{topic = "AK/Do", payload = <<"Payload C">>}),
	Storage:save(server, #publish{topic = "/Season/December", payload = <<"Payload D">>}),
	Storage:save(server, #publish{topic = "/Season/December", payload = <<"Payload DD">>}),
	Storage:save(server, #publish{topic = "Season/December/01", payload = <<"Payload E">>}),
	Storage:save(server, #publish{topic = "Season/December/02", payload = <<"Payload F">>}),
	Storage:save(server, #publish{topic = "Season/May/21", payload = <<"Payload G">>}),
	
	Storage:save(server, #session_state{client_id = "lemon", end_time = 10, will_publish = #publish{}}),
	Storage:save(server, #session_state{client_id = "orange", end_time = 20, will_publish = #publish{}}),
	Storage:save(server, #session_state{client_id = "apple", end_time = 30, will_publish = #publish{}}),

	R = Storage:get_all(server, {session, "lemon"}),
%	?debug_Fmt("::test:: after create session ~p", [R]),	
	?assertEqual(3, length(R)),
	R1 = Storage:get_all(server, {session, "orange"}),
%	?debug_Fmt("::test:: after create session ~p", [R1]),	
	?assertEqual(1, length(R1)),
	R2 = Storage:get_all(server, topic),
%	?debug_Fmt("::test:: after create topic ~p", [R2]),	
	?assertEqual(11, length(R2)),
	R3 = Storage:get_all(server, session_state),
%	?debug_Fmt("::test:: after create topic ~p", [R3]),	
	?assertEqual(3, length(R3)),

%% 	R2 = dets:match_object(connectpid_db, #storage_connectpid{_ = '_'}),
%% %	?debug_Fmt("::test:: after create ~p", [R2]),	
%% 	?assertEqual(3, length(R2)),
	?passed
end}.

read(X, Storage) -> {"read [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get(server, #primary_key{client_id = "lemon", packet_id = 101}),
%	?debug_Fmt("::test:: read returns R ~120p", [R]),	
 	?assertEqual(#publish{topic = "AK",payload = <<"Payload lemon 1">>}, R#storage_publish.document),
	Ra = Storage:get(server, #primary_key{client_id = "plum", packet_id = 101}),
%	?debug_Fmt("::test:: read returns Ra ~120p", [Ra]),	
 	?assertEqual(undefined, Ra),
 	R1 = Storage:get(server, #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}),
%	?debug_Fmt("::test:: read returns R1 ~120p", [R1]),	
 	?assertEqual([#storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}, callback = {erlang, timestamp}}], R1),
 	R1a = Storage:get(server, #subs_primary_key{topicFilter = "AK_Test", client_id = "lemon"}),
%	?debug_Fmt("::test:: read returns R1a ~120p", [R1a]),	
 	?assertEqual(undefined, R1a),
 	R2 = Storage:get(server, {client_id, "apple"}),
%	?debug_Fmt("::test:: read returns R2 ~120p", [R2]),	
 	?assertEqual(list_to_pid("<0.4.3>"), R2),
 	R2a = Storage:get(server, {client_id, "plum"}),
%	?debug_Fmt("::test:: read returns R2a ~120p", [R2a]),	
 	?assertEqual(undefined, R2a),
	R3 = Storage:get(server, {user_id, "alex"}),
	?debug_Fmt("::test:: read get User Info returns R3 ~120p", [R3]),	
	?assertEqual(#{password => crypto:hash(md5, <<"aaaaaaa">>), roles => ["USER", "ADMIN", "OWNER"]}, R3),
	R3a = Storage:get(server, {user_id, <<"alex">>}),
	?debug_Fmt("::test:: read get User Info returns R3a ~120p", [R3a]),	
	?assertEqual(#{password => crypto:hash(md5, <<"aaaaaaa">>), roles => ["USER", "ADMIN", "OWNER"]}, R3a),
	R4a = Storage:get(server, {topic, "AK"}),
%	?debug_Fmt("::test:: read returns R4a ~120p", [R4a]),	
	?assertEqual([#publish{topic = "AK", payload = <<"Payload A">>}], R4a),
	R4b = Storage:get(server, {topic, "AK/#"}),
	?debug_Fmt("::test:: read returns R4b ~120p", [R4b]),	
	?assertEqual(2, length(R4b)),
	?assert(lists:member(#publish{topic="AK/Test",payload= <<"Payload B">>}, R4b)),
	?assert(lists:member(#publish{topic="AK/Do",payload= <<"Payload C">>}, R4b)),
	R4c = Storage:get(server, {topic, "Season/+/#"}),
%	?debug_Fmt("::test:: read returns R4c ~120p", [R4c]),	
	?assertEqual(3, length(R4c)),
	?assert(lists:member(#publish{topic = "Season/December/01", payload = <<"Payload E">>}, R4c)),
	?assert(lists:member(#publish{topic = "Season/December/02", payload = <<"Payload F">>}, R4c)),
	?assert(lists:member(#publish{topic = "Season/May/21", payload = <<"Payload G">>}, R4c)),
	R4d = Storage:get(server, {topic, "/Season/December"}),
%	?debug_Fmt("::test:: read returns R4d ~120p", [R4d]),	
	?assertEqual(2, length(R4d)),
	?assert(lists:member(#publish{topic = "/Season/December", payload = <<"Payload D">>}, R4d)),
	?assert(lists:member(#publish{topic = "/Season/December", payload = <<"Payload DD">>}, R4d)),
	
	R4f = Storage:get(server, {session_client_id, "lemon"}),
	?assertMatch(#session_state{client_id="lemon", end_time=10}, R4f),
	?passed
end}.

extract_topic(X, Storage) -> {"extract topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get_client_topics(server, "orange"),
	?debug_Fmt("::test:: extract_topic returns ~120p", [R]),
	?assertEqual(4, length(R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="+/December",client_id="orange"},options=#subscription_options{max_qos=0},callback={size}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="+/December/+",shareName="A",client_id="orange"},options=#subscription_options{max_qos=0},callback={length}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="/+/December/+",client_id="orange"},options=#subscription_options{max_qos=2},callback={length}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="Winter/+",client_id="orange"},options=#subscription_options{max_qos=1},callback={mqtt_client_test, callback}}, R)),
	?passed
end}.

extract_matched_topic(X, Storage) -> {"extract matched topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get_matched_topics(server, #subs_primary_key{topicFilter = "Winter/December", client_id = "orange"}),
	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(2, length(R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="+/December",client_id="orange"},options=#subscription_options{max_qos=0},callback={size}}, R)),
	?assert(lists:member(#storage_subscription{key=#subs_primary_key{topicFilter="Winter/+",client_id="orange"},options=#subscription_options{max_qos=1},callback={mqtt_client_test, callback}}, R)),

	R1 = Storage:get_matched_topics(server, "Winter/December"),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),	
	?assertEqual(4, length(R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"apple"},#subscription_options{max_qos=2},{length}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},#subscription_options{max_qos=0},{size}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/#",undefined,"pear"},#subscription_options{max_qos=1},{length}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},#subscription_options{max_qos=1},{mqtt_client_test, callback}}, R1)),
	?passed
end}.

extract_matched_shared_topic(X, Storage) -> {"extract matched shared topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get_matched_shared_topics(server, "Year/December/12"),
%	?debug_Fmt("::test:: extract_matched_shared_topic returns ~120p", [R]),	
	?assertEqual(3, length(R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December/+","B","lemon"},#subscription_options{max_qos=0},{length}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December/+","A","orange"},#subscription_options{max_qos=0},{length}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December/+","A","apple"},#subscription_options{max_qos=0},{length}}, R)),
	?passed
end}.

read_all(X, Storage) -> {"read all [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get_all(server, {session, "lemon"}),
%	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(3, length(R)),
	R1 = Storage:get_all(server, session_state),
	?assertEqual(3, length(R1)),
	?passed
end}.
	
update(X, Storage) -> {"update [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:save(server, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "", payload = <<>>}}),
	R = Storage:get(server, #primary_key{client_id = "lemon", packet_id = 101}),
%	?debug_Fmt("::test:: read returns ~120p", [R]),
	?assertEqual(#publish{topic = "",payload = <<>>}, R#storage_publish.document),
	
	Storage:save(server, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = undefined}),
	R1 = Storage:get(server, #primary_key{client_id = "lemon", packet_id = 201}),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),
	?assertEqual(undefined, R1#storage_publish.document),
	
	Storage:save(server, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=2}, callback = {erlang, binary_to_list}}),
	[R2] = Storage:get(server, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}),
%	?debug_Fmt("::test:: read returns ~120p", [R2]),
	?assertEqual(2, R2#storage_subscription.options#subscription_options.max_qos),
	?assertEqual({erlang, binary_to_list}, R2#storage_subscription.callback),
	
	Storage:save(server, #session_state{client_id = "apple", end_time = 300, will_publish = #publish{topic="Will Topic"}}),
	R3 = Storage:get(server, {session_client_id, "apple"}),
	?assertMatch(#session_state{client_id = "apple", end_time = 300, will_publish = #publish{topic="Will Topic"}}, R3),
	?passed
end}.
	
delete(X, Storage) -> {"delete [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:remove(server, #primary_key{client_id = "lemon", packet_id = 101}),
	R = Storage:get(server, #primary_key{client_id = "lemon", packet_id = 101}),
%	?debug_Fmt("::test:: after delete ~p", [R]),	
	?assertEqual(undefined, R),

	Storage:remove(server, {user_id, "fedor"}),
	R1 = Storage:get(server, {user_id, "fedor"}),
%	?debug_Fmt("::test:: after delete ~p", [R1]),	
	?assertEqual(undefined, R1),
	
	Storage:remove(server, {topic,"/Season/December"}),
	R2 = Storage:get(server, {topic,"/Season/December"}),
%	?debug_Fmt("::test:: after delete ~p", [R2]),	
	?assertEqual([], R2),

	Storage:remove(server, {session_client_id,"lemon"}),
	R3 = Storage:get(server, {session_client_id,"lemon"}),
%	?debug_Fmt("::test:: after delete ~p", [R3]),	
	?assertEqual(undefined, R3),

	?passed
end}.
