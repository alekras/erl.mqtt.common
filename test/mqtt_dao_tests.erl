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

-module(mqtt_dao_tests).

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
	mqtt_mysql_dao:start(client),
	mqtt_mysql_dao:cleanup(client),

	mqtt_dets_dao:start(client),
	mqtt_dets_dao:cleanup(client).


do_stop(_X) ->
	mqtt_mysql_dao:cleanup(client),
	mqtt_mysql_dao:close(client),

	mqtt_dets_dao:cleanup(client),	
	mqtt_dets_dao:close(client).	

setup(dets) ->
	mqtt_dets_dao;
setup(mysql) ->
	mqtt_mysql_dao.

cleanup(_, _) ->
	ok.

create(X, Storage) -> {"create [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:save(client, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload lemon 1">>}}),
 	Storage:save(client, #storage_publish{key = #primary_key{client_id = "orange", packet_id = 101}, document = #publish{topic = "AK", payload = <<"Payload orange 1">>}}),
 	Storage:save(client, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 10101}, document = #publish{topic = "AK", payload = <<"Payload 2">>}}),
 	Storage:save(client, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = #publish{topic = "AK", payload = <<"Payload 3">>}}),
 
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}, callback = {erlang, timestamp}}),
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=1}, callback = {mqtt_client_test, callback}}),
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "apple"}, options = #subscription_options{max_qos=2}, callback = {length}}),
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/#", client_id = "pear"}, options = #subscription_options{max_qos=1}, callback = {length}}),
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+/2", client_id = "plum"}, options = #subscription_options{max_qos=2}, callback = {length}}),
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "/+/December/+", client_id = "orange"}, options = #subscription_options{max_qos=2}, callback = {length}}),
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December", client_id = "orange"}, options = #subscription_options{max_qos=0}, callback = {size}}),
 	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "+/December/+", shareName = "A", client_id = "apple"}, options = #subscription_options{max_qos=0}, callback = {length}}),
 
 	Storage:save(client, #storage_connectpid{client_id = "lemon", pid = list_to_pid("<0.4.1>")}),
 	Storage:save(client, #storage_connectpid{client_id = "orange", pid = list_to_pid("<0.4.2>")}),
 	Storage:save(client, #storage_connectpid{client_id = "apple", pid = list_to_pid("<0.4.3>")}),

	R = Storage:get_all(client, {session, "lemon"}),
%	?debug_Fmt("::test:: after create session ~p", [R]),	
	?assertEqual(3, length(R)),
	R1 = Storage:get_all(client, {session, "orange"}),
%	?debug_Fmt("::test:: after create session ~p", [R1]),	
	?assertEqual(1, length(R1)),
	R2 = Storage:get_all(client, topic),
%	?debug_Fmt("::test:: after create topic ~p", [R2]),	
	?assertEqual(8, length(R2)),

%% 	R2 = dets:match_object(connectpid_db, #storage_connectpid{_ = '_'}),
%% %	?debug_Fmt("::test:: after create ~p", [R2]),	
%% 	?assertEqual(3, length(R2)),
	?passed
end}.

read(X, Storage) -> {"read [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get(client, #primary_key{client_id = "lemon", packet_id = 101}),
%	?debug_Fmt("::test:: read returns R ~120p", [R]),	
 	?assertEqual(#publish{topic = "AK",payload = <<"Payload lemon 1">>}, R#storage_publish.document),
	Ra = Storage:get(client, #primary_key{client_id = "plum", packet_id = 101}),
%	?debug_Fmt("::test:: read returns Ra ~120p", [Ra]),	
 	?assertEqual(undefined, Ra),
 	[R1] = Storage:get(client, #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}),
%	?debug_Fmt("::test:: read returns R1 ~120p", [R1]),	
 	?assertEqual(#storage_subscription{key = #subs_primary_key{topicFilter = "AKtest", client_id = "lemon"}, options = #subscription_options{max_qos=0}, callback = {erlang, timestamp}}, R1),
 	R1a = Storage:get(client, #subs_primary_key{topicFilter = "AK_Test", client_id = "lemon"}),
%	?debug_Fmt("::test:: read returns R1a ~120p", [R1a]),	
 	?assertEqual(undefined, R1a),
 	R2 = Storage:get(client, {client_id, "apple"}),
%	?debug_Fmt("::test:: read returns R2 ~120p", [R2]),	
 	?assertEqual(list_to_pid("<0.4.3>"), R2),
 	R2a = Storage:get(client, {client_id, "plum"}),
%	?debug_Fmt("::test:: read returns R2a ~120p", [R2a]),	
 	?assertEqual(undefined, R2a),
	?passed
end}.

extract_topic(X, Storage) -> {"extract topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get_client_topics(client, "orange"),
	?debug_Fmt("::test:: read returns ~120p", [R]),
	?assertEqual(3, length(R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},
                                             {subscription_options,0,0,0,0,0},
                                             {size}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"/+/December/+",undefined,"orange"},
                                             {subscription_options,2,0,0,0,0},
                                             {length}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},
                                             {subscription_options,1,0,0,0,0},
                                             {mqtt_client_test,callback}}, R)),
	?passed
end}.
	
extract_matched_topic(X, Storage) -> {"extract matched topic [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get_matched_topics(client, #subs_primary_key{topicFilter = "Winter/December", client_id = "orange"}),
	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(2, length(R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},
                                             {subscription_options,1,0,0,0,0},
                                             {mqtt_client_test,callback}}, R)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},
                                             {subscription_options,0,0,0,0,0},
                                             {size}}, R)),

	R1 = Storage:get_matched_topics(client, "Winter/December"),
	?debug_Fmt("::test:: read returns ~120p", [R1]),	
	?assertEqual(4, length(R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"apple"},#subscription_options{max_qos=2},{length}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"+/December",undefined,"orange"},#subscription_options{max_qos=0},{size}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/#",undefined,"pear"},#subscription_options{max_qos=1},{length}}, R1)),
	?assert(lists:member({storage_subscription,{subs_primary_key,"Winter/+",undefined,"orange"},#subscription_options{max_qos=1},{mqtt_client_test, callback}}, R1)),
	?passed
end}.

read_all(X, Storage) -> {"read all [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	R = Storage:get_all(client, {session, "lemon"}),
%	?debug_Fmt("::test:: read returns ~120p", [R]),	
	?assertEqual(3, length(R)),
	?passed
end}.
	
update(X, Storage) -> {"update [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:save(client, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 101}, document = #publish{topic = "", payload = <<>>}}),
	R = Storage:get(client, #primary_key{client_id = "lemon", packet_id = 101}),
%	?debug_Fmt("::test:: read returns ~120p", [R]),
	?assertEqual(#publish{topic = "",payload = <<>>}, R#storage_publish.document),
	Storage:save(client, #storage_publish{key = #primary_key{client_id = "lemon", packet_id = 201}, document = undefined}),
	R1 = Storage:get(client, #primary_key{client_id = "lemon", packet_id = 201}),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),
	?assertEqual(undefined, R1#storage_publish.document),
	Storage:save(client, #storage_subscription{key = #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}, options = #subscription_options{max_qos=2}, callback = {erlang, binary_to_list}}),
	[R2] = Storage:get(client, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}),
%	?debug_Fmt("::test:: read returns ~120p", [R1]),
	?assertEqual(2, R2#storage_subscription.options#subscription_options.max_qos),
	?assertEqual({erlang, binary_to_list}, R2#storage_subscription.callback),
	?passed
end}.
	
delete(X, Storage) -> {"delete [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	Storage:remove(client, #primary_key{client_id = "lemon", packet_id = 101}),
	R = Storage:get(client, #primary_key{client_id = "lemon", packet_id = 101}),
%	?debug_Fmt("::test:: after delete ~p", [R]),	
	?assertEqual(undefined, R),
	
	Storage:remove(client, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}),
	R1 = Storage:get(client, #subs_primary_key{topicFilter = "Winter/+", client_id = "orange"}),
%	?debug_Fmt("::test:: after delete ~p", [R1]),	
	?assertEqual(undefined, R1),
	R2 = Storage:get_all(client, topic),	
%	?debug_Fmt("::test:: read returns ~120p", [R2]),	
	?assertEqual(7, length(R2)),
	
	Storage:remove(client, #subs_primary_key{client_id = "apple", _='_'}),
	R3 = Storage:get(client, #subs_primary_key{topicFilter = "+/December", client_id = "apple"}),
%	?debug_Fmt("::test:: after delete ~p", [R3]),	
	?assertEqual(undefined, R3),
	R4 = Storage:get_all(client, topic),	
%	?debug_Fmt("::test:: read returns ~120p", [R4]),	
	?assertEqual(5, length(R4)),

	?passed
end}.
