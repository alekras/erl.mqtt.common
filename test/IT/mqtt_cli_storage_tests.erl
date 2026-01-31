%%
%% Copyright (C) 2015-2026 by krasnop@bellsouth.net (Alexei Krasnopolski)
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
%% @copyright 2015-2026 Alexei Krasnopolski
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

%%-define(STORAGE_TYPE, dets).
%% -define(STORAGE_TYPE, mysql).
-define(STORAGE_TYPE, mnesia).

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
				{?STORAGE_TYPE, fun create/2},
				{?STORAGE_TYPE, fun read/2},
				{?STORAGE_TYPE, fun extract_topic/2},
				{?STORAGE_TYPE, fun extract_matched_topic/2},
				{?STORAGE_TYPE, fun read_all/2},
				{?STORAGE_TYPE, fun update/2},
				{?STORAGE_TYPE, fun delete/2}
			]
		}
	 }
	].

do_start() ->
	application:start(mqtt_common),
	lager:start(),
	Storage = setup(?STORAGE_TYPE),
	Storage:start(client),
	Storage:cleanup(client),
	?debug_Fmt("::test:: after do_start -> ~p~n", [Storage]),
	Storage.
%	Storage:start(server),
%	Storage:cleanup(server),
%	Storage:user(clean, undefined).

do_stop(Storage) ->
	?debug_Fmt("::test:: before do_stop -> ~p~n", [Storage]),
	application:stop(mqtt_common),
	Storage:close(client).	

setup(dets) ->
	mqtt_dets_storage;
setup(mnesia) ->
	mqtt_mnesia_storage;
setup(mysql) ->
	mqtt_mysql_storage.

cleanup(_X, _Y) ->
%%	?debug_Fmt("::test:: before cleanup -> ~p, ~p ~n", [_X, _Y]),
	cleanup_return.

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
