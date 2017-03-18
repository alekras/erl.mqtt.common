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

%% @since 2016-09-08
%% @copyright 2015-2017 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to dets_dao.


-module(mqtt_dets_dao).
%%
%% Include files
%%
-include("mqtt.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	start/1,
	close/1,
	save/2,
	remove/2,
	get/2,
	get_client_topics/2,
	get_matched_topics/2,
	get_all/2,
  cleanup/2,
	cleanup/1,
  exist/2
]).

db_id(client) ->
	{session_db_cli, subscription_db_cli, connectpid_db_cli};
db_id(server) ->
	{session_db_srv, subscription_db_srv, connectpid_db_srv}.
	
db_file(client) ->
	{"session-db-cli.bin", "subscription-db-cli.bin", "connectpid-db-cli.bin"};
db_file(server) ->
	{"session-db-srv.bin", "subscription-db-srv.bin", "connectpid-db-srv.bin"}.
	
start(End_Type) ->
	{Session_db, Subscription_db, ConnectionPid_db} = db_id(End_Type),
	{Session_DB_File, Subscription_DB_File, ConnectionPid_DB_File} = db_file(End_Type),
	case dets:open_file(Session_db, [{file, Session_DB_File}, {type, set}, {auto_save, 10000}, {keypos, #storage_publish.key}]) of
		{ok, Session_db} ->
			true;
		{error, Reason1} ->
			lager:error([{endtype, End_Type}], "Cannot open session_db dets: ~p~n", [Reason1]),
			false
	end
	and
	case dets:open_file(Subscription_db, [{file, Subscription_DB_File}, {type, set}, {auto_save, 10000}, {keypos, #storage_subscription.key}]) of
		{ok, Subscription_db} ->
			true;
		{error, Reason2} ->
			lager:error([{endtype, End_Type}], "Cannot open subscription_db dets: ~p~n", [Reason2]),
			false
	end
	and
	case dets:open_file(ConnectionPid_db, [{file, ConnectionPid_DB_File}, {type, set}, {auto_save, 10000}, {keypos, #storage_connectpid.client_id}]) of
		{ok, ConnectionPid_db} ->
			true;
		{error, Reason3} ->
			lager:error([{endtype, End_Type}], "Cannot open connectpid_db dets: ~p~n", [Reason3]),
			false
	end.

save(End_Type, #storage_publish{key = Key} = Document) ->
	{Session_db, _, _} = db_id(End_Type),
	case dets:insert(Session_db, Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "session_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
save(End_Type, #storage_subscription{key = Key} = Document) ->
	{_, Subscription_db, _} = db_id(End_Type),
	case dets:insert(Subscription_db, Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "subscription_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
save(End_Type, #storage_connectpid{client_id = Key} = Document) ->
	{_, _, ConnectionPid_db} = db_id(End_Type),
	case dets:insert(ConnectionPid_db, Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "connectpid_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end.

remove(End_Type, #primary_key{} = Key) ->
	{Session_db, _, _} = db_id(End_Type),
	case dets:match_delete(Session_db, #storage_publish{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
remove(End_Type, #subs_primary_key{} = Key) ->
	{_, Subscription_db, _} = db_id(End_Type),
	case dets:match_delete(Subscription_db, #storage_subscription{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
remove(End_Type, {client_id, Key}) ->
	{_, _, ConnectionPid_db} = db_id(End_Type),
	case dets:match_delete(ConnectionPid_db, #storage_connectpid{client_id = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end.

get(End_Type, #primary_key{} = Key) ->
	{Session_db, _, _} = db_id(End_Type),
	case dets:match_object(Session_db, #storage_publish{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[D] -> D;
		_ ->
			undefined
	end;
get(End_Type, #subs_primary_key{} = Key) -> %% @todo delete it
	{_, Subscription_db, _} = db_id(End_Type),
	case dets:match_object(Subscription_db, #storage_subscription{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[D] -> D;
		_ ->
			undefined
	end;
get(End_Type, {client_id, Key}) ->
	{_, _, ConnectionPid_db} = db_id(End_Type),
	case dets:match_object(ConnectionPid_db, #storage_connectpid{client_id = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[#storage_connectpid{pid = Pid}] -> Pid;
		_ ->
			undefined
	end.

get_client_topics(End_Type, Client_Id) ->
	{_, Subscription_db, _} = db_id(End_Type),
	MatchSpec = ets:fun2ms(fun(#storage_subscription{key = #subs_primary_key{topic = Top, client_id = CI}, qos = QoS, callback = CB}) when CI == Client_Id -> {Top, QoS, CB} end),
	dets:select(Subscription_db, MatchSpec).

get_matched_topics(End_Type, #subs_primary_key{topic = Topic, client_id = Client_Id}) ->
	{_, Subscription_db, _} = db_id(End_Type),
	Fun =
		fun (#storage_subscription{key = #subs_primary_key{topic = TopicFilter, client_id = CI}, qos = QoS, callback = CB}) when Client_Id =:= CI -> 
					case mqtt_connection:is_match(Topic, TopicFilter) of
						true -> {continue, {TopicFilter, QoS, CB}};
						false -> continue
					end;
				(_) -> continue
		end,
	dets:traverse(Subscription_db, Fun);
get_matched_topics(End_Type, Topic) ->
	{_, Subscription_db, _} = db_id(End_Type),
	Fun =
		fun (#storage_subscription{key = #subs_primary_key{topic = TopicFilter}} = Object) -> 
					case mqtt_connection:is_match(Topic, TopicFilter) of
						true -> {continue, Object};
						false -> continue
					end
		end,
	dets:traverse(Subscription_db, Fun).
	
get_all(End_Type, {session, ClientId}) ->
	{Session_db, _, _} = db_id(End_Type),
	case dets:match_object(Session_db, #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
get_all(End_Type, topic) ->
	{_, Subscription_db, _} = db_id(End_Type),
	case dets:match_object(Subscription_db, #storage_subscription{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> [Topic || #storage_subscription{key = #subs_primary_key{topic = Topic}} <- R]
	end.

cleanup(End_Type, ClientId) ->
	{Session_db, Subscription_db, _} = db_id(End_Type),
	case dets:match_delete(Session_db, #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of 
		{error, Reason1} -> 
			lager:error([{endtype, End_Type}], "match_delete failed: ~p~n", [Reason1]),
			ok;
		ok -> ok
	end,
	case dets:match_delete(Subscription_db, #storage_connectpid{client_id = ClientId, _ = '_'}) of 
		{error, Reason2} -> 
			lager:error([{endtype, End_Type}], "match_delete failed: ~p~n", [Reason2]),
			ok;
		ok -> ok
	end,
	remove(End_Type, {client_id, ClientId}).

cleanup(End_Type) ->
	{Session_db, Subscription_db, ConnectionPid_db} = db_id(End_Type),
	dets:delete_all_objects(Session_db),
	dets:delete_all_objects(Subscription_db),
	dets:delete_all_objects(ConnectionPid_db).

exist(End_Type, Key) ->
	{Session_db, _, _} = db_id(End_Type),
	case dets:member(Session_db, Key) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Exist failed: key=~p reason=~p~n", [Key, Reason]),
			false;
		R -> R
	end.

close(End_Type) -> 
	{Session_db, Subscription_db, ConnectionPid_db} = db_id(End_Type),
	dets:close(Session_db),
	dets:close(Subscription_db),
	dets:close(ConnectionPid_db).
%% ====================================================================
%% Internal functions
%% ====================================================================


