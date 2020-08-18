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

%% @since 2016-09-08
%% @copyright 2015-2020 Alexei Krasnopolski
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
	[session_db_cli, subscription_db_cli, connectpid_db_cli];
db_id(server) ->
	[session_db_srv, subscription_db_srv, connectpid_db_srv, users_db_srv, retain_db_srv].

db_type(client) -> [set, set, set];
db_type(server) -> [set, set, set, set, duplicate_bag].

db_id(1, client) -> session_db_cli;
db_id(1, server) -> session_db_srv;
db_id(2, client) -> subscription_db_cli;
db_id(2, server) -> subscription_db_srv;
db_id(3, client) -> connectpid_db_cli;
db_id(3, server) -> connectpid_db_srv;
db_id(4, server) -> users_db_srv;
db_id(5, server) -> retain_db_srv.

db_file(client) ->
	["session-db-cli.bin", "subscription-db-cli.bin", "connectpid-db-cli.bin"];
db_file(server) ->
	["session-db-srv.bin", "subscription-db-srv.bin", "connectpid-db-srv.bin", "users-db-srv.bin", "retain-db-srv.bin"].

end_type_2_name(client) -> mqtt_client;
end_type_2_name(server) -> mqtt_server.

start(End_Type) ->
	DB_Folder = application:get_env(end_type_2_name(End_Type), dets_home_folder, "dets-storage"),
	L = lists:zip3(db_id(End_Type), db_file(End_Type), db_type(End_Type)),
	L1 = [ 
		case dets:open_file(DB_ID, [{file, filename:join(DB_Folder, DB_File)}, {type, DB_Type}, {auto_save, 10000}, {keypos, 2}]) of
			{ok, DB_ID} ->
				true;
			{error, Reason1} ->
				lager:error([{endtype, End_Type}], "Cannot open ~p dets: ~p~n", [DB_ID, Reason1]),
				false
		end
	|| {DB_ID, DB_File, DB_Type} <- L],
	lists:foldl(fun(E, A) -> E and A end, true, L1).

save(End_Type, #storage_publish{key = Key} = Document) ->
	Session_db = db_id(1, End_Type),
	case dets:insert(Session_db, Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "session_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
save(End_Type, #storage_subscription{key = Key} = Document) ->
	Subscription_db = db_id(2, End_Type),
	case dets:insert(Subscription_db, Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "subscription_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
save(End_Type, #storage_connectpid{client_id = Key} = Document) ->
	ConnectionPid_db = db_id(3, End_Type),
	case dets:insert(ConnectionPid_db, Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "connectpid_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
save(server, #user{user_id = Key, password = Pswd} = Doc) ->
	User_db = db_id(4, server),
	case dets:insert(User_db, Doc#user{password = crypto:hash(md5, Pswd)}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "user_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
save(server, #publish{topic = Topic} = Doc) ->
	Retain_db = db_id(5, server),
	case dets:insert(Retain_db, #storage_retain{topic = Topic, document = Doc}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "retain_db: Insert failed: ~p; reason ~p~n", [Topic, Reason]),
			false;
		ok ->
			true
	end.

remove(End_Type, #primary_key{} = Key) ->
	Session_db = db_id(1, End_Type),
	case dets:match_delete(Session_db, #storage_publish{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
remove(End_Type, #subs_primary_key{} = Key) ->
	Subscription_db = db_id(2, End_Type),
	case dets:match_delete(Subscription_db, #storage_subscription{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
remove(End_Type, {client_id, Key}) ->
	ConnectionPid_db = db_id(3, End_Type),
	case dets:match_delete(ConnectionPid_db, #storage_connectpid{client_id = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
remove(server, {user_id, Key}) ->
	User_db = db_id(4, server),
	case dets:match_delete(User_db, #user{user_id = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
remove(server, {topic, Topic}) ->
	Retain_db = db_id(5, server),
	case dets:match_delete(Retain_db, #storage_retain{topic = Topic, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Delete is failed for topic: ~p with error code: ~p~n", [Topic, Reason]),
			false;
		ok -> true
	end.

get(End_Type, #primary_key{} = Key) ->
	Session_db = db_id(1, End_Type),
	case dets:match_object(Session_db, #storage_publish{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[D] -> D;
		_ ->
			undefined
	end;
get(End_Type, #subs_primary_key{} = Key) -> %% @todo delete it
	Subscription_db = db_id(2, End_Type),
	case dets:match_object(Subscription_db, #storage_subscription{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[D] -> D;
		_ ->
			undefined
	end;
get(End_Type, {client_id, Key}) ->
	ConnectionPid_db = db_id(3, End_Type),
	case dets:match_object(ConnectionPid_db, #storage_connectpid{client_id = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[#storage_connectpid{pid = Pid}] -> Pid;
		_ ->
			undefined
	end;
get(server, {user_id, Key}) ->
	User_db = db_id(4, server),
	case dets:match_object(User_db, #user{user_id = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[#user{password = Pswd}] -> Pswd;
		_ ->
			undefined
	end;
get(server, {topic, TopicFilter}) ->
	Retain_db = db_id(5, server),
	Fun =
		fun (#storage_retain{topic = Topic, document = Doc}) -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> {continue, Doc};
						false -> continue
					end
		end,
	dets:traverse(Retain_db, Fun).

get_client_topics(End_Type, Client_Id) ->
	Subscription_db = db_id(2, End_Type),
	MatchSpec = ets:fun2ms(
							 fun(#storage_subscription{key = #subs_primary_key{topic = Topic, client_id = CI}, options = Options, callback = CB}) when CI == Client_Id -> 
											{Topic, Options, CB}
							 end),
	dets:select(Subscription_db, MatchSpec).

get_matched_topics(End_Type, #subs_primary_key{topic = Topic, client_id = Client_Id}) ->
	Subscription_db = db_id(2, End_Type),
	Fun =
		fun (#storage_subscription{key = #subs_primary_key{topic = TopicFilter, client_id = CI}, options = Options, callback = CB}) when Client_Id =:= CI -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> {continue, {TopicFilter, Options, CB}};
						false -> continue
					end;
				(_) -> continue
		end,
	dets:traverse(Subscription_db, Fun);
get_matched_topics(End_Type, Topic) ->
	Subscription_db = db_id(2, End_Type),
	Fun =
		fun (#storage_subscription{key = #subs_primary_key{topic = TopicFilter}} = Object) -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> {continue, Object};
						false -> continue
					end
		end,
	dets:traverse(Subscription_db, Fun).
	
get_all(End_Type, {session, ClientId}) ->
	Session_db = db_id(1, End_Type),
	case dets:match_object(Session_db, #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
get_all(End_Type, topic) ->
	Subscription_db = db_id(2, End_Type),
	case dets:match_object(Subscription_db, #storage_subscription{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> [Topic || #storage_subscription{key = #subs_primary_key{topic = Topic}} <- R]
	end.

cleanup(End_Type, ClientId) ->
	Session_db = db_id(1, End_Type),
	Subscription_db = db_id(2, End_Type),
	case dets:match_delete(Session_db, #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of 
		{error, Reason1} -> 
			lager:error([{endtype, End_Type}], "match_delete failed: ~p~n", [Reason1]),
			ok;
		ok -> ok
	end,
	case dets:match_delete(Subscription_db, #storage_subscription{key = #subs_primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of
		{error, Reason2} -> 
			lager:error([{endtype, End_Type}], "match_delete failed: ~p~n", [Reason2]),
			ok;
		ok -> ok
	end,
	remove(End_Type, {client_id, ClientId}).

cleanup(End_Type) ->
	Session_db = db_id(1, End_Type),
	Subscription_db = db_id(2, End_Type),
	ConnectionPid_db = db_id(3, End_Type),
	dets:delete_all_objects(Session_db),
	dets:delete_all_objects(Subscription_db),
	dets:delete_all_objects(ConnectionPid_db),
	if End_Type =:= server ->
			Retain_db = db_id(5, server),
			dets:delete_all_objects(Retain_db);
		true -> ok
	end.

exist(End_Type, Key) ->
	Session_db = db_id(1, End_Type),
	case dets:member(Session_db, Key) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Exist failed: key=~p reason=~p~n", [Key, Reason]),
			false;
		R -> R
	end.

close(End_Type) -> 
	[dets:close(Db_Id) || Db_Id <- db_id(End_Type)].
%% ====================================================================
%% Internal functions
%% ====================================================================


