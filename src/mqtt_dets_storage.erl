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

%% @since 2016-09-08
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to dets_dao.


-module(mqtt_dets_storage).
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
	cleanup/2,
	cleanup/1,
	
	session/3,
	session_state/2,
	subscription/3,
	connect_pid/3,
	user/2,
	retain/2
]).

db_id(client) ->
	[session_db_cli, subscription_db_cli, connectpid_db_cli];
db_id(server) ->
	[session_db_srv, subscription_db_srv, connectpid_db_srv, users_db_srv, retain_db_srv, session_state_db_srv].

db_type(client) -> [set, set, set];
db_type(server) -> [set, set, set, set, duplicate_bag, set].

db_id(1, client) -> session_db_cli;
db_id(1, server) -> session_db_srv;
db_id(2, client) -> subscription_db_cli;
db_id(2, server) -> subscription_db_srv;
db_id(3, client) -> connectpid_db_cli;
db_id(3, server) -> connectpid_db_srv;
db_id(4, server) -> users_db_srv;
db_id(5, server) -> retain_db_srv;
db_id(6, server) -> session_state_db_srv.

db_file(client) ->
	["session-db-cli.bin", "subscription-db-cli.bin", "connectpid-db-cli.bin"];
db_file(server) ->
	["session-db-srv.bin", "subscription-db-srv.bin", "connectpid-db-srv.bin", "users-db-srv.bin", "retain-db-srv.bin", "session_state_db_srv.bin"].

end_type_2_name(client) -> mqtt_client;
end_type_2_name(server) -> mqtt_server.

start(End_Type) ->
	DB_Folder = application:get_env(end_type_2_name(End_Type), dets_home_folder, "dets-storage"),
	L = lists:zip3(db_id(End_Type), db_file(End_Type), db_type(End_Type)),
	L1 = [ 
		case dets:open_file(DB_ID, [{file, filename:join(DB_Folder, DB_File)}, {type, DB_Type}, {auto_save, 60000}, {keypos, 2}]) of
			{ok, DB_ID} ->
				true;
			{error, Reason1} ->
				lager:error([{endtype, End_Type}], "Cannot open ~p dets: ~p~n", [DB_ID, Reason1]),
				false
		end
	|| {DB_ID, DB_File, DB_Type} <- L],
	lists:foldl(fun(E, A) -> E and A end, true, L1).

session(save, #storage_publish{key = Key} = Document, End_Type) ->
	case dets:insert(db_id(1, End_Type), Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "session_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
session(exist, #primary_key{} = Key, End_Type) ->
	case dets:member(db_id(1, End_Type), Key) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Exist failed: key=~p reason=~p~n", [Key, Reason]),
			false;
		R -> R
	end;
session(get, #primary_key{} = Key, End_Type) ->
	case dets:match_object(db_id(1, End_Type), #storage_publish{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[D] -> D;
		_ ->
			undefined
	end;
session(get_all, all, End_Type) ->
	case dets:match_object(db_id(1, End_Type), #storage_publish{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
session(get_all, ClientId, End_Type) ->
	case dets:match_object(db_id(1, End_Type), #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
session(remove, #primary_key{} = Key, End_Type) ->
	case dets:match_delete(db_id(1, End_Type), #storage_publish{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
session(clean, ClientId, End_Type) ->
	case dets:match_delete(db_id(1, End_Type), #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of 
		{error, Reason1} -> 
			lager:error([{endtype, End_Type}], "match_delete failed: ~p~n", [Reason1]),
			ok;
		ok -> ok
	end;
session(close, _, End_Type) ->
	dets:close(db_id(1, End_Type)).

session_state(save, #session_state{} = Document) ->
	case dets:insert(db_id(6, server), Document) of
		{error, Reason} ->
			lager:error([{endtype, server}], "session_state_db: Insert failed: ~p; reason ~p~n", [Document#session_state.client_id, Reason]),
			false;
		ok ->
			true
	end;
session_state(exist, _Client_Id) -> false;
session_state(get, Client_Id) ->
	case dets:match_object(db_id(6, server), #session_state{client_id = Client_Id, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Get failed: client=~p reason=~p~n", [Client_Id, Reason]),
			undefined;
		[#session_state{} = Doc] -> Doc;
		_ ->
			undefined
	end;
session_state(get_all, _) ->
	case dets:match_object(db_id(6, server), #session_state{_ = '_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, server}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
session_state(remove, Client_Id) ->
	case dets:match_delete(db_id(6, server), #session_state{client_id = Client_Id, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Delete is failed for client Id: ~p with error code: ~p~n", [Client_Id, Reason]),
			false;
		ok -> true
	end;
session_state(clean, _) ->
	dets:delete_all_objects(db_id(6, server));
session_state(close, _) ->
	dets:close(db_id(6, server)).

subscription(save, #storage_subscription{key = Key} = Document, End_Type) ->
	case dets:insert(db_id(2, End_Type), Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "subscription_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
subscription(exist, Key, End_Type) ->
	case dets:member(db_id(2, End_Type), Key) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Exist failed: key=~p reason=~p~n", [Key, Reason]),
			false;
		R -> R
	end;
subscription(get, #subs_primary_key{} = Key, End_Type) -> %% @todo delete it
	case dets:match_object(db_id(2, End_Type), #storage_subscription{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
%		[] -> undefined;
		D when is_list(D) -> D;
		_ -> undefined
	end;
subscription(get_all, _, End_Type) ->
	case dets:match_object(db_id(2, End_Type), #storage_subscription{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> [Topic || #storage_subscription{key = #subs_primary_key{topicFilter = Topic}} <- R]
	end;
subscription(get_client_topics, Client_Id, End_Type) -> %% I do not use it @todo delete ???
	MatchSpec =
		ets:fun2ms(
			fun(#storage_subscription{key = #subs_primary_key{client_id = CI}} = Object) when CI == Client_Id -> 
				Object
			end
		),
	dets:select(db_id(2, End_Type), MatchSpec);
subscription(get_matched_topics, #subs_primary_key{topicFilter = Topic, client_id = Client_Id}, End_Type) -> %% only client side
	Fun =
		fun (#storage_subscription{key = #subs_primary_key{topicFilter = TopicFilter, client_id = CI}} = Object) when Client_Id =:= CI -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> {continue, Object};
						false -> continue
					end;
				(_) -> continue
		end,
	dets:traverse(db_id(2, End_Type), Fun);
subscription(get_matched_topics, Topic, End_Type) -> %% only server side
	Fun =
		fun (#storage_subscription{key = #subs_primary_key{topicFilter = TopicFilter, shareName = ShareName}} = Object) when  ShareName == undefined -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> {continue, Object};
						false -> continue
					end;
				(_) -> continue
		end,
	dets:traverse(db_id(2, End_Type), Fun);
subscription(get_matched_shared_topics, Topic, End_Type) -> %% only server side
	Fun =
		fun (#storage_subscription{key = #subs_primary_key{topicFilter = TopicFilter, shareName = ShareName}} = Object) when ShareName =/= undefined -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> {continue, Object};
						false -> continue
					end;
				(_) -> continue
		end,
	dets:traverse(db_id(2, End_Type), Fun);
subscription(remove, #subs_primary_key{} = Key, End_Type) ->
	case dets:match_delete(db_id(2, End_Type), #storage_subscription{key = Key, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
subscription(clean, ClientId, End_Type) ->
	MatchSpec = ets:fun2ms(
							 fun(#storage_subscription{key = #subs_primary_key{client_id = CI}}) when CI == ClientId -> 
									true
							 end),
	case dets:select_delete(db_id(2, End_Type), MatchSpec) of
%% 	case dets:match_delete(Subscription_db, #storage_subscription{key = #subs_primary_key{client_id = ClientId, _ = '_'}, _ = '_'}) of
		{error, Reason2} -> 
			lager:error([{endtype, End_Type}], "match_delete failed: ~p~n", [Reason2]);
		N ->
%%			lager:debug([{endtype, End_Type}], "match_delete returns: ~p~n", [N])
			N
	end;
subscription(close, _, End_Type) ->
	dets:close(db_id(2, End_Type)).

connect_pid(save, #storage_connectpid{client_id = Key} = Document, End_Type) ->
	case dets:insert(db_id(3, End_Type), Document) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "connectpid_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
connect_pid(exist, _Key, _End_Type) -> false;
connect_pid(get, Client_id, End_Type) ->
	case dets:match_object(db_id(3, End_Type), #storage_connectpid{client_id = Client_id, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Get failed: key=~p reason=~p~n", [Client_id, Reason]),
			undefined;
		[#storage_connectpid{pid = Pid}] -> Pid;
		_ -> undefined
	end;
connect_pid(get_all, _, End_Type) ->
	case dets:match_object(db_id(3, End_Type), #storage_connectpid{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, End_Type}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
connect_pid(remove, Client_id, End_Type) ->
	case dets:match_delete(db_id(3, End_Type), #storage_connectpid{client_id = Client_id, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Delete is failed for key: ~p with error code: ~p~n", [Client_id, Reason]),
			false;
		ok -> true
	end;
connect_pid(clean, Client_id, End_Type) ->
	connect_pid(remove, Client_id, End_Type);
connect_pid(close, _, End_Type) ->
	dets:close(db_id(3, End_Type)).

user(save, #user{user_id = Key, password = Pswd} = Doc) ->
	User_name = if is_binary(Key) -> Key; true -> list_to_binary(Key) end,
	case dets:insert(db_id(4, server), Doc#user{user_id = User_name, password = crypto:hash(md5, Pswd)}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "user_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false;
		ok ->
			true
	end;
user(exist, _Key) -> false;
user(get, Key) ->
	User_name = if is_binary(Key) -> Key; true -> list_to_binary(Key) end,
	case dets:match_object(db_id(4, server), #user{user_id = User_name, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		[#user{password = Pswd, roles = Roles}] -> #{password => list_to_binary(mqtt_data:binary_to_hex(Pswd)), roles => Roles};
		_ ->
			undefined
	end;
user(get_all, _) ->
	case dets:match_object(db_id(4, server), #user{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, server}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
user(remove, Key) ->
	User_name = if is_binary(Key) -> Key; true -> list_to_binary(Key) end,
	case dets:match_delete(db_id(4, server), #user{user_id = User_name, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Delete is failed for key: ~p with error code: ~p~n", [Key, Reason]),
			false;
		ok -> true
	end;
user(clean, _) ->
	dets:delete_all_objects(db_id(4, server));
user(close, _) ->
	dets:close(db_id(4, server)).

retain(save, #publish{topic = Topic} = Doc) ->
	case dets:insert(db_id(5, server), #storage_retain{topic = Topic, document = Doc}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "retain_db: Insert failed: ~p; reason ~p~n", [Topic, Reason]),
			false;
		ok ->
			true
	end;
retain(exist, _Key) -> false;
retain(get, TopicFilter) ->
	Fun =
		fun (#storage_retain{topic = Topic, document = Doc}) -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> {continue, Doc};
						false -> continue
					end
		end,
	dets:traverse(db_id(5, server), Fun);
retain(get_all, _) ->
	case dets:match_object(db_id(5, server), #storage_retain{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, server}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
retain(remove, Topic) ->
	case dets:match_delete(db_id(5, server), #storage_retain{topic = Topic, _ = '_'}) of
		{error, Reason} ->
			lager:error([{endtype, server}], "Delete is failed for topic: ~p with error code: ~p~n", [Topic, Reason]),
			false;
		ok -> true
	end;
retain(clean, _) ->
	dets:delete_all_objects(db_id(5, server));
retain(close, _) ->
	dets:close(db_id(5, server)).

cleanup(ClientId, End_Type) ->
	session(clean, ClientId, End_Type),
	subscription(clean, ClientId, End_Type),
	if End_Type =:= server ->
				session_state(remove, ClientId);
			true -> ok
	end.

cleanup(End_Type) ->
	dets:delete_all_objects(db_id(1, End_Type)),
	dets:delete_all_objects(db_id(2, End_Type)),
	dets:delete_all_objects(db_id(3, End_Type)),
	if End_Type =:= server ->
			dets:delete_all_objects(db_id(5, server)),
			dets:delete_all_objects(db_id(6, server));
		true -> ok
	end.

close(End_Type) -> 
	[dets:close(Db_Id) || Db_Id <- db_id(End_Type)].
%% ====================================================================
%% Internal functions
%% ====================================================================
%% -spec sort(List) -> SortedList when
%%   List :: [integer()],
%%   SortedList :: [integer()].
%% sort([Pivot | Tail]) ->
%% {Smaller, Larger} = partition(Pivot, Tail, [], []),
%% sort(Smaller) ++ [Pivot] ++ sort(Larger);
%% sort([]) -> [].
%% 
%% partition(Check, [Head | Tail], Smaller, Larger) ->
%%     case Head =< Check of
%%         true -> partition(Check, Tail, [Head | Smaller], Larger);
%%         false -> partition(Check, Tail, Smaller, [Head | Larger])
%%     end;
%% partition(_, [], Smaller, Larger) -> {Smaller, Larger}.

