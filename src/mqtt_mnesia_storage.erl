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

%% @since 2026-01-06
%% @copyright 2015-2026 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_mnesia_storage.


-module(mqtt_mnesia_storage).
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
	[session_cli, subscription_cli];
db_id(server) ->
	[session, subscription, connectpid, users, retain, session_state].

db_id(1, client) -> session_cli;
db_id(1, server) -> session;
db_id(2, client) -> subscription_cli;
db_id(2, server) -> subscription;
db_id(3, server) -> connectpid;
db_id(4, server) -> users;
db_id(5, server) -> retain;
db_id(6, server) -> session_state.

init_tables(Nodes, client) ->
	TNodes = if length(Nodes) =< 1 -> []; ?ELSE -> Nodes end, %% ???
	CT1 = mnesia:create_table(session_cli,
		[
			{disc_copies, TNodes},
			{record_name, storage_publish},
			{attributes, record_info(fields, storage_publish)},
			{type, set},
			{local_content, true}
		]),
	lager:info([{endtype, client}], "Create table 'session_cli' returns: ~p~n", [CT1]),
	CT2 = mnesia:create_table(subscription_cli,
		[
			{disc_copies, TNodes},
			{record_name, storage_subscription},
			{attributes, record_info(fields, storage_subscription)},
			{type, set},
			{local_content, true}
		]),
	lager:info([{endtype, client}], "Create table 'subscription_cli' returns: ~p~n", [CT2]);
init_tables(Nodes, server) ->
	lager:info([{endtype, server}], "init_tables() =: running mnesia nodes: ~p~n", [mnesia:system_info(running_db_nodes)]),
%%	TNodes = if length(Nodes) =< 1 -> []; ?ELSE -> Nodes end,
	TNodes = Nodes,
	CT1 = mnesia:create_table(session,
		[
			{disc_copies, TNodes},
			{record_name, storage_publish},
			{attributes, record_info(fields, storage_publish)},
			{type, set},
			{local_content, false}
		]),
	lager:info([{endtype, server}], "Create table 'session': ~p~n", [CT1]),
	CT2 = mnesia:create_table(subscription,
		[
			{disc_copies, TNodes},
			{record_name, storage_subscription},
			{attributes, record_info(fields, storage_subscription)},
			{type, set},
			{local_content, false}
		]),
	lager:info([{endtype, server}], "Create table 'subscription': ~p~n", [CT2]),
	CT3 = mnesia:create_table(connectpid,
		[
			{disc_copies, TNodes},
			{record_name, storage_connectpid},
			{attributes, record_info(fields, storage_connectpid)},
			{type, set},
			{local_content, false}
		]),
	lager:info([{endtype, server}], "Create table 'connectpid': ~p~n", [CT3]),
	CT4 = mnesia:create_table(retain,
		[
			{disc_copies, TNodes},
			{record_name, storage_retain},
			{attributes, record_info(fields, storage_retain)},
			{type, bag},
			{local_content, false}
		]),
	lager:info([{endtype, server}], "Create table 'retain': ~p~n", [CT4]),
	CT5 = mnesia:create_table(session_state,
		[
			{disc_copies, TNodes},
			{record_name, session_state},
			{attributes, record_info(fields, session_state)},
			{type, set},
			{local_content, false}
		]),
	lager:info([{endtype, server}], "Create table 'session_state': ~p~n", [CT5]),
	CT6 = mnesia:create_table(users,
		[
			{disc_copies, TNodes},
			{record_name, user},
			{attributes, record_info(fields, user)},
			{type, set},
			{local_content, false}
		]),
		lager:info([{endtype, server}], "Create table 'users': ~p~n", [CT6]).

check_cluster_connections([]) -> ok;
check_cluster_connections([Node | Nodes]) ->
	case net_adm:ping(Node) of
		pong -> check_cluster_connections(Nodes);
		pang -> 
			lager:error([{endtype, server}], "Node ~p does not connected to cluster.~n", [Node]),
			timer:sleep(5000),
			check_cluster_connections([Node | Nodes])
	end.

wait_for_tables_ready(End_Type) ->
	case mnesia:wait_for_tables(db_id(End_Type), 10000) of
		ok -> ok;
		{error, Reason} ->
			lager:error([{endtype, End_Type}], "Wait for tables returns ~p. ~n", [Reason]),
			timer:sleep(10000),
			wait_for_tables_ready(End_Type);
		{timeout, Tbls} -> 
			lager:error([{endtype, End_Type}], "Wait for tables completes with timeout. Tables: ~p. ~n", [Tbls]),
			wait_for_tables_ready(End_Type)
	end.

start(client) ->
	lager:info([{endtype, client}], "Mnesia directory: ~p~n", [mnesia:system_info(directory)]),
	lager:info([{endtype, client}], "Mnesia use_dir: ~p~n", [mnesia:system_info(use_dir)]),
	lager:info([{endtype, client}], "Mnesia schema location: ~p~n", [mnesia:system_info(schema_location)]),
	case mnesia:create_schema([node()]) of
		{error, {_, {already_exists, _}}} = Err ->
			lager:warning([{endtype, client}], "Mnesia was already initialized: ~p.~n", [Err]),
			mnesia:start();
		ok ->
			mnesia:start(),
			init_tables([node()], client);
		Error -> 	
			lager:error([{endtype, client}], "Mnesia create schema throws error: ~p~n", [Error])
	end,
	
	wait_for_tables_ready(client);
start(server) ->
	Nodes = application:get_env(mqtt_common, cluster_nodes, [node()]),
	lager:debug([{endtype, server}], "Nodes from sys.config: ~p~n", [Nodes]),
	start(server, Nodes).

start(server, []) ->
	start(server, [node()]);
%% standalone server (one node) :
start(server, [Node] = N) ->
	lager:info([{endtype, server}], "current node: ~p; ~p~n", [node(), Node]),
	lager:info([{endtype, server}], "Mnesia use_dir: ~p~n", [mnesia:system_info(use_dir)]),
	lager:info([{endtype, server}], "Mnesia is running: ~p~n", [mnesia:system_info(is_running)]),

	case mnesia:create_schema(N) of
		{error, {_, {already_exists, _}}} = Err ->
			lager:warning([{endtype, server}], "Mnesia was already initialized: ~p.~n", [Err]),
			lager:debug([{endtype, server}], "Mnesia use_dir: ~p~n", [mnesia:system_info(use_dir)]),
			case mnesia:system_info(use_dir) of
				false ->
					stopped = mnesia:stop(),
					ok = mnesia:delete_schema(N),
					ok = mnesia:create_schema(N),
					mnesia:start(),
					init_tables(N, server);
				true -> mnesia:start()
			end;
		ok ->
			mnesia:start(),
			init_tables(N, server);
		Error -> 	
			lager:error([{endtype, server}], "Mnesia create schema throws error: ~p~n", [Error])
	end,

	lager:info([{endtype, server}], "Mnesia directory: ~p~n", [mnesia:system_info(directory)]),
	lager:info([{endtype, server}], "Mnesia use_dir: ~p~n", [mnesia:system_info(use_dir)]),
	lager:info([{endtype, server}], "Mnesia schema location: ~p~n", [mnesia:system_info(schema_location)]),
	
	wait_for_tables_ready(server);
%% cluster of servers ( > 1 nodes) :
start(server, Sys_nodes) ->
	ok = check_cluster_connections(Sys_nodes),
	lager:info([{endtype, server}], "current node: ~p~n", [node()]),
	Nodes = nodes(),
	lager:info([{endtype, server}], "visible nodes: ~p~n", [Nodes]),
	Is_master = application:get_env(mqtt_common, mnesia_master, false),
	lager:info([{endtype, server}], "Is mnesia master: ~p~n", [Is_master]),

	case mnesia:create_schema(Nodes) of
		{error, {_, {already_exists, _}}} = Err ->
			lager:warning([{endtype, server}], "Mnesia was already initialized: ~p.~n", [Err]),
			mnesia:start();
		ok ->
			init(Nodes, Is_master);
		Error -> 	
			lager:error([{endtype, server}], "Mnesia create schema throws error: ~p~n", [Error])
	end,

	lager:info([{endtype, server}], "running mnesia nodes: ~p~n", [mnesia:system_info(running_db_nodes)]),
	lager:info([{endtype, server}], "Mnesia directory: ~p~n", [mnesia:system_info(directory)]),
	lager:info([{endtype, server}], "Mnesia use_dir: ~p~n", [mnesia:system_info(use_dir)]),
	lager:info([{endtype, server}], "Mnesia schema location: ~p~n", [mnesia:system_info(schema_location)]),
	
	wait_for_tables_ready(server).

%% init procedure of master node of cluster
init(Nodes, true) ->
	mnesia:stop(),
	global:register_name(barrier, self()),
	
	case mnesia:delete_schema(Nodes) of
		ok -> ok;
		{error, Reason} ->
			lager:error([{endtype, server}], "Delete schema returns error: ~p.~n", [Reason])
	end,
	case mnesia:create_schema(Nodes) of
		{error, {_, {already_exists, _}}} = Err ->
			lager:warning([{endtype, server}], "Mnesia was already initialized: ~p.~n", [Err]);
		ok ->
			global:send(barrier, barrier_up),
			mnesia:start(),
			init_tables(Nodes, server);
		Error -> 	
			lager:error([{endtype, server}], "Mnesia create schema throws error: ~p~n", [Error])
	end,

	wait_for_tables_ready(server),
	global:unregister_name(barrier),
	Tables = mnesia:system_info(tables),
	lager:info([{endtype, server}], "Mnesia tables: ~p~n", [Tables]),
	lager:info([{endtype, server}], "Mnesia schema is created. ~n", []);
%%  init procedure of slave node of cluster
init(_Nodes, false) ->
	mnesia:stop(),
	global:register_name(barrier, self()),
	
%% wait for barrier_up signal from master node
	receive
		barrier_up -> ok
	end,
	mnesia:start(),

	wait_for_tables_ready(server),
	global:unregister_name(barrier),
	Tables = mnesia:system_info(tables),
	lager:info([{endtype, server}], "Mnesia tables: ~p~n", [Tables]),
	lager:info([{endtype, server}], "Mnesia schema is created. ~n", []).

session(save, #storage_publish{key = Key} = Document, End_Type) ->
	Fun = fun() -> mnesia:write(db_id(1, End_Type), Document, write) end,
	case mnesia:transaction(Fun) of
		{atomic, _Res} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "session table: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false
	end;
session(exist, #primary_key{} = Key, End_Type) ->
	case session(get, Key, End_Type) of
		#storage_publish{} -> true;
		_ -> false
	end;
session(get, #primary_key{} = Key, End_Type) ->
	Fun = fun() -> mnesia:read(db_id(1, End_Type), Key, read) end,
	case mnesia:transaction(Fun) of
		{atomic, [#storage_publish{} = Doc]} -> Doc;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "session table: Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		_ -> undefined
	end;
session(get_all, all, End_Type) ->
	mnesia:dirty_match_object(db_id(1, End_Type), #storage_publish{_='_'});
session(get_all, ClientId, End_Type) ->
	mnesia:dirty_match_object(db_id(1, End_Type), #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'});
session(remove, #primary_key{} = Key, End_Type) ->
	mnesia:dirty_delete(db_id(1, End_Type), Key);
session(clean, ClientId, End_Type) ->
	R = mnesia:dirty_match_object(db_id(1, End_Type), #storage_publish{key = #primary_key{client_id = ClientId, _ = '_'}, _ = '_'}),
	[mnesia:dirty_delete(db_id(1, End_Type), Key) || #storage_publish{key = Key} <- R];
session(close, _, _) -> ok.

session_state(save, #session_state{client_id = Key} = Document) ->
	Fun = fun() -> mnesia:write(db_id(6, server), Document, write) end,
	case mnesia:transaction(Fun) of
		{atomic, _Res} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "session_state: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false
	end;
session_state(exist, _Client_Id) -> false;
session_state(get, Client_Id) ->
	case mnesia:dirty_read(db_id(6, server), Client_Id) of
		[#session_state{} = Doc] -> Doc;
		_ -> undefined
	end;
session_state(get_all, _) ->
	mnesia:dirty_match_object(db_id(6, server), #session_state{_='_'});
session_state(remove, Client_Id) ->
	mnesia:dirty_delete(db_id(6, server), Client_Id);
session_state(clean, _) ->
	case mnesia:clear_table(db_id(6, server)) of
		{atomic, ok} -> ok;
		{aborted, Reason} ->
			lager:error([{endtype, server}], "session_state: delete all failed: reason ~p~n", [Reason])
	end;
session_state(close, _) -> ok.

subscription(save, #storage_subscription{key = Key} = Document, End_Type) ->
	Fun = fun() -> mnesia:write(db_id(2, End_Type), Document, write) end,
	case mnesia:transaction(Fun) of
		{atomic, _Res} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "session_state: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false
	end;
subscription(exist, Key, End_Type) ->
	case mnesia:dirty_read(db_id(2, End_Type), Key) of
		[_] -> true;
		[] -> false;
		_ -> false
	end;
subscription(get, #subs_primary_key{} = Key, End_Type) -> %% @todo delete it
	Fun = fun() -> mnesia:read(db_id(2, End_Type), Key) end,
	case mnesia:transaction(Fun) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		_ -> undefined
	end;
subscription(get_all, _, End_Type) ->
	mnesia:dirty_match_object(db_id(2, End_Type), #storage_subscription{_='_'});
subscription(get_client_topics, Client_Id, End_Type) -> %% I do not use it @todo delete ???
	MatchSpec =
		ets:fun2ms(
			fun(#storage_subscription{key = #subs_primary_key{client_id = CI}} = Object) when CI == Client_Id -> 
				Object
			end
		),
	Fun = fun() -> mnesia:select(db_id(2, End_Type), MatchSpec) end,
	case mnesia:transaction(Fun) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "Get_client_topics failed: client_id=~p reason=~p~n", [Client_Id, Reason]),
			undefined;
		_ -> undefined
	end;
subscription(get_matched_topics, #subs_primary_key{topicFilter = Topic, client_id = Client_Id}, End_Type) -> %% only client side
	Constraint =
		fun (#storage_subscription{key = #subs_primary_key{topicFilter = TopicFilter, client_id = CI}} = Object, Acc) when Client_Id =:= CI -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> [Object | Acc];
						false -> Acc
					end;
				(_, Acc) -> Acc
		end,
	Find = fun() -> mnesia:foldl(Constraint, [], db_id(2, End_Type)) end,
	case mnesia:transaction(Find) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "Get_matched_topics failed: client_id=~p topic=~p reason=~p~n", [Client_Id, Topic, Reason]),
			undefined;
		_ -> undefined
	end;
subscription(get_matched_topics, Topic, End_Type) -> %% only server side
	Constraint =
		fun (#storage_subscription{key = #subs_primary_key{topicFilter = TopicFilter, shareName = ShareName}} = Object, Acc) when  ShareName == undefined -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> [Object | Acc];
						false -> Acc
					end;
				(_, Acc) -> Acc
		end,
	Find = fun() -> mnesia:foldl(Constraint, [], db_id(2, End_Type)) end,
	case mnesia:transaction(Find) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "Get_matched_topics failed: topic=~p reason=~p~n", [Topic, Reason]),
			undefined;
		_ -> undefined
	end;
subscription(get_matched_shared_topics, Topic, End_Type) -> %% only server side
	Constraint =
		fun (#storage_subscription{key = #subs_primary_key{topicFilter = TopicFilter, shareName = ShareName}} = Object, Acc) when ShareName =/= undefined -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> [Object | Acc];
						false -> Acc
					end;
				(_, Acc) -> Acc
		end,
	Find = fun() -> mnesia:foldl(Constraint, [], db_id(2, End_Type)) end,
	case mnesia:transaction(Find) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "Get_matched_shared_topics failed: topic=~p reason=~p~n", [Topic, Reason]),
			undefined;
		_ -> undefined
	end;
subscription(remove, #subs_primary_key{} = Key, End_Type) ->
	Fun =
		fun() ->
			Records = mnesia:match_object(db_id(2, End_Type), #storage_subscription{key = Key, _ = '_'}, write),
			[mnesia:delete(db_id(2, End_Type), K, write) || #storage_subscription{key = K} <- Records]
		end,
	case mnesia:transaction(Fun) of
		{atomic, _} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "Delete subscription is failed: key=~p reason=~p~n", [Key, Reason]),
			false;
		_ -> false
	end;
subscription(clean, ClientId, End_Type) ->
	Fun =
		fun() ->
			Records = mnesia:match_object(db_id(2, End_Type), #storage_subscription{key = #subs_primary_key{client_id = ClientId, _ = '_'}, _ = '_'}, write),
			[mnesia:delete(db_id(2, End_Type), Key, write) || #storage_subscription{key = Key} <- Records]
		end,
	case mnesia:transaction(Fun) of
		{atomic, _} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, End_Type}], "Clean subscription is failed: ClientId=~p reason=~p~n", [ClientId, Reason]),
			false;
		_ -> false
	end;
subscription(close, _, _) -> ok.

connect_pid(save, #storage_connectpid{client_id = Key} = Document, _) ->
	Fun = fun() -> mnesia:write(db_id(3, server), Document, write) end,
	case mnesia:transaction(Fun) of
		{atomic, _Res} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "connectpid_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false
	end;
connect_pid(exist, _Key, _) -> false;
connect_pid(get, Client_id, _) ->
	Fun = fun() -> mnesia:read(db_id(3, server), Client_id) end,
	case mnesia:transaction(Fun) of
		{atomic, [#storage_connectpid{pid = Pid}]} -> Pid;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Get failed: key=~p reason=~p~n", [Client_id, Reason]),
			undefined;
		_ -> undefined
	end;
connect_pid(get_all, _, _) ->
	Fun = fun() -> mnesia:match_object(db_id(3, server), #storage_connectpid{_='_'}, read) end,
	case mnesia:transaction(Fun) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Get_all failed: reason=~p~n", [Reason]),
			undefined;
		_ -> undefined
	end;
connect_pid(remove, Client_id, _) ->
	Fun = fun() -> mnesia:delete(db_id(3, server), Client_id, write) end,
	case mnesia:transaction(Fun) of
		{atomic, ok} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Delete connection_pid is failed: key=~p reason=~p~n", [Client_id, Reason]),
			false;
		_ -> false
	end;
connect_pid(clean, Client_id, _) ->
	connect_pid(remove, Client_id, server);
connect_pid(close, _, _) -> ok.

user(save, #user{user_id = Key, password = Pswd} = Doc) ->
	User_name = if is_binary(Key) -> Key; ?ELSE -> list_to_binary(Key) end,
	Fun = fun() -> mnesia:write(db_id(4, server), Doc#user{user_id = User_name, password = crypto:hash(md5, Pswd)}, write) end,
	case mnesia:transaction(Fun) of
		{atomic, _Res} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "user_db: Insert failed: ~p; reason ~p~n", [Key, Reason]),
			false
	end;
user(exist, _Key) -> false;
user(get, Key) ->
	User_name = if is_binary(Key) -> Key; ?ELSE -> list_to_binary(Key) end,
	Fun = fun() -> mnesia:read(db_id(4, server), User_name) end,
	case mnesia:transaction(Fun) of
		{atomic, [#user{password = Pswd, roles = Roles}]} -> #{password => list_to_binary(mqtt_data:binary_to_hex(Pswd)), roles => Roles};
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Get failed: key=~p reason=~p~n", [Key, Reason]),
			undefined;
		_ -> undefined
	end;
user(get_all, _) ->
	Fun = fun() -> mnesia:match_object(db_id(4, server), #user{_='_'}, read) end,
	case mnesia:transaction(Fun) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Get_all failed: reason=~p~n", [Reason]),
			[];
		_ -> []
	end;
user(remove, Key) ->
	User_name = if is_binary(Key) -> Key; true -> list_to_binary(Key) end,
	Fun = fun() -> mnesia:delete(db_id(4, server), User_name, write) end,
	case mnesia:transaction(Fun) of
		{atomic, ok} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Delete user is failed: key=~p reason=~p~n", [Key, Reason]),
			false;
		_ -> false
	end;
user(clean, _) ->
	case mnesia:clear_table(db_id(4, server)) of
		{atomic, ok} -> ok;
		{aborted, Reason} ->
			lager:error([{endtype, server}], "session_state: delete all failed: reason ~p~n", [Reason])
	end;
user(close, _) -> ok.

retain(save, #publish{topic = Topic} = Doc) ->
	Fun = fun() -> mnesia:write(db_id(5, server), #storage_retain{topic = Topic, document = Doc}, write) end,
	case mnesia:transaction(Fun) of
		{atomic, _Res} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "retain_tbl: Insert failed: ~p; reason ~p~n", [Topic, Reason]),
			false
	end;
retain(exist, _Key) -> false;
retain(get, TopicFilter) ->
	Constraint =
		fun (#storage_retain{topic = Topic, document = Doc}, Acc) -> 
					case mqtt_data:is_match(Topic, TopicFilter) of
						true -> [Doc | Acc];
						false -> Acc
					end;
				(_, Acc) -> Acc
		end,
	Find = fun() -> mnesia:foldl(Constraint, [], db_id(5, server)) end,
	case mnesia:transaction(Find) of
		{atomic, Res} when is_list(Res) -> Res;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Get_retain failed: topic=~p reason=~p~n", [TopicFilter, Reason]),
			undefined;
		_ -> undefined
	end;
retain(get_all, _) ->
	case mnesia:dirty_match_object(db_id(5, server), #storage_retain{_='_'}) of 
		{error, Reason} -> 
			lager:error([{endtype, server}], "match_object failed: ~p~n", [Reason]),
			[];
		R -> R
	end;
retain(remove, Topic) ->
	Fun = fun() -> mnesia:delete(db_id(5, server), Topic, write) end,
	case mnesia:transaction(Fun) of
		{atomic, ok} -> true;
		{aborted, Reason} -> 
			lager:error([{endtype, server}], "Delete retain is failed: topic=~p reason=~p~n", [Topic, Reason]),
			false;
		_ -> false
	end;
retain(clean, _) ->
	case mnesia:clear_table(db_id(5, server)) of
		{atomic, ok} -> ok;
		{aborted, Reason} ->
			lager:error([{endtype, server}], "retain: delete all failed: reason ~p~n", [Reason])
	end;
retain(close, _) -> ok.

cleanup(ClientId, End_Type) -> %% @todo rename to session_cleanup
	session(clean, ClientId, End_Type),
	subscription(clean, ClientId, End_Type),
	if End_Type =:= server ->
			session_state(remove, ClientId);
		?ELSE -> ok
	end.

cleanup(End_Type) ->
	mnesia:clear_table(db_id(1, End_Type)),
	mnesia:clear_table(db_id(2, End_Type)),
	if End_Type =:= server ->
			mnesia:clear_table(db_id(3, End_Type)),
			mnesia:clear_table(db_id(5, End_Type)),
			mnesia:clear_table(db_id(6, End_Type));
		true -> ok
	end.

close(_) ->
	mnesia:stop().

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

