%%
%% Copyright (C) 2015-2022 by krasnop@bellsouth.net (Alexei Krasnopolski)
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

%% @since 2023-04-24
%% @copyright 2015-2023 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to dets_dao.


-module(mqtt_mysql_storage).
%%
%% Include files
%%
-include("mqtt.hrl").
-include_lib("mysql_client/include/my.hrl").
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
	"mqtt_db_cli";
db_id(server) ->
	"mqtt_db_srv".

end_type_2_name(client) -> mqtt_client;
end_type_2_name(server) -> mqtt_server.

start(End_Type) ->
	MYSQL_SERVER_HOST_NAME = application:get_env(end_type_2_name(End_Type), mysql_host, "localhost"),
	MYSQL_SERVER_PORT = application:get_env(end_type_2_name(End_Type), mysql_port, 3306),
	MYSQL_USER = application:get_env(end_type_2_name(End_Type), mysql_user, "mqtt_user"),
	MYSQL_PASSWORD = application:get_env(end_type_2_name(End_Type), mysql_user, "mqtt_password"),
	R = my:start_client(),
	lager:info([{endtype, End_Type}], "Starting MySQL client connection to ~p:~p status: ~p",[MYSQL_SERVER_HOST_NAME, MYSQL_SERVER_PORT, R]),
	DB_name = db_id(End_Type),
	DS_def = #datasource{
		name = mqtt_storage,
		host = MYSQL_SERVER_HOST_NAME, 
		port = MYSQL_SERVER_PORT,
%		database = DB_name,
		user = MYSQL_USER, 
		password = MYSQL_PASSWORD, 
		flags = #client_options{}
	},
	case my:new_datasource(DS_def) of
		{ok, _Pid} ->
			Connect = datasource:get_connection(mqtt_storage),
			R0 = connection:execute_query(Connect, "CREATE DATABASE IF NOT EXISTS " ++ DB_name),
			lager:debug([{endtype, End_Type}], "create DB: ~p", [R0]),
			datasource:return_connection(mqtt_storage, Connect);
		#mysql_error{} -> ok
	end,
  datasource:close(mqtt_storage),

	case my:new_datasource(DS_def#datasource{database = DB_name}) of
		{ok, Pid} ->
			Conn = datasource:get_connection(mqtt_storage),
%%   		R1 = connection:execute_query(Conn, "CREATE DATABASE IF NOT EXISTS " ++ DB_name),
%% 			lager:debug([{endtype, End_Type}], "create DB: ~p", [R1]),
%% 
%% 			datasource:select_db(mqtt_storage, DB_name),

			Query1 =
				"CREATE TABLE IF NOT EXISTS session ("
				"client_id char(25) DEFAULT '',"
				" packet_id int DEFAULT 0,"
				" publish_rec blob,"
				" PRIMARY KEY (client_id, packet_id)"
				" ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
			R1 = connection:execute_query(Conn, Query1),
			lager:debug([{endtype, End_Type}], "create session table: ~p", [R1]),

			Query2 =
				"CREATE TABLE IF NOT EXISTS subscription ("
				"client_id char(25) DEFAULT '',"
				" topic varchar(512) DEFAULT ''," %% @todo make separate table 'topic'. Do I need it at all?
				" topic_re varchar(512),"           %% @todo make separate table 'topic'
				" share_name varchar(128) DEFAULT '',"
%%				" qos tinyint(1),"
				" options blob,"
				" callback blob,"
				" PRIMARY KEY (client_id, share_name, topic)"
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
			R2 = connection:execute_query(Conn, Query2),
			lager:debug([{endtype, End_Type}], "create subscription table: ~p", [R2]),

			Query3 =
				"CREATE TABLE IF NOT EXISTS connectpid ("
				"client_id char(25) DEFAULT '',"
				" pid tinyblob,"
				" PRIMARY KEY (client_id)"
				" ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
			R3 = connection:execute_query(Conn, Query3),
			lager:debug([{endtype, End_Type}], "create connectpid table: ~p", [R3]),

			if End_Type == server ->
					Query4 =
						"CREATE TABLE IF NOT EXISTS users ("
						"user_id char(25) DEFAULT '',"
						" password tinyblob,"
						" roles blob,"
						" PRIMARY KEY (user_id)"
						" ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
					R4 = connection:execute_query(Conn, Query4),
					lager:debug([{endtype, End_Type}], "create users table: ~p", [R4]),

					Query5 =
						"CREATE TABLE IF NOT EXISTS retain ("
						"topic varchar(512) DEFAULT '',"
						" publish_rec blob"
%						", PRIMARY KEY (topic)"
						") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
					R5 = connection:execute_query(Conn, Query5),
					lager:debug([{endtype, End_Type}], "create retain table: ~p", [R5]),
					
					Query6 =
						"CREATE TABLE IF NOT EXISTS session_state ("
						"client_id char(25) DEFAULT '',"
						" session_expiry_interval int DEFAULT 0,"
						" end_time int DEFAULT 0,"
						" will_publish_rec blob,"
						" PRIMARY KEY (client_id)"
						") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
					R6 = connection:execute_query(Conn, Query6),
					lager:debug([{endtype, End_Type}], "create session_state table: ~p", [R6]);
				 true -> ok
			end,

			datasource:return_connection(mqtt_storage, Conn),
			Pid;
		#mysql_error{} -> ok
	end.

session(save, #storage_publish{key = #primary_key{client_id = Client_Id, packet_id = Packet_Id}, document = Document}, End_Type) ->
	Query = ["REPLACE INTO session VALUES ('",
		Client_Id, "',",
		integer_to_list(Packet_Id), ",x'",
		mqtt_data:binary_to_hex(term_to_binary(Document)), "')"],
	execute_query(End_Type, Query);
session(exist, #primary_key{client_id = Client_Id, packet_id = Packet_Id}, End_Type) ->
	Query = [
		"SELECT packet_id FROM session WHERE client_id='",
		Client_Id, "' and packet_id=",
		integer_to_list(Packet_Id)],
	case execute_query(End_Type, Query) of
		[[_]] -> true;
		[] -> false
	end;
session(get, #primary_key{client_id = Client_Id, packet_id = Packet_Id}, End_Type) ->
	Query = ["SELECT publish_rec FROM session WHERE client_id='",
		Client_Id, "' and packet_id=",
		integer_to_list(Packet_Id)],
	case execute_query(End_Type, Query) of
		[] -> undefined;
		[[R2]] -> #storage_publish{key = #primary_key{client_id = Client_Id, packet_id = Packet_Id}, document = binary_to_term(R2)}
	end;
session(get_all, all, End_Type) ->
	Query = [
		"SELECT client_id, packet_id, publish_rec FROM session"],
	R = execute_query(End_Type, Query),
	[#storage_publish{key = #primary_key{client_id = CI, packet_id = PI}, document = binary_to_term(Publish_Rec)}
		|| [CI, PI, Publish_Rec] <- R];
session(get_all, Client_Id, End_Type) ->
	Query = [
		"SELECT client_id, packet_id, publish_rec FROM session WHERE client_id='",
		Client_Id, "'"],
	R = execute_query(End_Type, Query),
	[#storage_publish{key = #primary_key{client_id = CI, packet_id = PI}, document = binary_to_term(Publish_Rec)}
		|| [CI, PI, Publish_Rec] <- R];
session(remove, #primary_key{client_id = Client_Id, packet_id = Packet_Id}, End_Type) ->
	Query = ["DELETE FROM session WHERE client_id='",
		Client_Id, "' and packet_id=",
		integer_to_list(Packet_Id)],
	execute_query(End_Type, Query);
session(clean, ClientId, End_Type) ->
	Conn = datasource:get_connection(mqtt_storage),
  R1 = connection:execute_query(Conn, ["DELETE FROM session WHERE client_id='", ClientId, "'"]),
	lager:debug([{endtype, End_Type}], "session delete: ~p", [R1]),
	datasource:return_connection(mqtt_storage, Conn);
session(close, _, _) ->
	ok.

session_state(save, #session_state{client_id = Client_Id, session_expiry_interval = SessExp, end_time = End, will_publish = WillPubRec}) ->
	Query = ["REPLACE INTO session_state VALUES (",
		"'", Client_Id, "',",
		integer_to_list(SessExp), ",",
		integer_to_list(End), ",",
		"x'", mqtt_data:binary_to_hex(term_to_binary(WillPubRec)), "')"],
	execute_query(server, Query);
session_state(exist, _Client_Id) -> false;
session_state(get, Client_Id) ->
	Query = ["SELECT session_expiry_interval, end_time, will_publish_rec FROM session_state WHERE client_id='",
		Client_Id, "'"],
	case execute_query(server, Query) of
		[] -> undefined;
		[[SessExp, End, WillPubRec]] -> #session_state{client_id = Client_Id, session_expiry_interval = SessExp, end_time = End, will_publish = binary_to_term(WillPubRec)}
	end;
session_state(get_all, _) ->
	Query = ["SELECT client_id, session_expiry_interval, end_time, will_publish_rec FROM session_state"],
	R = execute_query(server, Query),
	[#session_state{client_id = CI, session_expiry_interval = SE, end_time = End, will_publish = binary_to_term(WillPubRec)}
		|| [CI, SE, End, WillPubRec] <- R];
session_state(remove, Client_Id) ->
	Query = ["DELETE FROM session_state WHERE client_id='",
		Client_Id, "'"],
	execute_query(server, Query);
session_state(clean, _) ->
	Conn = datasource:get_connection(mqtt_storage),
	R5 = connection:execute_query(Conn, "DELETE FROM session_state"),
	lager:debug([{endtype, server}], "session_state delete: ~p", [R5]),
	datasource:return_connection(mqtt_storage, Conn);
session_state(close, _) ->
	ok.

subscription(save, #storage_subscription{key = #subs_primary_key{client_id = Client_Id, shareName = ShareName, topicFilter = Topic}, options = Options, callback = CB}, End_Type) ->
	CBin = term_to_binary(CB),
	OptionsBin = term_to_binary(Options),
	Query = ["REPLACE INTO subscription VALUES ('",
		Client_Id, "','",
		Topic, "','",
		mqtt_data:topic_regexp(Topic), "'",
		if ShareName == undefined -> ",''"; true -> ",'" ++ ShareName ++ "'" end,
%		integer_to_list(QoS), ",x'",
		",x'", mqtt_data:binary_to_hex(OptionsBin), "'",
		",x'", mqtt_data:binary_to_hex(CBin), "')"],
	execute_query(End_Type, Query);
subscription(exist, _Key, _End_Type) -> false;
subscription(get, #subs_primary_key{client_id = Client_Id, topicFilter = Topic}, End_Type) -> %% @todo delete it
	Query = ["SELECT share_name, options, callback FROM subscription WHERE client_id='",
		Client_Id, "' and topic='",
		Topic, "'"],
	case execute_query(End_Type, Query) of
		[] -> undefined;
		List when is_list(List) ->
			[#storage_subscription{key = #subs_primary_key{
																			shareName = if ShareName  == "" -> undefined; true -> ShareName end,
																			topicFilter = Topic, 
																			client_id = Client_Id
																		},
															options = binary_to_term(Options), 
															callback = binary_to_term(CB)} || [ShareName, Options, CB] <- List];
		_ -> undefined
	end;
subscription(get_all, _, End_Type) ->
	Query = <<"SELECT topic FROM subscription">>,
%%	execute_query(End_Type, Query),
	[T || [T] <- execute_query(End_Type, Query)];
subscription(get_client_topics, Client_Id, End_Type) -> %% I do not use it @todo delete ???
	Query = ["SELECT share_name,topic,options,callback FROM subscription WHERE client_id='", Client_Id, "'"],
	[#storage_subscription{key = #subs_primary_key{client_id = Client_Id, shareName = if ShareName  == "" -> undefined; true -> ShareName end, topicFilter = TopicFilter},
												 options = binary_to_term(Options),
												 callback = binary_to_term(Callback)} || [ShareName, TopicFilter, Options, Callback] <- execute_query(End_Type, Query)];
subscription(get_matched_topics, #subs_primary_key{topicFilter = Topic, client_id = Client_Id}, End_Type) -> %% only client side
	Query = ["SELECT share_name,topic,options,callback FROM subscription WHERE client_id='",Client_Id,
					 "' and '",Topic,"' REGEXP topic_re"],
	L = execute_query(End_Type, Query),
	[#storage_subscription{key = #subs_primary_key{client_id = Client_Id, shareName = if ShareName  == "" -> undefined; true -> ShareName end, topicFilter = TopicFilter},
												 options = binary_to_term(Options),
												 callback = binary_to_term(CB)} || [ShareName, TopicFilter, Options, CB] <- L];
subscription(get_matched_topics, Topic, End_Type) -> %% only server side
	Query = ["SELECT client_id,topic,options,callback FROM subscription WHERE '",Topic,"' REGEXP topic_re and share_name = ''"],
	L = execute_query(End_Type, Query),
	[#storage_subscription{key = #subs_primary_key{client_id = Client_Id, topicFilter = TopicFilter},
												 options = binary_to_term(Options),
												 callback = binary_to_term(CB)}
		|| [Client_Id, TopicFilter, Options, CB] <- L];
subscription(get_matched_shared_topics, Topic, End_Type) -> %% only server side
	Query = ["SELECT client_id,share_name,topic,options,callback FROM subscription WHERE '",Topic,"' REGEXP topic_re and share_name != ''"],
	L = execute_query(End_Type, Query),
	[#storage_subscription{key = #subs_primary_key{client_id = Client_Id, shareName = ShareName, topicFilter = TopicFilter},
												 options = binary_to_term(Options),
												 callback = binary_to_term(CB)}
		|| [Client_Id, ShareName, TopicFilter, Options, CB] <- L];
subscription(remove, #subs_primary_key{client_id = Client_Id, _ = '_'}, End_Type) ->
	Query = ["DELETE FROM subscription WHERE client_id='",
		Client_Id, "'"],
	execute_query(End_Type, Query);
subscription(remove, #subs_primary_key{client_id = Client_Id, topicFilter = Topic}, End_Type) ->
	Query = ["DELETE FROM subscription WHERE client_id='",
		Client_Id, "' and topic='",
		Topic, "'"],
	execute_query(End_Type, Query);
subscription(clean, Client_Id, End_Type) ->
	Conn = datasource:get_connection(mqtt_storage),
	R2 = connection:execute_query(Conn, [
		"DELETE FROM subscription WHERE client_id='",
		Client_Id, "'"]),
	lager:debug([{endtype, End_Type}], "subscription delete: ~p", [R2]),
	datasource:return_connection(mqtt_storage, Conn);
subscription(close, _, _) ->
	ok.

connect_pid(save, #storage_connectpid{client_id = Client_Id, pid = Pid}, End_Type) ->
	Query = ["REPLACE INTO connectpid VALUES ('",
		Client_Id, "',x'",
		mqtt_data:binary_to_hex(term_to_binary(Pid)), "')"],
	execute_query(End_Type, Query);
connect_pid(exist, _Key, _End_Type) -> false;
connect_pid(get, Client_Id, End_Type) when is_binary(Client_Id)->
	connect_pid(get, binary_to_list(Client_Id), End_Type);
connect_pid(get, Client_Id, End_Type) when is_list(Client_Id) ->
	Query = [
		"SELECT pid FROM connectpid WHERE client_id='",
		Client_Id, "'"],
	case execute_query(End_Type, Query) of
		[] -> undefined;
		[[Pid]] -> binary_to_term(Pid)
	end;
connect_pid(get_all, _, End_Type) ->
	Query = ["SELECT pid FROM connectpid"],
	[Pid || [Pid] <- execute_query(End_Type, Query)];
connect_pid(remove, Client_Id, End_Type) ->
	Query = ["DELETE FROM connectpid WHERE client_id='", Client_Id, "'"],
	execute_query(End_Type, Query);
connect_pid(clean, Client_id, End_Type) ->
	connect_pid(remove, Client_id, End_Type);
connect_pid(close, _, _) ->
	ok.

user(save, #user{user_id = User_Id, password = Pswd, roles = Roles}) ->
	Query = ["REPLACE INTO users VALUES ('",
		User_Id, 
		"',x'", mqtt_data:binary_to_hex(crypto:hash(md5, Pswd)),
		"',x'", mqtt_data:binary_to_hex(term_to_binary(Roles)),
		"')"],
	execute_query(server, Query);
user(exist, _Key) -> false;
user(get, User_Id) ->
	Query = [
		"SELECT password, roles FROM users WHERE user_id='",
		User_Id, "'"],
	case execute_query(server, Query) of
		[] -> undefined;
		[[Password, Roles]] -> #{password => list_to_binary(mqtt_data:binary_to_hex(Password)), roles => binary_to_term(Roles)}
	end;
user(get_all, _) ->
	Query = ["SELECT password, roles FROM users"],
	R = execute_query(server, Query),
	[#{password => list_to_binary(mqtt_data:binary_to_hex(Password)), roles => binary_to_term(Roles)} || [Password, Roles] <- R];
user(remove, User_Id) ->
	Query = ["DELETE FROM users WHERE user_id='", User_Id, "'"],
	execute_query(server, Query);
user(clean, _) ->
	Query = ["DELETE FROM users"],
	execute_query(server, Query);
user(close, _) ->
	ok.

retain(save, #publish{topic = Topic} = Document) ->
	Query = ["REPLACE INTO retain VALUES ('",
		Topic, "',x'",
		mqtt_data:binary_to_hex(term_to_binary(Document)), "')"],
	execute_query(server, Query);
retain(exist, _Key) -> false;
retain(get, TopicFilter) ->
	Query = ["SELECT * FROM retain"],
	[binary_to_term(Publish_Rec) || [Topic, Publish_Rec] <- execute_query(server, Query), mqtt_data:is_match(Topic, TopicFilter)];
retain(get_all, _) ->
	Query = ["SELECT * FROM retain"],
	[binary_to_term(Publish_Rec) || [_, Publish_Rec] <- execute_query(server, Query)];
retain(remove, Topic) ->
	Query = ["DELETE FROM retain WHERE topic='", Topic, "'"],
	execute_query(server, Query);
retain(clean, _) ->
	Conn = datasource:get_connection(mqtt_storage),
	R4 = connection:execute_query(Conn, "DELETE FROM retain"),
	lager:debug([{endtype, server}], "retain delete: ~p", [R4]),
	datasource:return_connection(mqtt_storage, Conn);
retain(close, _) ->
	ok.

cleanup(ClientId, End_Type) ->
	session(clean, ClientId, End_Type),
	subscription(clean, ClientId, End_Type),
	if End_Type =:= server ->
				session_state(remove, ClientId);
		true -> ok
	end.

cleanup(End_Type) ->
	Conn = datasource:get_connection(mqtt_storage),
	R1 = connection:execute_query(Conn, "DELETE FROM session"),
	lager:debug([{endtype, End_Type}], "session delete: ~p", [R1]),
	R2 = connection:execute_query(Conn, "DELETE FROM subscription"),
	lager:debug([{endtype, End_Type}], "subscription delete: ~p", [R2]),
	R3 = connection:execute_query(Conn, "DELETE FROM connectpid"),
	lager:debug([{endtype, End_Type}], "connectpid delete: ~p", [R3]),
	if End_Type =:= server ->
		R4 = connection:execute_query(Conn, "DELETE FROM retain"),
		lager:debug([{endtype, End_Type}], "retain delete: ~p", [R4]),
		R5 = connection:execute_query(Conn, "DELETE FROM session_state"),
		lager:debug([{endtype, End_Type}], "session_state delete: ~p", [R5]);
		true -> ok
	end,
	datasource:return_connection(mqtt_storage, Conn).

close(_) -> 
	datasource:close(mqtt_storage).

%% ====================================================================
%% Internal functions
%% ====================================================================

execute_query(End_Type, Query) ->
	Conn = datasource:get_connection(mqtt_storage),
	Rez =
	case connection:execute_query(Conn, Query) of
		{_, R} ->
			lager:debug([{endtype, End_Type}], "Query: ~120p response: ~p", [Query, R]),
			R;
		Other ->
			lager:error([{endtype, End_Type}], "Error with Query: ~120p, ~120p", [Query, Other]),
			[]
	end,
	datasource:return_connection(mqtt_storage, Conn),
	Rez.

