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


-module(mqtt_mysql_dao).
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
				" qos tinyint(1),"
				" callback blob,"
				" PRIMARY KEY (topic, client_id)"
				" ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
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
						" PRIMARY KEY (user_id)"
						" ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
					R4 = connection:execute_query(Conn, Query4),
					lager:debug([{endtype, End_Type}], "create users table: ~p", [R4]),

					Query5 =
						"CREATE TABLE IF NOT EXISTS retain ("
						"topic varchar(512) DEFAULT '',"
						" publish_rec blob"
%						", PRIMARY KEY (topic)"
						" ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8",
					R5 = connection:execute_query(Conn, Query5),
					lager:debug([{endtype, End_Type}], "create retain table: ~p", [R5]);
				 true -> ok
			end,

			datasource:return_connection(mqtt_storage, Conn),
			Pid;
		#mysql_error{} -> ok
	end.

save(End_Type, #storage_publish{key = #primary_key{client_id = Client_Id, packet_id = Packet_Id}, document = Document}) ->
	Query = ["REPLACE INTO session VALUES ('",
		Client_Id, "',",
		integer_to_list(Packet_Id), ",x'",
		binary_to_hex(term_to_binary(Document)), "')"],
	execute_query(End_Type, Query);
save(End_Type, #storage_subscription{key = #subs_primary_key{client_id = Client_Id, topic = Topic}, qos = QoS, callback = CB}) ->
	CBin = term_to_binary(CB),
	Query = ["REPLACE INTO subscription VALUES ('",
		Client_Id, "','",
		Topic, "','",
		mqtt_socket_stream:topic_regexp(Topic), "',",
		integer_to_list(QoS), ",x'",
		binary_to_hex(CBin), "')"],
		execute_query(End_Type, Query);
save(End_Type, #storage_connectpid{client_id = Client_Id, pid = Pid}) ->
	Query = ["REPLACE INTO connectpid VALUES ('",
		Client_Id, "',x'",
		binary_to_hex(term_to_binary(Pid)), "')"],
	execute_query(End_Type, Query);
save(server, #user{user_id = User_Id, password = Pswd}) ->
	Query = ["REPLACE INTO users VALUES ('",
		User_Id, "',x'",
		binary_to_hex(term_to_binary(crypto:hash(md5, Pswd))), "')"],
	execute_query(server, Query);
save(server, #publish{topic = Topic} = Document) ->
	Query = ["REPLACE INTO retain VALUES ('",
		Topic, "',x'",
		binary_to_hex(term_to_binary(Document)), "')"],
	execute_query(server, Query).

remove(End_Type, #primary_key{client_id = Client_Id, packet_id = Packet_Id}) ->
	Query = ["DELETE FROM session WHERE client_id='",
		Client_Id, "' and packet_id=",
		integer_to_list(Packet_Id)],
	execute_query(End_Type, Query);
remove(End_Type, #subs_primary_key{client_id = Client_Id, _ = '_'}) ->
	Query = ["DELETE FROM subscription WHERE client_id='",
		Client_Id, "'"],
	execute_query(End_Type, Query);
remove(End_Type, #subs_primary_key{client_id = Client_Id, topic = Topic}) ->
	Query = ["DELETE FROM subscription WHERE client_id='",
		Client_Id, "' and topic='",
		Topic, "'"],
	execute_query(End_Type, Query);
remove(End_Type, {client_id, Client_Id}) ->
	Query = ["DELETE FROM connectpid WHERE client_id='", Client_Id, "'"],
	execute_query(End_Type, Query);
remove(server, {user_id, User_Id}) ->
	Query = ["DELETE FROM users WHERE user_id='", User_Id, "'"],
	execute_query(server, Query);
remove(server, {topic, Topic}) ->
	Query = ["DELETE FROM retain WHERE topic='", Topic, "'"],
	execute_query(server, Query).

get(End_Type, #primary_key{client_id = Client_Id, packet_id = Packet_Id}) ->
	Query = ["SELECT publish_rec FROM session WHERE client_id='",
		Client_Id, "' and packet_id=",
		integer_to_list(Packet_Id)],
	case execute_query(End_Type, Query) of
		[] -> undefined;
		[[R2]] -> #storage_publish{key = #primary_key{client_id = Client_Id, packet_id = Packet_Id}, document = binary_to_term(R2)}
	end;
get(End_Type, #subs_primary_key{client_id = Client_Id, topic = Topic}) -> %% @todo delete it
	Query = ["SELECT qos, callback FROM subscription WHERE client_id='",
		Client_Id, "' and topic='",
		Topic, "'"],
	case execute_query(End_Type, Query) of
		[] -> undefined;
		[[QoS, CB]] -> #storage_subscription{key = #subs_primary_key{topic = Topic, client_id = Client_Id}, qos = QoS, callback = binary_to_term(CB)}
	end;
get(End_Type, {client_id, Client_Id}) ->
	Query = [
		"SELECT pid FROM connectpid WHERE client_id='",
		Client_Id, "'"],
	case execute_query(End_Type, Query) of
		[] -> undefined;
		[[Pid]] -> binary_to_term(Pid)
	end;
get(server, {user_id, User_Id}) ->
	Query = [
		"SELECT password FROM users WHERE user_id='",
		User_Id, "'"],
	case execute_query(server, Query) of
		[] -> undefined;
		[[Password]] -> binary_to_term(Password)
	end;
get(server, {topic, TopicFilter}) ->
	Query = ["SELECT * FROM retain"],
	[binary_to_term(Publish_Rec) || [Topic, Publish_Rec] <- execute_query(server, Query), mqtt_socket_stream:is_match(Topic, TopicFilter)].

get_client_topics(End_Type, Client_Id) ->
	Query = ["SELECT topic, qos, callback FROM subscription WHERE client_id='", Client_Id, "'"],
	[{Topic, QoS, binary_to_term(Callback)} || [Topic, QoS, Callback] <- execute_query(End_Type, Query)].

get_matched_topics(End_Type, #subs_primary_key{topic = Topic, client_id = Client_Id}) ->
	Query = ["SELECT topic,qos,callback FROM subscription WHERE client_id='",Client_Id,
					 "' and '",Topic,"' REGEXP topic_re"],
	L = execute_query(End_Type, Query),
	[{TopicFilter, QoS, binary_to_term(CB)} || [TopicFilter, QoS, CB] <- L];
get_matched_topics(End_Type, Topic) ->
	Query = ["SELECT client_id,topic,qos,callback FROM subscription WHERE '",Topic,"' REGEXP topic_re"],
	L = execute_query(End_Type, Query),
	[#storage_subscription{key = #subs_primary_key{client_id = Client_Id, topic = TopicFilter}, qos = QoS, callback = binary_to_term(CB)}
		|| [Client_Id, TopicFilter, QoS, CB] <- L].
	
get_all(End_Type, {session, Client_Id}) ->
	Query = [
		"SELECT client_id, packet_id, publish_rec FROM session WHERE client_id='",
		Client_Id, "'"],
	R = execute_query(End_Type, Query),
	[#storage_publish{key = #primary_key{client_id = CI, packet_id = PI}, document = binary_to_term(Publish_Rec)}
		|| [CI, PI, Publish_Rec] <- R];
get_all(End_Type, topic) ->
	Query = <<"SELECT topic FROM subscription">>,
	execute_query(End_Type, Query),
	[T || [T] <- execute_query(End_Type, Query)].

cleanup(End_Type, Client_Id) ->
	Conn = datasource:get_connection(mqtt_storage),
  R1 = connection:execute_query(Conn, [
		"DELETE FROM session WHERE client_id='",
		Client_Id, "'"]),
	lager:debug([{endtype, End_Type}], "session delete: ~p", [R1]),
	R2 = connection:execute_query(Conn, [
		"DELETE FROM subscription WHERE client_id='",
		Client_Id, "'"]),
	lager:debug([{endtype, End_Type}], "subscription delete: ~p", [R2]),
  R3 = connection:execute_query(Conn, [
		"DELETE FROM connectpid WHERE client_id='",
		Client_Id, "'"]),
	lager:debug([{endtype, End_Type}], "connectpid delete: ~p", [R3]),
	datasource:return_connection(mqtt_storage, Conn).

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
		lager:debug([{endtype, End_Type}], "retain delete: ~p", [R4]);
		true -> ok
	end,
	datasource:return_connection(mqtt_storage, Conn).

exist(End_Type, #primary_key{client_id = Client_Id, packet_id = Packet_Id}) ->
	Query = [
		"SELECT packet_id FROM session WHERE client_id='",
		Client_Id, "' and packet_id=",
		integer_to_list(Packet_Id)],
	case execute_query(End_Type, Query) of
		[[_]] -> true;
		[] -> false
	end.

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

binary_to_hex(Binary) -> [conv(N) || <<N:4>> <= Binary].

% 48 = $0
% 87 = ($a - 10)
conv(N) when N < 10 -> N + 48; 
conv(N) -> N + 87. 

%% binary_to_hex(Binary) -> binary_to_hex(Binary, []).
%% 
%% binary_to_hex(<<>>, Hex) -> lists:reverse(Hex);
%% binary_to_hex(<<N:4, Binary/bitstring>>, Hex) when N < 10 ->
%% 	binary_to_hex(Binary, [(48 + N) | Hex]);
%% binary_to_hex(<<N:4, Binary/bitstring>>, Hex) ->
%% 	binary_to_hex(Binary, [(87 + N) | Hex]).

%% binary_to_hex(<<>>) -> [];
%% binary_to_hex(<<N:4, Binary/bitstring>>) when N < 10 ->
%% 	[(48 + N) | binary_to_hex(Binary)];
%% binary_to_hex(<<N:4, Binary/bitstring>>) ->
%% 	[(87 + N) | binary_to_hex(Binary)].
