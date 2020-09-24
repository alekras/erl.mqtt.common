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

%% @type connect() = #connect{} The record represents connection parameters.<br/> 
%% -record(<strong>mqtt_client_error</strong>, {
%% <dl>
%%   <dt>client_id :: string()</dt><dd>- The Client Identifier (ClientID) identifies the Client to the Server.
%%       Each Client connecting to the Server has a unique ClientID.</dd>
%%   <dt>user_name :: string()</dt><dd>- User name can be used by the Server for authentication and authorization.</dd>
%%   <dt>password :: binary()</dt><dd>- Password can be used to carry credential information.</dd>
%%   <dt>will = 0 :: 0 | 1</dt><dd>- If the Will Flag is set to 1 this indicates that a Will Message MUST be stored on the Server and associated with the Session.</dd>
%%   <dt>will_qos = 0 :: 0 | 1 | 2</dt><dd>- specifies the QoS level to be used when publishing the Will Message.</dd>
%%   <dt>will_retain = 0 :: 0 | 1</dt><dd>- specifies if the Will Message is to be retained when it is published.</dd>
%%   <dt>will_topic = "" :: string()</dt><dd>- Will message Topic.</dd>
%%   <dt>will_properties = [] :: list()</dt><dd>- The Will Properties field defines the Application Message properties
%%        to be sent with the Will Message when it is published, 
%%        and properties which define when to publish the Will Message.</dd>
%%   <dt>will_message = <<>> :: binary()</dt><dd>- Will message payload.</dd>
%%   <dt>clean_session = 1 :: 0 | 1</dt><dd>- This flag specifies whether the Connection starts a new Session or is a continuation of an existing Session.</dd>
%%   <dt>keep_alive :: integer()</dt>
%%     <dd>- It is the maximum time interval in seconds that is permitted to elapse between the point 
%%           at which the Client finishes transmitting one MQTT Control Packet 
%%           and the point it starts sending the next.</dd>
%%   <dt>properties = [] :: list()</dt><dd>- Connect packet properties.</dd>
%%   <dt>version = '3.1.1' :: '3.1' | '3.1.1' | '5.0'</dt><dd>- Version of MQTT protocol for the connection.</dd>
%% </dl>
%% }).
-record(connect, 
  {
    client_id :: string(),
    user_name :: string(),
    password :: binary(),
    will = 0 :: 0 | 1,
    will_qos = 0 :: 0 | 1 | 2,
    will_retain = 0 :: 0 | 1,
    will_topic = "" :: string(),
		will_properties = [] :: list(),
    will_message = <<>> :: binary(),
    clean_session = 1 :: 0 | 1,
		keep_alive :: integer(),
		properties = [] :: list(),
		version = '3.1.1' :: '3.1' | '3.1.1' | '5.0'
	}
).

-record(publish,
	{
		topic :: string(),
		dup = 0 :: 0 | 1,
		qos = 0 :: 0 | 1 | 2,
		retain = 0 :: 0 | 1,
		last_sent = none :: none | publish | pubrec | pubrel | pubcomp,
		dir = out :: in | out, 
		payload = <<>> :: binary(),
		properties = [] ::list()
	}
).

-record(subscription_options,
	{
		max_qos = 0 :: 0 | 1 | 2,
		nolocal = 0 :: 0 | 1,
		retain_as_published = 0 :: 0 | 1,
		retain_handling = 0 :: 0 | 1 | 2,
		identifier :: integer()
	}
).

-record(primary_key,
	{
		client_id :: string(),
		packet_id = 0 :: integer()
	}
).

-record(storage_publish,
	{
    key :: #primary_key{},
		document :: #publish{}
	}
).

-record(storage_retain,
	{
    topic :: string(),
		document :: #publish{}
	}
).

-record(subs_primary_key,
	{
		topicFilter :: string(),
		shareName = undefined :: undefined | string(),
		client_id :: string()
	}
).

-record(storage_subscription,
	{
		key :: #subs_primary_key{},
		options :: #subscription_options{}, %% qos = 0 :: 0 | 1 | 2,
		callback :: tuple()
	}
).

-record(storage_connectpid,
	{
		client_id :: string(),
		pid :: pid()
	}
).

-record(user,
	{
		user_id :: string(),
		password :: binary()
	}
).

-record(connection_state, 
  { socket :: port(),
		transport :: atom(),
		config = #connect{} :: #connect{},
		storage = mqtt_dets_dao :: atom(),
		end_type = client :: client | server,
		default_callback :: tuple(),
		session_present :: 0 | 1,
		connected = 0 :: 0 | 1, %% is used ?
		packet_id = 100 :: integer(),
%%		subscriptions = #{} :: map(), %% @todo keep in persistance storage
		topic_alias_in_map = #{} :: map(), %% TopicAlias => TopicName
		topic_alias_out_map = #{} :: map(), %% TopicAlias => TopicName
		processes = #{} :: map(), %% @todo keep in persistance storage for QoS =1,2
		tail = <<>> :: binary(),
		ping_count = 0 :: integer(), %% is used ?
		timer_ref :: reference(), %% for keep_alive
		test_flag :: atom() %% for testing only
  }
).

%% @type mqtt_client_error() = #mqtt_client_error{} The record represents an exception that is thrown by a client's module.<br/> 
%% -record(<strong>mqtt_client_error</strong>, {
%% <dl>
%%   <dt>type:: tcp | connection</dt><dd>- .</dd>
%%   <dt>errno = none:: none | integer()</dt><dd>- .</dd>
%%   <dt>source = []::string()</dt><dd>- .</dd>
%%   <dt>message = []::string()</dt><dd>- .</dd>
%% </dl>
%% }).
-record(mqtt_client_error, 
  {
    type:: tcp | connection, 
    errno = none:: none | integer(),
    source = []::string(), 
    message = []::string()
  }
).

-define(SOC_BUFFER_SIZE, 16#4000).
%-define(SOC_RECV_TIMEOUT, 60000).
-define(SOC_SEND_TIMEOUT, 60000).
-define(SOC_CONN_TIMEOUT, 60000).
-define(MQTT_GEN_SERVER_TIMEOUT, 1000).

-define(CONNECT_PACK_TYPE, 16#10:8).
-define(CONNACK_PACK_TYPE, 16#20:8).
-define(PUBLISH_PACK_TYPE, 16#3:4).
-define(PUBACK_PACK_TYPE,  16#40:8).
-define(PUBREC_PACK_TYPE, 16#50:8).
-define(PUBREL_PACK_TYPE, 16#62:8).
-define(PUBCOMP_PACK_TYPE, 16#70:8).
-define(SUBSCRIBE_PACK_TYPE, 16#82:8).
-define(SUBACK_PACK_TYPE, 16#90:8).
-define(UNSUBSCRIBE_PACK_TYPE, 16#A2:8).
-define(UNSUBACK_PACK_TYPE, 16#B0:8).
-define(PING_PACK_TYPE, 16#C0:8).
-define(PINGRESP_PACK_TYPE, 16#D0:8).
-define(DISCONNECT_PACK_TYPE, 16#E0:8).
-define(AUTH_PACK_TYPE, 16#F0:8).

-define(ELSE, true).