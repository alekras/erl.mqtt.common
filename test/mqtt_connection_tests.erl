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

-module(mqtt_connection_tests).

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

connection_genServer_test_() ->
	[{ setup,
			fun do_start/0,
			fun do_stop/1,
		{ foreachx,
			fun setup/1,
			fun cleanup/2,
			[
				{one, fun test/2},
				{two, fun test/2}
			]
		}
	 }
	].

do_start() ->
	?debug_Fmt("::test:: >>> do_start() ~n", []),
	lager:start(),
	mqtt_dets_dao:start(client),
	mqtt_dets_dao:cleanup(client),
	Transport = mock_tcp,
	mock_tcp:start(),
	Storage = mqtt_dets_dao,
	Socket = undefined,
	State = #connection_state{socket = Socket, transport = Transport, storage = Storage, end_type = client},
	{ok, Pid} = gen_server:start_link({local, conn_server}, mqtt_connection, State, [{timeout, ?MQTT_GEN_SERVER_TIMEOUT}]),
	Pid.


do_stop(_X) ->
	?debug_Fmt("::test:: >>> do_stop(~p) ~n", [_X]),
	mock_tcp:stop(),
	
	mqtt_dets_dao:cleanup(client),	
	mqtt_dets_dao:close(client).	

setup(one) ->
	?debug_Fmt("::test:: >>> setup(one) ~n", []),
	setup1;
setup(two) ->
	?debug_Fmt("::test:: >>> setup(two) ~n", []),
	setup2.

cleanup(X, Y) ->
	?debug_Fmt("::test:: >>> cleanup(~p,~p) ~n", [X,Y]).

test(X, Y) -> {"test [" ++ atom_to_list(X) ++ "]", timeout, 1, fun() ->
	?debug_Fmt("::test:: >>> test(~p, ~p) ~n", [X,Y]),
	gen_server:call(conn_server, {pingreq, fun(_Pong) -> ok end}),
	gen_server:call(conn_server, disconnect),
	?passed
end}.

