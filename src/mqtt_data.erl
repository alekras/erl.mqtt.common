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

%% @since 2020-07-09
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_data.


-module(mqtt_data).

-include("mqtt.hrl").
-include("mqtt_property.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	encode_variable_byte_integer/1,
	extract_variable_byte_integer/1,
	encode_utf8_string/1,
	extract_utf8_binary/1,
	extract_utf8_list/1,
	encode_binary_field/1,
	extract_binary_field/1,
	fieldSize/1,
	is_topicFilter_valid/1,
	is_match/2,
	topic_regexp/1,
	validate_config/1,
	validate_publish/2
]).

%% Variable byte Integer:
encode_variable_byte_integer(0) -> <<0>>;
encode_variable_byte_integer(Length) ->
	encode_rl(Length, <<>>).

encode_rl(0, Result) -> Result;
encode_rl(L, Result) -> 
	Rem = L div 128,
	EncodedByte = (L rem 128) bor (if Rem > 0 -> 16#80; true -> 0 end), 
	encode_rl(Rem, <<Result/binary, EncodedByte:8>>).

extract_variable_byte_integer(Binary) ->
	decode_rl(Binary, 1, 0).

decode_rl(_, MP, L) when MP > (128 * 128 * 128) -> {error, L};
decode_rl(<<0:1, EncodedByte:7, Binary/binary>>, MP, L) ->
	NewL = L + EncodedByte * MP,
	{Binary, NewL};
decode_rl(<<1:1, EncodedByte:7, Binary/binary>>, MP, L) ->
	NewL = L + EncodedByte * MP,
	decode_rl(Binary, MP * 128, NewL).

%% UTF8 string:
encode_utf8_string(String) ->
	Binary = 
		case unicode:characters_to_binary(String, utf8) of
			{error, _, _} -> error;
			{incomplete, _, _} -> error;
			R -> R
		end,
	Size = byte_size(Binary),
	<<Size:16, Binary/binary>>.

extract_utf8_binary(Binary) ->
	<<Size:16, UTF8_binary:Size/binary, Tail/binary>> = Binary,
	UTF8_string = 
		case unicode:characters_to_binary(UTF8_binary, utf8) of
			{error, _, _} -> error;
			{incomplete, _, _} -> error;
			R -> R
		end,
	{Tail, UTF8_string}.

extract_utf8_list(Binary) ->
	<<Size:16, UTF8_binary:Size/binary, Tail/binary>> = Binary,
	UTF8_list = 
		case unicode:characters_to_list(UTF8_binary, utf8) of
			{error, _, _} -> error;
			{incomplete, _, _} -> error;
			R -> R
		end,
	{Tail, UTF8_list}.

encode_binary_field(Binary) ->
	Size = byte_size(Binary),
	<<Size:16, Binary/binary>>.

extract_binary_field(InputStream) ->
	<<Size:16, Binary:Size/binary, Tail/binary>> = InputStream,
	{Tail, Binary}.
	
fieldSize(F) when is_list(F) -> length(F);
fieldSize(F) when is_binary(F) -> byte_size(F);
fieldSize(_) -> 0.

is_topicFilter_valid(TopicFilter) ->
%% $share/{ShareName}	
	{ok, Pattern} = re:compile("^(\\$share\\/(?<shareName>[^/\\+#]+){1}\\/)?"
														"(?<topicFilter>"
															"([^\\/\\+#]*|\\+)?"
															"(\\/[^\\/\\+#]*|\\/\\+)*"
															"(\\/#|#|\\/)?"
														"){1}$"),
	case re:run(TopicFilter, Pattern, [global, {capture, [shareName, topicFilter], list}]) of
		{match, [R]} -> 
			{true, R};
		_E ->
			false
	end.

is_match(Topic, TopicFilter) ->
	{ok, Pattern} = re:compile(topic_regexp(TopicFilter)),
	case re:run(Topic, Pattern, [global, {capture, [1], list}]) of
		{match, _R} -> true;
		_E ->		false
	end.

validate_config(#connect{client_id= ClientId, user_name= User, will= WillFlag,
												 will_publish = WillPubRec,
												 properties= Props, version= '5.0'}) ->
	true = validate_string_field(ClientId, "Client Id"),
	case re:run(ClientId,"^[0-9a-zA-Z]*$") of
		nomatch ->
			throw(#mqtt_client_error{type= name, message= "Client Id"});
		_ -> ok
	end,
	true = validate_string_field(User, "User name"),
	P = mqtt_property:validate(connect, Props),
	if not P -> throw(#mqtt_client_error{type= property, message= "Connect Properties"});
		 ?ELSE -> ok
	end,
	if (WillFlag == 1) and is_record(WillPubRec, publish) ->
			#publish{topic= WillTopic, payload= WillPayload, properties= WillProps} = WillPubRec,
			true = validate_string_field(WillTopic, "Will Topic"),
			case is_topicFilter_valid(WillTopic) of
				false -> throw(#mqtt_client_error{type= topic, message= "Will Topic"});
				_ -> ok
			end,
			WP = mqtt_property:validate(will, WillProps),
			if not WP -> throw(#mqtt_client_error{type= will_property, message= "Will Properties"});
				?ELSE -> ok
			end,
			case proplists:get_value(?Payload_Format_Indicator, WillProps, 0) of
				0 -> ok;
				1 -> true = validate_string_field(WillPayload, "Will Payload")
			end;
		?ELSE -> ok
	end,
	true;
validate_config(#connect{client_id= ClientId, user_name= User, will= WillFlag,
												 will_publish = WillPubRec}) ->
	true = validate_string_field(ClientId, "Client Id"),
	case re:run(ClientId,"^[0-9a-zA-Z]*$") of
		nomatch ->
			throw(#mqtt_client_error{type= name, message= "Client Id"});
		_ -> ok
	end,
	true = validate_string_field(User, "User name"),
	if (WillFlag == 1) and is_record(WillPubRec, publish) ->
			#publish{topic= WillTopic} = WillPubRec,
			true = validate_string_field(WillTopic, "Will Topic"),
			case is_topicFilter_valid(WillTopic) of
				false -> throw(#mqtt_client_error{type= topic, message= "Will Topic"});
				_ -> ok
			end;
		?ELSE -> ok
	end,
	true.

validate_publish('5.0', #publish{topic= Topic, payload= Payload, properties= Props}) ->
	true = validate_string_field(Topic, "Publish Topic"),
	case is_topicFilter_valid(Topic) of
		false -> throw(#mqtt_client_error{type= topic, message= "Publish Topic"});
		_ -> ok
	end,
	WP = mqtt_property:validate(publish, Props),
	if not WP -> throw(#mqtt_client_error{type= will_property, message= "Publish Properties"});
		?ELSE -> ok
	end,
	case proplists:get_value(?Payload_Format_Indicator, Props, 0) of
		0 -> ok;
		1 -> true = validate_string_field(Payload, "Publish Payload")
	end,
	true;
validate_publish(_, #publish{topic= Topic}) ->
	true = validate_string_field(Topic, "Publish Topic"),
	case is_topicFilter_valid(Topic) of
		false -> throw(#mqtt_client_error{type= topic, message= "Publish Topic"});
		_ -> ok
	end,
	true.

%% ====================================================================
%% Internal functions
%% ====================================================================

validate_string_field(String, Name) ->
	case unicode:characters_to_list(String, utf8) of
		{error, _, _} -> throw(#mqtt_client_error{type= utf8, message= Name});
		{incomplete, _, _} -> throw(#mqtt_client_error{type= utf8, message= Name});
		_ -> true
	end.
	
	
topic_regexp(TopicFilter) ->
	R1 = re:replace(TopicFilter, "\\+", "([^\\/]*)", [global, {return, list}]),
	R2 = re:replace(R1, "#", "(.*)", [global, {return, list}]),
	"^" ++ R2 ++ "$".


