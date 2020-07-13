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

%% @since 2020-03-26
%% @copyright 2015-2020 Alexei Krasnopolski
%% @author Alexei Krasnopolski <krasnop@bellsouth.net> [http://krasnopolski.org/]
%% @version {@version}
%% @doc @todo Add description to mqtt_property.

-module(mqtt_property).

%%
%% Include files
%%
-include("mqtt_property.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([parse/1, to_binary/1]).

parse(Binary) ->
	{RestBinary, Length} = mqtt_data:extract_variable_byte_integer(Binary),
	<<PropBinary:Length/binary, Tail/binary>> = RestBinary,
	Properties = parse(PropBinary, []),	
	{Properties, Tail}.

to_binary(Properties) ->
	Binary = form(Properties, <<>>),
	Length = byte_size(Binary),
	
	<<(mqtt_data:encode_variable_byte_integer(Length))/binary, Binary/binary>>.
%% ====================================================================
%% Internal functions
%% ====================================================================

parse(<<>>, Properties) -> Properties;
parse(Binary, Properties) -> 
	{RestBinary, PropCode} = mqtt_data:extract_variable_byte_integer(Binary),
	{Property, Tail} = property_retrieve(PropCode, RestBinary), %% @todo check property duplication: throw exception
	parse(Tail, [Property | Properties]).

%% One byte property
property_retrieve(PropertyName, Binary)
						when (PropertyName == ?Payload_Format_Indicator);
							(PropertyName == ?Request_Problem_Information);
							(PropertyName == ?Request_Response_Information);
							(PropertyName == ?Maximum_QoS);
							(PropertyName == ?Retain_Available);
							(PropertyName == ?Wildcard_Subscription_Available);
							(PropertyName == ?Subscription_Identifier_Available);
							(PropertyName == ?Shared_Subscription_Available) ->
	<<PropertyValue:8, RestBinary/binary>> = Binary,
	{{PropertyName, PropertyValue}, RestBinary};

%% Two byte property
property_retrieve(PropertyName, Binary)
						when (PropertyName == ?Server_Keep_Alive);
							(PropertyName == ?Receive_Maximum);
							(PropertyName == ?Topic_Alias_Maximum);
							(PropertyName == ?Topic_Alias) ->
	<<PropertyValue:16, RestBinary/binary>> = Binary,
	{{PropertyName, PropertyValue}, RestBinary};

%% Four byte property
property_retrieve(PropertyName, Binary)
						when (PropertyName == ?Message_Expiry_Interval);
							(PropertyName == ?Session_Expiry_Interval);
							(PropertyName == ?Will_Delay_Interval);
							(PropertyName == ?Maximum_Packet_Size) ->
	<<PropertyValue:32, RestBinary/binary>> = Binary,
	{{PropertyName, PropertyValue}, RestBinary};

%% UTF-8 property
property_retrieve(PropertyName, Binary)
						when (PropertyName == ?Content_Type);
							(PropertyName == ?Response_Topic);
							(PropertyName == ?Assigned_Client_Identifier);
							(PropertyName == ?Authentication_Method);
							(PropertyName == ?Response_Information);
							(PropertyName == ?Server_Reference);
							(PropertyName == ?Reason_String) ->
	{RestBinary, PropertyValue} = mqtt_data:extract_utf8_binary(Binary),
	{{PropertyName, PropertyValue}, RestBinary};

%% Binary property
property_retrieve(PropertyName, Binary)
						when (PropertyName == ?Correlation_Data);
							(PropertyName == ?Authentication_Data) ->
	{RestBinary, PropertyValue} = mqtt_data:extract_binary_field(Binary),
	{{PropertyName, PropertyValue}, RestBinary};

%% Variable byte integer property
property_retrieve(?Subscription_Identifier = PropertyName, Binary) ->
	{RestBinary, PropertyValue} = mqtt_data:extract_variable_byte_integer(Binary),
	{{PropertyName, PropertyValue}, RestBinary};

%% UTF-8 pair property
property_retrieve(?User_Property = PropertyName, Binary) ->
	{RestBinary0, Name} = mqtt_data:extract_utf8_binary(Binary),
	{RestBinary, Value} = mqtt_data:extract_utf8_binary(RestBinary0),
	{{PropertyName, [{name, Name}, {value, Value}]}, RestBinary};

property_retrieve(_, _Binary) ->
	error.

form([], Binary) -> Binary;
form([Property | Properties], Binary) ->
	PropBin = form_prop(Property),
	form(Properties, << PropBin/binary, Binary/binary>>).

%% One byte integer type
form_prop({PropertyName, PropertyValue})
							when (PropertyName == ?Payload_Format_Indicator);
							(PropertyName == ?Request_Problem_Information);
							(PropertyName == ?Request_Response_Information);
							(PropertyName == ?Maximum_QoS);
							(PropertyName == ?Retain_Available);
							(PropertyName == ?Wildcard_Subscription_Available);
							(PropertyName == ?Subscription_Identifier_Available);
							(PropertyName == ?Shared_Subscription_Available) ->
	<<(mqtt_data:encode_variable_byte_integer(PropertyName))/binary, PropertyValue:8>>;

%% Two bytes integer type
form_prop({PropertyName, PropertyValue})
						when (PropertyName == ?Server_Keep_Alive);
							(PropertyName == ?Receive_Maximum);
							(PropertyName == ?Topic_Alias_Maximum);
							(PropertyName == ?Topic_Alias) ->
	<<(mqtt_data:encode_variable_byte_integer(PropertyName))/binary, PropertyValue:16>>;

%% Four bytes integer type
form_prop({PropertyName, PropertyValue})
						when (PropertyName == ?Message_Expiry_Interval);
							(PropertyName == ?Session_Expiry_Interval);
							(PropertyName == ?Will_Delay_Interval);
							(PropertyName == ?Maximum_Packet_Size) ->
	<<(mqtt_data:encode_variable_byte_integer(PropertyName))/binary, PropertyValue:32>>;

%% UTF-8 type
form_prop({PropertyName, PropertyValue})
						when (PropertyName == ?Content_Type);
							(PropertyName == ?Response_Topic);
							(PropertyName == ?Assigned_Client_Identifier);
							(PropertyName == ?Authentication_Method);
							(PropertyName == ?Response_Information);
							(PropertyName == ?Server_Reference);
							(PropertyName == ?Reason_String) ->
	<<(mqtt_data:encode_variable_byte_integer(PropertyName))/binary,
		(mqtt_data:encode_utf8_string(PropertyValue))/binary>>;

%% Binary type
form_prop({PropertyName, PropertyValue})
						when (PropertyName == ?Correlation_Data);
							(PropertyName == ?Authentication_Data) ->
	<<(mqtt_data:encode_variable_byte_integer(PropertyName))/binary, (mqtt_data:encode_binary_field(PropertyValue))/binary>>;

%% Variable byte integer type
form_prop({?Subscription_Identifier = PropertyName, PropertyValue}) ->
	<<(mqtt_data:encode_variable_byte_integer(PropertyName))/binary, (mqtt_data:encode_variable_byte_integer(PropertyValue))/binary>>;

% UTF-8 Pair type
form_prop({?User_Property = PropertyName, PropertyValue}) ->
	[{name, Name}, {value, Value}] = PropertyValue,
	<<(mqtt_data:encode_variable_byte_integer(PropertyName))/binary,
		(mqtt_data:encode_utf8_string(Name))/binary,
		(mqtt_data:encode_utf8_string(Value))/binary>>;

form_prop({_, _PropertyValue}) ->
	error.

