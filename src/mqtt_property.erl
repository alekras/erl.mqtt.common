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

%% @since 2015-12-25
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
-export([parse/1]).

parse(Binary) ->
	Properties = [],
	{RestBinary, Length} = mqtt_input:decode_remaining_length(Binary),
	<<PropBinary:Length, Tail>> = RestBinary,
	Properties = parse(PropBinary, []),	
	{Properties, Tail}.

%% ====================================================================
%% Internal functions
%% ====================================================================

parse(<<>>, Properties) -> Properties;
parse(Binary, Properties) -> 
	{RestBinary, PropCode} = mqtt_input:decode_remaining_length(Binary),
	{Property, Tail} = property_retrieve(PropCode, RestBinary),
	parse(Tail, [Property | Properties]).

property_retrieve(?Payload_Format_Indicator = PropertyName, Binary) ->
	<<PropertyValue:8, RestBinary>> = Binary,
	{{PropertyName, PropertyValue}, RestBinary};
property_retrieve(_, _Binary) ->
	error.