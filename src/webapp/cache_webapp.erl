-module(cache_webapp).

-export( [ out/1 ] ).
-include_lib("cache.hrl").
-include_lib("yaws_api.hrl").

out(Arg) ->
    AppModData = Arg#arg.appmoddata,
    PathInfo = string:tokens(AppModData, "/"),
 
    Request = Arg#arg.req,
    %%io:format("Method is ~p~n", [Request#http_request.method]),

    HeadersRec = Arg#arg.headers,
    Others = HeadersRec#headers.other,
%%    io:format("Others is ~p~n", [Others]),
%%    {http_header,0,"X-Ecache-Header",undefined,"asdf"}
    {RequestHasHeader, RequestHeader}  = 
	case lists:keyfind("X-Scache-Header", 3, Others) of
	    false ->
		{false, ""};
	    {_, _, _, _, HeaderVal} ->
		{true, HeaderVal};
	    _ ->
		{false, ""}
	end,
    
    UrlRec = yaws_api:request_url(Arg),
    Scheme = UrlRec#url.scheme,
    RequestIsSecure = case Scheme of
		   "https" ->
		       true;
		   _ ->
		       false
	       end,
    RequestTuple = {RequestIsSecure, RequestHasHeader, RequestHeader},
    
    case Request#http_request.method of
	'GET' ->
	    case get(PathInfo, RequestTuple) of
		{{status, 200}, Data} ->
		    {content, "binary-stream", Data};
		{status, N} ->
		    {status, N}
	    end;
	'PUT' ->
	    %%io:format("clidata is ~p~n", [Arg#arg.clidata]),
	    put(PathInfo, Arg#arg.clidata, RequestTuple);
	'DELETE' ->
	    delete(PathInfo, RequestTuple);
	'POST' ->
	    {status, 405}
    end
.


%%-record(http_request, {method,
%%                       path,
%%                       version}).



get([ContextName, Key], RequestTuple) ->
    case cache:getContextRecord(ContextName) of
	undefined ->
	    {status, 404};
	ContextRecord ->
	    ContextTuple = getContextTuple(ContextRecord),
	    case requestPassesValidation(ContextTuple, RequestTuple) of
		true ->
		    Data = cache:get(ContextName, Key),
		    case Data of
			undefined ->
			    {status, 404};
			_ ->
			    {{status, 200}, Data}
		    end;
		false ->
		    {status, 403}
	    end
    end
;
get(_, _) ->
    %%io:format("get:  not found~n"),
    {status, 404}
.



delete([ContextName, Key], RequestTuple) ->
    case cache:getContextRecord(ContextName) of
	undefined ->
	    {status, 404};
	ContextRecord ->
	    ContextTuple = getContextTuple(ContextRecord),
	    case requestPassesValidation(ContextTuple, RequestTuple) of
		true ->
		    cache:delete(ContextName, Key),
		    {status, 200};
		false ->
		    {status, 403}
	    end
    end
;
delete(_, _) ->
    %%io:format("delete: not found~n"),
    {status, 404}
.

put([ContextName, Key], Data, RequestTuple) -> 
    case cache:getContextRecord(ContextName) of
	undefined ->
	    {status, 404};
	ContextRecord ->
	    ContextTuple = getContextTuple(ContextRecord),
	    case requestPassesValidation(ContextTuple, RequestTuple) of
		true ->
		    %%io:format("Data is ~p~n", [Data]),
		    cache:put(ContextName, Key, Data),
		    {status, 200};
		false ->
		    {status, 403}
	    end
    end
;
put([ContextName, Key, StrTTL], Data, RequestTuple) ->
    case cache:getContextRecord(ContextName) of
	undefined ->
	    {status, 404};
	ContextRecord ->
	    ContextTuple = getContextTuple(ContextRecord),
	    case requestPassesValidation(ContextTuple, RequestTuple) of
		true ->
		    cache:put(ContextName, Key, Data, erlang:list_to_integer(StrTTL)),
		    {status, 200};
		false ->
		    {status, 403}
	    end
    end
;
put(_, _, _) ->
    io:format("put: not found~n"),
    {status, 404}
.



getContextTuple(ContextRecord) ->
    UseSecure = ContextRecord#context.use_secure,
    UseHeader = ContextRecord#context.use_header,
    HeaderValue = ContextRecord#context.header_value,
    {UseSecure, UseHeader, HeaderValue}
.
    


%% {IsSecure, UseHeader, HeaderValue}, {RequestIsSecure, RequestHasHeader, RequestHeaderValue}
requestPassesValidation({true, true, HeaderValue}, {true, true, HeaderValue}) ->
    true;
requestPassesValidation({true, false, _}, {true, _, _}) ->
    true;
requestPassesValidation({false, true, HeaderValue}, {_, true, HeaderValue}) ->
    true;
requestPassesValidation({false, false, HeaderValue}, {_, _, _}) ->
    true;
requestPassesValidation({_, _, _}, {_, _, _}) ->
    false
.


