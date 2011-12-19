-module(admin_webapp).

-export( [ out/1 ] ).
-include_lib("cache.hrl").
-include_lib("yaws_api.hrl").

-define(ADMIN_COOKIE, "CacheAdminSession").

-define(REQUIRED_CONTEXT_PARAMS, ["context-name", "ttl", "memory-allocation", "use-header", "use-secure"]).

-define(OPTION_CONTEXT_PARAMS, ["header-value"]).

-define(ADD_ADMIN_USER_PARAMS, ["login-id", "password", "repeat-password", "first-name", "last-name"]).

-define(URL_REQUIRES_SESSION, 
	["add-admin-user", 
	 "delete-admin-user", 
	 "force-user-off",
	 "add-context", 
	 "ajax/add-context", 
	 "delete-context",
	 "ajax/delete-context", 
	 "change-context", 
	 "ajax/change-context",
	 "add-node", 
	 "ajax/add-node", 
	 "delete-node", 
	 "ajax/delete-node",
	 "cache-summary", 
	 "ajax/cache-summary"]).


%% return a read-only or a read-write view, based
%% on how many people are viewing the admin screen

out(Arg) ->
    AppModData = Arg#arg.appmoddata,
    Path = string:tokens(AppModData, "/"),
    
    %%io:format("path is ~p~n", [Path]),
    case lists:member(AppModData, ?URL_REQUIRES_SESSION) of
	true ->
	    %%io:format("Checkpoint 2~n"),
	    case validateLoggedInStatus(Arg) of
		true ->
		    %%io:format("Checkpoint 3~n"),
		    do(Path, Arg);
		false ->
		    {redirect_local, "/admin/login"}
	    end;
	false ->
	    do(Path, Arg)
    end
.

%% /admin/login
login(Arg) ->
    {IsValid, ListOfPostVars} = validatePost([?LOGIN_ID, ?PASSWORD], Arg),
    
    case IsValid of 
	false ->
	    {redirect_local, "/admin/login?error=0"};
	true ->
	    {_, {ok, LoginId}} = lists:keyfind(?LOGIN_ID, 1, ListOfPostVars),
	    {_, {ok, Password}} = lists:keyfind( ?PASSWORD, 1, ListOfPostVars),
	    case validateUserLogin(Arg, LoginId, Password) of
		true ->
		    %% set cookie values, add user to table of logged in
		    %% users
		    SessionId = erlang:md5(LoginId ++ Password),
		    addAdminUserToLoggedIn(SessionId, LoginId),
		    [
		     {redirect_local, "/admin/cache-summary"},
		     {yaws_api:setcookie(?ADMIN_COOKIE, SessionId)}
		    ];
		false ->
		    {redirect_local, "/admin/login?" ++ ?LOGIN_ID ++ "=" ++ LoginId}
	    end
    end
.

validatePost(ListOfParameters, Arg) ->
    ListOfPostVars =
	lists:foldl(fun(Param, Acc) ->
			    Acc ++ [{Param, yaws_api:postvar(Arg, Param)}] 
		    end,
		    [],
		    ListOfParameters
		   ),
    IsValid = (lists:keyfind(undefined, 2, ListOfPostVars) == true),
    {IsValid, ListOfPostVars}
.
			    
do(["login"], Arg) ->
    %%login(Arg)
    {page, "/cache/login.ysp"}
;

do(["validate-login"], Arg) ->
    login(Arg)
;

do(["add-admin-user"], Arg) ->
    case addAdminUser(Arg) of
	ok ->
	    {redirect_local, "/admin/cache-summary"};
	{error, Message} ->
	    {status, 403}
    end
;

do(["delete-admin-user"], Arg) ->
    case deleteAdminUser(Arg) of
	ok ->
	    {redirect_local, "/admin/cache-summary"};
	{error, Message} ->
	    {status, 403}
    end
;

do(["force-user-off"], Arg) ->
    ok;

%%-record(context, 
%%	{
%%	  context_name,        %% string value, which is used in the url to identify the type of object being cached: e.g. /cache/mycontext/mykey
%%	  use_header,          %% boolean: should the http header x-icache-header be used for validation?
%%	  header_value,        %% http header value for authentication.  The http header x-icache-header of the caller needs to be set to this
%%	  use_secure,          %% boolean indicating whether these need to be secure transactions
%%	  default_ttl,         %% in seconds, how long should the cache data be stored
%%	  memory_allocation,   %% memory, in bytes, to be used for this context
%%	  node_fragments       %% list of tuples of the atoms:  {Node, Fragment} eg {'node1@host1', context1_fragment_(hash(Node))} 
%%	}
%%).
%% ["context-name", "ttl", "memory-allocation", "use-header", "use-secure"]).
%% /admin/add-context
do(["add-context"], Arg) ->
    {IsValid, ListOfPostVars} = validatePost(?REQUIRED_CONTEXT_PARAMS, Arg),
    case IsValid of
	true ->
	    UseSecure = case yaws_api:postvar(Arg, "use-secure") of
			    {ok, "true"} ->
				true;
			    _ ->
				false
			end,
	    {ok, MemoryAlloc} = yaws_api:postvar(Arg, "memory-allocation"),
	    {ok, TTL} = yaws_api:postvar(Arg, "ttl"),
	    UseHeader = yaws_api:postvar(Arg, "use-header"),	    
	    case UseHeader of 
		{ok, "true"} ->
		    HeaderValue = yaws_api:postvar(Arg, "header-value"),
		    TTL = yaws_api:postvar(Arg, "ttl"),
		    case HeaderValue of
			{ok, Value} ->
			    ContextRecord = #context{
			      context_name = yaws_api:postvar(Arg, "context-name"),
			      use_header = true,
			      header_value = Value,
			      use_secure = UseSecure,
			      default_ttl = erlang:list_to_integer(TTL),
			      memory_allocation = erlang:list_to_integer(MemoryAlloc),
			      node_fragments = cache:getNodeList()
			     },
			    cache:createContext(ContextRecord),
			    {redirect_local, "/cache/cache-summary"};
			_ ->
			    {status, 403}
		    end;
		_ ->
		    ContextRecordNoHeader = #context{
		      context_name = yaws_api:postvar(Arg, "context-name"),
		      use_header = false,
		      header_value = "",
		      use_secure = UseSecure,
		      default_ttl = erlang:list_to_integer(yaws_api:postvar(Arg, "ttl")),
		      memory_allocation = erlang:list_to_integer(yaws_api:postvar(Arg, "memory-allocation")),
		      node_fragments = cache:getNodeList()
		     },
		    cache:createContext(ContextRecordNoHeader),
		    {redirect_local, "/cache/cache-summary"}
	    end;
	false ->
	    {status, 403}
    end
;

%% /admin/ajax/add-context
do(["ajax", "add-context"], Arg) ->
    Res = do(["add-context"], Arg),
    case Res of
	{status, N} ->
	    Res;
	{redirect_local, URL} ->
	    do(["ajax", "cache-summary"], Arg)
    end
;

%% /admin/delete-context
do(["delete-context"], Arg) ->
    ok;
do(["ajax", "delete-context"], Arg) ->
    ok;


%% /admin/change-context
do(["change-context"], Arg) ->
    ok
;
do(["ajax", "change-context"], Arg) ->
    ok
;
do(["add-node"], Arg) ->
    case validatePost(["node-name"], Arg) of
	{false, _} ->
	    {status, 403};
	{true, _} ->
	    {ok, NodeName} = yaws_api:postvar(Arg, "node-name"),
	    Node = cache:getContextAtom(NodeName),
	    cache:addNode(Node),
	    {redirect_local, "/admin/cache-summary"}
    end
;

%% /admin/delete-node
do(["ajax", "add-node"], Arg) ->
    case do(["add-node"], Arg) of
	{status, N} ->
	    {status, N};
	{redirect_local, _} ->
	    {status, 200}
    end
;
		     
%% /admin/add-node
do(["delete-node"], Arg) ->
    case validatePost(["node-name"], Arg) of
	{false, _} ->
	    {status, 403};
	{true, _} ->
	    {ok, NodeName} = yaws_api:postvar(Arg, "node-name"),
	    Node = cache:getContextAtom(NodeName),
	    cache:deleteNode(Node),
	    {redirect_local, "/admin/cache-summary"}
    end
;

%% /admin/delete-node
do(["ajax", "delete-node"], Arg) ->
    case do(["delete-node"], Arg) of
	{status, N} ->
	    {status, N};
	{redirect_local, _} ->
	    {status, 200}
    end
;

%%cache_context_use,"context1",
%%                    [{'dh1@www.thegreenreport.org',260798,29320400,6375},
%%                     {'dh2@www.thegreenreport.org',261208,29334200,6377},
%%                     {'dh3@www.thegreenreport.org',263030,29564200,6427},
%%                     {'dh4@www.thegreenreport.org',258656,29072000,6320}],
%%                    1043692,117290800,25499}]

%% /admin/cache-summary
do(["cache-summary"], Arg) ->
    {page, "/admin/cache-summary.ysp"}
%%    CacheSummary = cache_monitor:getCacheUseSummary(),
%%    ok
;

%-record(cache_context_use, 
%%	{
%%	  context_name,
%%	  context_node_data, %% [{node, memory overhead, object memory, number of objects}, ...]
%%	  total_memory_overhead,
%%	  total_memory_use,
%%	  total_number_of_objects
%%	}
%%).
do(["ajax", "cache-summary"], Arg) ->
    CacheSummary = cache_monitor:getCacheUseSummary(),
    JsonString = 
	"["  ++  
	createJsonCacheSummary(CacheSummary)
	++
	"]",
    
    {content, "application/json", JsonString}
;
do(_, _) ->
    {error, "invalid url"}
.
	  
%% /admin/logout
logout(Arg) ->
    SessionId = getCookieValue(?ADMIN_COOKIE, Arg),
    removeAdminFromLoggedIn(SessionId),
    [
     {redirect_local, "/admin/login"},
     {yaws_api:setcookie(?ADMIN_COOKIE, "")}
    ]
.

getPostVarsList(Arg, []) ->
    [];
getPostVarsList(Arg, [FirstParam | Rest ]) ->
    {ok, ParamValue} = yaws_api:postvar(Arg, FirstParam),
    [ParamValue | getPostVarsList(Arg, Rest) ]
.

adminUserExists(LoginId) ->
    AdminRec = mnesia:dirty_read(admin, LoginId),
    case AdminRec of
	[] ->
	    false;
	[AdminUser | _ ] ->
	    true
    end
.

addAdminUser(Arg, {LoginId, Password, Password, FirstName, LastName}, true) ->
    I = getCurrentTimeMicro(),
    Salt = integer_to_list(I),
    Key = getCryptoKey(Arg),
    IVec = << I:64 >>,
    EncryptedPassword = uncheckedSymmetricEncrypt(Key, IVec, Salt, Password),

    AdminRec = #admin{login_id = LoginId,
		      password = EncryptedPassword,
		      salt = Salt,
		      first_name = FirstName,
		      last_name = LastName
		     },
    Fun = fun() ->
		  mnesia:write(AdminRec)
	  end,
    mnesia:transaction(Fun),
    ok
;
addAdminUser(_, {LoginId, Password, Password, FirstName, LastName}, false) ->
    {error, "Login id is already in use"}
;
addAdminUser(_, {LoginId, Password, NonRepeatedPassword, FirstName, LastName}, _) ->
    {error, "Password and repeated password do not match"}
.

addAdminUser(Arg) ->
    {IsValid, Params} = validatePost(?ADD_ADMIN_USER_PARAMS, Arg),
    {ok, LoginName} = yaws_api:postvar(Arg, "login-id"),
    case IsValid of
	true ->
	    ListOfVals = getPostVarsList(Arg, ?ADD_ADMIN_USER_PARAMS),
	    AddAdminTuple = erlang:list_to_tuple(ListOfVals), %%{LoginId, Password, RepeatPass, FirstName, LastName}
	    addAdminUser(Arg, AddAdminTuple, adminUserExists(LoginName));
	false ->
	    {error, "Invalid parameters for admin user"}
    end
.

deleteAdminUser(Arg) ->
    {IsValid, Params} = validatePost(["login-id"], Arg),
    case IsValid of
	true ->
	    {ok, LoginId} = yaws_api:postvar(Arg, "login-id"),
	    Fun = fun() ->
			  mnesia:delete(admin, LoginId)
		  end,
	    mnesia:transaction(Fun),
	    SessionId = getCookieValue(?ADMIN_COOKIE, Arg),
	    removeAdminFromLoggedIn(SessionId),
	    ok;
	false ->
	    {error, "No login id specified"}
    end
.

forceAdminUserOff(Arg) ->
    ok.



%% /admin/add-context
addContext(Arg) ->
    ok
.

%% /admin/ajax/add-context
addContextAjax(Arg) ->
    ok
.

%% /admin/delete-context
deleteContext(Arg) ->
    ok.
deleteContextAjax(Arg) ->
    ok.

%% /admin/change-context
changeContext( Arg) ->
    ok
.
changeContextAjax( Arg) ->
    ok
.
		     
%% /admin/add-node
addNode(Arg) ->
    ok.

%% /admin/delete-node
deleteNode(Arg) ->
    ok.

%% /admin/cache-summary
getSummary(Arg) ->
    ok
.
getSummaryAjax(Arg) ->
    ok
.

getCryptoKey(Arg) ->
    CryptoKey = case lists:keyfind("crypto_key", 1, Arg#arg.opaque) of
		    { _, Key} ->
			Key;
		    false ->
			undefined
		end
.

getCurrentTimeMicro() ->
	{Macro, Sec, Micro} = now(),
	Now = Macro * 1000000000000 + Sec * 1000000 + Micro,
	io:format("Now is ~p~n", [integer_to_list(Now)]),
	Now
.

%%-record(admin,
%%	{
%%	  login_id,
%%	  password,
%%	  salt,
%%	  first_name,
%%	  last_name
%%	}
%% 
%%).

%%-record(logged_in, 
%%	{
%%	  key = 0,
%%	  opaque    %% [{session id, login_id, time_logged_in, last_access_time}, {...}]	  
%%	}
%%).

getCookieValue(CookieName, Arg) ->
    Headers = Arg#arg.headers,
    Cookies = Headers#headers.cookie,
    yaws_api:find_cookie_val(CookieName, Headers)
.

indexOf([], _, _) ->
    -1;
indexOf([First | Rest], Index, EqualsFun) ->
    case EqualsFun(First) of
	true ->
	    Index;
	false ->
	    indexOf(Rest, Index + 1, EqualsFun)
    end
.



getPositionOfLoggedInUser(StrSessionId, Arg) ->
    ListOfLoggedInUsers = getLoggedIn(),
    CookieValue = getCookieValue(?ADMIN_COOKIE, Arg),
    Len = length(ListOfLoggedInUsers),
    case Len of
	1 ->
	    1;
	0 ->
	    -1;
	N when N > 1 ->
	    EqualsFun = fun(Tuple) ->
				SessionId = erlang:element(1, Tuple),
				SessionId == CookieValue
			end,
	    indexOf(ListOfLoggedInUsers, 1, EqualsFun);
	_ ->
	    -1
    end
.
validateLoggedInStatus(Arg) ->
    true
%%    CookieVal = getCookieValue(?ADMIN_COOKIE, Arg),
%%    case CookieVal of
%%	[] ->
%%	    false;
%%	_ ->
%%	    ListOfLoggedInUsers = getLoggedIn(),
%%	    AdminTuple = lists:keyfind(CookieVal, 1, ListOfLoggedInUsers),
%%	    case AdminTuple of
%%		false ->
%%		    false;
%%		_ ->
%%		    true
%%	    end
%%    end
.


getLoggedIn() ->
    LoggedInRecList = mnesia:dirty_read(logged_in, 0),
    case LoggedInRecList of
	[] ->
	    [];
	[LoggedInRec] ->
	    LoggedInRec#logged_in.opaque
    end
.

validateRegistration(Arg) ->
    ok
.

addAdminUserToLoggedIn(SessionId, LoginId) ->
    ListOfLoggedInUsers = getLoggedIn(),
    case lists:keyfind(SessionId, 1, ListOfLoggedInUsers) of
	false ->
	    case lists:keyfind(LoginId, 2, ListOfLoggedInUsers) of
		false ->
		    NewList = ListOfLoggedInUsers ++ [{SessionId, LoginId, cache:secs(), cache:secs()}],
		    LoggedIn = #logged_in{opaque = NewList},
		    mnesia:dirty_write(LoggedIn);
		_ ->
		    {error, "already logged in"}
	    end;
	_ ->
	    {error, "already logged in"}
    end
.
removeAdminFromLoggedIn(SessionId) ->		
    ListOfLoggedInUsers = getLoggedIn(),
    NewList = lists:keydelete(SessionId, 1, ListOfLoggedInUsers),
    NewLoggedInRec = #logged_in{opaque = NewList},
    Fun = fun() ->
		  mnesia:write(NewLoggedInRec)
	  end,
    Res = mnesia:transaction(Fun),
    case Res of
	{atomic, _} ->
	    ok;
	_ ->
	    error
    end
.
    

%% {error, Error Message},
%% ok
validateUserLogin(Arg, LoginId, Password) ->
    CryptoKey = getCryptoKey(Arg),
    case CryptoKey of
	undefined ->
	    {error, "No crypto key defined"};
	_ ->
	    User = mnesia:dirty_read(admin, LoginId),
	    case User of
		[] ->
		    {error, "No user defined"};
		_ ->		    
		    I = erlang:list_to_integer(User#admin.salt),
		    IVec = << I:64 >>,
		    EncryptedPassword = ?MODULE:uncheckedSymmetricEncrypt(CryptoKey, IVec, User#admin.salt, Password),
		    case EncryptedPassword == User#admin.password of
			true ->
			    ok;
			false ->
			    {error, "Password does not match user id"}
		    end %% end case				
	    end %% case User of
    end
.


createNodeSummary([]) ->
    []
;
createNodeSummary([NodeData]) -> 
    {Node, Overhead, Mem, Num} = NodeData,
    "{" ++
	"\"nodeName\": \"" ++ erlang:atom_to_list(Node) ++ "\"," ++
	"\"memoryOverhead\": " ++ erlang:integer_to_list(Overhead) ++ "," ++
	"\"memoryUse\": " ++ erlang:integer_to_list(Mem) ++ "," ++
	"\"numberOfObjects\": " ++ erlang:integer_to_list(Num) ++
	"}"
;
createNodeSummary([NodeData | Rest ]) ->
    {Node, Overhead, Mem, Num} = NodeData,
    "{" ++
	"\"nodeName\": \"" ++ erlang:atom_to_list(Node) ++ "\"," ++
	"\"memoryOverhead\": " ++ erlang:integer_to_list(Overhead) ++ "," ++
	"\"memoryUse\": " ++ erlang:integer_to_list(Mem) ++ "," ++
	"\"numberOfObjects\": " ++ erlang:integer_to_list(Num) ++
	"},"
	++
	createNodeSummary(Rest)
.

createJsonCacheSummary([]) ->
    [];
createJsonCacheSummary([CacheContextUse]) ->
    "{" ++
	"\"contextName\": \"" ++ CacheContextUse#cache_context_use.context_name ++ "\"," ++
	"\"totalMemoryUse\": " ++ erlang:integer_to_list(CacheContextUse#cache_context_use.total_memory_use) ++ "," ++
	"\"totalMemoryOverhead\": " ++ erlang:integer_to_list(CacheContextUse#cache_context_use.total_memory_overhead) ++ "," ++
	"\"totalNumberOfObjects\": " ++ erlang:integer_to_list(CacheContextUse#cache_context_use.total_number_of_objects) ++ "," ++
	"\"nodeData\": [" ++ createNodeSummary(CacheContextUse#cache_context_use.context_node_data) ++ "]" ++
	"}"
;
createJsonCacheSummary([CacheContextUse | Rest]) ->
    "{" ++
	"\"contextName\": \"" ++ CacheContextUse#cache_context_use.context_name ++ "\"," ++
	"\"totalMemoryUse\": " ++ erlang:integer_to_list(CacheContextUse#cache_context_use.total_memory_use) ++ "," ++
	"\"totalMemoryOverhead\": " ++ erlang:integer_to_list(CacheContextUse#cache_context_use.total_memory_overhead) ++ "," ++
	"\"totalNumberOfObjects\": " ++ erlang:integer_to_list(CacheContextUse#cache_context_use.total_number_of_objects) ++ "," ++
	"\"nodeData\": [" ++ createNodeSummary(CacheContextUse#cache_context_use.context_node_data) ++ "]" ++
	"},"
	++
	createJsonCacheSummary(Rest)
.    

uncheckedSymmetricEncrypt(Key, IVec, Seed, Data) ->
%%    CryptoKey = getMd5(Seed, Key),
    crypto:blowfish_cfb64_encrypt(Key, IVec, Seed ++ Data)
.
uncheckedSymmetricDecrypt(Key, IVec, Seed, Data) ->
%%    CryptoKey = getMd5(Seed, Key),
    crypto:blowfish_cfb64_decrypt(Key, IVec, Data)
.
