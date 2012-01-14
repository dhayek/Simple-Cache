
-module(cache).

%%-behaviour(application).

-include_lib("cache.hrl").

-export([init/0, initForceNew/0,  start/0,  stop/0, listToAtom/1]).


-export([ put/3, put/4, put/7, get/2, delete/2, putFragRecord/2, deleteFragRecord/2, secs/0 ]).
-export( [ getNodeList/0, getNodeListRecord/0, createTableFragmentName/2, createContext/1, deleteContext/1, 
	   contextExists/1, getContextRecord/1, getContextAtom/1, isCacheReorg/0 ]).
-export( [ deleteNodeTableCopyInfo/1, deleteNodeTableFragmentInfo/1, reCacheRecord/2  ] ).
-export( [ deleteNodeTableInfo/1, resetCacheReorg/0, clearAllData/0, clearContextData/1, getAllContexts/0  ] ).

-export( [writeConfDataFromNodeList/0,  createRehashedCacheDataRecord/3, addNode/1, deleteNode/1, nodeDown/1 ] ).


init() ->
    ListOfNodeTuples = getListOfNodeTuples(),
    case ListOfNodeTuples of
	{error, Message} ->
	    erlang:exit(Message);
	 {NodeListTuples, LookupNodeListTuples, ListOfContexts} ->
	    NodeList = getNodeListFromConfig(NodeListTuples),
	    LookupNodeList = getNodeListFromConfig(LookupNodeListTuples),
	    case areMutuallyExclusive(NodeList, LookupNodeList) of
		false ->
		    erlang:exit("The nodes used for the cache contain some members from the nodes used for lookup.");
		true ->
		    AllNodes = NodeList ++ LookupNodeList,
		    case allNodesConnected(AllNodes) of
			false ->
			    erlang:exit("All nodes are not connectable.");
			true ->
			    mnesia:create_schema(AllNodes),
			    lists:foreach(fun(Node) ->
						  rpc:call(Node, mnesia, start, [])
					  end,
					  AllNodes),    
			    cache_data_ddl:createCacheTables(AllNodes),
			    NodeListRec = #node_list{node_list = NodeListTuples, lookup_node_list = LookupNodeListTuples},
			    Fun = fun() ->
					  mnesia:write(NodeListRec)
				  end,
			    mnesia:transaction(Fun),
%%			    mnesia:dirty_write(node_list, NodeListRec),
			    mnesia:force_load_table(node_list),
			    CacheReorgRec = #cache_reorg{is_reorg=false, reorg_type="", node_affected=undefined, 
							 current_number_of_nodes = length(AllNodes),
							 nodes_running_reorg = 0},
			    mnesia:dirty_write(cache_reorg, CacheReorgRec),
			    mnesia:force_load_table(cache_reorg),
			    lists:foreach(fun(ContextConfig) ->
						  {ContextName, UseHeader, HeaderValue, UseSecure, DefaultTTL, MemoryAllocation} = ContextConfig,
						  ContextRec = #context{
						    context_name = ContextName,
						    use_header = UseHeader,
						    header_value = HeaderValue,
						    use_secure = UseSecure,
						    default_ttl = DefaultTTL,
						    memory_allocation = MemoryAllocation},
						  ?MODULE:createContext(ContextRec),
						  createCacheLookupTables(ContextName, LookupNodeListTuples)
					  end,
					  ListOfContexts),
			    lists:foreach(fun(Node) ->
						  rpc:call(Node, cache_monitor, start, [])
					  end,
					  AllNodes)
		    end
	    end
    end
.

createCacheLookupTables(ContextName, ListOfLookupNodeTuples) ->
    lists:foreach(fun(LookupNodeTuple) ->
			  {NodeKey, ListOfNodes} = LookupNodeTuple,
			  Table = getCacheLookupRecordTable(ContextName, NodeKey),
			  mnesia:create_table(Table,
					      [
					       {ram_copies, ListOfNodes},
					       {attributes, ?CACHE_LOOKUP_RECORD_DATA}
					      ]
					     )
		  end,
		  ListOfLookupNodeTuples
		 )
.

getCacheLookupRecordTable(ContextName, NodeKey) ->
    StrTableName = ?CACHE_LOOKUP_TABLE_PREFIX ++ "_" ++ ContextName ++ "_" ++ NodeKey,
    Table = listToAtom(StrTableName)
.

initForceNew() ->
    ListOfNodeTuples = getListOfNodeTuples(),
    case ListOfNodeTuples of
	{error, Message} ->
	    erlang:exit(Message);
	{NodeListTuples, LookupNodeListTuples, _} ->
	    AllNodes = getAllNodesFromConfigTuples(NodeListTuples, LookupNodeListTuples),
	    lists:foreach(fun(Node) ->
				  rpc:call(Node, cache_monitor, stop, []),
				  rpc:call(Node, mnesia, stop, [])
			  end,
			  AllNodes),
	    mnesia:delete_schema(AllNodes),
	    init()
    end
.


start() ->
    ListOfNodeTuples = getListOfNodeTuples(),
    case ListOfNodeTuples of
	{error, Message} ->
	    erlang:exit(Message);
	{NodeListTuples, LookupNodeListTuples, _} ->
	    case areMutuallyExclusive(NodeListTuples, LookupNodeListTuples) of
		false ->
		    erlang:exit("The nodes used for the cache contain some members from the nodes used for lookup.");
		true ->
		    ListOfNodes =   getAllNodesFromConfigTuples(NodeListTuples, LookupNodeListTuples),
		    StartStatus = start(ListOfNodes),
		    case StartStatus of 
			{error, Message} ->
			    erlang:exit(Message);
			ok ->
			    ok
		    end
	    end
    end
.
start(ListOfNodes) when erlang:is_list(ListOfNodes) ->
    case allNodesConnected(ListOfNodes) of
	false ->
	    {error, "All nodes are not connectable"};
	true ->
	    lists:foreach(fun(Node) ->
				  rpc:call(Node, mnesia, start, [])
			  end,
			  ListOfNodes),
	    mnesia:force_load_table(node_list),
	    mnesia:force_load_table(context),
	    initTableFragmentMemoryData(),
	    lists:foreach(fun(Node) ->
				  rpc:call(Node, cache_monitor, start, [])
			  end,
			  ListOfNodes),
	    ok
    end
.

stop() ->
    NodeListRec = getNodeListRecord(),
    NodeList = lists:foldl(fun({_, L}, Acc) ->
				   Acc ++ L
			   end,
			   [],
			   NodeListRec#node_list.node_list
			  ),
    LookupNodeList = lists:foldl(fun({_, L}, Acc) ->
					 Acc ++ L
				 end,
				 [],
				 NodeListRec#node_list.lookup_node_list
				),
    AllNodes = NodeList ++ LookupNodeList,
    lists:foreach(fun(Node) ->
			  rpc:call(Node, cache_monitor, stop, [])
		  end,
		  AllNodes)
.

areMutuallyExclusive(L1, L2) when is_list(L1) and is_list (L2) ->
    lists:foldl(fun(N, IsMutuallyExclusive) ->
			case IsMutuallyExclusive of
			    false ->
				false;
			    true ->
				not lists:member(N, L1)
			end
		end,
		true,
		L2)
.

getNodeListFromConfig(NodeListTuples) ->
    NodeList = lists:foldl(fun(ConfigTuple, Acc) ->
				   {_, List} = ConfigTuple,
				   Acc ++ List
			   end,
			   [],
			   NodeListTuples)
.
getAllNodesFromConfigTuples(NodeListTuples, LookupNodeListTuples) ->
    AllNodeTuples = lists:merge(NodeListTuples, LookupNodeListTuples),
    ListOfNodes = lists:foldl(fun(NodeTuple, Acc) -> 
				      {_, L} = NodeTuple,
				      Acc ++ L
			      end, 
			      [], AllNodeTuples)
.

getListOfNodeTuples() ->
    getConfData()
.

writeConfDataFromNodeList() ->
    CacheHome = os:getenv("SCACHE_HOME"),
    case CacheHome of
	false ->
	    {error, "Environment variable SCACHE_HOME not defined"};
	_ ->
	    ConfFile = filename:join(CacheHome, "cache.conf"),
	    NodeListRecord = getNodeListRecord(),
	    OpenFileResults = file:open(ConfFile, [write]),
	    case OpenFileResults of
		{error, Reason} ->
		    io:format("Error opening file: ~p~n", [Reason]);
		{ok, IoDevice} ->
		    io:fwrite(IoDevice, "{~n", []),
		    io:fwrite(IoDevice, "\t{~n", []),

		    io:fwrite(IoDevice, "\t\tnode_list,~n", []),
		    NewList = lists:map(fun({Key, L}) ->
						{"" ++ Key ++ "", L}
					end,
					NodeListRecord#node_list.node_list
				       ),
		    io:fwrite(IoDevice, "\t\t[~n", []),
		    printListElement(IoDevice, "\t\t\t", NewList),
		    io:fwrite(IoDevice, "\t\t]~n", []),
		    io:fwrite(IoDevice, "\t},~n", []),


		    io:fwrite(IoDevice, "\t{~n", []),
		    io:fwrite(IoDevice, "\t\tcache_lookup_node_list,~n", []),
		    NewLookupList = lists:map(fun({Key, L}) ->
						{"" ++ Key ++ "", L}
					end,
					NodeListRecord#node_list.lookup_node_list
				       ),
		    io:fwrite(IoDevice, "\t\t[~n", []),
		    printListElement(IoDevice, "\t\t\t", NewLookupList),
		    io:fwrite(IoDevice, "\t\t]~n", []),
		    io:fwrite(IoDevice, "\t},~n", []),

		    io:fwrite(IoDevice, "\t{~n", []),

		    io:fwrite(IoDevice, "\t\tcontexts,~n", []),
		    ListOfContextKeys = getAllContexts(),
		    ListOfContextRecs = lists:map(fun(ContextName) ->
							  [Rec] = mnesia:dirty_read(context, ContextName),
							  {_, _, UseHeader, HeaderVal, UseSecure, TTL, MemSize} = Rec,
							  {"" ++ ContextName ++ "", UseHeader, "" ++ HeaderVal,
							   UseSecure, TTL, MemSize}
						  end,
						  ListOfContextKeys
						 ),
		    io:fwrite(IoDevice, "\t\t[~n", []),
		    printListElement(IoDevice, "\t\t\t", ListOfContextRecs),
		    io:fwrite(IoDevice, "\t\t]~n", []),
		    io:fwrite(IoDevice, "\t}~n", []),

		    io:fwrite(IoDevice, "}~n", []),
		    io:fwrite(IoDevice, ".", []),
		    file:close(IoDevice)
	    end
    end
.

printListElement(IoDevice, Prefix, L) when length(L) == 1 ->
    [Elem] = L,
    io:fwrite(IoDevice, Prefix ++ "~p~n", [Elem])
;
printListElement(IoDevice, Prefix, [FirstElem | Rest]) ->
    io:fwrite(IoDevice, Prefix ++ "~p,~n", [FirstElem]),
    printListElement(IoDevice, Prefix, Rest)
.

getConfData() ->
    CacheHome = os:getenv("SCACHE_HOME"),
    case CacheHome of
	false ->
	    {error, "Environment variable SCACHE_HOME not defined"};
	_ ->
	    ConfFile = filename:join(CacheHome, "cache.conf"),
	    FileContents = file:read_file(ConfFile),
	    case FileContents of
		{ok, FileBin} ->
		    FileStr = erlang:binary_to_list(FileBin),
%%		    io:format("FileStr ~p~n", [FileStr]),
		    {ok, Scanned, _} = erl_scan:string(FileStr),
		    {ok, Parsed} = erl_parse:parse_exprs(Scanned),
		    {value, ListOfNodeTuples, _} = erl_eval:exprs(Parsed, []),
		    {{node_list, NodeList}, {cache_lookup_node_list, LookupNodeList}, {contexts, ContextList}} = ListOfNodeTuples,
		    {NodeList, LookupNodeList, ContextList};
		_ ->
		    {error, "Error reading file."}
	    end
    end
.


initTableFragmentMemoryData() ->
    NodeListRecord = getNodeListRecord(),
    ListOfContexts = getAllContexts(),
    lists:foreach(
      fun(NodeTuple) ->
	      {NodeKey, NodeList} = NodeTuple,
	      lists:foreach(
		fun(ContextName) ->
			TableFragmentName = createTableFragmentName(ContextName, NodeKey),
			Table = ?MODULE:getContextAtom(TableFragmentName),
			TableFragmentMemoryRec = 
			    #table_fragment_memory {
			  table_fragment_name = TableFragmentName,
			  table_fragment = Table,
			  memory_use = 0			    
			 },
			FragFun = fun() ->
					  mnesia:write(TableFragmentMemoryRec)
				  end,
			mnesia:transaction(FragFun)
		end,
		ListOfContexts
	       )
      end,
      NodeListRecord#node_list.node_list)
.

allNodesConnected(ListOfNodes) ->
    lists:foldl(
      fun(Node, IsConnected) ->
	      case IsConnected of 
		  false ->
		      false;
		  true ->
		      PongRes = net_adm:ping(Node),
		      case PongRes of
			  pong ->
			      true;
			  _ ->
			      false
		      end
	      end
      end,
      true,
      ListOfNodes)
.
    
put(ContextName, Key, Data) ->
    ContextRec = ?MODULE:getContextRecord(ContextName),
    put(ContextName, Key, Data, ContextRec#context.default_ttl)
.


put(ContextName, Key, Data, TTL) ->
    Secs = ?MODULE:secs(),
    put(ContextName, Key, Data, Secs, Secs, TTL, Secs + TTL)
.

put(ContextName, Key, Data, StoreTime, LastAccessTime, TTL, ExpireTime) ->

    NodeListRec = getNodeListRecord(),
    NodeList = NodeListRec#node_list.node_list,
    LookupNodeList = NodeListRec#node_list.lookup_node_list,
    
    CacheData = createCacheDataRecord(ContextName, Key, Data, StoreTime, LastAccessTime, TTL, ExpireTime),

    TableFragment = erlang:element(1, CacheData),
    TableFragmentName = erlang:atom_to_list(TableFragment),
    DataSize = size(Data),
    NodeListRec = getNodeListRecord(),
    Fun = fun() ->
		  mnesia:write(CacheData),
		  [FragMemoryRec] = mnesia:read(table_fragment_memory, TableFragmentName),
		  CurrMemUse = FragMemoryRec#table_fragment_memory.memory_use,
		  MemUse = CurrMemUse + DataSize,
		  NewFragMemoryRec = FragMemoryRec#table_fragment_memory{memory_use = MemUse},
		  mnesia:write(NewFragMemoryRec),		  
		  LookupCacheRecord = createLookupCacheDataRecord(ContextName, Key, TableFragmentName, NodeListRec#node_list.lookup_node_list),
		  mnesia:write(LookupCacheRecord)
	  end,
    mnesia:transaction( Fun )
.    

putFragRecord(FragmentRecord, TableFragmentName) ->
    {_, _, Data, _, _, _, _} = FragmentRecord,
    %%mnesia:dirty_write(FragmentRecord),
    DataSize = size(Data),
    Fun = fun() ->
		  mnesia:write(FragmentRecord),
		  [FragRec] = mnesia:read(table_fragment_memory, TableFragmentName),
		  CurrMemUse = FragRec#table_fragment_memory.memory_use,
		  MemUse = CurrMemUse + DataSize,
		  NewFragRec = FragRec#table_fragment_memory{memory_use = MemUse},
		  mnesia:write(NewFragRec)
	  end,
    mnesia:transaction(Fun)
.

isCacheReorg() ->
    Fun = fun() ->
		  mnesia:read(cache_reorg, 0)
	  end,
    QueryRes = mnesia:transaction(Fun),
    case QueryRes of
	{atomic, [ReorgRecord | _ ]} ->
	    case ReorgRecord#cache_reorg.is_reorg of
		false ->
		    false;
		true ->
		    {true, ReorgRecord#cache_reorg.reorg_type, ReorgRecord#cache_reorg.node_affected,
		     ReorgRecord#cache_reorg.current_number_of_nodes}
		end;
	_ ->
	    false
    end
.
getNodeList() ->
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    NodeListRecord#node_list.node_list
.
getNodeListRecord() ->
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    NodeListRecord
.

deleteFragRecord(FragmentRecord, TableFragmentName) ->
    {_, _, Data, _, _, _, _} = FragmentRecord,
    DataSize = size(Data),
    Fun = fun() ->
		  mnesia:delete_object(FragmentRecord),
		  [FragRec] = mnesia:read(table_fragment_memory, TableFragmentName),
		  CurrMemUse = FragRec#table_fragment_memory.memory_use,
		  MemUse = CurrMemUse - DataSize,
		  NewFragRec = FragRec#table_fragment_memory{memory_use = MemUse},
		  mnesia:write(NewFragRec)
	  end,
    mnesia:transaction(Fun)
.
    
createCacheDataRecord(ContextName, Key, Data, TTL) ->
    Secs = cache:secs(),
    createCacheDataRecord(ContextName, Key, Data, Secs, Secs, TTL, Secs + TTL)
.
createCacheDataRecord(ContextName, Key, Data, StoreTime, LastAccessTime, TTL, ExpireTime) ->
    Secs = secs(),
    NodeListRecord = cache:getNodeListRecord(),
    ListSize = length(NodeListRecord#node_list.node_list),
    HashIndex = erlang:phash2(Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, NodeListRecord#node_list.node_list),
    TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
    Table = ?MODULE:getContextAtom(TableFragmentName),
    CacheData = {
      Table,
      Key,
      Data,
      StoreTime,
      LastAccessTime,
      TTL,
      ExpireTime
     },
    CacheData
.


%% ContextName, Key, and TableName are all strings
createLookupCacheDataRecord(ContextName, Key, TableName, ListOfLookupNodeTuples) ->
    ListSize = length(ListOfLookupNodeTuples),
    HashIndex = erlang:phash2(ContextName ++ Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, ListOfLookupNodeTuples),
    {NodeKey, NodeList} = NodeTuple,
    Table = getCacheLookupRecordTable(ContextName, NodeKey),
    {
      Table,
      erlang:list_to_binary(Key),
      erlang:list_to_binary(TableName)
    }
.

reCacheRecord(ContextName, Record) ->
    {OldTable, Key, Data, StoreTime, LastAccessTime, TTL, ExpireTime} = Record,
    ?MODULE:put(ContextName, Key, Data, StoreTime, LastAccessTime, TTL, ExpireTime)
    %%?MODULE:delete(ContextName, Key)
.

%% [key, data, store_time, last_access_time, ttl, expire_time]
createRehashedCacheDataRecord(ContextName, CacheDataRecord, ListOfNodeTuples) ->
    {OldTable, Key, Data, StoreTime, LastAccessTime, TTL, ExpireTime} = CacheDataRecord,
    ListSize = length(ListOfNodeTuples),
    HashIndex = erlang:phash2(Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, ListOfNodeTuples),
    {NodeKey, NodeList} = NodeTuple,
    TableFragmentName = createTableFragmentName(ContextName, NodeKey),
    Table = ?MODULE:getContextAtom(TableFragmentName),

    NewCacheData = {
      Table,
      Key,
      Data,
      StoreTime,
      LastAccessTime,
      TTL,
      ExpireTime
     },
    NewCacheData
.
      
get(ContextName, Key) ->
    NodeListRec = getNodeListRecord(),
    ListSize = length(NodeListRec#node_list.lookup_node_list),
    HashIndex = erlang:phash2(ContextName ++ Key, ListSize) + 1,
    LookupNodeTuple = lists:nth(HashIndex, NodeListRec#node_list.lookup_node_list),
    {LookupNodeKey, LookupNodeList} = LookupNodeTuple,
    LookupTable = getCacheLookupRecordTable(ContextName, LookupNodeKey),
    LookupRecordList = mnesia:dirty_read(LookupTable, erlang:list_to_binary(Key)),
    case LookupRecordList of
	[] ->
	    undefined;
	[LookupRecord | _] ->
	    CacheTableFragmentBin = erlang:element(3, LookupRecord),
	    TableStr = erlang:binary_to_list(CacheTableFragmentBin),
	    Table = listToAtom(TableStr),
	    Fun = fun() ->
			  mnesia:read(Table, Key)
		  end,
	    Result = mnesia:transaction( Fun ),
	    
	    case Result of
		{atomic, [Data | _ ]} ->
		    RetVal = erlang:element(3, Data),
		    RetVal;
		_ ->
		    undefined
	    end
    end
.

delete(ContextName, Key) ->
    NodeListRec = getNodeListRecord(),
    ListSize = length(NodeListRec#node_list.lookup_node_list),
    HashIndex = erlang:phash2(ContextName ++ Key, ListSize) + 1,
    LookupNodeTuple = lists:nth(HashIndex, NodeListRec#node_list.lookup_node_list),
    {NodeKey, NodeList} = LookupNodeTuple,
    LookupTable = getCacheLookupRecordTable(ContextName, NodeKey),
    LookupRecordList = mnesia:dirty_read(LookupTable, erlang:list_to_binary(Key)),
    case LookupRecordList of
	[] ->
	    noop;
	[LookupRecord | _] ->
	    CacheTableFragmentBin = erlang:element(3, LookupRecord),
	    TableFragmentName = erlang:binary_to_list(CacheTableFragmentBin),
	    Table = listToAtom(TableFragmentName),
	    Fun = fun() ->
			  mnesia:delete(LookupTable, erlang:list_to_binary(Key), write),
			  QueryRes = mnesia:read(Table, Key, write),
			  case QueryRes of
			      [ContextFragRec | _ ] ->
				  {_, _, Data, _, _, _, _} = ContextFragRec,
				  DataSize = size(Data),
				  mnesia:delete(Table, Key, write),
				  [FragMemoryRec] = mnesia:read(table_fragment_memory, TableFragmentName),
				  CurrMemUse = FragMemoryRec#table_fragment_memory.memory_use,
				  MemUse = CurrMemUse - DataSize,
				  NewFragMemoryRec = FragMemoryRec#table_fragment_memory{memory_use = MemUse},
				  mnesia:write(NewFragMemoryRec);
			      _ ->
				  noop
			  end
		  end,
	    Result = mnesia:transaction( Fun )
    end
.

secs() ->
        T = now(),
        {M, S, _} = T,
        1000000 * M + S
.


createContext(ContextRecord) ->
    Fun = fun() ->
		  mnesia:write(ContextRecord)
	  end,
    mnesia:transaction(Fun),

    ContextName = ContextRecord#context.context_name,
    addContext(ContextName)
.


addContext(ContextName) ->
    NodeListRecord = getNodeListRecord(),
    lists:foreach(fun(NodeTuple) ->
			  {NodeKey, NodeList} = NodeTuple,
			  TableFragmentName = createTableFragmentName(ContextName, NodeKey),
			  Table = ?MODULE:getContextAtom(TableFragmentName),
			  mnesia:create_table(
			    Table,
			    [
			     {ram_copies, NodeList},
			     {attributes, ?CACHE_RECORD_DATA},
			     {index, [store_time, last_access_time, expire_time]}
			    ]
			   ),
			  TableFragmentMemoryRec = 
			      #table_fragment_memory {
			    table_fragment_name = TableFragmentName,
			    table_fragment = Table,
			    memory_use = 0			    
			   },
			  FragFun = fun() ->
					    mnesia:write(TableFragmentMemoryRec)
				    end,
			  mnesia:transaction(FragFun),
			  lists:foreach(fun(Node) ->
						{config_changed_listener, Node} ! {context_added, ContextName}
					end,
					NodeList
				       )
		  end,
		  NodeListRecord#node_list.node_list
		 ),
    lists:foreach(fun(LookupNodeTuple) ->
			  {LookupKey, LookupNodeList} = LookupNodeTuple,
			  LookupTable = getCacheLookupRecordTable(ContextName, LookupKey),
			  mnesia:create_table(
			    LookupTable,
			    [
			     {ram_copies, LookupNodeList},
			     {attributes, ?CACHE_LOOKUP_RECORD_DATA}
			    ]
			   ),
			  lists:foreach(fun(Node) ->
						{config_changed_listener, Node} ! {context_added, ContextName}
					end,
					LookupNodeList
				       )
		  end,
		  NodeListRecord#node_list.lookup_node_list
		 )
.

%% ContextName : string()
%% Node : atom()
createTableFragmentName(ContextName, NodeKey) ->
    ContextName ++ "_fragment_" ++ NodeKey
.
createLookupTableFragmentName(ContextName, NodeKey) ->
    ?CACHE_LOOKUP_TABLE_PREFIX ++ "_" ++ ContextName ++ "_" ++ NodeKey
.


deleteContext(ContextName) ->
    NodeListRecord = getNodeListRecord(),
    lists:foreach(fun({LookupNodeKey, LookupNodeList}) ->
			  LookupTable = getCacheLookupRecordTable(ContextName, LookupNodeKey),
			  mnesia:delete_table(LookupTable),
			  lists:foreach(fun(Node) ->
						{config_changed_listener, Node} ! {context_deleted, ContextName}
					end,
					LookupNodeList
				       )
		  end,
		  NodeListRecord#node_list.lookup_node_list
		 ),
    lists:foreach(fun({NodeKey, NodeList}) ->
			  TableFragmentName = createTableFragmentName(ContextName, NodeKey),
			  Table = ?MODULE:getContextAtom(TableFragmentName),
			  mnesia:delete_table(Table),
			  DelFun = fun() ->
					   mnesia:delete(table_fragment_memory, TableFragmentName)
				   end,
			  mnesia:transaction(DelFun),
			  lists:foreach(fun(Node) ->
						{config_changed_listener, Node} ! {context_deleted, ContextName}
					end,
					NodeList
				       )
		  end,
		  NodeListRecord#node_list.node_list
		 )
.

clearAllData() ->
    Fun = fun() ->
		  mnesia:all_keys(context)
	  end,
    Res = mnesia:transaction(Fun),
    %%io:format("Res is ~p~n", [Res]),
    case Res of
	{atomic, ListOfKeys} ->
	    lists:foreach(fun(Key) ->
				  clearContextData(Key)
			  end,
			  ListOfKeys),
	    ok;
	_ ->
	    noop
    end     
.


clearContextData(ContextName) ->
    NodeListRecord = getNodeListRecord(),
    lists:foreach(fun({LookupNodeKey, LookupNodeList}) ->
			  LookupTable = getCacheLookupRecordTable(ContextName, LookupNodeKey),
			  mnesia:clear_table(LookupTable)
		  end,
		  NodeListRecord#node_list.lookup_node_list
		 ),
    lists:foreach(fun({NodeKey, NodeList}) ->
			  TableFragmentName = createTableFragmentName(ContextName, NodeKey),
			  Table = ?MODULE:getContextAtom(TableFragmentName),
			  mnesia:clear_table( Table ),
			  MemFun = fun() ->
					   [FragMemoryRec] = mnesia:read(table_fragment_memory, TableFragmentName),
					   NewFragMemoryRec =
					       FragMemoryRec#table_fragment_memory{memory_use = 0},
					   mnesia:write(NewFragMemoryRec)
				   end,
			  mnesia:transaction(MemFun)
		  end,
		  NodeListRecord#node_list.node_list
		 )
.

listToAtom(L) ->
    Atom = try
	       list_to_existing_atom(L) 
	   catch 
	       Class:Error -> 
		   list_to_atom(L) 
	   end
.

getContextAtom(Context) ->
    Atom = try
	       list_to_existing_atom(Context) 
	   catch 
	       Class:Error -> 
		   list_to_atom(Context) 
	   end
.

contextExists(ContextName) ->
    ContextRec = ?MODULE:getContextRecord(ContextName),
    case ContextRec of
	undefined ->
	    false;
	_ ->
	    true
    end
.
    
getContextRecord(ContextName) ->
    Res = mnesia:dirty_read(context, ContextName),
    case Res of
	[] ->
	    undefined;
	[Rec | _ ] ->
	    Rec
    end
.

resetCacheReorg() ->
    NodeList = getNodeList(),
    CR = #cache_reorg{
      key = 0,
      is_reorg = false,
      reorg_type = "",
      node_affected = undefined,
      current_number_of_nodes = length(NodeList),
      nodes_running_reorg = 0},
    mnesia:dirty_write(CR)
.

getAllContexts() ->
    Fun = fun() ->
		  mnesia:all_keys(context)
	  end,
    Result = mnesia:transaction(Fun),
    case Result of
	{atomic, ListOfContexts} ->
	    ListOfContexts;
	_ ->
	    []
    end
.


getTableKeys(Table) ->
    Fun = fun() ->
		  mnesia:all_keys(Table)
	  end,
    Result = mnesia:transaction(Fun),
    case Result of
	{atomic, ListOfKeys} ->
	    ListOfKeys;
	_ ->
	    []
    end
.

%% Node is a tuple of atoms of the format {"NodeKey", [node_name1@w.x.y.z, node_name2@a.b.c.d, ...]}
addNode(NodeTuple) when erlang:is_tuple(NodeTuple) == true ->

    %% is this a valid Node Tuple?  Does the NodeKey already exists or are
    %% any of the nodes already being used?
    io:format("checkpoint 1~n"),
    NodeListRecord = getNodeListRecord(),
    io:format("checkpoint 2~n"),
    case NodeTuple of
	{NodeKey, ListOfNodes} ->
	    ListOfNodeKeys = lists:map(fun({Key, _}) ->
					       Key
				       end,
				       NodeListRecord#node_list.node_list
				      ),
	    ListOfCurrentNodes = getNodeListFromConfig(NodeListRecord#node_list.node_list),
	    case {areMutuallyExclusive([NodeKey], ListOfNodeKeys), areMutuallyExclusive(ListOfNodes, ListOfCurrentNodes)} of
		{true, true} ->
		    NewNodeListRecord = NodeListRecord#node_list{node_list = NodeListRecord#node_list.node_list ++ [NodeTuple]},
		    case initializeAddedNodes(NodeTuple) of
			{error, Message} ->
			    {error, Message};
			ok ->
			    Fun = fun() ->
					  mnesia:write(NewNodeListRecord)
				  end,
			    mnesia:transaction(Fun),
			    ListOfLookupNodes = getNodeListFromConfig(NewNodeListRecord#node_list.lookup_node_list),
			    lists:foreach(fun({_, NodeList}) ->
						  lists:foreach(fun(Node) ->
									{config_changed_listener, Node} ! node_added
								end,
								NodeList
							       )
					  end,
					  NewNodeListRecord#node_list.node_list ++ 
					      NewNodeListRecord#node_list.lookup_node_list
					 )	
		    end;
		_ ->
		    {error, "This key or one of the nodes already exists."}
	    end;
	_ ->
	    {error, "Incorrect format for adding a node - should be: {NodeKey, [node1, node2, ...]}, where NodeKey is a string and nodeN is an atom."}
    end
.
initializeAddedNodes({NodeKey, ListOfNodes}) when erlang:is_list(ListOfNodes) == true ->    
    case allNodesConnected(ListOfNodes) of
	false ->
	    {error, "Not all nodes are connectable."};
	true ->
	    lists:foreach(fun(Node) ->
				  deleteNodeTableCopyInfo(Node),
				  rpc:call(Node, mnesia, start, []),
				  mnesia:change_config(extra_db_nodes, [Node]),
				  mnesia:add_table_copy(schema, Node, ram_copies),
				  mnesia:change_table_copy_type(schema, Node, disc_copies),
				  mnesia:add_table_copy(context, Node, disc_copies),
				  mnesia:add_table_copy(admin, Node, disc_copies),
				  mnesia:add_table_copy(node_list, Node, disc_copies),
				  mnesia:add_table_copy(cache_reorg, Node, disc_copies),
				  mnesia:add_table_copy(table_fragment_memory, Node, disc_copies),
				  mnesia:add_table_copy(logged_in, Node, disc_copies)
			  end,
			  ListOfNodes),
	    %%FirstNode = lists:nth(1, ListOfNodes),
	    ListOfContexts = getAllContexts(),
	    lists:foreach(
	      fun(ContextName) ->		      
		      TableFragmentName = createTableFragmentName(ContextName, NodeKey),
		      Table = ?MODULE:getContextAtom(TableFragmentName),
		      mnesia:create_table(
			Table,
			[
			 {ram_copies, ListOfNodes},
			 {attributes, ?CACHE_RECORD_DATA},
			 {index, [store_time, last_access_time, expire_time]}
			]
		       ),
		      lists:foreach(fun(Node) ->
					    rpc:call(Node, mnesia, force_load_table, [Table])
				    end,
				    ListOfNodes),
		      TableFragmentMemoryRec = 
			  #table_fragment_memory {
			table_fragment_name = TableFragmentName,
			table_fragment = Table,
			memory_use = 0			    
		       },
		      FragFun = fun() ->
					mnesia:write(TableFragmentMemoryRec)
				end,
		      mnesia:transaction(FragFun)		
	      end,
	      ListOfContexts
	     ),
	    ok
    end
.

deleteNodeTableInfo({NodeKey, ListOfNodes}) ->
    deleteNodeTableFragmentInfo(NodeKey),
    lists:foreach(fun(Node) ->
			  deleteNodeTableCopyInfo(Node)
		  end,
		  ListOfNodes
		 ),
    ok
.

deleteNodeTableFragmentInfo(NodeKey) ->
    ListOfContexts = getAllContexts(),
    lists:foreach(
      fun(ContextName) ->
	      TableFragmentName = createTableFragmentName(ContextName, NodeKey),
	      Table = ?MODULE:getContextAtom(TableFragmentName),
	      mnesia:delete_table(Table),	      
	      FragFun = fun() ->
				mnesia:delete(table_fragment_memory, TableFragmentName, write)
			end,
	      mnesia:transaction(FragFun)		
      end,
      ListOfContexts
     )
.    

deleteNodeTableCopyInfo(Node) ->
    mnesia:del_table_copy(cache_reorg, Node),
    mnesia:del_table_copy(node_list, Node),
    mnesia:del_table_copy(admin, Node),
    mnesia:del_table_copy(context, Node),
    mnesia:del_table_copy(table_fragment_memory, Node),
    mnesia:del_table_copy(logged_in, Node),
    rpc:call(Node, mnesia, stop, []),
    mnesia:del_table_copy(schema, Node)
.    

deleteNode(NodeKey)  ->
    io:format("checkpoint 1~n"),
    NodeListRecord = getNodeListRecord(),
    io:format("checkpoint 2~n"),

    ListOfCurrentNodes = getNodeListFromConfig(NodeListRecord#node_list.node_list),
    ListOfNodeKeys = lists:map(fun({Key, _}) ->
				       Key
			       end,
			       NodeListRecord#node_list.node_list
			      ),
    case lists:member(NodeKey, ListOfNodeKeys) of
	false ->
	    {error, "The node key " ++ NodeKey ++ " is not configured in this cache"};
	true ->
	    NodeTuple = lists:keyfind(NodeKey, 1, NodeListRecord#node_list.node_list),
	    NewNodeListTuples = lists:keydelete(NodeKey, 1, NodeListRecord#node_list.node_list),
	    NewNodeListRecord = NodeListRecord#node_list{node_list = NewNodeListTuples},
	    Fun = fun() -> 
			  mnesia:write(NewNodeListRecord)
		  end,
	    mnesia:transaction(Fun),
	    {_, ListOfDeletedNodes} = NodeTuple,
	    io:format("list of delete nodes is ~p~n", [ListOfDeletedNodes]),

	    lists:foreach(fun(Node) ->
				  io:format("Send delete to ~p~n", [Node]),
				  {node_deleted_listener, Node} ! {"this is a messaage"},
				  {node_deleted_listener, Node} ! {delete, NodeTuple}
			  end,
			  ListOfDeletedNodes
			 ),
	    lists:foreach(fun({_, ListOfNodes}) ->
				  lists:foreach(fun(Node) ->
							{config_changed_listener, Node} ! node_deleted
						end,
						ListOfNodes
					       )
			  end,
			  NewNodeListRecord#node_list.node_list ++ NewNodeListRecord#node_list.node_list
			 )
    end
.

nodeDown(Node) ->
    deleteNodeTableFragmentInfo(Node),
    io:format("checkpoint 1~n"),
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    io:format("checkpoint 2~n"),
    [CacheReorgRecord] = mnesia:dirty_read(cache_reorg, 0),
    
    
    case CacheReorgRecord#cache_reorg.is_reorg of
	true ->
	    NewCacheReorgRecord = #cache_reorg{
	      current_number_of_nodes = length(NodeListRecord#node_list.node_list) - 1,
	      nodes_running_reorg = length(NodeListRecord#node_list.node_list) - 1
	     },
	    mnesia:dirty_write(NewCacheReorgRecord);    
	false ->
	    ok
    end,

    NodeList = NodeListRecord#node_list.node_list,
    NewNodeList = lists:delete(Node, NodeList),
    NewNodeListRecord = NodeListRecord#node_list{node_list = NewNodeList},
    mnesia:dirty_write(NewNodeListRecord),
    
    lists:foreach(fun(N) ->
			  {change_node_configuration, N} ! {delete, Node}
		  end,
		  NewNodeList
		 )
.
       
