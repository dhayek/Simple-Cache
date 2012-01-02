
-module(cache).

%%-behaviour(application).

-include_lib("cache.hrl").

-export([init/0, initForceNew/0,  start/0,  stop/0, listToAtom/1]).


-export([ put/3, put/4, get/2, delete/2, putFragRecord/2, deleteFragRecord/2, secs/0 ]).
-export( [ getNodeList/0, createTableFragmentName/2, createContext/1, deleteContext/1, 
	   contextExists/1, getContextRecord/1, getContextAtom/1, isCacheReorg/0 ]).

-export( [ deleteNodeTableInfo/1, resetCacheReorg/0, clearAllData/0, clearContextData/1, getAllContexts/0  ] ).

-export( [ createRehashedCacheDataRecord/3, addNode/1, deleteNode/1, nodeDown/1 ] ).


init() ->
    ListOfNodeTuples = getListOfNodeTuples(),
    case ListOfNodeTuples of
	{error, Message} ->
	    erlang:exit(Message);
	 {NodeListTuples, LookupNodeListTuples, _} ->
	   
	    case areMutuallyExclusive(NodeListTuples, LookupNodeListTuples) of
		false ->
		    erlang:exit("The nodes used for the cache contain some members from the nodes used for lookup.");
		true ->
		    AllNodes =   getAllNodesFromTuples(NodeListTuples, LookupNodeListTuples),
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
			    mnesia:dirty_write(node_list, NodeListRec),
			    CacheReorgRec = #cache_reorg{is_reorg=false, reorg_type="", node_affected=undefined, 
							 current_number_of_nodes = length(AllNodes),
							 nodes_running_reorg = 0},
			    mnesia:dirty_write(cache_reorg, CacheReorgRec),
			    %%lists:foreach(fun(Node) ->
			%%			  rpc:call(Node, cache_monitor, start, [])
			%%		  end,
			%%		  AllNodes),
			    lists:foreach(fun(Tuple) ->
						  createCacheLookupTable(Tuple)
					  end,
					  LookupNodeListTuples
					 )
		    end
	    end
    end
.

createCacheLookupTable(Tuple) ->
    ListOfNodes = erlang:tuple_to_list(Tuple),
    FirstNode = lists:nth(1, ListOfNodes), 
    TableName = getCacheLookupRecordTable(FirstNode), 
    mnesia:create_table(TableName,
			[
			 {ram_copies, ListOfNodes},
			 {attributes, ?CACHE_LOOKUP_RECORD_DATA}
			]
		       )
.

getCacheLookupRecordTable(Node) ->
    StrTableName = ?CACHE_LOOKUP_TABLE_PREFIX ++ erlang:integer_to_list(erlang:phash2(Node, 1000000000)),
    TableName = listToAtom(StrTableName)
.

initForceNew() ->
    ListOfNodeTuples = getListOfNodeTuples(),
    case ListOfNodeTuples of
	{error, Message} ->
	    erlang:exit(Message);
	{NodeListTuples, LookupNodeListTuples, _} ->
	    AllNodes = getAllNodesFromTuples(NodeListTuples, LookupNodeListTuples),
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
		    ListOfNodes =   getAllNodesFromTuples(NodeListTuples, LookupNodeListTuples),
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
	    lists:foreach(fun(Node) ->
				  rpc:call(Node, cache_monitor, start, [])
			  end,
			  ListOfNodes),
	    mnesia:force_load_table(node_list),
	    mnesia:force_load_table(context),
	    initTableFragmentMemoryData(),
	    ok
    end
.

stop() ->
    cache_monitor:stop(),
    lists:foreach(fun(Node) ->
			  rpc:call(Node, cache_monitor, stop, [])
		  end,
		  nodes())
%%    {delete_node, node()} ! stop
.

areMutuallyExclusive(NodeListTuples, LookupNodeListTuples) when is_list(NodeListTuples) and is_list (LookupNodeListTuples) ->
    L1 = lists:foldl(fun(NodeTuple, Acc) -> 
			     Nodes = tuple_to_list(NodeTuple),
			     Acc ++ Nodes
		     end, 
		     [], NodeListTuples),
    L2 = lists:foldl(fun(NodeTuple, Acc) -> 
			     Nodes = tuple_to_list(NodeTuple),
			     Acc ++ Nodes
		     end, 
		     [], LookupNodeListTuples),
    lists:foldl(fun(Node, IsMutuallyExclusive) ->
			case IsMutuallyExclusive of
			    false ->
				false;
			    true ->
				not lists:member(Node, L1)
			end
		end,
		true,
		L2)
.

getAllNodesFromTuples(NodeListTuples, LookupNodeListTuples) ->
    AllNodeTuples = lists:merge(NodeListTuples, LookupNodeListTuples),
    ListOfNodes = lists:foldl(fun(NodeTuple, Acc) -> 
					       Nodes = tuple_to_list(NodeTuple),
					       Acc ++ Nodes
				       end, 
				       [], AllNodeTuples)
.

getListOfNodeTuples() ->
    getConfData()
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
		    io:format("FileStr ~p~n", [FileStr]),
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
	      lists:foreach(
		fun(ContextName) ->
			TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
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
    ReorgTuple = isCacheReorg(),
    OrigNodeList = getNodeList(),
    NodeListRec = getNodeListRecord(),
    NodeList = NodeListRec#node_list.node_list,
    LookupNodeList = NodeListRec#node_list.lookup_node_list,
    
    CacheData = createCacheDataRecord(ContextName, Key, Data, TTL, NodeList),

    TableFragment = erlang:element(1, CacheData),
    TableFragmentName = erlang:atom_to_list(TableFragment),
    DataSize = size(Data),
    Fun = fun() ->
		  mnesia:write(CacheData),
		  [FragMemoryRec] = mnesia:read(table_fragment_memory, TableFragmentName),
		  CurrMemUse = FragMemoryRec#table_fragment_memory.memory_use,
		  MemUse = CurrMemUse + DataSize,
		  NewFragMemoryRec = FragMemoryRec#table_fragment_memory{memory_use = MemUse},
		  mnesia:write(NewFragMemoryRec),		  
		  [NodeListRec] = mnesia:read(node_list, 0),		  
		  LookupCacheRecord = createLookupCacheDataRecord(ContextName, Key, TableFragmentName, NodeListRec#node_list.lookup_node_list),
		  mnesia:write(LookupCacheRecord)
	  end,
    mnesia:transaction( Fun )

%%    case ReorgTuple of
%%	false ->
%%	    put(ContextName, Key, Data, TTL, OrigNodeList);
%%	{true, ReorgType, NodeAffected, NumberOfNodes} ->	    
%%	    NewNodeList = case ReorgType of
%%			      "add" ->
%%				  OrigNodeList ++ [NodeAffected];
%%			      "delete" ->
%%				  lists:delete(NodeAffected, OrigNodeList);
%%			      _ ->
%%				  OrigNodeList
%%			  end,
%%	    put(ContextName, Key, Data, TTL, NewNodeList)
%%    end
.



put(ContextName, Key, Data, TTL, NodeList) ->
    CacheData = createCacheDataRecord(ContextName, Key, Data, TTL, NodeList),
    TableFragment = erlang:element(1, CacheData),
    TableFragmentName = erlang:atom_to_list(TableFragment),
    DataSize = size(Data),
    Fun = fun() ->
		  mnesia:write(CacheData),
		  %%io:format("Cache data is ~p~n", [CacheData]),
		  [FragMemoryRec] = mnesia:read(table_fragment_memory, TableFragmentName),
		  CurrMemUse = FragMemoryRec#table_fragment_memory.memory_use,
		  MemUse = CurrMemUse + DataSize,
		  %%io:format("Curr mem use ~p;  Data size ~p~n", [CurrMemUse, DataSize]),
		  NewFragMemoryRec = FragMemoryRec#table_fragment_memory{memory_use = MemUse},
		  mnesia:write(NewFragMemoryRec),
		  [NodeListRec] = mnesia:read(node_list, 0),		  
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


%%-record(cache_reorg, 
%%	{
%%	  key = 0,
%%	  is_reorg,                %% atom - true or false
%%	  reorg_type,              %% string - "add" or "delete"
%%	  node_affected,           %% atom - the name of the node being added or removed
%%	  current_number_of_nodes, %% integer value
%%	  nodes_running_reorg      %% integer value - how many nodes are still running a reorg operation
%%	}
%%).
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
    %%mnesia:dirty_delete_object(FragmentRecord),
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
    Secs = secs(),
%%    ContextRec = ?MODULE:getContextRecord(ContextName),    
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    ListSize = length(NodeListRecord#node_list.node_list),
    HashIndex = erlang:phash2(Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, NodeListRecord#node_list.node_list),
    TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
    Table = ?MODULE:getContextAtom(TableFragmentName),

    CacheData = {
      Table,
      Key,
      Data,
      Secs,
      Secs,
      TTL,
      Secs + TTL
     },
    CacheData
.

createCacheDataRecord(ContextName, Key, Data, TTL, ListOfNodes) ->
    Secs = secs(),
    ListSize = length(ListOfNodes),
    HashIndex = erlang:phash2(Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, ListOfNodes),
    TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
    Table = ?MODULE:getContextAtom(TableFragmentName),

    CacheData = {
      Table,
      Key,
      Data,
      Secs,
      Secs,
      TTL,
      Secs + TTL
     },
    CacheData
.

%% ContextName, Key, and TableName are all strings
createLookupCacheDataRecord(ContextName, Key, TableName, ListOfLookupNodes) ->
    ListSize = length(ListOfLookupNodes),
    HashIndex = erlang:phash2(ContextName ++ Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, ListOfLookupNodes),
    Node = erlang:element(1, NodeTuple),
    Table = getCacheLookupRecordTable(Node),
    {
      Table,
      {erlang:list_to_binary(ContextName), erlang:list_to_binary(Key)},
      erlang:list_to_binary(TableName)
    }
.


%% [key, data, store_time, last_access_time, ttl, expire_time]
createRehashedCacheDataRecord(ContextName, CacheDataRecord, ListOfNodes) ->
    {OldTable, Key, Data, StoreTime, LastAccessTime, TTL, ExpireTime} = CacheDataRecord,
    ListSize = length(ListOfNodes),
    HashIndex = erlang:phash2(Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, ListOfNodes),
    TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
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
      

%%get(ContextName, Key) -> 
%%    ReorgTuple = isCacheReorg(),
%%    OrigNodeList = getNodeList(),
%%    case ReorgTuple of
%%	false ->
%%	    get(ContextName, Key, OrigNodeList);
%%	{true, ReorgType, NodeAffected, NumberOfNodes} ->	    
%%	    NewNodeList = case ReorgType of
%%			      "add" ->
%%				  OrigNodeList ++ [NodeAffected];
%%			      "delete" ->
%%				  lists:delete(NodeAffected, OrigNodeList);
%%			      _ ->
%%				  OrigNodeList
%%			  end,
%%	    {Data1, Data2} = {get(ContextName, Key, OrigNodeList), get(ContextName, Key, NewNodeList)},
%%	    %%io:format("data1 and data2 are ~p ~p~n", [Data1, Data2]),
%%	    case {Data1, Data2} of
%%		{undefined, undefined} ->
%%		    undefined;
%%		{D1, undefined} ->
%%		    D1;	
%%		{undefined, D2} ->
%%		    D2;
%%		_ ->
%%		    undefined			
%%	    end
%%    end
%%.

get(ContextName, Key) ->
    NodeListRec = getNodeListRecord(),
    ListSize = length(NodeListRec#node_list.lookup_node_list),
    HashIndex = erlang:phash2(ContextName ++ Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, NodeListRec#node_list.lookup_node_list),
    Node = erlang:element(1, NodeTuple),
    LookupTable = getCacheLookupRecordTable(Node),
    LookupRecordList = mnesia:dirty_read(LookupTable, {erlang:list_to_binary(ContextName), erlang:list_to_binary(Key)}),
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

get(ContextName, Key, ListOfNodes) ->
    ContextRec = ?MODULE:getContextRecord(ContextName),
    ListSize = length(ListOfNodes),
    HashIndex = erlang:phash2(Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, ListOfNodes),
    TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
    Table = ?MODULE:getContextAtom(TableFragmentName),

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
.

%%delete(ContextName, Key) ->
%%    ReorgTuple = isCacheReorg(),
%%    OrigNodeList = getNodeList(),
%%
%%    case ReorgTuple of
%%	false ->
%%	    delete(ContextName, Key, OrigNodeList);
%%	{true, ReorgType, NodeAffected, NumberOfNodes} ->	    
%%	    NewNodeList = case ReorgType of
%%			      "add" ->
%%				  OrigNodeList ++ [NodeAffected];
%%			      "delete" ->
%%				  lists:delete(NodeAffected, OrigNodeList);
%%			      _ ->
%%				  OrigNodeList
%%			  end,
%%	    delete(ContextName, Key, OrigNodeList), 
%%	    delete(ContextName, Key, NewNodeList)
%%    end
%%.

delete(ContextName, Key) ->
    NodeListRec = getNodeListRecord(),
    ListSize = length(NodeListRec#node_list.lookup_node_list),
    HashIndex = erlang:phash2(ContextName ++ Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, NodeListRec#node_list.lookup_node_list),
    Node = erlang:element(1, NodeTuple),
    LookupTable = getCacheLookupRecordTable(Node),
    LookupRecordList = mnesia:dirty_read(LookupTable, {erlang:list_to_binary(ContextName), erlang:list_to_binary(Key)}),
    case LookupRecordList of
	[] ->
	    noop;
	[LookupRecord | _] ->
	    CacheTableFragmentBin = erlang:element(3, LookupRecord),
	    TableFragmentName = erlang:binary_to_list(CacheTableFragmentBin),
	    Table = listToAtom(TableFragmentName),
	    Fun = fun() ->
			  mnesia:delete(LookupTable, {erlang:list_to_binary(ContextName), erlang:list_to_binary(Key)}, write),
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


delete(ContextName, Key, ListOfNodes) ->
    ContextRec = ?MODULE:getContextRecord(ContextName),    
    ListSize = length(ListOfNodes),
    HashIndex = erlang:phash2(Key, ListSize) + 1,
    NodeTuple = lists:nth(HashIndex, ListOfNodes),
    TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
    Table = ?MODULE:getContextAtom(TableFragmentName),
    
    Fun = fun() ->
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
    mnesia:transaction( Fun )
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
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    lists:foreach(fun(NodeTuple) ->
			  Node = erlang:element(1, NodeTuple),
			  TableFragmentName = createTableFragmentName(ContextName, Node),
			  Table = ?MODULE:getContextAtom(TableFragmentName),
			  mnesia:create_table(
			    Table,
			    [
			     {ram_copies, [Node]},
			     {attributes, ?CACHE_RECORD_DATA},
			     {index, [store_time, last_access_time, expire_time]}
			    ]
			   ),
			  OtherNodes = 
			      case erlang:tuple_size(NodeTuple) > 1 of
				  true ->
				      lists:sublist(tuple_to_list(NodeTuple), 2, erlang:tuple_size(NodeTuple));
				  false ->
				      []
			      end,
			  lists:foreach(fun(OtherNode) -> 
						mnesia:add_table_copy(Table, OtherNode, ram_copies)
					end, OtherNodes),
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
		  NodeListRecord#node_list.node_list
		 )	  
	.

%% ContextName : string()
%% Node : atom()
createTableFragmentName(ContextName, Node) ->
    ContextName ++ "_fragment_" ++ erlang:integer_to_list(erlang:phash2(Node, 1000000000))
.



deleteContext(ContextName) ->
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    lists:foreach(fun(Node) ->
			  TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, Node)),
			  Table = ?MODULE:getContextAtom(TableFragmentName),
			  mnesia:delete_table(Table),
			  DelFun = fun() ->
					   mnesia:delete(table_fragment_memory, TableFragmentName)
				   end,
			  mnesia:transaction(DelFun)			      
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
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    lists:foreach(fun(LookupNodeTuple) ->
			  FirstNode = erlang:element(1, LookupNodeTuple),
			  Table = getCacheLookupRecordTable(FirstNode),
			  ContextBin = erlang:list_to_binary(ContextName),
			  ListOfKeys = mnesia:dirty_all_keys(Table),
			  ListOfContextKeys = lists:filter(
						fun({TableContext, _}) ->
							ContextBin == TableContext
						end,
						ListOfKeys
					       ),
			  lists:foreach(fun(ContextKey) ->
						mnesia:dirty_delete(Table, ContextKey)
					end,
					ListOfContextKeys
				       )
		  end,
		  NodeListRecord#node_list.lookup_node_list
		 ),
    lists:foreach(fun(NodeTuple) ->
			  TableFragmentName = createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
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







%% Node is a tuple of atoms of the format {node_name1@w.x.y.z, node_name2@a.b.c.d, ...}
addNode(NodeTuple) when erlang:is_tuple(NodeTuple) == true ->
    io:format("checkpoint 1~n"),
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    io:format("checkpoint 2~n"),
    [CacheReorgRecord] = mnesia:dirty_read(cache_reorg, 0),
    
    case CacheReorgRecord#cache_reorg.is_reorg of
	true ->
	    {error, "Node change is in process.  Unable to add node now."};
	false ->
	    NodeListTuples = NodeListRecord#node_list.node_list,
	    LookupNodeListTuples = NodeListRecord#node_list.lookup_node_list,
	    case 
		{areMutuallyExclusive([NodeTuple], NodeListTuples),
		 areMutuallyExclusive([NodeTuple], LookupNodeListTuples)} of
		{true, true} ->
		    addNode(erlang:tuple_to_list(NodeTuple));
		{false, true} ->
		    {error, "One or more of the nodes is already a member of the cache"};
		{true, false} ->
		    {error, "One or more of the nodes is already a member of the lookup table nodes"};
		_ ->
		    {error, "One or more of the nodes is already a member of either the cache or the lookup table nodes"}
	    end
    end		
;
addNode(ListOfNodes) when erlang:is_list(ListOfNodes) == true ->    
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
	    FirstNode = lists:nth(1, ListOfNodes),
	    ListOfContexts = getAllContexts(),
	    lists:foreach(
	      fun(ContextName) ->		      
		      TableFragmentName = createTableFragmentName(ContextName, FirstNode),
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
%%	    NewCacheReorgRecord = #cache_reorg{
%%	      key = 0,
%%	      is_reorg = true,
%%	      reorg_type = "add",
%%	      node_affected = Node,
%%	      current_number_of_nodes = length(NodeListRecord#node_list.node_list),
%%	      nodes_running_reorg = length(NodeListRecord#node_list.node_list)
%%	     },
%%	    mnesia:dirty_write(NewCacheReorgRecord),
%%	    rpc:call(Node, cache_monitor, start, []),
%%	    lists:foreach(fun(N) ->
%%				  {change_node_configuration, N} ! {add, Node}
%%			  end,
%%			  NodeListRecord#node_list.node_list
%%			 ),
	    ok
    end
.

deleteNodeTableInfo(Node) ->
    deleteNodeTableFragmentInfo(Node),
    deleteNodeTableCopyInfo(Node),
    ok
.

deleteNodeTableFragmentInfo(Node) ->
    ListOfContexts = getAllContexts(),
    lists:foreach(
      fun(ContextName) ->
	      TableFragmentName = createTableFragmentName(ContextName, Node),
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

deleteNode(Node) when erlang:is_atom(Node) == true ->
    io:format("checkpoint 1~n"),
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    io:format("checkpoint 2~n"),
    [CacheReorgRecord] = mnesia:dirty_read(cache_reorg, 0),
    
    case CacheReorgRecord#cache_reorg.is_reorg of
	true ->
	    {error, "Node change is in process.  Unable to delete node now."};
	false ->
	    NodeList = NodeListRecord#node_list.node_list,
	    case lists:member(Node, NodeList) of
		false ->
		    {error, "Node is not a member of the cache"};
		true ->
		    io:format("checkpoint 3~n"),
		    NewCacheReorgRecord = #cache_reorg{
		      key = 0,
		      is_reorg = true,
		      reorg_type = "delete",
		      node_affected = Node,
		      current_number_of_nodes = length(NodeListRecord#node_list.node_list),
		      nodes_running_reorg = length(NodeListRecord#node_list.node_list)
		     },
		    mnesia:dirty_write(NewCacheReorgRecord),
		    lists:foreach(fun(N) ->
					  {change_node_configuration, N} ! {delete, Node}
				  end,
				  NodeListRecord#node_list.node_list
				 ),
		    ok
	    end
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
       
