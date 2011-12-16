-module(cache_monitor).

-include_lib("cache.hrl").

-export( [ start/0, stop/0, getAllCacheInfo/0, getContextCacheInfo/1, 
	   createNodeConfigurationChangeListener/0, changeConfigurationLoop/0 ] ).

-export( [ createListeners/0, createStaleCacheEntriesListener/0, deleteStaleCacheEntriesLoop/0 ] ).
-export( [ createMemoryMonitorListener/0, createMemoryPruneListener/0, monitorMemoryLoop/0, pruneMemoryLoop/0 ]  ).
-export( [ createNodeMonitorListener/0, monitorNodeLoop/0, nodeDownFunction/2, nodeDownFunction/1  ] ).
-export( [ createStopListener/0, stopListener/0 ]).
-export( [ getAllContexts/0, deleteNode/1, addNode/1, getTableKeys/1  ] ).
-export( [ deleteStaleCacheEntries/0, deleteStaleCacheEntries/1] ).
-export( [ checkMemory/0, pruneCache/6 ] ).

-export( [ tableInfo/2,  getCacheUseSummary/0, getCacheMemoryUse/0, getCacheObjectsInCache/0, 
	   getCacheContextUseSummary/1, getCacheContextMemoryUse/1, getCacheContextObjectsInCache/1 ] ).


start() ->
    createListeners()
%%    createNodeConfigurationChangeListener()
.

stop() ->
%%    {change_node_configuration, node()} ! stop,
    {stop_listener, node()} ! stop
.

getAllCacheInfo() ->
    ok.

getContextCacheInfo(A) ->
    ok.


createListeners() ->
    ?MODULE:createNodeConfigurationChangeListener(),
    ?MODULE:createStaleCacheEntriesListener(),
    ?MODULE:createMemoryMonitorListener(),
    ?MODULE:createMemoryPruneListener(),
    ?MODULE:createNodeMonitorListener(),
    ?MODULE:createStopListener()
.

createStopListener() ->
    register(stop_listener, spawn(?MODULE, stopListener, []))
.
stopListener() ->
    receive
	stop ->
	    {change_node_configuration, node()} ! stop,
	    {delete_stale_entries, node()} ! stop,
	    {memory_monitor, node()} ! stop,
	    {memory_prune, node()} ! stop,
	    {node_monitor, node()} ! stop;
	_ ->
	    stopListener()
    end
.

createNodeConfigurationChangeListener() ->
    register(change_node_configuration, spawn(?MODULE, changeConfigurationLoop, []))
.
createStaleCacheEntriesListener() ->
    register(delete_stale_entries, spawn(?MODULE, deleteStaleCacheEntriesLoop, []))
.
createMemoryMonitorListener() ->
    register(memory_monitor, spawn(?MODULE, monitorMemoryLoop, []))
.
createMemoryPruneListener() ->
    register(memory_prune, spawn(?MODULE, pruneMemoryLoop, []))
.
createNodeMonitorListener() ->
    register(node_monitor, spawn(?MODULE, monitorNodeLoop, [])),
    {node_monitor, node()} ! start
.

changeConfigurationLoop() ->
    receive
	{delete, Node} ->
	    io:format("Node ~p recieved delete message for Node ~p~n", [node(), Node]),
	    ?MODULE:deleteNode(Node),
	    changeConfigurationLoop();
	{add, Node} ->
	    io:format("Node ~p recieved add message for Node ~p~n", [node(), Node]),
	    ?MODULE:addNode(Node),
	    changeConfigurationLoop();
	stop ->
	    io:format("Node ~p recieved stop changeConfiguration message~n", [node()]),
	    ok
    end
.

deleteStaleCacheEntriesLoop() ->
    receive
	stop ->
	    io:format("Node ~p recieved stop deleteStaleEntries message~n", [node()]),
	    ok
    after 2000 ->
	    ?MODULE:deleteStaleCacheEntries(),
	    ?MODULE:deleteStaleCacheEntriesLoop()
    end
.

monitorMemoryLoop() ->
    receive 
	stop ->
	    io:format("Node ~p recieved stop monitorMemory message~n", [node()]),
	    ok
	after 2000 ->
		?MODULE:checkMemory(),
		monitorMemoryLoop()
	end
.

pruneMemoryLoop() ->
    receive 
	stop ->
	    io:format("Node ~p recieved stop pruneMemory message~n", [node()]),
	    ok;
	{prune, ContextName, TableFragmentName, Table, AllowedMemory, MemUse, TTL} ->
	    %%io:format("Will prune memory~n"),
	    ?MODULE:pruneCache(ContextName, TableFragmentName, Table, AllowedMemory, MemUse, TTL + 20),
	    pruneMemoryLoop()
    end
.

monitorNodeLoop() ->
    receive
	start ->
	    NodeList = cache:getNodeList(),
	    lists:foreach(
	      fun(Node) ->
		      erlang:monitor_node(Node, true)
	      end,
	      NodeList
	     ),
	    monitorNodeLoop();
	{nodedown, NodeDown} ->
	    NodeList = cache:getNodeList(),
	    case length(NodeList) > 1 of
		true ->
		    [Node1, Node2 | _ ] = NodeList,
		    nodeDownFunction({Node1, Node2}, {node(), NodeDown});
		false ->
		    noop
	    end,
	    monitorNodeLoop();
	{nodeadded, Node} ->
	    erlang:monitor_node(Node, true),
	    monitorNodeLoop();
	stop ->
	    ok
    end
.

nodeDownFunction({N1, N2}, {N1, NodeDown}) ->
    nodeDownFunction(NodeDown);
nodeDownFunction({NodeDown, N2}, {N2, NodeDown}) ->
    nodeDownFunction(NodeDown);
nodeDownFunction({_, _}, {_, _}) ->
    ok
.
nodeDownFunction(NodeDown) ->
    cache:deleteNode(NodeDown)
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

%% createTableFragmentName(ContextName, Node) ->

deleteNode(Node) ->
    ListOfContexts = ?MODULE:getAllContexts(),
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    OrigNodeList = NodeListRecord#node_list.node_list,
    NewNodeList = lists:delete(Node, OrigNodeList),
    lists:foreach(
      fun(ContextName) ->
	      LocalTableFragmentName = cache:createTableFragmentName(ContextName, node()),
	      Table = cache:getContextAtom(LocalTableFragmentName),
	      ListOfKeys = getTableKeys(Table),
	      lists:foreach(fun(Key) ->
				    RecordList = mnesia:dirty_read(Table, Key),
				    case RecordList of
					[] ->
					    ok;
					[Record | _ ] ->
					    NewRecord = cache:createRehashedCacheDataRecord(ContextName, Record, NewNodeList),
					    case Record == NewRecord of
						true ->
						    ok;
						false ->
						    NewTable = erlang:element(1, NewRecord),
						    NewTableName = erlang:atom_to_list(NewTable),
						    cache:putFragRecord(NewRecord, NewTableName),
						    cache:deleteFragRecord(Record, LocalTableFragmentName)
					    end
				    end
			    end,
			    ListOfKeys
			   )
      end,
      ListOfContexts
     ),
    UpdateTablesFun = fun() ->
			      [CacheReorgRecord] = mnesia:read(cache_reorg, 0),
			      [NodeListRecord] = mnesia:read(node_list, 0),
			      
			      NodesRunning = CacheReorgRecord#cache_reorg.nodes_running_reorg,
			      case NodesRunning == 1 of 
				  true ->
				      NewCacheReorgRecord = #cache_reorg{
					key = 0,
					is_reorg = false,
					reorg_type = "",
					node_affected = undefined,
					current_number_of_nodes = length(NodeListRecord#node_list.node_list) - 1,
					nodes_running_reorg = 0
				       },
				      mnesia:write(NewCacheReorgRecord),
				      NewNodeList = lists:delete(Node, NodeListRecord#node_list.node_list),
				      NewNodeListRecord = NodeListRecord#node_list{node_list = NewNodeList},
				      io:format("--- in delete node:  new cachereorgrecord is: ~p~n", [NewCacheReorgRecord]),
				      mnesia:write(NewNodeListRecord),
				      cache:deleteNodeTableInfo(Node);				  
				  false ->
				      case NodesRunning > 1 of
					  true ->
					      %%NodesRunning = CacheReorgRecord#cache_reorg.nodes_running_reorg,
					      UnfinishedCacheReorgRecord = CacheReorgRecord#cache_reorg{
						key = 0,
						nodes_running_reorg = NodesRunning - 1
					       },
					      io:format("new cachereorgrecord is: ~p~n", [UnfinishedCacheReorgRecord]),
					      mnesia:write(UnfinishedCacheReorgRecord);
					  false ->
					      noop
				      end
			      end
		      end,
    mnesia:transaction(UpdateTablesFun)								 
.




addNode(Node) ->
    ListOfContexts = ?MODULE:getAllContexts(),
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    OrigNodeList = NodeListRecord#node_list.node_list,
    NewNodeList = OrigNodeList ++ [Node],
    lists:foreach(
      fun(ContextName) ->
	      LocalTableFragmentName = cache:createTableFragmentName(ContextName, node()),
	      Table = cache:getContextAtom(LocalTableFragmentName),
	      ListOfKeys = getTableKeys(Table),
	      lists:foreach(fun(Key) ->
				    RecordList = mnesia:dirty_read(Table, Key),
				    case RecordList of
					[] ->
					    ok;
					[Record | _ ] ->
					    NewRecord = cache:createRehashedCacheDataRecord(ContextName, Record, NewNodeList),
					    case Record == NewRecord of
						true ->
						    ok;
						false ->
						    NewTable = erlang:element(1, NewRecord),
						    NewTableName = erlang:atom_to_list(NewTable),
						    cache:putFragRecord(NewRecord, NewTableName),
						    cache:deleteFragRecord(Record, LocalTableFragmentName)
					    end
				    end
			    end,
			    ListOfKeys
			   )
      end,
      ListOfContexts
     ),
    UpdateTablesFun = fun() ->
			      [CacheReorgRecord] = mnesia:read(cache_reorg, 0),
			      [NodeListRecord] = mnesia:read(node_list, 0),
			      
			      NodesRunning = CacheReorgRecord#cache_reorg.nodes_running_reorg,
			      case NodesRunning == 1 of 
				  true ->
				      NewCacheReorgRecord = #cache_reorg{
					key = 0,
					is_reorg = false,
					reorg_type = "",
					node_affected = undefined,
					current_number_of_nodes = length(NodeListRecord#node_list.node_list) + 1,
					nodes_running_reorg = 0
				       },
				      mnesia:write(NewCacheReorgRecord),
				      NewNodeList = NodeListRecord#node_list.node_list ++ [Node],
				      NewNodeListRecord = NodeListRecord#node_list{node_list = NewNodeList},
				      io:format("--- new cachereorgrecord is: ~p~n", [NewCacheReorgRecord]),
				      mnesia:write(NewNodeListRecord);
				  false ->
				      io:format("in addNode: NodesRunning ~p~n", [NodesRunning]),
				      case NodesRunning > 1 of
					  true ->
					      %%NodesRunning = CacheReorgRecord#cache_reorg.nodes_running_reorg,
					      UnfinishedCacheReorgRecord = CacheReorgRecord#cache_reorg{
						key = 0,
						nodes_running_reorg = NodesRunning - 1
					       },
					      io:format("new cachereorgrecord is: ~p~n", [UnfinishedCacheReorgRecord]),
					      mnesia:write(UnfinishedCacheReorgRecord);
					  false ->
					      noop
				      end
			      end
		      end,
    mnesia:transaction(UpdateTablesFun)
.


deleteStaleCacheEntries() ->
    ListOfContexts = ?MODULE:getAllContexts(),
    lists:foreach( fun(ContextName) ->
			   ?MODULE:deleteStaleCacheEntries(ContextName)
		   end,
		   ListOfContexts)
.


%% [key, data, store_time, last_access_time, ttl, expire_time]
deleteStaleCacheEntries(ContextName) ->

    TableName = cache:createTableFragmentName(ContextName, node()),
    %%io:format("tablename is ~p~n", [TableName]),
    Table = cache:getContextAtom(TableName),
    Secs = cache:secs(),
    MatchHead = {'$1', '$2', '$3', '$4', '$5', '$6', '$7' },
    Guard = {'>', Secs, '$7'},
    Result = '$2',
    Fun = fun() -> 
		  Res = mnesia:select(Table, [{MatchHead, [Guard], [Result]}], read)
%%		  Res = mnesia:select(Table, [{MatchHead, [Guard], [Result]}], 50, read)
	  end,
    Res = mnesia:transaction(Fun),

    %%io:format("res is ~p~n", [Res]),
    case Res of
	{atomic, KeyList} when erlang:is_list(KeyList) ->
	    Len = length(KeyList),
	    %%io:format("KeyList length is ~p~n", [Len]);
	    lists:foreach(fun(Key) -> cache:delete(ContextName, Key) end, KeyList);
	{atomic, []} ->
	    io:format("did not delete~n");
	{atomic, {List, Cont}} ->
	    %% io:format("got a continuation. list size is ~p~n", [length(List)]),
	    lists:foreach(fun(Key) -> cache:delete(ContextName, Key) end, List);
	{atomic, '$end_of_table'} ->
	    ok; %%io:format("got end of table~n");
	_ ->
	    io:format("apparently an error~n")
    end


.

checkMemory() ->
    ListOfContexts = ?MODULE:getAllContexts(),
    [NodeListRecord] = mnesia:dirty_read(node_list, 0),
    ListOfNodes = NodeListRecord#node_list.node_list,
    WordSize = erlang:system_info(wordsize),
    NumberOfNodes = length(ListOfNodes),
    lists:foreach(
      fun(ContextName) ->
	      TableName = cache:createTableFragmentName(ContextName, node()),
	      %%io:format("tablename is ~p~n", [TableName]),
	      Table = cache:getContextAtom(TableName),
	      MemUse = getMemoryUse(TableName), %% mnesia:table_info(Table, memory) * WordSize,
	      %%io:format("memory use for table ~p is ~p~n", [TableName, MemUse]),
	      ContextRecord = cache:getContextRecord(ContextName),
	      case ContextRecord of
		  undefined ->
		      ok;
		  _ ->
		      AllowedMemory = ContextRecord#context.memory_allocation / NumberOfNodes,
		      case MemUse > AllowedMemory of
			  true ->
			      {memory_prune, node()} ! {prune, ContextName, TableName, Table, AllowedMemory, MemUse, cache:secs()};
			  false ->
			      ok
		      end
	      end
      end,
      ListOfContexts
     )
.


pruneCache(ContextName, TableFragmentName, Table, AllowedMemory, MemUse, OlderThan) when MemUse =< AllowedMemory ->
    ok;
pruneCache(ContextName, TableFragmentName, Table, AllowedMemory, MemUse, OlderThan) when MemUse > AllowedMemory ->

    MatchHead = {'$1', '$2', '$3', '$4', '$5', '$6', '$7' },
    Guard = {'>', OlderThan, '$7'},
    Result = '$2',
    Fun = fun() ->
		  Res = mnesia:select(Table, [{MatchHead, [Guard], [Result]}], read)
	  end,
    Res = mnesia:transaction(Fun),

    %%io:format("res is ~p~n", [Res]),
    case Res of
	{atomic, KeyList} when erlang:is_list(KeyList) ->
	    %% Len = length(KeyList),
	    %% io:format("pruneCache: KeyList length is ~p~n", [Len]),
	    lists:foreach(fun(Key) -> cache:delete(ContextName, Key) end, KeyList);
	{atomic, []} ->
	    io:format("did not delete~n");
	{atomic, {List, Cont}} ->
	    %% io:format("got a continuation. list size is ~p~n", [length(List)]),
	    lists:foreach(fun(Key) -> mnesia:delete(ContextName, Key) end, List);
	{atomic, '$end_of_table'} ->
	    %% io:format("got end of table~n"),
	    ok; %%io:format("got end of table~n");
	_ ->
	    io:format("apparently an error~n")
    end,
    
    NewMemUse = getMemoryUse(TableFragmentName), %%mnesia:table_info(Table, memory) * erlang:system_info(wordsize),
    pruneCache(ContextName, TableFragmentName, Table, AllowedMemory, NewMemUse, OlderThan + 20)
.

getMemoryUse(TableFragmentName) ->
    FragRecResults = mnesia:dirty_read(table_fragment_memory, TableFragmentName),
    case FragRecResults of
	[FragRec] ->
	    FragRec#table_fragment_memory.memory_use;
	_ ->
	    0
    end
.

tableInfo(Table, Info) ->
    mnesia:table_info(Table, Info)
.

%%-record(cache_context_use, 
%%	{
%%	  context_name,
%%	  context_node_data, %% [{node, memory overhead, object memory, number of objects}, ...]
%%	  total_memory_overhead,
%%	  total_memory_use,
%%	  total_number_of_objects
%%	}
%%).

getCacheUseSummary() ->
    ContextList = cache:getAllContexts(),
    lists:foldl(fun(ContextName, ContextDataList) ->
			ContextDataList ++ [getCacheContextUseSummary(ContextName)]
		end,
		[],
		ContextList
	       )
.

getCacheMemoryUse() ->
    ok.

getCacheObjectsInCache() ->
    ok.


getCacheContextUseSummary(ContextName) ->
    ReorgTuple = cache:isCacheReorg(),
    OrigNodeList = cache:getNodeList(),
    NodeList = 
	case ReorgTuple of
	    false ->
		OrigNodeList;
	{true, ReorgType, NodeAffected, NumberOfNodes} ->
		case ReorgType of
		    "add" ->
			OrigNodeList ++ [NodeAffected];
		    _ ->
			OrigNodeList
		end
	end,    
    DataList = 
	lists:foldl(fun(Node, NodeDataList) ->
			    TableFragmentName = cache:createTableFragmentName(ContextName, Node),
			    Table = cache:getContextAtom(TableFragmentName),
			    {MemoryOverhead, MemUse} = getCacheContextMemoryAtTable(TableFragmentName, Table),
			    NumberOfObjectsAtNode = getCacheContextObjectsAtTable(Table),
			    NodeDataList ++ [{Node, MemoryOverhead, MemUse, NumberOfObjectsAtNode}]
		    end,
		    [],
		    NodeList
		   ),
    {TotalMemoryOverhead, TotalMemoryUse, TotalNumberOfObjects} = 
	lists:foldl(fun(NodeData, {MemoryOverhead, MemoryUse, NumberOfObjects}) ->
			    {MemoryOverhead + erlang:element(2, NodeData),
			     MemoryUse + erlang:element(3, NodeData),
			     NumberOfObjects + erlang:element(4, NodeData)
			    }
		    end,
		    {0, 0, 0},
		    DataList
		   ),
			     
    #cache_context_use{
		     context_name = ContextName,
		     context_node_data = DataList,
		     total_memory_overhead = TotalMemoryOverhead,
		     total_memory_use = TotalMemoryUse,
		     total_number_of_objects = TotalNumberOfObjects
		    }
   
.

getCacheContextMemoryUse(ContextName) ->
    ReorgTuple = cache:isCacheReorg(),
    OrigNodeList = cache:getNodeList(),
    NodeList = 
	case ReorgTuple of
	    false ->
		OrigNodeList;
	{true, ReorgType, NodeAffected, NumberOfNodes} ->
		case ReorgType of
		    "add" ->
			OrigNodeList ++ [NodeAffected];
		    _ ->
			OrigNodeList
		end
	end,

    lists:foldl(fun(Node, {OverHeadMemory, ObjectMemory}) ->
			TableFragmentName = cache:createTableFragmentName(ContextName, Node),
			MemUse = case getMemoryUse(TableFragmentName) of
				     M when erlang:is_integer(M) ->
					 M;
				     _ ->
					 0
				 end,
			
			Table = cache:getContextAtom(TableFragmentName),
			Result = mnesia:activity(transaction, fun ?MODULE:tableInfo/2, [Table, memory], mnesia_frag),
			NodeOverhead = case Result of
				   N when erlang:is_integer(N) ->
				       N;
				   _ ->
				       0
			       end,
			{OverHeadMemory + NodeOverhead, ObjectMemory + MemUse}
		end,
		{0, 0},
		NodeList
	       )
.

getCacheContextMemoryAtTable(TableFragmentName, Table) ->
    MemUse = case getMemoryUse(TableFragmentName) of
		 M when erlang:is_integer(M) ->
		     M;
		 _ ->
		     0
	     end,
    Result = mnesia:activity(transaction, fun ?MODULE:tableInfo/2, [Table, memory], mnesia_frag),
    NodeOverhead = case Result of
		       N when erlang:is_integer(N) ->
			   N;
		       _ ->
			   0
		   end,
    {NodeOverhead, MemUse}
.
    
getCacheContextObjectsAtTable(Table) ->
    Result = mnesia:activity(transaction, fun ?MODULE:tableInfo/2, [Table, size], mnesia_frag),
    Size = case Result of
	       N when erlang:is_integer(N) ->
		   N;
	       _ ->
		   0
	   end,    
    Size
.

getCacheContextObjectsInCache(ContextName) ->
    ReorgTuple = cache:isCacheReorg(),
    OrigNodeList = cache:getNodeList(),
    NodeList = 
	case ReorgTuple of
	    false ->
		OrigNodeList;
	{true, ReorgType, NodeAffected, NumberOfNodes} ->
		case ReorgType of
		    "add" ->
			OrigNodeList ++ [NodeAffected];
		    _ ->
			OrigNodeList
		end
	end,
    lists:foldl(fun(Node, CurrNum) ->
			TableFragmentName = cache:createTableFragmentName(ContextName, Node),
			Table = cache:getContextAtom(TableFragmentName),
			Result = mnesia:activity(transaction, fun ?MODULE:tableInfo/2, [Table, size], mnesia_frag),
			Size = case Result of
				   N when erlang:is_integer(N) ->
				       N;
				   _ ->
				       0
			       end,
			CurrNum + Size
		end,
		0,
		NodeList
	       )
.
