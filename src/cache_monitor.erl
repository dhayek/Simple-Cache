-module(cache_monitor).

-include_lib("cache.hrl").

-export( [ start/0, stop/0, getAllCacheInfo/0, getContextCacheInfo/1, 
	   createNodeConfigurationChangeListener/0 ] ). %%changeConfigurationLoop/0 ] ).

-export( [ createListeners/0, createStaleCacheEntriesListener/0, deleteStaleCacheEntriesLoop/0 ] ).
-export( [ createNodeDeletedListener/0, configChangedLoop/0 ] ).
-export( [ createMemoryMonitorListener/0, createMemoryPruneListener/0, monitorMemoryLoop/0, pruneMemoryLoop/0 ]  ).
-export( [ createNodeMonitorListener/0, monitorNodeLoop/0, nodeDownFunction/2, nodeDownFunction/1, monitorNodeDeletion/0  ] ).
-export( [ createStopListener/0, stopListener/0 ]).
-export( [  deleteNode/1,  getTableKeys/1  ] ).  %% getAllContexts/0, addNode/1,
-export( [ deleteStaleCacheEntries/0, deleteStaleCacheEntries/1] ).
-export( [ checkMemory/0, pruneCache/6, getNodeListTupleEntryForThisNode/0 ] ).

-export( [ tableInfo/2,  getCacheUseSummary/0, getCacheMemoryUse/0, getCacheObjectsInCache/0, 
	   getCacheContextUseSummary/1, getCacheContextMemoryUse/1, getCacheContextObjectsInCache/1 ] ).


start() ->
    io:format("will create listeners~n"),
    createListeners()
.

stop() ->
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
    ?MODULE:createNodeDeletedListener(),
    %%?MODULE:createNodeMonitorListener(),
    ?MODULE:createStopListener()
.

createStopListener() ->
    register(stop_listener, spawn(?MODULE, stopListener, []))
.
stopListener() ->
    receive
	stop ->
	    {config_changed_listener, node()} ! stop,
	    {delete_stale_entries, node()} ! stop,
	    {memory_monitor, node()} ! stop,
	    {node_deleted_listener, node()} ! stop,
	    {memory_prune, node()} ! stop;	
	    %%{node_monitor, node()} ! stop;
	_ ->
	    stopListener()
    end
.


%% {node_deleted_listener, Node} ! {delete, NodeTuple}
createNodeConfigurationChangeListener() ->
    register(config_changed_listener, spawn(?MODULE, configChangedLoop, []))
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
createNodeDeletedListener() ->
    io:format("willcreate nodeDeltedListener~n"),
    register(node_deleted_listener, spawn(?MODULE, monitorNodeDeletion, []))
%%    {node_deleted_listener, node()} ! start
.


configChangedLoop() ->
    receive
	{delete, Node} ->
	    io:format("Node ~p recieved delete message for Node ~p~n", [node(), Node]),
	    ?MODULE:deleteNode(Node),
	    configChangedLoop();
	{add, Node} ->
	    io:format("Node ~p recieved add message for Node ~p~n", [node(), Node]),
	    ?MODULE:addNode(Node),
	    configChangedLoop();
	node_added ->
	    io:format("Node added.  ~p will respond.~n", [node()]),
	    changeConfig({node_added}),
	    configChangedLoop();
	node_deleted ->
	    io:format("Node deleted.  ~p will respond.~n", [node()]),
	    changeConfig({node_deleted}),
	    configChangedLoop();
	{context_added, ContextName} ->
	    io:format("context added.  ~p will respond.~n", [node()]),
	    changeConfig({context_added, ContextName}),
	    configChangedLoop();
	{context_deleted, ContextName} ->
	    io:format("context deleted.  ~p will respond.~n", [node()]),
	    changeConfig({context_deleted, ContextName}),
	    configChangedLoop();
	stop ->
	    io:format("Node ~p recieved stop changeConfiguration message~n", [node()]),
	    ok
    end
.

changeConfig({node_added}) ->
    cache:writeConfDataFromNodeList(),
    ok
    ;
changeConfig({node_deleted}) ->
    cache:writeConfDataFromNodeList(),
    ok;
changeConfig({context_added, ContextName}) ->
    cache:writeConfDataFromNodeList(),
    ok;
changeConfig({context_deleted, ContextName}) ->
    cache:writeConfDataFromNodeList(),
    ok
.

monitorNodeDeletion() ->
    io:format("will start receive for node deletion~n"),
    receive
	{delete, NodeTuple} ->
	    io:format("node ~p will delete~n", [node()]),
	    deleteNode(NodeTuple),	    
	    ok;
	stop ->
	    io:format("Node ~p recieved stop monitorNodeDeletion message~n", [node()]),
	    ok;
	go ->
	    io:format("will go ~n"),
	    monitorNodeDeletion();
	_ ->
	    io:format("got a random message~n"),
	    monitorNodeDeletion()	    
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

deleteNode({NodeKey, ListOfNodes}) ->
    io:format("will delete this node~n"),
    ListOfContexts = cache:getAllContexts(),
    Node = getFirstReachableNode(ListOfNodes),
    deleteNode({NodeKey, Node, ListOfNodes}, node())
%%    
.


deleteNode({NodeKey, Node, ListOfNodes}, Node) ->
    ListOfContexts = cache:getAllContexts(),
    lists:foreach(
      fun(ContextName) ->
	      io:format("will recache for Context~p~n", [ContextName]),
	      LocalTableFragmentName = cache:createTableFragmentName(ContextName, NodeKey),
	      Table = cache:getContextAtom(LocalTableFragmentName),
	      ListOfKeys = getTableKeys(Table),
	      lists:foreach(fun(Key) ->
				    RecordList = mnesia:dirty_read(Table, Key),
				    case RecordList of
					[] ->
					    ok;
					[Record | _ ] ->
					    cache:reCacheRecord(ContextName, Record)
				    end
			    end,
			    ListOfKeys
			   )
      end,
      ListOfContexts
     ),
    cache:deleteNodeTableInfo({NodeKey, ListOfNodes}),
    ok
;
deleteNode({NodeKey, Node, _}, OtherNode) ->
    noop
.

deleteStaleCacheEntries() ->
    ListOfContexts = cache:getAllContexts(),
    lists:foreach( fun(ContextName) ->
			   ?MODULE:deleteStaleCacheEntries(ContextName)
		   end,
		   ListOfContexts)
.

getNodeListTupleEntryForThisNode() ->
    NodeListTuples = cache:getNodeList(),
    lists:foldl(fun(NodeTuple, N) ->
			case N of 
			    undefined ->
				{_, ListOfNodes} = NodeTuple,
				case lists:member(node(), ListOfNodes) of
				    true ->
					NodeTuple;
				    _ ->
					undefined
				end;
			    _ ->
				N
			end
		end,
		undefined,
		NodeListTuples
	       )
.

%% [key, data, store_time, last_access_time, ttl, expire_time]
deleteStaleCacheEntries(ContextName) ->
    NodeListTuple = getNodeListTupleEntryForThisNode(),
    case NodeListTuple of
	undefined ->
	    {error, "No node list defined for this node."};
	_ ->
	    {NodeKey, ListOfNodes} = NodeListTuple,
	    Node = getFirstReachableNode(ListOfNodes),
	    case Node of
		undefined ->
		    noop;
		_ ->
		    deleteStaleCacheEntries(ContextName, NodeKey, Node)
	    end
    end
.

deleteStaleCacheEntries(ContextName, NodeKey, Node) ->
    TableName = cache:createTableFragmentName(ContextName, NodeKey),
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

getFirstReachableNode([]) ->
    undefined;
getFirstReachableNode([FirstNode | Rest]) ->
    case net_adm:ping(FirstNode) of
	pong ->
	    FirstNode;
	_ ->
	    getFirstReachableNode(Rest)
    end
.

checkMemory() ->
    ListOfContexts = cache:getAllContexts(),
    NodeListRecord = cache:getNodeListRecord(),
    ListOfNodeTuples = NodeListRecord#node_list.node_list,
    WordSize = erlang:system_info(wordsize),
    NumberOfNodes = length(ListOfNodeTuples),
    NodeListTuple = getNodeListTupleEntryForThisNode(),
    case NodeListTuple of 
	undefined ->
	    noop;
	_ ->
	    {NodeKey, ListOfNodes} = NodeListTuple,
	    lists:foreach(
	      fun(ContextName) ->
		      Node = getFirstReachableNode(ListOfNodes),
		      TableName = cache:createTableFragmentName(ContextName, NodeKey),
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
				      {memory_prune, Node} ! {prune, ContextName, TableName, Table, AllowedMemory, MemUse, cache:secs()};
				  false ->
				      ok
			      end
		      end
	      end,
	      ListOfContexts
	     )
    end
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
	lists:foldl(fun(NodeTuple, NodeDataList) ->
			    Node = erlang:element(1, NodeTuple),
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

    lists:foldl(fun(NodeTuple, {OverHeadMemory, ObjectMemory}) ->
			TableFragmentName = cache:createTableFragmentName(ContextName, erlang:element(1, NodeTuple)),
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
