
-module(cache_data_ddl).


-export ( [ createCacheTables/0, createCacheTables/1   ]  ).
-export ( [ createNodeListTable/0, createContextFragmentMemoryTable/0, 
	    createAdminTable/0, createContextTable/0, createCacheReorgTable/0,
	    createLoggedInTable/0 
	  ] ).

-export( [  ]).

-include_lib("cache.hrl").

createCacheTables(ListOfNodes) when erlang:is_list(ListOfNodes) ->
    AllNodes = case ListOfNodes of
		   [] ->
		       [node()];
		   _ ->
		       ListOfNodes
	       end,
    createContextTable(AllNodes),
    createContextFragmentMemoryTable(AllNodes),
    createAdminTable(AllNodes),
    createNodeListTable(AllNodes),
    createCacheReorgTable(AllNodes),
    createLoggedInTable(AllNodes)
. 

createCacheTables() ->
%%    mnesia:create_table(
%%      cache_data,
%%      [
%%       {attributes, record_info(fields, cache_data)},
%%       {ram_copies, [node() | nodes()]},
%%       {index, [store_time, last_access_time, expire_time]}
%%      ]
%%     ),
    AllNodes = [node() | nodes()],
    createContextTable(AllNodes),
    createContextFragmentMemoryTable(AllNodes),
    createAdminTable(AllNodes),
    createNodeListTable(AllNodes),
    createCacheReorgTable(AllNodes),
    createLoggedInTable(AllNodes)
.

createContextTable() ->
    mnesia:create_table(
      context,
      [
       {attributes, record_info(fields, context)},
       {disc_copies, [node() | nodes() ]}       
      ]
     )
.    

createContextFragmentMemoryTable() ->
    mnesia:create_table(
      table_fragment_memory,
      [
       {attributes, record_info(fields, table_fragment_memory)},
       {ram_copies, [node() | nodes() ]}       
      ]
     )
.    


createAdminTable() ->
    mnesia:create_table(
      admin,
      [
       {attributes, record_info(fields, admin)},
       {disc_copies, [node() | nodes()]}       
      ]
     )    
.

createNodeListTable() ->
    mnesia:create_table(
      node_list,
      [
       {attributes, record_info(fields, node_list)},
       {disc_copies, [node() | nodes()]}       
      ]
     )
.    

createCacheReorgTable() ->
    mnesia:create_table(
      cache_reorg,
      [
       {attributes, record_info(fields, cache_reorg)},
       {disc_copies, [node() | nodes()]}       
      ]
     )
.

createLoggedInTable() ->
    mnesia:create_table(
      logged_in,
      [
       {attributes, record_info(fields, logged_in)},
       {disc_copies, [node() | nodes()]}       
      ]
     )
.    


createContextTable(NodeList) ->
    mnesia:create_table(
      context,
      [
       {attributes, record_info(fields, context)},
       {disc_copies, NodeList}       
      ]
     )
.    

createContextFragmentMemoryTable(NodeList) ->
    mnesia:create_table(
      table_fragment_memory,
      [
       {attributes, record_info(fields, table_fragment_memory)},
       {ram_copies, NodeList}       
      ]
     )
.    


createAdminTable(NodeList) ->
    mnesia:create_table(
      admin,
      [
       {attributes, record_info(fields, admin)},
       {disc_copies, NodeList}       
      ]
     )    
.

createNodeListTable(NodeList) ->
    mnesia:create_table(
      node_list,
      [
       {attributes, record_info(fields, node_list)},
       {disc_copies, NodeList}       
      ]
     )
.    

createCacheReorgTable(NodeList) ->
    mnesia:create_table(
      cache_reorg,
      [
       {attributes, record_info(fields, cache_reorg)},
       {disc_copies, NodeList}       
      ]
     )
.

createLoggedInTable(NodeList) ->
    mnesia:create_table(
      logged_in,
      [
       {attributes, record_info(fields, logged_in)},
       {disc_copies, NodeList}       
      ]
     )
.    
