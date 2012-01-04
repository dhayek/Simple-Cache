

-define(LOGIN_ID, "login-id").
-define(PASSWORD, "password").
-define(PASSWORD_REPEAT, "password-repeat").
-define(FIRST_NAME, "first-name").
-define(LAST_NAME, "last-name").

-define(DEFAULT_SECS_TTL, 7200).

-define(CACHE_RECORD_DATA, [key, data, store_time, last_access_time, ttl, expire_time]).

-define(CACHE_LOOKUP_RECORD_DATA, [key, table_name]).  %% key =  <<"Key">>, table_name = <<"Table Name">>

-define(CACHE_LOOKUP_TABLE_PREFIX, "cache_lookup").


-record(cache_data, {
	  key,               %% string value
	  data,              %% arbitrary binary data
	  store_time,        %% time in seconds that this was added to the cache
	  last_access_time,  %% on access, this gets updated and added back to the cache.  Used for eviction in a LRU algorithm
	  ttl,               %% seconds for valid data, either user-supplied, or the default created for the context of this cache data
	  expire_time        %% calculated value in seconds, used for eviction
	  
}).


-record(context, 
	{
	  context_name,        %% string value, which is used in the url to identify the type of object being cached: e.g. /cache/mycontext/mykey
	  use_header,          %% boolean: should the http header x-icache-header be used for validation?
	  header_value,        %% http header value for authentication.  The http header x-icache-header of the caller needs to be set to this
	  use_secure,          %% boolean indicating whether these need to be secure transactions
	  default_ttl,         %% in seconds, how long should the cache data be stored
	  memory_allocation   %% memory, in bytes, to be used for this context
	}
).

-record(table_fragment_memory,
	{
	  table_fragment_name, %% string value
	  table_fragment,      %% atom representing the table itself
	  memory_use = 0
	}
).


-record(table_lookup,
		{
		  key,            %% <<"Key value">>
		  table_name      %% string value for the table where the data for this
                                  %% context and key is located  
		 }
).


-record(cache_reorg, 
	{
	  key = 0,
	  is_reorg,                %% atom - true or false
	  reorg_type,              %% string - "add" or "delete"
	  node_affected,           %% atom - the name of the node being added or removed
	  current_number_of_nodes, %% integer value
	  nodes_running_reorg      %% integer value - how many nodes are still running a reorg operation
	}
).

-record(node_list, 
	{
	  key = 0,
	  node_list,               %% list of tuples of the form {node1@www.xxx.yyy.zzz, node2@aaa.bbb.ccc.ddd, ...}
                                   %% Each tuple contains the primary and backup node.  Note that there may be zero or
                                   %% more backup nodes.
	  lookup_node_list
	}
).

-record(admin,
	{
	  login_id,
	  password,
	  salt,
	  first_name,
	  last_name
	}
   
).

-record(logged_in, 
	{
	  key = 0,
	  opaque = []    %% [{session id, login_id, time_logged_in, last_access_time}, {...}]	  
	}
).


-record(cache_context_use, 
	{
	  context_name,
	  context_node_data, %% [{node, memory overhead, object memory, number of objects}, ...]
	  total_memory_overhead,
	  total_memory_use,
	  total_number_of_objects
	}
).
