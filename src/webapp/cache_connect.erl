-module(cache_connect).

-include_lib("yaws_api.hrl").
-include_lib("yaws.hrl").

-export( [ start/1 ] ).

start(SConfRec) ->
    
    io:format("SConfRec is ~p~n", [SConfRec]),
    PriNodeTuple = lists:keyfind("primary_cache_node", 1, SConfRec#sconf.opaque),
    io:format("tuple is ~p~n", [PriNodeTuple]),
    case PriNodeTuple of
	{"primary_cache_node", NodeName} ->
	    Node = cache:listToAtom(NodeName),
	    net_adm:ping(Node),
	    mnesia:start(),
	    mnesia:change_config(extra_db_nodes, [Node]),
	    mnesia:add_table_copy(schema, node(), ram_copies);
	_ ->
	    io:format("Can not connect to cache nodes.  Check your configuration or your cookie value.~n"),
	    yaws:stop(),
	    erlang:exit()
    end

.
