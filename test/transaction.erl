-module(transaction).

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").

destroy_reopen(DbName, Options) ->
    _ = rocksdb:destroy(DbName, []),
    _ = rocksdb_test_util:rm_rf(DbName),
    {ok, Db, _} = rocksdb:open_optimistic_transaction_db(DbName, Options, [{"default", []}]),
    Db.

close_destroy(Db, DbName) ->
    rocksdb:close(Db),
    rocksdb:destroy(DbName, []),
    rocksdb_test_util:rm_rf(DbName).

basic_test() ->
    Db = destroy_reopen("transaction_testdb", [{create_if_missing, true}]),

    {ok, Transaction} = rocksdb:transaction(Db, []),

    ok = rocksdb:transaction_put(Transaction, <<"a">>, <<"v1">>),
    ok = rocksdb:transaction_put(Transaction, <<"b">>, <<"v2">>),

    ?assertEqual(not_found, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),

    ?assertEqual({ok, <<"v1">>}, rocksdb:transaction_get(Transaction, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:transaction_get(Transaction, <<"b">>, [])),

    ok = rocksdb:transaction_commit(Transaction),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),

    ok = rocksdb:release_transaction(Transaction),

    
    close_destroy(Db, "transaction_testdb"),
    ok.

delete_test() ->
    Db = destroy_reopen("transaction_testdb", [{create_if_missing, true}]),

    {ok, Transaction} = rocksdb:transaction(Db, []),

    ok = rocksdb:transaction_put(Transaction, <<"a">>, <<"v1">>),
    ok = rocksdb:transaction_put(Transaction, <<"b">>, <<"v2">>),
    ok = rocksdb:transaction_delete(Transaction, <<"b">>),
    ok = rocksdb:transaction_delete(Transaction, <<"c">>),

    ?assertEqual(not_found, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),

    ?assertEqual({ok, <<"v1">>}, rocksdb:transaction_get(Transaction, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:transaction_get(Transaction, <<"b">>, [])),

    ok = rocksdb:transaction_commit(Transaction),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),

    close_destroy(Db, "transaction_testdb"),
    ok.

cf_iterators_test() ->
    Db = destroy_reopen("transaction_testdb", [{create_if_missing, true}]),
    {ok, TestH} = rocksdb:create_column_family(Db, "test", []),

    rocksdb:put(Db, <<"a">>, <<"x">>, []),
    rocksdb:put(Db, <<"b">>, <<"y">>, []),
    rocksdb:put(Db, TestH, <<"a">>, <<"x1">>, []),
    rocksdb:put(Db, TestH, <<"b">>, <<"y1">>, []),

    {ok, Txn} = rocksdb:transaction(Db, []),

    ok = rocksdb:transaction_put(Txn, <<"c">>, <<"v1">>),
    ok = rocksdb:transaction_put(Txn, TestH, <<"d">>, <<"v2">>),
    %% ok = rocksdb:transaction_put(Txn, TestH, <<"e">>, <<"v2">>),

    {ok, DefaultIt} = rocksdb:transaction_iterator(Txn, []),
    {ok, TestIt} = rocksdb:transaction_iterator(Txn, TestH, []),

    {ok, PlainIt} = rocksdb:iterator(Db, TestH, []),

    ?assertEqual({ok, <<"a">>, <<"x">>}, rocksdb:iterator_move(DefaultIt, <<>>)),
    ?assertEqual({ok, <<"a">>, <<"x1">>}, rocksdb:iterator_move(TestIt, <<>>)),
    ?assertEqual({ok, <<"a">>, <<"x1">>}, rocksdb:iterator_move(PlainIt, <<>>)),

    ?assertEqual({ok, <<"b">>, <<"y">>}, rocksdb:iterator_move(DefaultIt, next)),
    ?assertEqual({ok, <<"c">>, <<"v1">>}, rocksdb:iterator_move(DefaultIt, next)),

    ?assertEqual({ok, <<"b">>, <<"y1">>}, rocksdb:iterator_move(TestIt, next)),
    ?assertEqual({ok, <<"d">>, <<"v2">>}, rocksdb:iterator_move(TestIt, next)),


    ?assertEqual({ok, <<"b">>, <<"y1">>}, rocksdb:iterator_move(PlainIt, next)),
    ?assertEqual({error, invalid_iterator}, rocksdb:iterator_move(PlainIt, next)),

    ?assertEqual({ok, <<"b">>, <<"y">>}, rocksdb:iterator_move(DefaultIt, prev)),
    ?assertEqual({ok, <<"b">>, <<"y1">>}, rocksdb:iterator_move(TestIt, prev)),
    ok = rocksdb:iterator_close(TestIt),
    ok = rocksdb:iterator_close(PlainIt),
    ok = rocksdb:iterator_close(DefaultIt),

    rocksdb:transaction_commit(Txn),
    rocksdb:release_transaction(Txn),

    close_destroy(Db, "transaction_testdb").


rollback_test() ->
    Db = destroy_reopen("transaction_testdb", [{create_if_missing, true}]),

    {ok, Transaction} = rocksdb:transaction(Db, []),

    ok = rocksdb:transaction_put(Transaction, <<"a">>, <<"v1">>),
    ok = rocksdb:transaction_put(Transaction, <<"b">>, <<"v2">>),

    ok = rocksdb:transaction_commit(Transaction),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),

    %% Create a second transaction with changes that will be rolled back
    {ok, Transaction1} = rocksdb:transaction(Db, []),

    ok = rocksdb:transaction_put(Transaction1, <<"a">>, <<"v2">>),
    ok = rocksdb:transaction_put(Transaction1, <<"c">>, <<"v3">>),

    ok = rocksdb:transaction_delete(Transaction1, <<"b">>),
    ok = rocksdb:transaction_delete(Transaction1, <<"d">>),

    ok = rocksdb:transaction_rollback(Transaction1),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"c">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"d">>, [])),

    close_destroy(Db, "transaction_testdb"),
    ok.

get_for_update_test() ->
    Db = destroy_reopen("transaction_testdb", [{create_if_missing, true}]),

    %% Put some initial data
    ok = rocksdb:put(Db, <<"key1">>, <<"value1">>, []),
    ok = rocksdb:put(Db, <<"key2">>, <<"value2">>, []),

    %% Start a transaction
    {ok, Txn} = rocksdb:transaction(Db, []),

    %% Get with conflict tracking
    ?assertEqual({ok, <<"value1">>}, rocksdb:transaction_get_for_update(Txn, <<"key1">>, [])),
    ?assertEqual({ok, <<"value2">>}, rocksdb:transaction_get_for_update(Txn, <<"key2">>, [])),
    ?assertEqual(not_found, rocksdb:transaction_get_for_update(Txn, <<"key3">>, [])),

    %% Modify the value within the transaction
    ok = rocksdb:transaction_put(Txn, <<"key1">>, <<"modified1">>),

    %% Get the modified value
    ?assertEqual({ok, <<"modified1">>}, rocksdb:transaction_get_for_update(Txn, <<"key1">>, [])),

    ok = rocksdb:transaction_commit(Txn),

    %% Verify the committed value
    ?assertEqual({ok, <<"modified1">>}, rocksdb:get(Db, <<"key1">>, [])),

    ok = rocksdb:release_transaction(Txn),
    close_destroy(Db, "transaction_testdb"),
    ok.
