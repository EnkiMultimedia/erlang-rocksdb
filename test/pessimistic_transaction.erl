-module(pessimistic_transaction).

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").

destroy_reopen(DbName, Options) ->
    _ = rocksdb:destroy(DbName, []),
    _ = rocksdb_test_util:rm_rf(DbName),
    {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(DbName, Options, [{"default", []}]),
    Db.

close_destroy(Db, DbName) ->
    rocksdb:close(Db),
    rocksdb:destroy(DbName, []),
    rocksdb_test_util:rm_rf(DbName).

%% Basic CRUD operations test
basic_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Transaction} = rocksdb:pessimistic_transaction(Db, []),

    ok = rocksdb:pessimistic_transaction_put(Transaction, <<"a">>, <<"v1">>),
    ok = rocksdb:pessimistic_transaction_put(Transaction, <<"b">>, <<"v2">>),

    %% Data not visible outside transaction before commit
    ?assertEqual(not_found, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),

    %% Data visible inside transaction
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Transaction, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:pessimistic_transaction_get(Transaction, <<"b">>, [])),

    ok = rocksdb:pessimistic_transaction_commit(Transaction),

    %% Data visible after commit
    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),

    ok = rocksdb:release_pessimistic_transaction(Transaction),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Delete operation test
delete_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Transaction} = rocksdb:pessimistic_transaction(Db, []),

    ok = rocksdb:pessimistic_transaction_put(Transaction, <<"a">>, <<"v1">>),
    ok = rocksdb:pessimistic_transaction_put(Transaction, <<"b">>, <<"v2">>),
    ok = rocksdb:pessimistic_transaction_delete(Transaction, <<"b">>),
    ok = rocksdb:pessimistic_transaction_delete(Transaction, <<"c">>),

    ?assertEqual(not_found, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),

    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Transaction, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:pessimistic_transaction_get(Transaction, <<"b">>, [])),

    ok = rocksdb:pessimistic_transaction_commit(Transaction),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),

    ok = rocksdb:release_pessimistic_transaction(Transaction),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Rollback test
rollback_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    %% First transaction - commit some data
    {ok, Transaction} = rocksdb:pessimistic_transaction(Db, []),

    ok = rocksdb:pessimistic_transaction_put(Transaction, <<"a">>, <<"v1">>),
    ok = rocksdb:pessimistic_transaction_put(Transaction, <<"b">>, <<"v2">>),

    ok = rocksdb:pessimistic_transaction_commit(Transaction),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),

    ok = rocksdb:release_pessimistic_transaction(Transaction),

    %% Second transaction - make changes then rollback
    {ok, Transaction1} = rocksdb:pessimistic_transaction(Db, []),

    ok = rocksdb:pessimistic_transaction_put(Transaction1, <<"a">>, <<"v2">>),
    ok = rocksdb:pessimistic_transaction_put(Transaction1, <<"c">>, <<"v3">>),

    ok = rocksdb:pessimistic_transaction_delete(Transaction1, <<"b">>),

    ok = rocksdb:pessimistic_transaction_rollback(Transaction1),

    %% Original values should remain
    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"c">>, [])),

    ok = rocksdb:release_pessimistic_transaction(Transaction1),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% GetForUpdate test - acquire exclusive lock on read
get_for_update_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    %% Put some initial data
    ok = rocksdb:put(Db, <<"a">>, <<"v1">>, []),

    {ok, Transaction} = rocksdb:pessimistic_transaction(Db, []),

    %% GetForUpdate acquires a lock on the key
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get_for_update(Transaction, <<"a">>, [])),

    %% Can still read with regular get
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Transaction, <<"a">>, [])),

    %% Update the value
    ok = rocksdb:pessimistic_transaction_put(Transaction, <<"a">>, <<"v2">>),

    %% New value visible in transaction
    ?assertEqual({ok, <<"v2">>}, rocksdb:pessimistic_transaction_get(Transaction, <<"a">>, [])),

    ok = rocksdb:pessimistic_transaction_commit(Transaction),

    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"a">>, [])),

    ok = rocksdb:release_pessimistic_transaction(Transaction),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Lock timeout test
lock_timeout_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    %% Put some initial data
    ok = rocksdb:put(Db, <<"a">>, <<"v1">>, []),

    %% Transaction 1 acquires lock via GetForUpdate
    {ok, Txn1} = rocksdb:pessimistic_transaction(Db, []),
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get_for_update(Txn1, <<"a">>, [])),

    %% Transaction 2 tries to lock the same key with short timeout
    {ok, Txn2} = rocksdb:pessimistic_transaction(Db, [{lock_timeout, 100}]),
    Result = rocksdb:pessimistic_transaction_get_for_update(Txn2, <<"a">>, []),
    ?assertMatch({error, _}, Result),

    ok = rocksdb:pessimistic_transaction_rollback(Txn1),
    ok = rocksdb:pessimistic_transaction_rollback(Txn2),
    ok = rocksdb:release_pessimistic_transaction(Txn1),
    ok = rocksdb:release_pessimistic_transaction(Txn2),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Column family test
column_family_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),
    {ok, TestH} = rocksdb:create_column_family(Db, "test", []),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    %% Put in default and test column families
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1">>),
    ok = rocksdb:pessimistic_transaction_put(Txn, TestH, <<"a">>, <<"cf_v1">>),

    %% Values are different per column family
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),
    ?assertEqual({ok, <<"cf_v1">>}, rocksdb:pessimistic_transaction_get(Txn, TestH, <<"a">>, [])),

    ok = rocksdb:pessimistic_transaction_commit(Txn),

    %% Verify after commit
    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"cf_v1">>}, rocksdb:get(Db, TestH, <<"a">>, [])),

    ok = rocksdb:release_pessimistic_transaction(Txn),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Iterator test
iterator_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    %% Put some initial data
    ok = rocksdb:put(Db, <<"a">>, <<"v1">>, []),
    ok = rocksdb:put(Db, <<"b">>, <<"v2">>, []),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    %% Add data in transaction
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"c">>, <<"v3">>),

    {ok, It} = rocksdb:pessimistic_transaction_iterator(Txn, []),

    %% Iterator should see both committed and uncommitted data
    ?assertEqual({ok, <<"a">>, <<"v1">>}, rocksdb:iterator_move(It, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"v2">>}, rocksdb:iterator_move(It, next)),
    ?assertEqual({ok, <<"c">>, <<"v3">>}, rocksdb:iterator_move(It, next)),
    ?assertEqual({error, invalid_iterator}, rocksdb:iterator_move(It, next)),

    ok = rocksdb:iterator_close(It),

    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Iterator with column family test
cf_iterator_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),
    {ok, TestH} = rocksdb:create_column_family(Db, "test", []),

    ok = rocksdb:put(Db, <<"a">>, <<"v1">>, []),
    ok = rocksdb:put(Db, TestH, <<"x">>, <<"cf_v1">>, []),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    ok = rocksdb:pessimistic_transaction_put(Txn, <<"b">>, <<"v2">>),
    ok = rocksdb:pessimistic_transaction_put(Txn, TestH, <<"y">>, <<"cf_v2">>),

    {ok, DefaultIt} = rocksdb:pessimistic_transaction_iterator(Txn, []),
    {ok, TestIt} = rocksdb:pessimistic_transaction_iterator(Txn, TestH, []),

    %% Default CF iterator
    ?assertEqual({ok, <<"a">>, <<"v1">>}, rocksdb:iterator_move(DefaultIt, <<>>)),
    ?assertEqual({ok, <<"b">>, <<"v2">>}, rocksdb:iterator_move(DefaultIt, next)),

    %% Test CF iterator
    ?assertEqual({ok, <<"x">>, <<"cf_v1">>}, rocksdb:iterator_move(TestIt, <<>>)),
    ?assertEqual({ok, <<"y">>, <<"cf_v2">>}, rocksdb:iterator_move(TestIt, next)),

    ok = rocksdb:iterator_close(DefaultIt),
    ok = rocksdb:iterator_close(TestIt),

    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Test DB options (lock_timeout, deadlock_detect)
db_options_test() ->
    DbName = "pessimistic_tx_testdb",
    _ = rocksdb:destroy(DbName, []),
    _ = rocksdb_test_util:rm_rf(DbName),

    %% Open with custom TransactionDB options
    DbOpts = [{create_if_missing, true}],
    TxnDbOpts = [{lock_timeout, 5000}, {deadlock_detect, true}, {num_stripes, 8}],
    {ok, Db, _} = rocksdb:open_pessimistic_transaction_db(DbName, DbOpts ++ TxnDbOpts, [{"default", []}]),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"test">>, <<"value">>),
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    ?assertEqual({ok, <<"value">>}, rocksdb:get(Db, <<"test">>, [])),

    close_destroy(Db, DbName),
    ok.

%% Test transaction options (set_snapshot, deadlock_detect, lock_timeout)
txn_options_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    %% Put some initial data
    ok = rocksdb:put(Db, <<"a">>, <<"v1">>, []),

    %% Create transaction with options
    TxnOpts = [{set_snapshot, true}, {deadlock_detect, true}, {lock_timeout, 1000}],
    {ok, Txn} = rocksdb:pessimistic_transaction(Db, TxnOpts),

    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),

    ok = rocksdb:pessimistic_transaction_put(Txn, <<"b">>, <<"v2">>),
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.
