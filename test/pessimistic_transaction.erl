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

%% Basic savepoint test - set and rollback to savepoint
savepoint_basic_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    %% Put initial data
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1">>),

    %% Set savepoint
    ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),

    %% Put more data after savepoint
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"b">>, <<"v2">>),
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1_modified">>),

    %% Verify both values are visible in transaction
    ?assertEqual({ok, <<"v1_modified">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:pessimistic_transaction_get(Txn, <<"b">>, [])),

    %% Rollback to savepoint
    ok = rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn),

    %% After rollback: 'a' has original value, 'b' is gone
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:pessimistic_transaction_get(Txn, <<"b">>, [])),

    %% Commit and verify
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Multiple savepoints test
multiple_savepoints_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    %% First operation
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1">>),

    %% First savepoint
    ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),

    %% Second operation
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"b">>, <<"v2">>),

    %% Second savepoint
    ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),

    %% Third operation
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"c">>, <<"v3">>),

    %% All three should be visible
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:pessimistic_transaction_get(Txn, <<"b">>, [])),
    ?assertEqual({ok, <<"v3">>}, rocksdb:pessimistic_transaction_get(Txn, <<"c">>, [])),

    %% Rollback to second savepoint - removes 'c'
    ok = rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn),

    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:pessimistic_transaction_get(Txn, <<"b">>, [])),
    ?assertEqual(not_found, rocksdb:pessimistic_transaction_get(Txn, <<"c">>, [])),

    %% Rollback to first savepoint - removes 'b'
    ok = rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn),

    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:pessimistic_transaction_get(Txn, <<"b">>, [])),
    ?assertEqual(not_found, rocksdb:pessimistic_transaction_get(Txn, <<"c">>, [])),

    %% Commit and verify only 'a' is saved
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"b">>, [])),
    ?assertEqual(not_found, rocksdb:get(Db, <<"c">>, [])),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Pop savepoint test - discard savepoint without rollback
pop_savepoint_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1">>),

    %% Set savepoint
    ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),

    ok = rocksdb:pessimistic_transaction_put(Txn, <<"b">>, <<"v2">>),

    %% Pop savepoint (discard it without rolling back)
    ok = rocksdb:pessimistic_transaction_pop_savepoint(Txn),

    %% Both values should still be visible
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:pessimistic_transaction_get(Txn, <<"b">>, [])),

    %% Now rollback_to_savepoint should fail (no savepoint exists)
    ?assertMatch({error, _}, rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn)),

    %% Commit - both values should be saved
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
    ?assertEqual({ok, <<"v2">>}, rocksdb:get(Db, <<"b">>, [])),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Error case: rollback_to_savepoint without setting one
no_savepoint_error_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1">>),

    %% Try to rollback without a savepoint
    ?assertMatch({error, _}, rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn)),

    %% Try to pop without a savepoint
    ?assertMatch({error, _}, rocksdb:pessimistic_transaction_pop_savepoint(Txn)),

    %% Data should still be there
    ?assertEqual({ok, <<"v1">>}, rocksdb:pessimistic_transaction_get(Txn, <<"a">>, [])),

    ok = rocksdb:pessimistic_transaction_rollback(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Test transaction ID
get_id_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Txn1} = rocksdb:pessimistic_transaction(Db, []),
    {ok, Txn2} = rocksdb:pessimistic_transaction(Db, []),

    %% Each transaction should have a unique ID
    {ok, Id1} = rocksdb:pessimistic_transaction_get_id(Txn1),
    {ok, Id2} = rocksdb:pessimistic_transaction_get_id(Txn2),

    ?assert(is_integer(Id1)),
    ?assert(is_integer(Id2)),
    ?assertNotEqual(Id1, Id2),

    ok = rocksdb:pessimistic_transaction_rollback(Txn1),
    ok = rocksdb:pessimistic_transaction_rollback(Txn2),
    ok = rocksdb:release_pessimistic_transaction(Txn1),
    ok = rocksdb:release_pessimistic_transaction(Txn2),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Test get_waiting_txns - not waiting case
get_waiting_txns_not_waiting_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    %% When not waiting on anything, should return empty list
    {ok, Result} = rocksdb:pessimistic_transaction_get_waiting_txns(Txn),

    ?assert(is_map(Result)),
    ?assertMatch(#{waiting_txns := []}, Result),

    ok = rocksdb:pessimistic_transaction_rollback(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Test get_waiting_txns - with lock contention
%% This test spawns a process that will block trying to acquire a lock
get_waiting_txns_contention_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    %% Put initial data
    ok = rocksdb:put(Db, <<"key">>, <<"v1">>, []),

    %% Transaction 1 acquires lock
    {ok, Txn1} = rocksdb:pessimistic_transaction(Db, []),
    {ok, Id1} = rocksdb:pessimistic_transaction_get_id(Txn1),
    {ok, <<"v1">>} = rocksdb:pessimistic_transaction_get_for_update(Txn1, <<"key">>, []),

    %% Spawn a process that will try to get the same lock with a long timeout
    Self = self(),
    Pid = spawn(fun() ->
        {ok, Txn2} = rocksdb:pessimistic_transaction(Db, [{lock_timeout, 10000}]),
        Self ! {txn2_started, Txn2},
        %% This will block waiting for Txn1's lock
        Result = rocksdb:pessimistic_transaction_get_for_update(Txn2, <<"key">>, []),
        Self ! {txn2_result, Result, Txn2}
    end),

    %% Wait for Txn2 to start
    Txn2 = receive
        {txn2_started, T2} -> T2
    after 1000 ->
        error(timeout_waiting_for_txn2)
    end,

    %% Give some time for Txn2 to start waiting
    timer:sleep(100),

    %% Check waiting txns for Txn2 - should be waiting on Txn1
    {ok, WaitInfo} = rocksdb:pessimistic_transaction_get_waiting_txns(Txn2),

    ?assert(is_map(WaitInfo)),
    #{waiting_txns := WaitingTxns, key := WaitingKey} = WaitInfo,

    %% Should be waiting on Txn1
    ?assertEqual([Id1], WaitingTxns),
    ?assertEqual(<<"key">>, WaitingKey),

    %% Release Txn1's lock
    ok = rocksdb:pessimistic_transaction_commit(Txn1),
    ok = rocksdb:release_pessimistic_transaction(Txn1),

    %% Txn2 should now succeed
    receive
        {txn2_result, {ok, <<"v1">>}, _} -> ok;
        {txn2_result, Other, _} -> error({unexpected_result, Other})
    after 5000 ->
        exit(Pid, kill),
        error(timeout_waiting_for_txn2_result)
    end,

    %% Cleanup Txn2
    ok = rocksdb:pessimistic_transaction_rollback(Txn2),
    ok = rocksdb:release_pessimistic_transaction(Txn2),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.

%% Test multi_get - batch get without locks
multi_get_test() ->
    Db = destroy_reopen("pessimistic_tx_testdb", [{create_if_missing, true}]),

    %% Put some initial data
    ok = rocksdb:put(Db, <<"key1">>, <<"value1">>, []),
    ok = rocksdb:put(Db, <<"key2">>, <<"value2">>, []),
    ok = rocksdb:put(Db, <<"key3">>, <<"value3">>, []),

    %% Start a transaction and add some changes
    {ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

    %% Add a new key and modify an existing one
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"key4">>, <<"value4">>),
    ok = rocksdb:pessimistic_transaction_put(Txn, <<"key1">>, <<"modified1">>),

    %% Test multi_get - should see uncommitted changes within transaction
    Results = rocksdb:pessimistic_transaction_multi_get(Txn, [<<"key1">>, <<"key2">>, <<"key4">>, <<"key5">>], []),
    ?assertEqual([{ok, <<"modified1">>}, {ok, <<"value2">>}, {ok, <<"value4">>}, not_found], Results),

    %% Commit and verify
    ok = rocksdb:pessimistic_transaction_commit(Txn),
    ok = rocksdb:release_pessimistic_transaction(Txn),

    %% Verify via regular multi_get
    Results2 = rocksdb:multi_get(Db, [<<"key1">>, <<"key2">>, <<"key4">>], []),
    ?assertEqual([{ok, <<"modified1">>}, {ok, <<"value2">>}, {ok, <<"value4">>}], Results2),

    close_destroy(Db, "pessimistic_tx_testdb"),
    ok.
