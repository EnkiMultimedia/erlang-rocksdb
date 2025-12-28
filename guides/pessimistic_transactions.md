# Pessimistic Transactions

Pessimistic Transactions provide strict ACID guarantees with row-level locking, deadlock detection, and lock timeouts. They are ideal for high-contention workloads where multiple transactions frequently attempt to update the same keys.

## Pessimistic vs Optimistic Transactions

| Feature | Optimistic | Pessimistic |
|---------|------------|-------------|
| Locking | Validation at commit | Lock on write/GetForUpdate |
| Conflict handling | Retry on commit failure | Block/timeout on lock acquisition |
| Best for | Low contention workloads | High contention workloads |
| Deadlock | N/A | Detection & timeout |

## Opening a Pessimistic Transaction Database

```erlang
%% Basic open
{ok, Db, [DefaultCF]} = rocksdb:open_pessimistic_transaction_db(
    "my_db",
    [{create_if_missing, true}],
    [{"default", []}]
).

%% With transaction database options
Options = [
    {create_if_missing, true},
    {lock_timeout, 5000},        %% Lock wait timeout in ms (default 1000)
    {deadlock_detect, true},     %% Enable deadlock detection
    {max_num_locks, -1},         %% Max locks per CF (-1 = unlimited)
    {num_stripes, 16}            %% Lock table concurrency
],
{ok, Db, CFs} = rocksdb:open_pessimistic_transaction_db("my_db", Options, [{"default", []}]).
```

## Basic Operations

### Creating a Transaction

```erlang
%% Create a transaction with default options
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []).

%% Create with transaction-specific options
TxnOpts = [
    {set_snapshot, true},       %% Use snapshot for consistent reads
    {deadlock_detect, true},    %% Enable deadlock detection for this txn
    {lock_timeout, 2000}        %% Override default lock timeout (ms)
],
{ok, Txn} = rocksdb:pessimistic_transaction(Db, [], TxnOpts).
```

### Put, Get, Delete

```erlang
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

%% Put acquires a lock on the key
ok = rocksdb:pessimistic_transaction_put(Txn, <<"key1">>, <<"value1">>),

%% Get reads without acquiring a lock
{ok, Value} = rocksdb:pessimistic_transaction_get(Txn, <<"key1">>, []),

%% Delete acquires a lock on the key
ok = rocksdb:pessimistic_transaction_delete(Txn, <<"key1">>),

%% Commit the transaction
ok = rocksdb:pessimistic_transaction_commit(Txn),

%% Release resources
ok = rocksdb:release_pessimistic_transaction(Txn).
```

### Column Family Support

All operations support column families:

```erlang
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

%% Operations with column family
ok = rocksdb:pessimistic_transaction_put(Txn, CfHandle, <<"key">>, <<"value">>),
{ok, Value} = rocksdb:pessimistic_transaction_get(Txn, CfHandle, <<"key">>, []),
ok = rocksdb:pessimistic_transaction_delete(Txn, CfHandle, <<"key">>),

ok = rocksdb:pessimistic_transaction_commit(Txn).
```

## GetForUpdate - Exclusive Lock on Read

Use `get_for_update` to acquire an exclusive lock when reading a key. This prevents other transactions from modifying the key until your transaction commits or rolls back.

```erlang
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

%% Read and lock the key
{ok, Value} = rocksdb:pessimistic_transaction_get_for_update(Txn, <<"key">>, []),

%% Now we have an exclusive lock - other transactions will block/timeout
%% if they try to write or get_for_update on this key

%% Optionally modify the value
ok = rocksdb:pessimistic_transaction_put(Txn, <<"key">>, <<"new_value">>),

ok = rocksdb:pessimistic_transaction_commit(Txn).
```

## Rollback

Discard all changes made in the transaction:

```erlang
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

ok = rocksdb:pessimistic_transaction_put(Txn, <<"key">>, <<"value">>),

%% Changed our mind - rollback
ok = rocksdb:pessimistic_transaction_rollback(Txn),

%% Always release the transaction
ok = rocksdb:release_pessimistic_transaction(Txn).
```

## Savepoints

Savepoints allow you to mark a point in the transaction that you can roll back to without rolling back the entire transaction.

```erlang
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

%% First operation
ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1">>),

%% Set a savepoint
ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),

%% More operations after savepoint
ok = rocksdb:pessimistic_transaction_put(Txn, <<"b">>, <<"v2">>),
ok = rocksdb:pessimistic_transaction_put(Txn, <<"c">>, <<"v3">>),

%% Rollback to savepoint - undoes b and c, keeps a
ok = rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn),

%% Commit - only 'a' will be saved
ok = rocksdb:pessimistic_transaction_commit(Txn).
```

### Nested Savepoints

Multiple savepoints can be nested:

```erlang
ok = rocksdb:pessimistic_transaction_put(Txn, <<"a">>, <<"v1">>),
ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),     %% Savepoint 1

ok = rocksdb:pessimistic_transaction_put(Txn, <<"b">>, <<"v2">>),
ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),     %% Savepoint 2

ok = rocksdb:pessimistic_transaction_put(Txn, <<"c">>, <<"v3">>),

%% Rollback to savepoint 2 - undoes 'c'
ok = rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn),

%% Rollback to savepoint 1 - undoes 'b'
ok = rocksdb:pessimistic_transaction_rollback_to_savepoint(Txn).
```

### Pop Savepoint

Discard a savepoint without rolling back:

```erlang
ok = rocksdb:pessimistic_transaction_set_savepoint(Txn),
ok = rocksdb:pessimistic_transaction_put(Txn, <<"key">>, <<"value">>),

%% Discard the savepoint - changes are kept
ok = rocksdb:pessimistic_transaction_pop_savepoint(Txn).
```

## Iterators

Create an iterator that sees uncommitted changes in the transaction:

```erlang
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),

%% Add uncommitted data
ok = rocksdb:pessimistic_transaction_put(Txn, <<"c">>, <<"v3">>),

%% Create iterator - sees both committed and uncommitted data
{ok, Iter} = rocksdb:pessimistic_transaction_iterator(Txn, []),

%% Use standard iterator operations
{ok, Key, Value} = rocksdb:iterator_move(Iter, first),
{ok, NextKey, NextValue} = rocksdb:iterator_move(Iter, next),

ok = rocksdb:iterator_close(Iter),
ok = rocksdb:pessimistic_transaction_commit(Txn).
```

With column family:

```erlang
{ok, Iter} = rocksdb:pessimistic_transaction_iterator(Txn, CfHandle, []).
```

## Transaction Introspection

### Get Transaction ID

Each transaction has a unique ID:

```erlang
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),
{ok, TxnId} = rocksdb:pessimistic_transaction_get_id(Txn).
%% TxnId is a non-negative integer
```

### Get Waiting Transactions

Find out what transactions are blocking the current transaction:

```erlang
{ok, WaitInfo} = rocksdb:pessimistic_transaction_get_waiting_txns(Txn).
%% Returns:
%% #{column_family_id => 0,
%%   key => <<"locked_key">>,
%%   waiting_txns => [TxnId1, TxnId2, ...]}
```

This is useful for debugging lock contention or implementing custom monitoring.

## Lock Timeout and Deadlock Detection

### Lock Timeout

When a transaction tries to acquire a lock held by another transaction, it will wait up to the lock timeout:

```erlang
%% Transaction 1 acquires a lock
{ok, Txn1} = rocksdb:pessimistic_transaction(Db, []),
{ok, _} = rocksdb:pessimistic_transaction_get_for_update(Txn1, <<"key">>, []),

%% Transaction 2 tries to lock the same key with short timeout
{ok, Txn2} = rocksdb:pessimistic_transaction(Db, [], [{lock_timeout, 100}]),
Result = rocksdb:pessimistic_transaction_get_for_update(Txn2, <<"key">>, []),
%% Result will be {error, {timed_out, _}} after 100ms
```

### Deadlock Detection

Enable deadlock detection to automatically detect and break deadlocks:

```erlang
Options = [{deadlock_detect, true}],
{ok, Db, _} = rocksdb:open_pessimistic_transaction_db("db", Options, [{"default", []}]).
```

When a deadlock is detected, one of the transactions will receive a `busy` or `timed_out` error.

## Error Handling

Pessimistic transactions can return specific errors:

- `{error, {busy, Reason}}` - Write conflict or lock contention
- `{error, {timed_out, Reason}}` - Lock acquisition timed out
- `{error, {expired, Reason}}` - Transaction expired
- `{error, {try_again, Reason}}` - Transient error, retry the operation

Example error handling:

```erlang
case rocksdb:pessimistic_transaction_get_for_update(Txn, Key, []) of
    {ok, Value} ->
        %% Success
        process_value(Value);
    not_found ->
        %% Key doesn't exist
        handle_not_found();
    {error, {timed_out, _}} ->
        %% Lock timeout - another transaction holds the lock
        handle_timeout();
    {error, {busy, _}} ->
        %% Write conflict
        handle_conflict()
end.
```

## Complete Example

```erlang
transfer_funds(Db, FromAccount, ToAccount, Amount) ->
    {ok, Txn} = rocksdb:pessimistic_transaction(Db, [], [{deadlock_detect, true}]),
    try
        %% Lock both accounts
        {ok, FromBalance} = rocksdb:pessimistic_transaction_get_for_update(
            Txn, FromAccount, []),
        {ok, ToBalance} = rocksdb:pessimistic_transaction_get_for_update(
            Txn, ToAccount, []),

        FromBalanceInt = binary_to_integer(FromBalance),
        ToBalanceInt = binary_to_integer(ToBalance),

        case FromBalanceInt >= Amount of
            true ->
                NewFrom = integer_to_binary(FromBalanceInt - Amount),
                NewTo = integer_to_binary(ToBalanceInt + Amount),

                ok = rocksdb:pessimistic_transaction_put(Txn, FromAccount, NewFrom),
                ok = rocksdb:pessimistic_transaction_put(Txn, ToAccount, NewTo),
                ok = rocksdb:pessimistic_transaction_commit(Txn),
                ok;
            false ->
                rocksdb:pessimistic_transaction_rollback(Txn),
                {error, insufficient_funds}
        end
    catch
        _:Error ->
            rocksdb:pessimistic_transaction_rollback(Txn),
            {error, Error}
    after
        rocksdb:release_pessimistic_transaction(Txn)
    end.
```
