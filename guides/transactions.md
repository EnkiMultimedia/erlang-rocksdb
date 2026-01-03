# Optimistic Transactions

Optimistic Transactions provide light-weight optimistic concurrency control for workloads that do not expect high contention between multiple transactions.

## Optimistic vs Pessimistic Transactions

| Feature | Optimistic | Pessimistic |
|---------|------------|-------------|
| Locking | Validation at commit | Lock on write/GetForUpdate |
| Conflict handling | Retry on commit failure | Block/timeout on lock acquisition |
| Best for | Low contention workloads | High contention workloads |
| Deadlock | N/A | Detection & timeout |
| Performance | Better for read-heavy | Better for write-heavy |

Choose Optimistic Transactions when:
- Write conflicts are rare
- You have many non-transactional writes alongside transactions
- You prefer retry logic over blocking

Choose [Pessimistic Transactions](pessimistic_transactions.md) when:
- Multiple transactions frequently update the same keys
- You need strict locking guarantees
- You want automatic deadlock detection

## Opening an Optimistic Transaction Database

```erlang
%% Basic open
{ok, Db, [DefaultCF]} = rocksdb:open_optimistic_transaction_db(
    "my_db",
    [{create_if_missing, true}],
    [{"default", []}]
).

%% With column families
CfOpts = [],
{ok, Db, [DefaultCF, DataCF]} = rocksdb:open_optimistic_transaction_db(
    "my_db",
    [{create_if_missing, true}],
    [{"default", CfOpts}, {"data", CfOpts}]
).
```

## Basic Operations

### Creating a Transaction

```erlang
%% Create a transaction with default options
{ok, Txn} = rocksdb:transaction(Db, []).

%% Create with write options
WriteOpts = [{sync, true}],
{ok, Txn} = rocksdb:transaction(Db, WriteOpts).
```

### Put, Get, Delete

```erlang
{ok, Txn} = rocksdb:transaction(Db, []),

%% Put a key-value pair
ok = rocksdb:transaction_put(Txn, <<"key1">>, <<"value1">>),

%% Get a value
{ok, Value} = rocksdb:transaction_get(Txn, <<"key1">>, []),

%% Delete a key
ok = rocksdb:transaction_delete(Txn, <<"key1">>),

%% Commit the transaction
ok = rocksdb:transaction_commit(Txn).
```

### Column Family Support

All operations support column families:

```erlang
{ok, Txn} = rocksdb:transaction(Db, []),

%% Operations with column family
ok = rocksdb:transaction_put(Txn, CfHandle, <<"key">>, <<"value">>),
{ok, Value} = rocksdb:transaction_get(Txn, CfHandle, <<"key">>, []),
ok = rocksdb:transaction_delete(Txn, CfHandle, <<"key">>),

ok = rocksdb:transaction_commit(Txn).
```

## Reading Uncommitted Data

Transactions can read their own uncommitted changes:

```erlang
%% Write to database first
ok = rocksdb:put(Db, <<"a">>, <<"old_a">>, []),
ok = rocksdb:put(Db, <<"b">>, <<"old_b">>, []),

%% Start transaction
{ok, Txn} = rocksdb:transaction(Db, []),

%% Modify within transaction
ok = rocksdb:transaction_put(Txn, <<"a">>, <<"new_a">>),

%% Read sees uncommitted change
{ok, <<"new_a">>} = rocksdb:transaction_get(Txn, <<"a">>, []),

%% Read sees committed value for unmodified key
{ok, <<"old_b">>} = rocksdb:transaction_get(Txn, <<"b">>, []),

ok = rocksdb:transaction_commit(Txn).
```

## Iterators

Create an iterator that sees uncommitted changes in the transaction:

```erlang
{ok, Txn} = rocksdb:transaction(Db, []),

%% Add uncommitted data
ok = rocksdb:transaction_put(Txn, <<"key1">>, <<"value1">>),
ok = rocksdb:transaction_put(Txn, <<"key2">>, <<"value2">>),

%% Create iterator - sees both committed and uncommitted data
{ok, Iter} = rocksdb:transaction_iterator(Txn, []),

%% Use standard iterator operations
{ok, Key, Value} = rocksdb:iterator_move(Iter, first),
{ok, NextKey, NextValue} = rocksdb:iterator_move(Iter, next),

ok = rocksdb:iterator_close(Iter),
ok = rocksdb:transaction_commit(Txn).
```

With column family:

```erlang
{ok, Iter} = rocksdb:transaction_iterator(Txn, CfHandle, []).
```

## Conflict Detection and Retry

Optimistic transactions validate at commit time. If another transaction modified the same keys, commit will fail:

```erlang
retry_transaction(Db, Key, UpdateFun) ->
    retry_transaction(Db, Key, UpdateFun, 3).

retry_transaction(_Db, _Key, _UpdateFun, 0) ->
    {error, max_retries};
retry_transaction(Db, Key, UpdateFun, Retries) ->
    {ok, Txn} = rocksdb:transaction(Db, []),
    try
        case rocksdb:transaction_get(Txn, Key, []) of
            {ok, Value} ->
                NewValue = UpdateFun(Value),
                ok = rocksdb:transaction_put(Txn, Key, NewValue),
                case rocksdb:transaction_commit(Txn) of
                    ok -> ok;
                    {error, {busy, _}} ->
                        %% Conflict detected, retry
                        retry_transaction(Db, Key, UpdateFun, Retries - 1)
                end;
            not_found ->
                {error, not_found}
        end
    catch
        _:Error ->
            {error, Error}
    end.
```

## Error Handling

Optimistic transactions can return these errors on commit:

- `{error, {busy, Reason}}` - Write conflict detected at commit time
- `{error, {try_again, Reason}}` - Transient error, retry the operation

```erlang
case rocksdb:transaction_commit(Txn) of
    ok ->
        %% Success
        ok;
    {error, {busy, _}} ->
        %% Another transaction modified the same keys
        %% Rollback and retry with fresh data
        handle_conflict();
    {error, Reason} ->
        %% Other error
        {error, Reason}
end.
```

## Complete Example

```erlang
increment_counter(Db, CounterKey) ->
    {ok, Txn} = rocksdb:transaction(Db, []),
    try
        Value = case rocksdb:transaction_get(Txn, CounterKey, []) of
            {ok, V} -> binary_to_integer(V);
            not_found -> 0
        end,
        NewValue = integer_to_binary(Value + 1),
        ok = rocksdb:transaction_put(Txn, CounterKey, NewValue),
        case rocksdb:transaction_commit(Txn) of
            ok ->
                {ok, Value + 1};
            {error, {busy, _}} ->
                %% Conflict - retry
                increment_counter(Db, CounterKey)
        end
    catch
        _:Error ->
            {error, Error}
    end.
```
