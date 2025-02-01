The Erlang Rocksdb binding provides access to most of the key/value API of the rocksdb library. 

## Installation

Download the sources from our [Gitlab repository](https://gitlab.com/barrel-db/erlang-rocksdb)

To build the application simply run 'rebar3 compile'. 

> Note: since the version **0.26.0**, `cmake>=3.4` is required to install `erlang-rocksdb`.

To run tests run 'rebar3 eunit'. To generate doc, run 'rebar3 edoc'.

Or add it to your rebar config


```erlang
{deps, [
    ....
    {rocksdb, {rocksdb, "1.2.0"}}
]}.
```

Or your mix config file:

```
{:rocksdb, "~> 1.0"}
```

## Basic operations


### Open a database

```erlang

Path = "/tmp/erocksdb.fold.test",
Options = [{create_if_missing, true}],
{ok, DB} = rocksdb:open(Path, Options).

```

This will create a nif resource. 

> Makes sure that the process that opened it will stay open across or the session (or that at least one process own it) to prevent any garbage collection of it which will close the database and kill all the resources related (iterators, column families) ...


### Closing a database

When you are done with a database run the following function

```erlang
ok = rocksdb:close(DB).
```

> the database will be automatically closed if no process own it.

### Read and Writes

The library provides put, delete, and get methods to modify/query the database. For example, the following code moves the value stored under key1 to key2.

```erlang
rocksdb:put(Db, <<"my key">>, <<"my value">>),
case rocksdb:get(Db, <<"my key">>, []) of
  {ok, Value} => io:format("retrieved value %p~n", [Value]);
  not_found => io:format("value not found~n", []);
  Error -> io:format("operational problem encountered: %p~n", [Error])
end,
rocksdb:delete(Db, <<"my key">>).
```

### Atomic Updates

Note that if the process dies after the Put of key2 but before the delete of key1, the same value may be left stored under multiple keys. Such problems can be avoided by using the function `rocksdb:write/3` class to atomically apply a set of updates:

```erlang
Batch = [
  {put, <<"key1">>, <<"value1">>},
  {delete, <<"keytodelete">>},
  ...
],
rocksdb:write(DB, Batch, [])
```

You can also use the [Batch API](Batch-API) that gives you more control about the atomic transactions.

### Synchronous writes

Using the setting `{sync, true}` in any write operations will makes sure to store synchronously on the filesystem your data. 


### Iterate your data

You can start an iterator using the function `rocksdb:iterator/2` :

```erlang
ItrOptions = [],
{ok, Itr} = iterator(DB, ItrOptions)
```

You close the iterator using the function `rocksdb:iterator_close/1`:
```erlang
rocksdb:iterator_close(Itr)
```

then you can move to the previous or next data using the function `iterator:move/2` :

* move to the next key: `rocksdb:iterator_move(Itr, next)`
* move to the previous key: `rocksdb:iterator_move(Itr, prev)`
* start at the first key: `rocksdb:iterator_move(Itr, first)`
* start at the last key: `rocksdb:iterator_move(Itr, last)`
* move to a key: `rocksdb:iterator_move(Itr, Key)` or  `rocksdb:iterator_move(Itr,{seek, Key})`
* move to the previous key of: `rocksdb:iterator_move(Itr,{seek_for_prev, Key})`

> An iterator doesn't support parallel operations, so make sure to use it accordingly.

Prefix seek can be optimized by putting your database in "prefix seek" mode using  the option `prefix_extractor` for your DB or column family is specified. See the [Prefix Seek](prefix-seek) API for more information.

### Snapshots

Snapshots provide consistent read-only views over the entire state of the key-value store.The `{snapshot, Snapshot}` can be need to be set in the read options to indicate that a read should operate on a particular version of the DB state.


```erlang
{ok, Snapshot} = rocksdb:snapshot(Db),
ReadOptions = [{snapshot, Snapshot}],
{ok, Itr} = rocksdb:iterator(DB, ReadOptions),
... read using iter to view the state when the snapshot was created ...
rocksdb:iterator_close(Itr),
rocksdb:release_snapshot(Snapshot)
```

> Note that when a snapshot is no longer needed, it should be released using the `rocksdb:release_snapshot/1` function. This allows the implementation to get rid of state that was being maintained just to support reading as of that snapshot.


### Column Families

[Column Families](column_families) provide a way to logically partition the database. Users can provide atomic writes of multiple keys across multiple column families and read a consistent view from them.

### Backup and Checkpoint

[Backup](how_to_backup_rocksdb) allows users to create periodic incremental backups in a remote file system (think about HDFS or S3) and recover from any of them.

[Checkpoints](Checkpoints) provides the ability to take a snapshot of a running RocksDB database in a separate directory. Files are hardlinked, rather than copied, if possible, so it is a relatively lightweight operation.

### Merge Operator

Starting in version 0.21.0, Erlang Rocksdb supports a [Merge Operator](Erlang-Merge-Operator) for Erlang data types. A [Bitset Merge Operator](bitset_merge_operator) and a [Counter Merge Operator](counter_merge_operator) are also available to change a bit at a position in a string or maintain a simple counter.

### Transactions

Erlang RocksDB has preliminary support of multi-operation transactions. See [Transactions](transactions)