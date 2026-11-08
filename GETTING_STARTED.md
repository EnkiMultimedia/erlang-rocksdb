The Erlang Rocksdb binding provides access to most of the key/value API of the rocksdb library. 

## Installation

Download the sources from our [Github repository](https://github.com/EnkiMultimedia/erlang-rocksdb)

To build the application simply run 'rebar3 compile'. 

> Note: `cmake>=3.12` is required to install `erlang-rocksdb`.

To run tests run 'rebar3 eunit'. To generate doc, run 'rebar3 ex_doc'.

Or add it to your rebar config


```erlang
{deps, [
    ....
    {rocksdb, "3.2.1"}
]}.
```

Or your mix config file:

```
{:rocksdb, "~> 3.1"}
```

## Basic operations


### Open a database

```erlang

Path = "/tmp/erocksdb.fold.test",
Options = [{create_if_missing, true}],
{ok, DB} = rocksdb:open(Path, Options).

```

This gives you a nif resource, the database handle.

> Keep the handle reachable for as long as you need the database: hold it in a live process, in ETS or in `persistent_term`. Once nothing references it any more, it is garbage collected, which closes the database and everything tied to it (iterators, column families). See [OTP Integration](otp_integration.md) for where to keep it in an application.


### Closing a database

When you are done with a database run the following function

```erlang
ok = rocksdb:close(DB).
```

> The database is also closed for you once nothing holds the handle any more.

### Read and Writes

You modify and query the database with `put`, `get` and `delete`. Each of them takes read or write options as its last argument, so pass `[]` when you have none. This writes a key, reads it back, then deletes it:

```erlang
ok = rocksdb:put(Db, <<"my key">>, <<"my value">>, []),
case rocksdb:get(Db, <<"my key">>, []) of
  {ok, Value} -> io:format("retrieved value ~p~n", [Value]);
  not_found -> io:format("value not found~n", []);
  Error -> io:format("operational problem encountered: ~p~n", [Error])
end,
ok = rocksdb:delete(Db, <<"my key">>, []).
```

### Batch Reads with multi_get

When you need to retrieve multiple keys at once, `multi_get/3` is more efficient than calling `get/3` multiple times. It retrieves all keys in a single operation:

```erlang
%% Insert some data
ok = rocksdb:put(Db, <<"key1">>, <<"value1">>, []),
ok = rocksdb:put(Db, <<"key2">>, <<"value2">>, []),
ok = rocksdb:put(Db, <<"key3">>, <<"value3">>, []),

%% Retrieve multiple keys at once
Keys = [<<"key1">>, <<"key2">>, <<"key3">>, <<"missing">>],
Results = rocksdb:multi_get(Db, Keys, []),
%% Results = [{ok, <<"value1">>}, {ok, <<"value2">>}, {ok, <<"value3">>}, not_found]
```

The results are returned in the same order as the input keys. Each result is either:
- `{ok, Value}` - the key was found
- `not_found` - the key does not exist
- `{error, Reason}` - an error occurred

For column families, use `multi_get/4`:

```erlang
Results = rocksdb:multi_get(Db, ColumnFamily, Keys, []).
```

You can also use snapshots with `multi_get` to get a consistent view:

```erlang
{ok, Snapshot} = rocksdb:snapshot(Db),
Results = rocksdb:multi_get(Db, Keys, [{snapshot, Snapshot}]),
rocksdb:release_snapshot(Snapshot).
```

### Atomic Updates

If your process dies between two writes, you can be left with the same value stored under several keys. Collect the writes in a batch and apply them in one atomic operation instead:

```erlang
{ok, Batch} = rocksdb:batch(),
ok = rocksdb:batch_put(Batch, <<"key1">>, <<"value1">>),
ok = rocksdb:batch_delete(Batch, <<"keytodelete">>),
ok = rocksdb:write_batch(Db, Batch, []),
ok = rocksdb:release_batch(Batch).
```

See the [Batch API](batch_api.md) guide for savepoints, rollbacks and the other batch operations.

### Synchronous writes

Using the setting `{sync, true}` in any write operations will makes sure to store synchronously on the filesystem your data. 


### Iterate your data

You can start an iterator using the function `rocksdb:iterator/2` :

```erlang
ItrOptions = [],
{ok, Itr} = rocksdb:iterator(DB, ItrOptions)
```

You close the iterator using the function `rocksdb:iterator_close/1`:
```erlang
rocksdb:iterator_close(Itr)
```

You move the iterator with `rocksdb:iterator_move/2`:

* move to the next key: `rocksdb:iterator_move(Itr, next)`
* move to the previous key: `rocksdb:iterator_move(Itr, prev)`
* start at the first key: `rocksdb:iterator_move(Itr, first)`
* start at the last key: `rocksdb:iterator_move(Itr, last)`
* move to a key: `rocksdb:iterator_move(Itr, Key)` or  `rocksdb:iterator_move(Itr,{seek, Key})`
* move to the previous key of: `rocksdb:iterator_move(Itr,{seek_for_prev, Key})`

> An iterator doesn't support parallel operations, so make sure to use it accordingly.

You can optimize prefix seeks by putting your database in "prefix seek" mode with the `prefix_extractor` option on the database or the column family. See [Prefix Seek](prefix_seek.html) for more information.

### Wide-Column Entities

Starting in version 2.0.0, Erlang RocksDB supports RocksDB's Wide-Column Entity API. Entities allow storing structured data with multiple named columns per key, providing a more flexible schema than simple key-value pairs.

```erlang
%% Store an entity with multiple columns
Key = <<"user:123">>,
Columns = [
    {<<"name">>, <<"Alice">>},
    {<<"email">>, <<"alice@example.com">>},
    {<<"age">>, <<"30">>}
],
ok = rocksdb:put_entity(Db, Key, Columns, []),

%% Retrieve entity columns as a proplist
{ok, Result} = rocksdb:get_entity(Db, Key, []),
Name = proplists:get_value(<<"name">>, Result),  %% <<"Alice">>

%% Delete entity (same as regular delete)
ok = rocksdb:delete_entity(Db, Key, []).
```

When iterating, you can access columns for any entry:

```erlang
{ok, Itr} = rocksdb:iterator(Db, []),
{ok, Key, _Value} = rocksdb:iterator_move(Itr, first),
{ok, Columns} = rocksdb:iterator_columns(Itr),
%% For regular key-values, returns [{<<>>, Value}] (single default column)
%% For entities, returns all stored columns
rocksdb:iterator_close(Itr).
```

See [Wide-Column Entities](wide_column_entities.html) for more details.

### Snapshots

Snapshots give you a consistent read-only view over the entire state of the key-value store. Set `{snapshot, Snapshot}` in the read options to make a read operate on that version of the database state.


```erlang
{ok, Snapshot} = rocksdb:snapshot(Db),
ReadOptions = [{snapshot, Snapshot}],
{ok, Itr} = rocksdb:iterator(Db, ReadOptions),
... read using iter to view the state when the snapshot was created ...
rocksdb:iterator_close(Itr),
rocksdb:release_snapshot(Snapshot)
```

> Note that when a snapshot is no longer needed, it should be released using the `rocksdb:release_snapshot/1` function. This allows the implementation to get rid of state that was being maintained just to support reading as of that snapshot.


### Column Families

[Column Families](column_families.html) provide a way to logically partition the database. Users can provide atomic writes of multiple keys across multiple column families and read a consistent view from them.

### Backup and Checkpoint

[Backup](how_to_backup_rocksdb.html) allows users to create periodic incremental backups in a remote file system (think about HDFS or S3) and recover from any of them.

[Checkpoints](checkpoints.html) provides the ability to take a snapshot of a running RocksDB database in a separate directory. Files are hardlinked, rather than copied, if possible, so it is a relatively lightweight operation.

### Merge Operator

Starting in version 0.21.0, Erlang Rocksdb supports a [Merge Operator](erlang_merge_operator.html) for Erlang data types. A [Bitset Merge Operator](bitset_merge_operator.html) and a [Counter Merge Operator](counter_merge_operator.html) are also available to change a bit at a position in a string or maintain a simple counter.

### Transactions

Erlang RocksDB has preliminary support of multi-operation transactions. See [Transactions](transactions.html)

### SST File Operations

You can work directly with SST (Sorted String Table) files, the storage format RocksDB uses, for bulk data loading, offline inspection and data migration.

#### Creating SST Files

Use `sst_file_writer` to create SST files outside the database:

```erlang
%% Create an SST file
{ok, Writer} = rocksdb:sst_file_writer_open([], "/tmp/data.sst"),

%% Add key-value pairs (MUST be in sorted order)
ok = rocksdb:sst_file_writer_put(Writer, <<"key1">>, <<"value1">>),
ok = rocksdb:sst_file_writer_put(Writer, <<"key2">>, <<"value2">>),
ok = rocksdb:sst_file_writer_put(Writer, <<"key3">>, <<"value3">>),

%% Finish and get file info
{ok, FileInfo} = rocksdb:sst_file_writer_finish(Writer, with_file_info),
ok = rocksdb:release_sst_file_writer(Writer),

%% FileInfo contains: file_path, smallest_key, largest_key,
%% file_size, num_entries, sequence_number
```

#### Ingesting SST Files

Load SST files directly into a database:

```erlang
{ok, Db} = rocksdb:open("/tmp/mydb", [{create_if_missing, true}]),

%% Ingest SST files into the database
ok = rocksdb:ingest_external_file(Db, ["/tmp/data.sst"], []),

%% Data is now queryable
{ok, <<"value1">>} = rocksdb:get(Db, <<"key1">>, []),

%% Ingest into a specific column family
ok = rocksdb:ingest_external_file(Db, ColumnFamily, ["/tmp/cf_data.sst"], []),

rocksdb:close(Db).
```

#### Reading SST Files

Inspect SST files without loading them into a database:

```erlang
%% Open an SST file for reading
{ok, Reader} = rocksdb:sst_file_reader_open([], "/tmp/data.sst"),

%% Get table properties (metadata)
{ok, Props} = rocksdb:sst_file_reader_get_table_properties(Reader),
io:format("Entries: ~p, Size: ~p bytes~n",
    [maps:get(num_entries, Props), maps:get(data_size, Props)]),

%% Iterate through the file
{ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, []),
{ok, Key1, Value1} = rocksdb:sst_file_reader_iterator_move(Itr, first),
{ok, Key2, Value2} = rocksdb:sst_file_reader_iterator_move(Itr, next),

%% Seek to a specific key
{ok, Key, Value} = rocksdb:sst_file_reader_iterator_move(Itr, {seek, <<"key2">>}),

%% Verify file integrity
ok = rocksdb:sst_file_reader_verify_checksum(Reader),

%% Cleanup
ok = rocksdb:sst_file_reader_iterator_close(Itr),
ok = rocksdb:release_sst_file_reader(Reader).
```

See [SST Files Guide](sst_files.html) for more details including options, best practices, and advanced use cases.
