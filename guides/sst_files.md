# SST Files Guide

SST (Sorted String Table) files are the core storage format of RocksDB. Erlang RocksDB provides comprehensive support for creating, ingesting, and reading SST files directly, enabling powerful use cases like bulk data loading, data migration, and offline inspection.

## Overview

The SST file API consists of three main components:

1. **SstFileWriter** - Create SST files outside the database
2. **IngestExternalFile** - Load SST files into a database
3. **SstFileReader** - Read and inspect SST files without loading them

## Creating SST Files

### Basic Usage

```erlang
%% Create an SST file writer
{ok, Writer} = rocksdb:sst_file_writer_open(Options, "/tmp/data.sst"),

%% Add key-value pairs (MUST be in sorted order by key)
ok = rocksdb:sst_file_writer_put(Writer, <<"apple">>, <<"fruit">>),
ok = rocksdb:sst_file_writer_put(Writer, <<"banana">>, <<"fruit">>),
ok = rocksdb:sst_file_writer_put(Writer, <<"carrot">>, <<"vegetable">>),

%% Finish writing (required to finalize the file)
ok = rocksdb:sst_file_writer_finish(Writer),
ok = rocksdb:release_sst_file_writer(Writer).
```

### Getting File Information

```erlang
%% Finish with file info to get metadata about the created file
{ok, FileInfo} = rocksdb:sst_file_writer_finish(Writer, with_file_info),

%% FileInfo is a map containing:
%% - file_path: Path to the created file
%% - smallest_key: Smallest key in the file
%% - largest_key: Largest key in the file
%% - file_size: Size in bytes
%% - num_entries: Number of key-value pairs
%% - sequence_number: Assigned sequence number
```

### Key Ordering Requirement

Keys MUST be added in sorted order (ascending lexicographic order). Adding an out-of-order key will return an error:

```erlang
{ok, Writer} = rocksdb:sst_file_writer_open([], "/tmp/test.sst"),
ok = rocksdb:sst_file_writer_put(Writer, <<"b">>, <<"2">>),

%% This will fail - "a" comes before "b"
{error, _} = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"1">>).
```

### Other Operations

```erlang
%% Add a merge operation
ok = rocksdb:sst_file_writer_merge(Writer, Key, Value),

%% Add a delete tombstone
ok = rocksdb:sst_file_writer_delete(Writer, Key),

%% Add a range delete (deletes all keys in [BeginKey, EndKey))
ok = rocksdb:sst_file_writer_delete_range(Writer, BeginKey, EndKey),

%% Get current file size during writing
Size = rocksdb:sst_file_writer_file_size(Writer).
```

### Wide-Column Entities

You can also write wide-column entities to SST files:

```erlang
Columns = [
    {<<"name">>, <<"Alice">>},
    {<<"email">>, <<"alice@example.com">>}
],
ok = rocksdb:sst_file_writer_put_entity(Writer, <<"user:1">>, Columns).
```

### Merge Operations

You can add merge operations to SST files. When the file is ingested into a database that has a merge operator configured, the merge operations will be applied during compaction or read.

#### Using the Counter Merge Operator

The counter merge operator allows atomic increment/decrement operations. Values are represented as ASCII strings:

```erlang
%% Create SST file with counter merge operator
Options = [{merge_operator, counter_merge_operator}],
{ok, Writer} = rocksdb:sst_file_writer_open(Options, "/tmp/counters.sst"),

%% Add merge operations for counters (keys must be in sorted order)
%% The value format is ASCII string: <<"100">> means increment by 100
ok = rocksdb:sst_file_writer_merge(Writer, <<"counter:pageviews">>, <<"100">>),
ok = rocksdb:sst_file_writer_merge(Writer, <<"counter:users">>, <<"1">>),

ok = rocksdb:sst_file_writer_finish(Writer),
ok = rocksdb:release_sst_file_writer(Writer),

%% Open database with the same merge operator
{ok, Db} = rocksdb:open("/tmp/mydb", [
    {create_if_missing, true},
    {merge_operator, counter_merge_operator}
]),

%% Ingest the SST file
ok = rocksdb:ingest_external_file(Db, ["/tmp/counters.sst"], []),

%% Read the counter value (result is ASCII string)
{ok, <<"100">>} = rocksdb:get(Db, <<"counter:pageviews">>, []),

%% Values accumulate - ingesting <<"50">> would result in <<"150">>
```

#### Using the Erlang Merge Operator

The Erlang merge operator supports various data type operations:

```erlang
%% Create SST file for list append operations
Options = [{merge_operator, erlang_merge_operator}],
{ok, Writer} = rocksdb:sst_file_writer_open(Options, "/tmp/lists.sst"),

%% Merge operation to append to a list
%% Format: {list_append, Elements}
MergeValue = term_to_binary({list_append, [item1, item2]}),
ok = rocksdb:sst_file_writer_merge(Writer, <<"mylist">>, MergeValue),

ok = rocksdb:sst_file_writer_finish(Writer),
ok = rocksdb:release_sst_file_writer(Writer),

%% Open database with erlang_merge_operator
{ok, Db} = rocksdb:open("/tmp/mydb", [
    {create_if_missing, true},
    {merge_operator, erlang_merge_operator}
]),

%% First put an initial list
ok = rocksdb:put(Db, <<"mylist">>, term_to_binary([existing]), []),

%% Ingest the merge operations
ok = rocksdb:ingest_external_file(Db, ["/tmp/lists.sst"], []),

%% Read the merged list
{ok, Bin} = rocksdb:get(Db, <<"mylist">>, []),
List = binary_to_term(Bin),
%% List = [existing, item1, item2]
```

#### Using the Bitset Merge Operator

The bitset merge operator requires a size parameter (in bits) and uses ASCII format for operations:

```erlang
%% Create SST file for bitset operations
%% The merge operator needs a size: {bitset_merge_operator, SizeInBits}
Options = [{merge_operator, {bitset_merge_operator, 64}}],
{ok, Writer} = rocksdb:sst_file_writer_open(Options, "/tmp/bitsets.sst"),

%% Set bit at position 5 using <<"+N">> format
ok = rocksdb:sst_file_writer_merge(Writer, <<"flags:user1">>, <<"+5">>),

ok = rocksdb:sst_file_writer_finish(Writer),
ok = rocksdb:release_sst_file_writer(Writer),

%% Open database with same bitset merge operator
{ok, Db} = rocksdb:open("/tmp/mydb", [
    {create_if_missing, true},
    {merge_operator, {bitset_merge_operator, 64}}
]),

%% Initialize with zeros (8 bytes = 64 bits)
ok = rocksdb:put(Db, <<"flags:user1">>, <<0:64/unsigned>>, []),

%% Ingest the SST file
ok = rocksdb:ingest_external_file(Db, ["/tmp/bitsets.sst"], []),

%% Bit 5 is now set
{ok, Value} = rocksdb:get(Db, <<"flags:user1">>, []).
%% To clear a bit, use <<"-N">> format
%% To reset all bits, use <<"">> (empty binary)
```

#### Important Notes for Merge Operations

1. **Matching Merge Operator**: The database must be opened with the same merge operator that was used when creating the SST file, otherwise merge operations won't work correctly.

2. **Key Ordering**: Like all SST file operations, merge keys must be added in sorted order.

3. **Multiple Merges per Key**: You can add multiple merge operations for the same key - they will be combined during compaction:

```erlang
%% Multiple increments for the same counter
ok = rocksdb:sst_file_writer_merge(Writer, <<"counter:a">>, <<10:64/signed-little-integer>>),
%% ... other keys ...
%% Note: You cannot add another merge for "counter:a" here because keys must be in order
%% To add multiple merges for the same key, you'd need to do it in the database after ingestion
```

4. **Mixing Puts and Merges**: You can mix put and merge operations in the same SST file, but remember the key ordering requirement:

```erlang
ok = rocksdb:sst_file_writer_put(Writer, <<"a">>, <<"value">>),
ok = rocksdb:sst_file_writer_merge(Writer, <<"b">>, MergeValue),  %% OK - "b" > "a"
ok = rocksdb:sst_file_writer_put(Writer, <<"c">>, <<"value">>),   %% OK - "c" > "b"
```

### Writer Options

The first argument to `sst_file_writer_open/2` accepts database options:

```erlang
Options = [
    {compression, snappy},           %% Compression: snappy, lz4, zstd, none
    {block_size, 4096},              %% Block size in bytes
    {bloom_filter_policy, 10}        %% Bloom filter bits per key
],
{ok, Writer} = rocksdb:sst_file_writer_open(Options, FilePath).
```

## Ingesting SST Files

### Basic Ingestion

```erlang
{ok, Db} = rocksdb:open("/tmp/mydb", [{create_if_missing, true}]),

%% Ingest one or more SST files
ok = rocksdb:ingest_external_file(Db, ["/tmp/data1.sst", "/tmp/data2.sst"], []),

%% Data is now available
{ok, Value} = rocksdb:get(Db, <<"key1">>, []).
```

### Ingesting into Column Families

```erlang
{ok, Db, [_DefaultCf, DataCf]} = rocksdb:open_with_cf(
    "/tmp/mydb",
    [{create_if_missing, true}, {create_missing_column_families, true}],
    [{"default", []}, {"data", []}]
),

%% Ingest into specific column family
ok = rocksdb:ingest_external_file(Db, DataCf, ["/tmp/data.sst"], []),

%% Query from that column family
{ok, Value} = rocksdb:get(Db, DataCf, <<"key1">>, []).
```

### Ingest Options

```erlang
Options = [
    %% Move files instead of copying (uses hard links, faster)
    {move_files, true},

    %% Fall back to copy if move fails
    {failed_move_fall_back_to_copy, true},

    %% Ensure consistency with existing snapshots
    {snapshot_consistency, true},

    %% Allow assigning global sequence numbers
    {allow_global_seqno, true},

    %% Allow blocking flush if overlap with memtable
    {allow_blocking_flush, true},

    %% Ingest at bottommost level (for backfill scenarios)
    {ingest_behind, false},

    %% Verify block checksums before ingestion
    {verify_checksums_before_ingest, true},

    %% Verify file checksum if present
    {verify_file_checksum, true},

    %% Fail if cannot place in bottommost level
    {fail_if_not_bottommost_level, false},

    %% Fill block cache during ingestion
    {fill_cache, true}
],
ok = rocksdb:ingest_external_file(Db, Files, Options).
```

## Reading SST Files

The SstFileReader allows you to inspect SST files without loading them into a database. This is useful for:

- Verifying file integrity
- Inspecting file contents and metadata
- Debugging data issues
- Data migration and validation

### Opening an SST File

```erlang
{ok, Reader} = rocksdb:sst_file_reader_open(Options, "/tmp/data.sst").
```

### Getting Table Properties

```erlang
{ok, Props} = rocksdb:sst_file_reader_get_table_properties(Reader),

%% Props is a map containing extensive metadata:
io:format("Number of entries: ~p~n", [maps:get(num_entries, Props)]),
io:format("Data size: ~p bytes~n", [maps:get(data_size, Props)]),
io:format("Index size: ~p bytes~n", [maps:get(index_size, Props)]),
io:format("Filter size: ~p bytes~n", [maps:get(filter_size, Props)]),
io:format("Compression: ~s~n", [maps:get(compression_name, Props)]),
io:format("Deletions: ~p~n", [maps:get(num_deletions, Props)]),
io:format("Merge operands: ~p~n", [maps:get(num_merge_operands, Props)]),
io:format("Creation time: ~p~n", [maps:get(creation_time, Props)]).
```

### Table Properties Reference

| Property | Description |
|----------|-------------|
| `data_size` | Size of data blocks in bytes |
| `index_size` | Size of index blocks in bytes |
| `index_partitions` | Number of index partitions |
| `top_level_index_size` | Size of top-level index |
| `filter_size` | Size of bloom filter (if any) |
| `raw_key_size` | Total raw key size |
| `raw_value_size` | Total raw value size |
| `num_data_blocks` | Number of data blocks |
| `num_entries` | Number of key-value entries |
| `num_deletions` | Number of delete tombstones |
| `num_merge_operands` | Number of merge operations |
| `num_range_deletions` | Number of range deletions |
| `format_version` | SST file format version |
| `fixed_key_len` | Fixed key length (0 if variable) |
| `column_family_id` | Column family ID |
| `column_family_name` | Column family name |
| `filter_policy_name` | Filter policy name |
| `comparator_name` | Key comparator name |
| `merge_operator_name` | Merge operator name |
| `prefix_extractor_name` | Prefix extractor name |
| `compression_name` | Compression algorithm name |
| `compression_options` | Compression options string |
| `creation_time` | Unix timestamp when created |
| `oldest_key_time` | Oldest key time |
| `file_creation_time` | File creation time |

### Iterating Through an SST File

```erlang
{ok, Reader} = rocksdb:sst_file_reader_open([], "/tmp/data.sst"),
{ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, []),

%% Move to first entry
{ok, Key1, Value1} = rocksdb:sst_file_reader_iterator_move(Itr, first),

%% Move to next entry
{ok, Key2, Value2} = rocksdb:sst_file_reader_iterator_move(Itr, next),

%% Move to last entry
{ok, LastKey, LastValue} = rocksdb:sst_file_reader_iterator_move(Itr, last),

%% Move to previous entry
{ok, PrevKey, PrevValue} = rocksdb:sst_file_reader_iterator_move(Itr, prev),

%% Seek to a specific key (or the first key >= target)
{ok, Key, Value} = rocksdb:sst_file_reader_iterator_move(Itr, {seek, <<"target">>}),

%% Seek for prev (find largest key <= target)
{ok, Key, Value} = rocksdb:sst_file_reader_iterator_move(Itr, {seek_for_prev, <<"target">>}),

%% When iterator is exhausted or invalid
{error, invalid_iterator} = rocksdb:sst_file_reader_iterator_move(Itr, next),

%% Cleanup
ok = rocksdb:sst_file_reader_iterator_close(Itr),
ok = rocksdb:release_sst_file_reader(Reader).
```

### Iterating All Entries

```erlang
iterate_all(Reader) ->
    {ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, []),
    iterate_loop(Itr, rocksdb:sst_file_reader_iterator_move(Itr, first), []).

iterate_loop(Itr, {ok, Key, Value}, Acc) ->
    iterate_loop(Itr, rocksdb:sst_file_reader_iterator_move(Itr, next),
                 [{Key, Value} | Acc]);
iterate_loop(Itr, {error, invalid_iterator}, Acc) ->
    ok = rocksdb:sst_file_reader_iterator_close(Itr),
    lists:reverse(Acc).
```

### Iterator Options

```erlang
Options = [
    %% Verify block checksums during iteration
    {verify_checksums, true},

    %% Fill block cache during iteration
    {fill_cache, true}
],
{ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, Options).
```

### Multiple Iterators

You can create multiple independent iterators from the same reader:

```erlang
{ok, Reader} = rocksdb:sst_file_reader_open([], "/tmp/data.sst"),

%% Create two iterators
{ok, Itr1} = rocksdb:sst_file_reader_iterator(Reader, []),
{ok, Itr2} = rocksdb:sst_file_reader_iterator(Reader, []),

%% Position them independently
{ok, _, _} = rocksdb:sst_file_reader_iterator_move(Itr1, first),
{ok, _, _} = rocksdb:sst_file_reader_iterator_move(Itr2, last),

%% They can be moved independently
{ok, _, _} = rocksdb:sst_file_reader_iterator_move(Itr1, next),
{ok, _, _} = rocksdb:sst_file_reader_iterator_move(Itr2, prev),

%% Cleanup
ok = rocksdb:sst_file_reader_iterator_close(Itr1),
ok = rocksdb:sst_file_reader_iterator_close(Itr2),
ok = rocksdb:release_sst_file_reader(Reader).
```

### Verifying Checksums

```erlang
{ok, Reader} = rocksdb:sst_file_reader_open([], "/tmp/data.sst"),

%% Verify all block checksums
ok = rocksdb:sst_file_reader_verify_checksum(Reader),

%% With options
ok = rocksdb:sst_file_reader_verify_checksum(Reader, []),

ok = rocksdb:release_sst_file_reader(Reader).
```

## Best Practices

### Parallel SST File Creation

For bulk loading large datasets, create multiple SST files in parallel:

```erlang
%% Partition your data by key range
Partitions = partition_data(Data, NumPartitions),

%% Create SST files in parallel
Results = pmap(fun({PartitionId, PartitionData}) ->
    Path = io_lib:format("/tmp/data_~p.sst", [PartitionId]),
    create_sst_file(Path, PartitionData)
end, Partitions),

%% Ingest all files at once
Files = [Path || {ok, Path} <- Results],
ok = rocksdb:ingest_external_file(Db, Files, [{move_files, true}]).
```

### Key Range Planning

When creating multiple SST files for ingestion, ensure key ranges don't overlap:

```erlang
%% Good: Non-overlapping key ranges
%% File 1: "a" to "m"
%% File 2: "n" to "z"

%% Bad: Overlapping key ranges (may cause issues)
%% File 1: "a" to "n"
%% File 2: "m" to "z"  %% Overlaps with File 1
```

### Memory Considerations

- SST file writers buffer data in memory before flushing
- For very large files, consider splitting into multiple smaller files
- Use appropriate block sizes based on your data characteristics

### Error Handling

Always handle errors and clean up resources:

```erlang
create_sst_file(Path, Data) ->
    case rocksdb:sst_file_writer_open([], Path) of
        {ok, Writer} ->
            try
                write_data(Writer, Data),
                rocksdb:sst_file_writer_finish(Writer)
            after
                rocksdb:release_sst_file_writer(Writer)
            end;
        {error, Reason} ->
            {error, Reason}
    end.
```

## Use Cases

### Bulk Data Loading

Load large datasets efficiently by creating SST files externally:

```erlang
%% 1. Sort your data by key
SortedData = lists:sort(fun({K1, _}, {K2, _}) -> K1 =< K2 end, Data),

%% 2. Create SST file
{ok, Writer} = rocksdb:sst_file_writer_open(
    [{compression, lz4}],
    "/tmp/bulk_data.sst"
),
lists:foreach(fun({K, V}) ->
    ok = rocksdb:sst_file_writer_put(Writer, K, V)
end, SortedData),
ok = rocksdb:sst_file_writer_finish(Writer),
ok = rocksdb:release_sst_file_writer(Writer),

%% 3. Ingest into database
{ok, Db} = rocksdb:open("/tmp/mydb", [{create_if_missing, true}]),
ok = rocksdb:ingest_external_file(Db, ["/tmp/bulk_data.sst"],
    [{move_files, true}]).
```

### Bulk Loading Counters

Load pre-aggregated counter data using merge operations:

```erlang
%% Suppose you have aggregated page view counts from logs
PageViews = [
    {<<"page:/home">>, 15000},
    {<<"page:/about">>, 3200},
    {<<"page:/contact">>, 1500},
    {<<"page:/products">>, 8700}
],

%% Sort by key
SortedPageViews = lists:sort(PageViews),

%% Create SST file with counter merge operations
Options = [{merge_operator, counter_merge_operator}],
{ok, Writer} = rocksdb:sst_file_writer_open(Options, "/tmp/pageviews.sst"),

lists:foreach(fun({Key, Count}) ->
    %% Counter merge format: ASCII string representation
    Value = integer_to_binary(Count),
    ok = rocksdb:sst_file_writer_merge(Writer, Key, Value)
end, SortedPageViews),

ok = rocksdb:sst_file_writer_finish(Writer),
ok = rocksdb:release_sst_file_writer(Writer),

%% Open database with counter merge operator
{ok, Db} = rocksdb:open("/tmp/analytics", [
    {create_if_missing, true},
    {merge_operator, counter_merge_operator}
]),

%% Ingest the counters
ok = rocksdb:ingest_external_file(Db, ["/tmp/pageviews.sst"], []),

%% Read a counter (result is ASCII string)
{ok, HomeViews} = rocksdb:get(Db, <<"page:/home">>, []),
io:format("Home page views: ~s~n", [HomeViews]),
%% Output: Home page views: 15000

%% Subsequent ingestions will add to existing counters
%% If you ingest another file with <<"page:/home">> -> <<"500">>,
%% the total will become <<"15500">>
```

### Data Migration

Migrate data between databases:

```erlang
%% Read from source SST file
{ok, Reader} = rocksdb:sst_file_reader_open([], "/source/data.sst"),
{ok, Itr} = rocksdb:sst_file_reader_iterator(Reader, []),

%% Transform and write to new SST file
{ok, Writer} = rocksdb:sst_file_writer_open([], "/dest/data.sst"),
migrate_loop(Itr, Writer, rocksdb:sst_file_reader_iterator_move(Itr, first)),

ok = rocksdb:sst_file_writer_finish(Writer),
ok = rocksdb:release_sst_file_writer(Writer),
ok = rocksdb:sst_file_reader_iterator_close(Itr),
ok = rocksdb:release_sst_file_reader(Reader).

migrate_loop(Itr, Writer, {ok, Key, Value}) ->
    %% Transform data if needed
    NewValue = transform(Value),
    ok = rocksdb:sst_file_writer_put(Writer, Key, NewValue),
    migrate_loop(Itr, Writer, rocksdb:sst_file_reader_iterator_move(Itr, next));
migrate_loop(_Itr, _Writer, {error, invalid_iterator}) ->
    ok.
```

### File Verification

Verify SST file integrity before ingestion:

```erlang
verify_sst_file(Path) ->
    case rocksdb:sst_file_reader_open([], Path) of
        {ok, Reader} ->
            try
                %% Verify checksums
                case rocksdb:sst_file_reader_verify_checksum(Reader) of
                    ok ->
                        %% Get and validate properties
                        {ok, Props} = rocksdb:sst_file_reader_get_table_properties(Reader),
                        NumEntries = maps:get(num_entries, Props),
                        io:format("File ~s: ~p entries, checksums OK~n", [Path, NumEntries]),
                        ok;
                    {error, Reason} ->
                        io:format("File ~s: checksum verification failed: ~p~n", [Path, Reason]),
                        {error, Reason}
                end
            after
                rocksdb:release_sst_file_reader(Reader)
            end;
        {error, Reason} ->
            io:format("Cannot open ~s: ~p~n", [Path, Reason]),
            {error, Reason}
    end.
```

## API Reference

### SstFileWriter Functions

| Function | Description |
|----------|-------------|
| `sst_file_writer_open(Options, Path)` | Open a new SST file for writing |
| `sst_file_writer_put(Writer, Key, Value)` | Add a key-value pair |
| `sst_file_writer_put_entity(Writer, Key, Columns)` | Add a wide-column entity |
| `sst_file_writer_merge(Writer, Key, Value)` | Add a merge operation |
| `sst_file_writer_delete(Writer, Key)` | Add a delete tombstone |
| `sst_file_writer_delete_range(Writer, Begin, End)` | Add a range delete |
| `sst_file_writer_finish(Writer)` | Finalize the file |
| `sst_file_writer_finish(Writer, with_file_info)` | Finalize and return file info |
| `sst_file_writer_file_size(Writer)` | Get current file size |
| `release_sst_file_writer(Writer)` | Release the writer resource |

### IngestExternalFile Functions

| Function | Description |
|----------|-------------|
| `ingest_external_file(Db, Files, Options)` | Ingest files into default CF |
| `ingest_external_file(Db, Cf, Files, Options)` | Ingest files into specific CF |

### SstFileReader Functions

| Function | Description |
|----------|-------------|
| `sst_file_reader_open(Options, Path)` | Open an SST file for reading |
| `sst_file_reader_iterator(Reader, Options)` | Create an iterator |
| `sst_file_reader_iterator_move(Itr, Action)` | Move the iterator |
| `sst_file_reader_iterator_close(Itr)` | Close the iterator |
| `sst_file_reader_get_table_properties(Reader)` | Get table properties |
| `sst_file_reader_verify_checksum(Reader)` | Verify checksums |
| `sst_file_reader_verify_checksum(Reader, Options)` | Verify with options |
| `release_sst_file_reader(Reader)` | Release the reader resource |
