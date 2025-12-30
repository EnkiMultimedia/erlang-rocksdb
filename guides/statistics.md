# RocksDB Statistics Guide

This guide explains how to collect and use RocksDB statistics in Erlang RocksDB. Statistics provide insights into database performance, I/O operations, cache efficiency, and more.

## Getting Started

### Creating a Statistics Object

First, create a statistics object and attach it to your database:

```erlang
%% Create a statistics collector
{ok, Stats} = rocksdb:new_statistics(),

%% Open database with statistics enabled
{ok, Db} = rocksdb:open("my_db", [
    {create_if_missing, true},
    {statistics, Stats}
]).
```

### Reading Statistics

Use `rocksdb:statistics_ticker/2` to read counter values:

```erlang
{ok, KeysWritten} = rocksdb:statistics_ticker(Stats, number_keys_written),
io:format("Keys written: ~p~n", [KeysWritten]).
```

Use `rocksdb:statistics_histogram/2` to read histogram data:

```erlang
{ok, WriteHist} = rocksdb:statistics_histogram(Stats, db_write),
io:format("Write latency - median: ~.2f us, p99: ~.2f us~n",
          [maps:get(median, WriteHist), maps:get(percentile99, WriteHist)]).
```

### Histogram Data Format

Histogram results are returned as a map with the following keys:

| Key | Type | Description |
|-----|------|-------------|
| `median` | float | 50th percentile value |
| `percentile95` | float | 95th percentile value |
| `percentile99` | float | 99th percentile value |
| `average` | float | Mean value |
| `standard_deviation` | float | Standard deviation |
| `max` | float | Maximum observed value |
| `count` | integer | Number of samples |
| `sum` | integer | Sum of all samples |

### Setting Statistics Level

Control the level of detail collected:

```erlang
%% Set to collect all statistics
ok = rocksdb:set_stats_level(Stats, stats_all),

%% Or disable expensive timing stats
ok = rocksdb:set_stats_level(Stats, stats_except_timers).
```

Available levels:
- `stats_disable_all` - Disable all statistics
- `stats_except_tickers` - Collect histograms only
- `stats_except_histogram_or_timers` - Collect tickers only (no histograms or timing)
- `stats_except_timers` - Collect everything except timing measurements
- `stats_except_detailed_timers` - Collect everything except detailed timing
- `stats_except_time_for_mutex` - Collect everything except mutex timing
- `stats_all` - Collect all statistics (default)

### Cleanup

Release the statistics object when done:

```erlang
ok = rocksdb:close(Db),
ok = rocksdb:release_statistics(Stats).
```

---

## Complete Ticker Reference

Tickers are simple counters that track cumulative values.

### Database Operation Tickers

| Ticker | Description |
|--------|-------------|
| `number_keys_written` | Total number of keys written to the database |
| `number_keys_read` | Total number of keys read from the database |
| `number_keys_updated` | Total number of keys updated (via merge) |
| `bytes_written` | Total bytes written to the database |
| `bytes_read` | Total bytes read from the database |
| `iter_bytes_read` | Total bytes read through iterators |

### Iterator Tickers

| Ticker | Description |
|--------|-------------|
| `number_db_seek` | Number of seek operations on iterators |
| `number_db_next` | Number of next operations on iterators |
| `number_db_prev` | Number of prev operations on iterators |
| `number_db_seek_found` | Number of seek operations that found a key |
| `number_db_next_found` | Number of next operations that found a key |
| `number_db_prev_found` | Number of prev operations that found a key |

### Block Cache Tickers

| Ticker | Description |
|--------|-------------|
| `block_cache_miss` | Total block cache misses |
| `block_cache_hit` | Total block cache hits |
| `block_cache_add` | Number of blocks added to cache |
| `block_cache_add_failures` | Number of failures adding blocks to cache |
| `block_cache_index_miss` | Index block cache misses |
| `block_cache_index_hit` | Index block cache hits |
| `block_cache_filter_miss` | Filter block cache misses |
| `block_cache_filter_hit` | Filter block cache hits |
| `block_cache_data_miss` | Data block cache misses |
| `block_cache_data_hit` | Data block cache hits |
| `block_cache_bytes_read` | Total bytes read from block cache |
| `block_cache_bytes_write` | Total bytes written to block cache |

**Example - Calculate cache hit ratio:**

```erlang
{ok, Hits} = rocksdb:statistics_ticker(Stats, block_cache_hit),
{ok, Misses} = rocksdb:statistics_ticker(Stats, block_cache_miss),
HitRatio = case Hits + Misses of
    0 -> 0.0;
    Total -> Hits / Total * 100
end,
io:format("Block cache hit ratio: ~.2f%~n", [HitRatio]).
```

### Memtable Tickers

| Ticker | Description |
|--------|-------------|
| `memtable_hit` | Number of reads served from memtable |
| `memtable_miss` | Number of reads not found in memtable |

### Write Path Tickers

| Ticker | Description |
|--------|-------------|
| `write_done_by_self` | Writes completed by the calling thread |
| `write_done_by_other` | Writes batched and completed by another thread |
| `wal_file_synced` | Number of WAL file sync operations |
| `stall_micros` | Total microseconds spent in write stalls |

### Compaction Tickers

| Ticker | Description |
|--------|-------------|
| `compact_read_bytes` | Bytes read during compaction |
| `compact_write_bytes` | Bytes written during compaction |
| `flush_write_bytes` | Bytes written during memtable flush |
| `compaction_key_drop_newer_entry` | Keys dropped due to newer version existing |
| `compaction_key_drop_obsolete` | Keys dropped due to being obsolete (deleted) |
| `compaction_key_drop_range_del` | Keys dropped due to range delete |
| `compaction_key_drop_user` | Keys dropped by user compaction filter |
| `compaction_cancelled` | Number of cancelled compactions |
| `number_superversion_acquires` | Superversion acquire operations |
| `number_superversion_releases` | Superversion release operations |

### BlobDB Tickers

| Ticker | Description |
|--------|-------------|
| `blob_db_num_put` | Number of put operations |
| `blob_db_num_write` | Number of write operations |
| `blob_db_num_get` | Number of get operations |
| `blob_db_num_multiget` | Number of multi-get operations |
| `blob_db_num_seek` | Number of seek operations |
| `blob_db_num_next` | Number of next operations |
| `blob_db_num_prev` | Number of prev operations |
| `blob_db_num_keys_written` | Number of keys written |
| `blob_db_num_keys_read` | Number of keys read |
| `blob_db_bytes_written` | Total bytes written |
| `blob_db_bytes_read` | Total bytes read |
| `blob_db_write_inlined` | Writes stored inline (not in blob) |
| `blob_db_write_inlined_ttl` | Inline writes with TTL |
| `blob_db_write_blob` | Writes stored in blob files |
| `blob_db_write_blob_ttl` | Blob writes with TTL |
| `blob_db_blob_file_bytes_written` | Bytes written to blob files |
| `blob_db_blob_file_bytes_read` | Bytes read from blob files |
| `blob_db_blob_file_synced` | Number of blob file syncs |
| `blob_db_blob_index_expired_count` | Expired blob index entries |
| `blob_db_blob_index_expired_size` | Size of expired blob index entries |
| `blob_db_blob_index_evicted_count` | Evicted blob index entries |
| `blob_db_blob_index_evicted_size` | Size of evicted blob index entries |
| `blob_db_gc_num_files` | Blob files processed by GC |
| `blob_db_gc_num_new_files` | New blob files created by GC |
| `blob_db_gc_failures` | Number of GC failures |
| `blob_db_gc_num_keys_relocated` | Keys relocated during GC |
| `blob_db_gc_bytes_relocated` | Bytes relocated during GC |
| `blob_db_fifo_num_files_evicted` | Files evicted by FIFO compaction |
| `blob_db_fifo_num_keys_evicted` | Keys evicted by FIFO compaction |
| `blob_db_fifo_bytes_evicted` | Bytes evicted by FIFO compaction |
| `blob_db_cache_miss` | Blob cache misses |
| `blob_db_cache_hit` | Blob cache hits |
| `blob_db_cache_add` | Blobs added to cache |
| `blob_db_cache_add_failures` | Failed blob cache additions |
| `blob_db_cache_bytes_read` | Bytes read from blob cache |
| `blob_db_cache_bytes_write` | Bytes written to blob cache |

### Transaction Tickers

| Ticker | Description |
|--------|-------------|
| `txn_prepare_mutex_overhead` | Time spent waiting on prepare mutex (ns) |
| `txn_old_commit_map_mutex_overhead` | Time spent waiting on commit map mutex (ns) |
| `txn_duplicate_key_overhead` | Time spent on duplicate key checking (ns) |
| `txn_snapshot_mutex_overhead` | Time spent waiting on snapshot mutex (ns) |
| `txn_get_try_again` | Number of TryAgain errors from transaction gets |

---

## Complete Histogram Reference

Histograms track the distribution of values over time.

### Database Operation Histograms

| Histogram | Description |
|-----------|-------------|
| `db_get` | Get operation latency (microseconds) |
| `db_write` | Write operation latency (microseconds) |
| `db_multiget` | Multi-get operation latency (microseconds) |
| `db_seek` | Iterator seek latency (microseconds) |

**Example - Monitor read latency:**

```erlang
{ok, GetHist} = rocksdb:statistics_histogram(Stats, db_get),
io:format("Get latency:~n"),
io:format("  Median: ~.2f us~n", [maps:get(median, GetHist)]),
io:format("  P95:    ~.2f us~n", [maps:get(percentile95, GetHist)]),
io:format("  P99:    ~.2f us~n", [maps:get(percentile99, GetHist)]),
io:format("  Max:    ~.2f us~n", [maps:get(max, GetHist)]).
```

### Compaction and Flush Histograms

| Histogram | Description |
|-----------|-------------|
| `compaction_time` | Compaction duration (microseconds) |
| `flush_time` | Memtable flush duration (microseconds) |

### I/O Histograms

| Histogram | Description |
|-----------|-------------|
| `sst_read_micros` | SST file read latency (microseconds) |
| `sst_write_micros` | SST file write latency (microseconds) |
| `table_sync_micros` | SST file sync latency (microseconds) |
| `wal_file_sync_micros` | WAL file sync latency (microseconds) |
| `bytes_per_read` | Bytes per read operation |
| `bytes_per_write` | Bytes per write operation |

### BlobDB Histograms

| Histogram | Description |
|-----------|-------------|
| `blob_db_key_size` | Key size distribution (bytes) |
| `blob_db_value_size` | Value size distribution (bytes) |
| `blob_db_write_micros` | Write latency (microseconds) |
| `blob_db_get_micros` | Get latency (microseconds) |
| `blob_db_multiget_micros` | Multi-get latency (microseconds) |
| `blob_db_seek_micros` | Seek latency (microseconds) |
| `blob_db_next_micros` | Next latency (microseconds) |
| `blob_db_prev_micros` | Prev latency (microseconds) |
| `blob_db_blob_file_write_micros` | Blob file write latency (microseconds) |
| `blob_db_blob_file_read_micros` | Blob file read latency (microseconds) |
| `blob_db_blob_file_sync_micros` | Blob file sync latency (microseconds) |
| `blob_db_compression_micros` | Compression time (microseconds) |
| `blob_db_decompression_micros` | Decompression time (microseconds) |

### Transaction Histograms

| Histogram | Description |
|-----------|-------------|
| `num_op_per_transaction` | Number of operations per transaction |

---

## Example: Comprehensive Monitoring

Here's a complete example that monitors key database metrics:

```erlang
-module(db_monitor).
-export([report/1]).

report(Stats) ->
    %% Operation counts
    {ok, KeysWritten} = rocksdb:statistics_ticker(Stats, number_keys_written),
    {ok, KeysRead} = rocksdb:statistics_ticker(Stats, number_keys_read),

    %% Cache efficiency
    {ok, CacheHits} = rocksdb:statistics_ticker(Stats, block_cache_hit),
    {ok, CacheMisses} = rocksdb:statistics_ticker(Stats, block_cache_miss),
    CacheHitRatio = safe_ratio(CacheHits, CacheHits + CacheMisses),

    %% Memtable efficiency
    {ok, MemHits} = rocksdb:statistics_ticker(Stats, memtable_hit),
    {ok, MemMisses} = rocksdb:statistics_ticker(Stats, memtable_miss),
    MemHitRatio = safe_ratio(MemHits, MemHits + MemMisses),

    %% Write stalls
    {ok, StallMicros} = rocksdb:statistics_ticker(Stats, stall_micros),

    %% Latency histograms
    {ok, GetHist} = rocksdb:statistics_histogram(Stats, db_get),
    {ok, WriteHist} = rocksdb:statistics_histogram(Stats, db_write),

    io:format("=== RocksDB Statistics ===~n"),
    io:format("Keys written: ~p, Keys read: ~p~n", [KeysWritten, KeysRead]),
    io:format("Block cache hit ratio: ~.2f%~n", [CacheHitRatio * 100]),
    io:format("Memtable hit ratio: ~.2f%~n", [MemHitRatio * 100]),
    io:format("Total stall time: ~.2f ms~n", [StallMicros / 1000]),
    io:format("Get latency p99: ~.2f us~n", [maps:get(percentile99, GetHist)]),
    io:format("Write latency p99: ~.2f us~n", [maps:get(percentile99, WriteHist)]),
    ok.

safe_ratio(_, 0) -> 0.0;
safe_ratio(Num, Denom) -> Num / Denom.
```

---

## Statistics with Column Families

When using column families, create one statistics object and share it:

```erlang
{ok, Stats} = rocksdb:new_statistics(),
{ok, Db, [DefaultCf, DataCf, IndexCf]} = rocksdb:open_with_cf(
    "my_db",
    [{create_if_missing, true}, {statistics, Stats}],
    [{"default", []}, {"data", []}, {"index", []}]
),

%% Statistics are aggregated across all column families
{ok, TotalKeysWritten} = rocksdb:statistics_ticker(Stats, number_keys_written).
```

---

## Statistics with Transactions

Statistics also work with pessimistic transaction databases:

```erlang
{ok, Stats} = rocksdb:new_statistics(),
{ok, Db, _} = rocksdb:open_pessimistic_transaction_db(
    "txn_db",
    [{create_if_missing, true}, {statistics, Stats}],
    [{"default", []}]
),

%% Perform transactions...
{ok, Txn} = rocksdb:pessimistic_transaction(Db, []),
ok = rocksdb:pessimistic_transaction_put(Txn, <<"key">>, <<"value">>),
ok = rocksdb:pessimistic_transaction_commit(Txn),
ok = rocksdb:release_pessimistic_transaction(Txn),

%% Check transaction-specific stats
{ok, OpsPerTxn} = rocksdb:statistics_histogram(Stats, num_op_per_transaction),
io:format("Avg ops per transaction: ~.2f~n", [maps:get(average, OpsPerTxn)]).
```

---

## Performance Considerations

1. **Statistics overhead**: Collecting statistics has a small performance cost. Use `stats_except_timers` or `stats_except_detailed_timers` in production if timing precision isn't critical.

2. **Polling frequency**: Statistics are cumulative. Poll them periodically and compute deltas for rate metrics.

3. **Memory**: The statistics object uses minimal memory but should be released when no longer needed.

4. **Thread safety**: Statistics are thread-safe and can be read from any Erlang process.
