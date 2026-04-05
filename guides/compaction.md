# Compaction

Compaction is a background process that merges SST files to reduce space amplification, remove deleted/expired keys, and improve read performance. This guide covers compaction strategies, manual compaction, and background work control.

## Compaction Styles

RocksDB supports three compaction styles:

### Level Compaction (Default)

Level compaction organizes data into multiple levels. New data goes to Level 0, then gets compacted into deeper levels over time.

```erlang
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_style, level},
    %% Trigger L0->L1 compaction when L0 has 4 files
    {level0_file_num_compaction_trigger, 4},
    %% Target 64MB files
    {target_file_size_base, 64 * 1024 * 1024},
    %% Each level is 10x larger than previous
    {max_bytes_for_level_multiplier, 10}
]).
```

Best for: General workloads with mixed reads and writes.

### Universal Compaction

Universal compaction keeps all data in Level 0 and merges files based on size ratios.

```erlang
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_style, universal}
]).
```

Best for: Write-heavy workloads where write amplification matters more than space.

### FIFO Compaction

FIFO compaction deletes oldest files when size limit is reached. Works well with TTL.

```erlang
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_style, fifo},
    {ttl, 3600},  %% 1 hour TTL
    {compaction_options_fifo, [
        {max_table_files_size, 1024 * 1024 * 1024},  %% 1GB max
        {allow_compaction, true}
    ]}
]).
```

Best for: Time-series data, logs, or cache-like workloads.

## Manual Compaction

Use `compact_range/4` or `compact_range/5` to trigger compaction manually.

### Full Database Compaction

```erlang
%% Compact entire database
ok = rocksdb:compact_range(Db, undefined, undefined, []).
```

### Range Compaction

```erlang
%% Compact keys from "a" to "z"
ok = rocksdb:compact_range(Db, <<"a">>, <<"z">>, []).
```

### Column Family Compaction

```erlang
%% Compact specific column family
ok = rocksdb:compact_range(Db, CfHandle, undefined, undefined, []).
```

### Compact Range Options

```erlang
ok = rocksdb:compact_range(Db, undefined, undefined, [
    %% Force compaction of bottommost level (removes deleted keys)
    {bottommost_level_compaction, force},
    %% Allow write stalls during compaction
    {allow_write_stall, true},
    %% Use multiple threads for this compaction
    {max_subcompactions, 4}
]).
```

Available options:

| Option | Type | Description |
|--------|------|-------------|
| `bottommost_level_compaction` | `skip \| if_have_compaction_filter \| force \| force_optimized` | How to handle bottommost level |
| `exclusive_manual_compaction` | `boolean()` | Wait for other compactions to finish |
| `change_level` | `boolean()` | Move files to target level |
| `target_level` | `integer()` | Target level for moved files |
| `allow_write_stall` | `boolean()` | Allow write stalls |
| `max_subcompactions` | `non_neg_integer()` | Parallelism within compaction |

## Background Work Control

### Pause and Resume

Use these functions when you need to take a consistent backup or perform maintenance:

```erlang
%% Pause all background work (flush and compaction)
ok = rocksdb:pause_background_work(Db),

%% Take backup while paused...
ok = rocksdb:checkpoint(Db, "/backup/path"),

%% Resume background work
ok = rocksdb:continue_background_work(Db).
```

> **Note:** While background work is paused, writes will eventually stall when memtables fill up. Keep the pause duration short.

### Disable Manual Compaction

Prevent manual compaction calls while allowing automatic compaction:

```erlang
%% Disable manual compaction
%% If a manual compaction is running, this waits for it to complete
ok = rocksdb:disable_manual_compaction(Db),

%% ... perform operations that shouldn't be interrupted by manual compaction ...

%% Re-enable manual compaction
ok = rocksdb:enable_manual_compaction(Db).
```

### Disable Automatic Compaction

For full control, disable automatic compaction entirely:

```erlang
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {disable_auto_compactions, true}
]).

%% Trigger compaction manually when needed
ok = rocksdb:compact_range(Db, undefined, undefined, []).
```

## Compaction Tuning

### Thread Configuration

```erlang
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    %% Total background threads for flush and compaction
    {max_background_jobs, 8},
    %% Parallelism within a single compaction job
    {max_subcompactions, 4}
]).
```

### Read-ahead for Compaction

Improve compaction performance on HDDs:

```erlang
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_readahead_size, 2 * 1024 * 1024}  %% 2MB read-ahead
]).
```

## Monitoring Compaction

Use statistics to track compaction performance:

```erlang
{ok, Stats} = rocksdb:new_statistics(),
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {statistics, Stats}
]),

%% ... after some operations ...

%% Check compaction metrics
{ok, ReadBytes} = rocksdb:statistics_ticker(Stats, compact_read_bytes),
{ok, WriteBytes} = rocksdb:statistics_ticker(Stats, compact_write_bytes),
io:format("Compaction: read ~p bytes, wrote ~p bytes~n", [ReadBytes, WriteBytes]).
```

Key statistics:

| Ticker | Description |
|--------|-------------|
| `compact_read_bytes` | Total bytes read during compaction |
| `compact_write_bytes` | Total bytes written during compaction |
| `compaction_key_drop_newer_entry` | Duplicate keys removed |
| `compaction_key_drop_obsolete` | Obsolete keys removed |
| `compaction_cancelled` | Cancelled compactions |

## Use Case Examples

### Backup with Consistent State

```erlang
backup_database(Db, BackupPath) ->
    %% Pause to get consistent state
    ok = rocksdb:pause_background_work(Db),
    try
        ok = rocksdb:checkpoint(Db, BackupPath)
    after
        %% Always resume, even on error
        ok = rocksdb:continue_background_work(Db)
    end.
```

### Force Cleanup of Deleted Keys

```erlang
%% After bulk delete, force compaction to reclaim space
cleanup_deleted_keys(Db) ->
    rocksdb:compact_range(Db, undefined, undefined, [
        {bottommost_level_compaction, force}
    ]).
```

### Time-Series with Auto-Expiry

```erlang
open_timeseries_db(Path) ->
    rocksdb:open(Path, [
        {create_if_missing, true},
        {compaction_style, fifo},
        {ttl, 86400},  %% 24 hours
        {compaction_options_fifo, [
            {max_table_files_size, 10 * 1024 * 1024 * 1024},  %% 10GB
            {allow_compaction, true}
        ]}
    ]).
```
