# Time-to-Live (TTL) Support

RocksDB supports automatic expiration of key-value pairs through Time-to-Live (TTL). Keys inserted into a TTL-enabled database will be automatically deleted after a specified duration.

## How TTL Works

When you open a database with TTL enabled:

1. **Timestamp Suffixing**: A 32-bit timestamp (creation time) is automatically appended to each value during `put` operations
2. **Expiration Check**: During compaction, RocksDB checks if `timestamp + ttl < current_time`
3. **Lazy Deletion**: Expired keys are removed only during compaction (not immediately upon expiration)

## Important Behaviors

- **Non-Strict Guarantees**: Keys are guaranteed to exist for at least TTL seconds, but may persist longer until compaction runs
- **Stale Reads**: `get` and iterator operations may return expired entries if compaction hasn't run yet
- **Read-Only Mode**: Opens the database without triggering compactions, so expired keys won't be removed
- **Variable TTL**: Different TTL values can be used across different database opens

## Basic Usage

### Opening a Database with TTL

```erlang
%% Open a database with 1 hour (3600 seconds) TTL
{ok, Db} = rocksdb:open_with_ttl(
    "my_ttl_db",
    [{create_if_missing, true}],
    3600,   % TTL in seconds
    false   % read_only flag
).

%% All standard operations work normally
ok = rocksdb:put(Db, <<"key1">>, <<"value1">>, []),
{ok, <<"value1">>} = rocksdb:get(Db, <<"key1">>, []),

%% Close when done
ok = rocksdb:close(Db).
```

### TTL Expiration Example

```erlang
%% Open with 1 second TTL for demonstration
{ok, Db} = rocksdb:open_with_ttl("ttl_test", [{create_if_missing, true}], 1, false),

%% Insert a key
ok = rocksdb:put(Db, <<"temp_key">>, <<"temp_value">>, []),

%% Key exists immediately
{ok, <<"temp_value">>} = rocksdb:get(Db, <<"temp_key">>, []),

%% Wait for TTL to expire
timer:sleep(2000),

%% Key may still exist (compaction hasn't run)
%% Force compaction to trigger cleanup
ok = rocksdb:compact_range(Db, <<"a">>, <<"z">>, []),

%% Now the key is gone
not_found = rocksdb:get(Db, <<"temp_key">>, []),

ok = rocksdb:close(Db).
```

### Read-Only Mode

```erlang
%% Open in read-only mode - no compactions will run
{ok, Db} = rocksdb:open_with_ttl("my_ttl_db", [], 3600, true),

%% Can read but not write
{ok, Value} = rocksdb:get(Db, <<"key">>, []),

%% Note: Expired keys won't be cleaned up in read-only mode
ok = rocksdb:close(Db).
```

## Column Family Support

### Opening with Multiple Column Families (each with its own TTL)

```erlang
%% Open with column families, each having a different TTL
{ok, Db, [DefaultCF, SessionsCF, CacheCF]} = rocksdb:open_with_ttl_cf(
    "multi_ttl_db",
    [{create_if_missing, true}],
    [
        {"default", [], 86400},      % 24 hours
        {"sessions", [], 3600},      % 1 hour
        {"cache", [], 300}           % 5 minutes
    ],
    false
).

%% Write to different column families
ok = rocksdb:put(Db, DefaultCF, <<"user:1">>, <<"data">>, []),
ok = rocksdb:put(Db, SessionsCF, <<"sess:abc">>, <<"token">>, []),
ok = rocksdb:put(Db, CacheCF, <<"cache:xyz">>, <<"cached">>, []),

ok = rocksdb:close(Db).
```

### Creating a Column Family with TTL

```erlang
%% First open a TTL database
{ok, Db} = rocksdb:open_with_ttl("my_db", [{create_if_missing, true}], 3600, false),

%% Create a new column family with a specific TTL
{ok, NewCF} = rocksdb:create_column_family_with_ttl(
    Db,
    "temp_data",
    [],      % Column family options
    600      % 10 minute TTL
),

%% Use the new column family
ok = rocksdb:put(Db, NewCF, <<"key">>, <<"value">>, []),

ok = rocksdb:close(Db).
```

### Getting and Setting TTL Dynamically

```erlang
{ok, Db, [DefaultCF]} = rocksdb:open_with_ttl_cf(
    "my_db",
    [{create_if_missing, true}],
    [{"default", [], 3600}],
    false
),

%% Get current TTL for a column family
{ok, CurrentTTL} = rocksdb:get_ttl(Db, DefaultCF),
io:format("Current TTL: ~p seconds~n", [CurrentTTL]),

%% Set a new TTL for the column family
ok = rocksdb:set_ttl(Db, DefaultCF, 7200),  % Change to 2 hours

%% Set default TTL for the database
ok = rocksdb:set_ttl(Db, 1800),  % 30 minutes

ok = rocksdb:close(Db).
```

## Alternative: Compaction Filter TTL

For more control over TTL behavior, you can use compaction filters with timestamp-based rules. This is useful when your keys contain embedded timestamps.

```erlang
%% TTL based on timestamp embedded in key
%% Format: {ttl_from_key, Offset, Length, TTLSeconds}
{ok, Db} = rocksdb:open("my_db", [
    {create_if_missing, true},
    {compaction_filter, #{
        rules => [{ttl_from_key, 0, 8, 3600}]  % Read 8 bytes at offset 0 as timestamp
    }}
]),

%% Create keys with embedded timestamps
Timestamp = erlang:system_time(second),
Key = <<Timestamp:64/big, "mydata">>,
ok = rocksdb:put(Db, Key, <<"value">>, []),

ok = rocksdb:close(Db).
```

See the [Compaction Filters Guide](compaction_filters.md) for more details.

## API Reference

### rocksdb:open_with_ttl/4

```erlang
-spec open_with_ttl(Name, DBOpts, TTL, ReadOnly) ->
    {ok, db_handle()} | {error, any()}.
```

Opens a database with TTL support.

| Parameter | Type | Description |
|-----------|------|-------------|
| Name | `file:filename_all()` | Path to the database directory |
| DBOpts | `db_options()` | Database options |
| TTL | `integer()` | Time-to-live in seconds (0 or negative = infinity) |
| ReadOnly | `boolean()` | If true, opens in read-only mode |

### rocksdb:open_with_ttl_cf/4

```erlang
-spec open_with_ttl_cf(Name, DBOpts, CFDescriptors, ReadOnly) ->
    {ok, db_handle(), [cf_handle()]} | {error, any()}.
```

Opens a database with multiple column families, each with its own TTL.

| Parameter | Type | Description |
|-----------|------|-------------|
| Name | `file:filename_all()` | Path to the database directory |
| DBOpts | `db_options()` | Database options |
| CFDescriptors | `[{Name, CFOpts, TTL}]` | List of column family descriptors with TTLs |
| ReadOnly | `boolean()` | If true, opens in read-only mode |

### rocksdb:create_column_family_with_ttl/4

```erlang
-spec create_column_family_with_ttl(DBHandle, Name, CFOpts, TTL) ->
    {ok, cf_handle()} | {error, any()}.
```

Creates a new column family with a specific TTL.

### rocksdb:get_ttl/2

```erlang
-spec get_ttl(DBHandle, CFHandle) -> {ok, integer()} | {error, any()}.
```

Gets the current TTL for a column family.

### rocksdb:set_ttl/2

```erlang
-spec set_ttl(DBHandle, TTL) -> ok | {error, any()}.
```

Sets the default TTL for the database.

### rocksdb:set_ttl/3

```erlang
-spec set_ttl(DBHandle, CFHandle, TTL) -> ok | {error, any()}.
```

Sets the TTL for a specific column family.

## Best Practices

1. **Trigger Compaction for Immediate Cleanup**: If you need expired keys removed immediately, call `rocksdb:compact_range/4`

2. **Use Appropriate TTL Values**: Very short TTLs (< 1 second) may cause excessive data churn

3. **Don't Mix TTL and Non-TTL Opens**: Always use `open_with_ttl` functions to access a TTL database. Using regular `open` will return corrupted values (with timestamp suffix)

4. **Consider Column Family TTLs**: Use different TTLs for different data types by organizing them into column families

5. **Monitor Disk Usage**: Expired keys consume disk space until compaction runs

## Warnings

- **Value Corruption**: Opening a TTL database with regular `rocksdb:open` will return corrupted values because of the timestamp suffix
- **Short TTLs**: Using very small TTL values may delete your entire database quickly
- **No Immediate Expiration**: TTL expiration is lazy - keys persist until compaction
