# Compaction Filters

Compaction filters allow you to modify or delete key-value pairs during RocksDB's compaction process. This is useful for implementing TTL-based expiration, removing obsolete data, or transforming values without reading and rewriting the entire database.

erlang-rocksdb provides two modes for compaction filters:

1. **Declarative Rules Mode** - Fast C++ execution for common patterns
2. **Erlang Callback Mode** - Full flexibility for complex logic

## Declarative Rules Mode

Declarative rules are processed entirely in C++ for maximum performance. Use this mode when your filtering logic matches one of the built-in patterns.

### Available Rules

| Rule | Description |
|------|-------------|
| `{key_prefix, Binary}` | Delete keys starting with the given prefix |
| `{key_suffix, Binary}` | Delete keys ending with the given suffix |
| `{key_contains, Binary}` | Delete keys containing the given pattern |
| `{value_empty}` | Delete keys with empty values |
| `{value_prefix, Binary}` | Delete keys whose values start with the given prefix |
| `{ttl_from_key, Offset, Length, TTLSeconds}` | Delete expired keys based on timestamp embedded in the key |
| `{always_delete}` | Delete all keys (use with caution) |

### Basic Example

```erlang
%% Delete all keys with "tmp_" prefix or "_expired" suffix
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_filter, #{
        rules => [
            {key_prefix, <<"tmp_">>},
            {key_suffix, <<"_expired">>}
        ]
    }}
]).

%% Write some data
ok = rocksdb:put(Db, <<"tmp_session123">>, <<"data">>, []),
ok = rocksdb:put(Db, <<"user_data">>, <<"important">>, []),
ok = rocksdb:put(Db, <<"cache_expired">>, <<"stale">>, []),

%% Force compaction to apply filters
ok = rocksdb:compact_range(Db, undefined, undefined, [
    {bottommost_level_compaction, force}
]),

%% After compaction:
%% - "tmp_session123" is deleted (matches prefix rule)
%% - "cache_expired" is deleted (matches suffix rule)
%% - "user_data" is kept (no rule matches)
```

### TTL from Key

The `ttl_from_key` rule extracts a timestamp from the key bytes and deletes the entry if it has expired.

```erlang
%% Keys have 8-byte big-endian timestamp prefix, TTL is 1 hour
{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_filter, #{
        rules => [{ttl_from_key, 0, 8, 3600}]  % offset=0, length=8, ttl=3600s
    }}
]).

%% Create a key with current timestamp
Timestamp = erlang:system_time(second),
Key = <<Timestamp:64/big, "mydata">>,
ok = rocksdb:put(Db, Key, <<"value">>, []),

%% After 1 hour and compaction, this key will be automatically deleted
```

### Multiple Rules

Multiple rules are evaluated in order. A key is deleted if ANY rule matches.

```erlang
{compaction_filter, #{
    rules => [
        {key_prefix, <<"tmp_">>},      % Temporary data
        {key_prefix, <<"cache_">>},    % Cache entries
        {key_suffix, <<"_old">>},      % Old versions
        {value_empty}                   % Empty values
    ]
}}
```

## Erlang Callback Mode

For complex filtering logic that can't be expressed with declarative rules, use the Erlang callback mode. Your Erlang process receives batches of keys during compaction and decides the fate of each one.

### Handler Process

```erlang
%% Start a filter handler process
start_filter_handler() ->
    spawn_link(fun filter_loop/0).

filter_loop() ->
    receive
        {compaction_filter, BatchRef, Keys} ->
            %% Keys = [{Level, Key, Value}, ...]
            Decisions = [decide(Key, Value) || {_Level, Key, Value} <- Keys],
            rocksdb:compaction_filter_reply(BatchRef, Decisions),
            filter_loop();
        stop ->
            ok
    end.

decide(Key, Value) ->
    case should_delete(Key, Value) of
        true -> remove;
        false -> keep
        %% Or modify the value:
        %% {change_value, NewValue}
    end.

should_delete(<<"expired_", _/binary>>, _Value) -> true;
should_delete(_Key, <<>>) -> true;  % Empty values
should_delete(_Key, _Value) -> false.
```

### Configuration Options

The `handler` must be a **PID** of a running Erlang process that will receive filter messages:

```erlang
%% Start the handler and get its PID
HandlerPid = start_filter_handler(),

{ok, Db} = rocksdb:open("mydb", [
    {create_if_missing, true},
    {compaction_filter, #{
        handler => HandlerPid,   % PID of the handler process
        batch_size => 100,       % Keys per batch (default: 100)
        timeout => 5000          % Timeout in ms (default: 5000)
    }}
]).
```

### Decision Types

The handler must reply with a list of decisions, one per key:

| Decision | Effect |
|----------|--------|
| `keep` | Keep the key-value pair unchanged |
| `remove` | Delete the key |
| `{change_value, NewBinary}` | Replace the value with NewBinary |

### Timeout and Error Handling

The compaction filter is designed to be robust:

- **Timeout**: If the handler doesn't respond within the timeout, all keys in the batch are kept (safe fallback)
- **Dead Handler**: If the handler process is dead, all keys are kept
- **Invalid Response**: Keys with invalid decisions are kept

This ensures that compaction never hangs or loses data due to handler issues.

### Example: Value Transformation

```erlang
filter_loop() ->
    receive
        {compaction_filter, BatchRef, Keys} ->
            Decisions = lists:map(fun({_Level, Key, Value}) ->
                case Key of
                    <<"compressed_", _/binary>> ->
                        %% Decompress old format
                        {change_value, zlib:uncompress(Value)};
                    <<"legacy_", _/binary>> ->
                        %% Remove legacy entries
                        remove;
                    _ ->
                        keep
                end
            end, Keys),
            rocksdb:compaction_filter_reply(BatchRef, Decisions),
            filter_loop()
    end.
```

## Forcing Compaction

Compaction filters only run during RocksDB's compaction process. To ensure your filters are applied to all data, use the `bottommost_level_compaction` option:

```erlang
%% Force compaction filter to run on ALL data, including bottommost level
ok = rocksdb:compact_range(Db, undefined, undefined, [
    {bottommost_level_compaction, force}
]).
```

### Bottommost Level Compaction Options

| Option | Description |
|--------|-------------|
| `skip` | Don't compact bottommost level |
| `if_have_compaction_filter` | Compact bottommost only if a filter is configured |
| `force` | Always compact bottommost level |
| `force_optimized` | Force with RocksDB optimizations |

## Performance Considerations

1. **Prefer Declarative Rules**: They run in C++ without Erlang process overhead
2. **Batch Size**: Larger batches reduce overhead but increase latency
3. **Timeout**: Set appropriately for your workload - too short causes false keeps
4. **Handler Process**: Use a dedicated process, don't block on slow operations

## Use Cases

- **TTL Expiration**: Delete data older than a certain age
- **Cache Cleanup**: Remove cache entries with specific prefixes
- **Data Migration**: Transform values from old formats during compaction
- **Soft Deletes**: Remove tombstone markers after a grace period
- **Compaction Filtering**: Remove entries that match certain patterns
