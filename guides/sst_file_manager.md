# SST File Manager

The SST File Manager provides control over disk space usage and SST file deletion rate limiting in RocksDB. It tracks all SST files across databases that share the manager, allowing you to:

- Track total disk space used by SST files
- Limit the maximum allowed disk space
- Rate limit SST file deletions to reduce I/O spikes
- Query which files are being tracked

## Creating an SST File Manager

```erlang
%% Create with default options
{ok, Env} = rocksdb:default_env(),
{ok, Mgr} = rocksdb:new_sst_file_manager(Env).

%% Create with options
Options = [
    {delete_rate_bytes_per_sec, 1048576},  %% 1 MB/s deletion rate limit
    {max_trash_db_ratio, 0.25},            %% Max ratio of trash to DB size
    {bytes_max_delete_chunk, 67108864}     %% 64 MB max delete chunk
],
{ok, Mgr} = rocksdb:new_sst_file_manager(Env, Options).
```

## Using with a Database

Pass the SST File Manager when opening a database:

```erlang
{ok, Env} = rocksdb:default_env(),
{ok, Mgr} = rocksdb:new_sst_file_manager(Env),

DbOptions = [
    {create_if_missing, true},
    {env, Env},
    {sst_file_manager, Mgr}
],
{ok, Db} = rocksdb:open("/path/to/db", DbOptions).
```

Multiple databases can share the same SST File Manager for unified disk space tracking:

```erlang
{ok, Db1} = rocksdb:open("/path/to/db1", [{sst_file_manager, Mgr} | BaseOpts]),
{ok, Db2} = rocksdb:open("/path/to/db2", [{sst_file_manager, Mgr} | BaseOpts]).
%% Both databases' SST files are tracked by the same manager
```

## Configuration Flags

Set configuration options at runtime using `sst_file_manager_flag/3`:

```erlang
%% Set maximum allowed disk space (bytes)
ok = rocksdb:sst_file_manager_flag(Mgr, max_allowed_space_usage, 10737418240).  %% 10 GB

%% Set buffer size reserved for compactions (bytes)
ok = rocksdb:sst_file_manager_flag(Mgr, compaction_buffer_size, 1073741824).    %% 1 GB

%% Set deletion rate limit (bytes per second, 0 = unlimited)
ok = rocksdb:sst_file_manager_flag(Mgr, delete_rate_bytes_per_sec, 1048576).    %% 1 MB/s

%% Set max ratio of trash to DB size
ok = rocksdb:sst_file_manager_flag(Mgr, max_trash_db_ratio, 0.5).
```

## Querying Information

Get information about the SST File Manager:

```erlang
%% Get all info as a proplist
Info = rocksdb:sst_file_manager_info(Mgr).
%% Returns:
%% [{total_size, 123456789},
%%  {delete_rate_bytes_per_sec, 1048576},
%%  {max_trash_db_ratio, 0.25},
%%  {total_trash_size, 0},
%%  {is_max_allowed_space_reached, false},
%%  {max_allowed_space_reached_including_compactions, false}]

%% Get specific item
TotalSize = rocksdb:sst_file_manager_info(Mgr, total_size).
IsMaxReached = rocksdb:sst_file_manager_info(Mgr, is_max_allowed_space_reached).
```

Available info items:
- `total_size` - Total size of all tracked SST files (bytes)
- `delete_rate_bytes_per_sec` - Current deletion rate limit
- `max_trash_db_ratio` - Max trash to DB size ratio
- `total_trash_size` - Size of files pending deletion
- `is_max_allowed_space_reached` - Whether max space limit is reached
- `max_allowed_space_reached_including_compactions` - Whether limit is reached including pending compactions

## Getting Tracked Files

Get a list of all SST files being tracked:

```erlang
TrackedFiles = rocksdb:sst_file_manager_tracked_files(Mgr).
%% Returns: [{<<"/path/to/db/000123.sst">>, 4567890}, ...]

%% Each element is {FilePath, SizeInBytes}
lists:foreach(fun({Path, Size}) ->
    io:format("File: ~s, Size: ~p bytes~n", [Path, Size])
end, TrackedFiles).
```

## Cleanup

Release the SST File Manager when done:

```erlang
ok = rocksdb:release_sst_file_manager(Mgr).
```

## Complete Example

```erlang
disk_space_management() ->
    {ok, Env} = rocksdb:default_env(),

    %% Create manager with rate limiting
    {ok, Mgr} = rocksdb:new_sst_file_manager(Env, [
        {delete_rate_bytes_per_sec, 10485760}  %% 10 MB/s
    ]),

    %% Set max allowed space to 50 GB
    ok = rocksdb:sst_file_manager_flag(Mgr, max_allowed_space_usage, 53687091200),

    %% Open database with manager
    {ok, Db} = rocksdb:open("/tmp/mydb", [
        {create_if_missing, true},
        {env, Env},
        {sst_file_manager, Mgr}
    ]),

    %% ... use database ...

    %% Monitor disk usage
    case rocksdb:sst_file_manager_info(Mgr, is_max_allowed_space_reached) of
        true ->
            io:format("Warning: Disk space limit reached!~n"),
            %% List all tracked files
            Files = rocksdb:sst_file_manager_tracked_files(Mgr),
            TotalSize = lists:sum([Size || {_, Size} <- Files]),
            io:format("Total tracked: ~p bytes in ~p files~n",
                      [TotalSize, length(Files)]);
        false ->
            ok
    end,

    ok = rocksdb:close(Db),
    ok = rocksdb:release_sst_file_manager(Mgr),
    ok = rocksdb:destroy_env(Env).
```
