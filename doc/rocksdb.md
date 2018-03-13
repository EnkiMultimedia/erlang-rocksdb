

# Module rocksdb #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

Erlang Wrapper for RocksDB.

<a name="types"></a>

## Data Types ##




### <a name="type-access_hint">access_hint()</a> ###


<pre><code>
access_hint() = normal | sequential | willneed | none
</code></pre>




### <a name="type-backup_engine">backup_engine()</a> ###


__abstract datatype__: `backup_engine()`




### <a name="type-backup_info">backup_info()</a> ###


<pre><code>
backup_info() = #{id =&gt; non_neg_integer(), timestamp =&gt; non_neg_integer(), size =&gt; non_neg_integer(), number_files =&gt; non_neg_integer()}
</code></pre>




### <a name="type-batch_handle">batch_handle()</a> ###


__abstract datatype__: `batch_handle()`




### <a name="type-block_based_table_options">block_based_table_options()</a> ###


<pre><code>
block_based_table_options() = [{no_block_cache, boolean()} | {block_size, pos_integer()} | {block_cache, <a href="#type-cache_handle">cache_handle()</a>} | {block_cache_size, pos_integer()} | {bloom_filter_policy, BitsPerKey::pos_integer()} | {format_version, 0 | 1 | 2} | {cache_index_and_filter_blocks, boolean()}]
</code></pre>




### <a name="type-cache_handle">cache_handle()</a> ###


__abstract datatype__: `cache_handle()`




### <a name="type-cf_handle">cf_handle()</a> ###


__abstract datatype__: `cf_handle()`




### <a name="type-cf_options">cf_options()</a> ###


<pre><code>
cf_options() = [{block_cache_size_mb_for_point_lookup, non_neg_integer()} | {memtable_memory_budget, pos_integer()} | {write_buffer_size, pos_integer()} | {max_write_buffer_number, pos_integer()} | {min_write_buffer_number_to_merge, pos_integer()} | {compression, <a href="#type-compression_type">compression_type()</a>} | {num_levels, pos_integer()} | {level0_file_num_compaction_trigger, integer()} | {level0_slowdown_writes_trigger, integer()} | {level0_stop_writes_trigger, integer()} | {max_mem_compaction_level, pos_integer()} | {target_file_size_base, pos_integer()} | {target_file_size_multiplier, pos_integer()} | {max_bytes_for_level_base, pos_integer()} | {max_bytes_for_level_multiplier, pos_integer()} | {max_compaction_bytes, pos_integer()} | {soft_rate_limit, float()} | {hard_rate_limit, float()} | {arena_block_size, integer()} | {disable_auto_compactions, boolean()} | {purge_redundant_kvs_while_flush, boolean()} | {compaction_style, <a href="#type-compaction_style">compaction_style()</a>} | {filter_deletes, boolean()} | {max_sequential_skip_in_iterations, pos_integer()} | {inplace_update_support, boolean()} | {inplace_update_num_locks, pos_integer()} | {table_factory_block_cache_size, pos_integer()} | {in_memory_mode, boolean()} | {block_based_table_options, <a href="#type-block_based_table_options">block_based_table_options()</a>} | {level_compaction_dynamic_level_bytes, boolean()}]
</code></pre>




### <a name="type-compaction_style">compaction_style()</a> ###


<pre><code>
compaction_style() = level | universal | fifo | none
</code></pre>




### <a name="type-compression_type">compression_type()</a> ###


<pre><code>
compression_type() = snappy | zlib | bzip2 | lz4 | lz4h | none
</code></pre>




### <a name="type-db_handle">db_handle()</a> ###


__abstract datatype__: `db_handle()`




### <a name="type-db_options">db_options()</a> ###


<pre><code>
db_options() = [{env, <a href="#type-env">env()</a>} | {total_threads, pos_integer()} | {create_if_missing, boolean()} | {create_missing_column_families, boolean()} | {error_if_exists, boolean()} | {paranoid_checks, boolean()} | {max_open_files, integer()} | {max_total_wal_size, non_neg_integer()} | {use_fsync, boolean()} | {db_paths, [#db_path{path = <a href="file.md#type-filename_all">file:filename_all()</a>, target_size = non_neg_integer()}]} | {db_log_dir, <a href="file.md#type-filename_all">file:filename_all()</a>} | {wal_dir, <a href="file.md#type-filename_all">file:filename_all()</a>} | {delete_obsolete_files_period_micros, pos_integer()} | {max_background_jobs, pos_integer()} | {max_background_compactions, pos_integer()} | {max_background_flushes, pos_integer()} | {max_log_file_size, non_neg_integer()} | {log_file_time_to_roll, non_neg_integer()} | {keep_log_file_num, pos_integer()} | {max_manifest_file_size, pos_integer()} | {table_cache_numshardbits, pos_integer()} | {wal_ttl_seconds, non_neg_integer()} | {wal_size_limit_mb, non_neg_integer()} | {manifest_preallocation_size, pos_integer()} | {allow_mmap_reads, boolean()} | {allow_mmap_writes, boolean()} | {is_fd_close_on_exec, boolean()} | {skip_log_error_on_recovery, boolean()} | {stats_dump_period_sec, non_neg_integer()} | {advise_random_on_open, boolean()} | {access_hint, <a href="#type-access_hint">access_hint()</a>} | {compaction_readahead_size, non_neg_integer()} | {use_adaptive_mutex, boolean()} | {bytes_per_sync, non_neg_integer()} | {skip_stats_update_on_db_open, boolean()} | {wal_recovery_mode, <a href="#type-wal_recovery_mode">wal_recovery_mode()</a>} | {allow_concurrent_memtable_write, boolean()} | {enable_write_thread_adaptive_yield, boolean()} | {db_write_buffer_size, non_neg_integer()} | {in_memory, boolean()} | {rate_limiter, <a href="#type-rate_limiter_handle">rate_limiter_handle()</a>}]
</code></pre>




### <a name="type-env">env()</a> ###


__abstract datatype__: `env()`




### <a name="type-env_handle">env_handle()</a> ###


__abstract datatype__: `env_handle()`




### <a name="type-env_priority">env_priority()</a> ###


<pre><code>
env_priority() = priority_high | priority_low
</code></pre>




### <a name="type-fold_fun">fold_fun()</a> ###


<pre><code>
fold_fun() = fun(({Key::binary(), Value::binary()}, any()) -&gt; any())
</code></pre>




### <a name="type-fold_keys_fun">fold_keys_fun()</a> ###


<pre><code>
fold_keys_fun() = fun((Key::binary(), any()) -&gt; any())
</code></pre>




### <a name="type-iterator_action">iterator_action()</a> ###


<pre><code>
iterator_action() = first | last | next | prev | binary() | {seek, binary()} | {seek_for_prev, binary()}
</code></pre>




### <a name="type-itr_handle">itr_handle()</a> ###


__abstract datatype__: `itr_handle()`




### <a name="type-options">options()</a> ###


<pre><code>
options() = <a href="#type-db_options">db_options()</a> | <a href="#type-cf_options">cf_options()</a>
</code></pre>




### <a name="type-rate_limiter_handle">rate_limiter_handle()</a> ###


__abstract datatype__: `rate_limiter_handle()`




### <a name="type-read_options">read_options()</a> ###


<pre><code>
read_options() = [{verify_checksums, boolean()} | {fill_cache, boolean()} | {iterate_upper_bound, binary()} | {tailing, boolean()} | {total_order_seek, boolean()} | {snapshot, <a href="#type-snapshot_handle">snapshot_handle()</a>}]
</code></pre>




### <a name="type-snapshot_handle">snapshot_handle()</a> ###


__abstract datatype__: `snapshot_handle()`




### <a name="type-wal_recovery_mode">wal_recovery_mode()</a> ###


<pre><code>
wal_recovery_mode() = tolerate_corrupted_tail_records | absolute_consistency | point_in_time_recovery | skip_any_corrupted_records
</code></pre>




### <a name="type-write_actions">write_actions()</a> ###


<pre><code>
write_actions() = [{put, Key::binary(), Value::binary()} | {put, ColumnFamilyHandle::<a href="#type-cf_handle">cf_handle()</a>, Key::binary(), Value::binary()} | {delete, Key::binary()} | {delete, ColumnFamilyHandle::<a href="#type-cf_handle">cf_handle()</a>, Key::binary()} | {single_delete, Key::binary()} | {single_delete, ColumnFamilyHandle::<a href="#type-cf_handle">cf_handle()</a>, Key::binary()} | clear]
</code></pre>




### <a name="type-write_options">write_options()</a> ###


<pre><code>
write_options() = [{sync, boolean()} | {disable_wal, boolean()} | {ignore_missing_column_families, boolean()}]
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#batch-0">batch/0</a></td><td>create a new batch in memory.</td></tr><tr><td valign="top"><a href="#batch_clear-1">batch_clear/1</a></td><td>reset the batch, clear all operations.</td></tr><tr><td valign="top"><a href="#batch_count-1">batch_count/1</a></td><td>return the number of operations in the batch.</td></tr><tr><td valign="top"><a href="#batch_delete-2">batch_delete/2</a></td><td>batch implementation of delete operation to the batch.</td></tr><tr><td valign="top"><a href="#batch_delete-3">batch_delete/3</a></td><td>like <code>batch_delete/2</code> but apply the operation to a column family.</td></tr><tr><td valign="top"><a href="#batch_put-3">batch_put/3</a></td><td>add a put operation to the batch.</td></tr><tr><td valign="top"><a href="#batch_put-4">batch_put/4</a></td><td>like <code>put/3</code> but apply the operation to a column family.</td></tr><tr><td valign="top"><a href="#batch_rollback-1">batch_rollback/1</a></td><td>rollback the operations to the latest checkpoint.</td></tr><tr><td valign="top"><a href="#batch_savepoint-1">batch_savepoint/1</a></td><td>store a checkpoint in the batch to which you can rollback later.</td></tr><tr><td valign="top"><a href="#batch_single_delete-2">batch_single_delete/2</a></td><td>batch implementation of single_delete operation to the batch.</td></tr><tr><td valign="top"><a href="#batch_single_delete-3">batch_single_delete/3</a></td><td>like <code>batch_single_delete/2</code> but apply the operation to a column family.</td></tr><tr><td valign="top"><a href="#batch_tolist-1">batch_tolist/1</a></td><td>return all the operation sin the batch as a list of operations.</td></tr><tr><td valign="top"><a href="#checkpoint-2">checkpoint/2</a></td><td>take a snapshot of a running RocksDB database in a separate directory
http://rocksdb.org/blog/2609/use-checkpoints-for-efficient-snapshots/.</td></tr><tr><td valign="top"><a href="#close-1">close/1</a></td><td>Close RocksDB.</td></tr><tr><td valign="top"><a href="#close_backup-1">close_backup/1</a></td><td>stop and close the backup
note: experimental for testing only.</td></tr><tr><td valign="top"><a href="#close_batch-1">close_batch/1</a></td><td></td></tr><tr><td valign="top"><a href="#close_updates_iterator-1">close_updates_iterator/1</a></td><td>close the transaction log.</td></tr><tr><td valign="top"><a href="#count-1">count/1</a></td><td>Return the approximate number of keys in the default column family.</td></tr><tr><td valign="top"><a href="#count-2">count/2</a></td><td>
Return the approximate number of keys in the specified column family.</td></tr><tr><td valign="top"><a href="#create_column_family-3">create_column_family/3</a></td><td>Create a new column family.</td></tr><tr><td valign="top"><a href="#create_new_backup-2">create_new_backup/2</a></td><td>Captures the state of the database in the latest backup.</td></tr><tr><td valign="top"><a href="#default_env-0">default_env/0</a></td><td>return a default db environment.</td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td>Delete a key/value pair in the default column family.</td></tr><tr><td valign="top"><a href="#delete-4">delete/4</a></td><td>Delete a key/value pair in the specified column family.</td></tr><tr><td valign="top"><a href="#delete_backup-2">delete_backup/2</a></td><td>deletes a specific backup.</td></tr><tr><td valign="top"><a href="#delete_range-4">delete_range/4</a></td><td>Removes the database entries in the range ["BeginKey", "EndKey"), i.e.,
including "BeginKey" and excluding "EndKey".</td></tr><tr><td valign="top"><a href="#delete_range-5">delete_range/5</a></td><td>Removes the database entries in the range ["BeginKey", "EndKey").</td></tr><tr><td valign="top"><a href="#destroy-2">destroy/2</a></td><td>Destroy the contents of the specified database.</td></tr><tr><td valign="top"><a href="#destroy_column_family-1">destroy_column_family/1</a></td><td>Destroy a column family.</td></tr><tr><td valign="top"><a href="#destroy_env-1">destroy_env/1</a></td><td>destroy an environment.</td></tr><tr><td valign="top"><a href="#drop_column_family-1">drop_column_family/1</a></td><td>Drop a column family.</td></tr><tr><td valign="top"><a href="#flush-1">flush/1</a></td><td>Flush all mem-table data.</td></tr><tr><td valign="top"><a href="#flush-2">flush/2</a></td><td>Flush all mem-table data.</td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td>Calls Fun(Elem, AccIn) on successive elements in the default column family
starting with AccIn == Acc0.</td></tr><tr><td valign="top"><a href="#fold-5">fold/5</a></td><td>Calls Fun(Elem, AccIn) on successive elements in the specified column family
Other specs are same with fold/4.</td></tr><tr><td valign="top"><a href="#fold_keys-4">fold_keys/4</a></td><td>Calls Fun(Elem, AccIn) on successive elements in the default column family
starting with AccIn == Acc0.</td></tr><tr><td valign="top"><a href="#fold_keys-5">fold_keys/5</a></td><td>Calls Fun(Elem, AccIn) on successive elements in the specified column family
Other specs are same with fold_keys/4.</td></tr><tr><td valign="top"><a href="#garbage_collect_backup-1">garbage_collect_backup/1</a></td><td> Will delete all the files we don't need anymore
It will do the full scan of the files/ directory and delete all the
files that are not referenced.</td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td>Retrieve a key/value pair in the default column family.</td></tr><tr><td valign="top"><a href="#get-4">get/4</a></td><td>Retrieve a key/value pair in the specified column family.</td></tr><tr><td valign="top"><a href="#get_backup_info-1">get_backup_info/1</a></td><td>Returns info about backups in backup_info.</td></tr><tr><td valign="top"><a href="#get_capacity-1">get_capacity/1</a></td><td> returns the maximum configured capacity of the cache.</td></tr><tr><td valign="top"><a href="#get_latest_sequence_number-1">get_latest_sequence_number/1</a></td><td>get latest sequence from the log.</td></tr><tr><td valign="top"><a href="#get_pinned_usage-1">get_pinned_usage/1</a></td><td> returns the memory size for the entries in use by the system.</td></tr><tr><td valign="top"><a href="#get_property-2">get_property/2</a></td><td>Return the RocksDB internal status of the default column family specified at Property.</td></tr><tr><td valign="top"><a href="#get_property-3">get_property/3</a></td><td>Return the RocksDB internal status of the specified column family specified at Property.</td></tr><tr><td valign="top"><a href="#get_snapshot_sequence-1">get_snapshot_sequence/1</a></td><td>returns Snapshot's sequence number.</td></tr><tr><td valign="top"><a href="#get_usage-1">get_usage/1</a></td><td>returns the memory size for a specific entry in the cache.</td></tr><tr><td valign="top"><a href="#is_empty-1">is_empty/1</a></td><td>is the database empty.</td></tr><tr><td valign="top"><a href="#iterator-2">iterator/2</a></td><td>Return a iterator over the contents of the database.</td></tr><tr><td valign="top"><a href="#iterator-3">iterator/3</a></td><td>Return a iterator over the contents of the database.</td></tr><tr><td valign="top"><a href="#iterator_close-1">iterator_close/1</a></td><td>
Close a iterator.</td></tr><tr><td valign="top"><a href="#iterator_move-2">iterator_move/2</a></td><td>
Move to the specified place.</td></tr><tr><td valign="top"><a href="#iterator_refresh-1">iterator_refresh/1</a></td><td>
Refresh iterator.</td></tr><tr><td valign="top"><a href="#iterators-3">iterators/3</a></td><td>
Return a iterator over the contents of the specified column family.</td></tr><tr><td valign="top"><a href="#list_column_families-2">list_column_families/2</a></td><td>List column families.</td></tr><tr><td valign="top"><a href="#mem_env-0">mem_env/0</a></td><td>return a memory environment.</td></tr><tr><td valign="top"><a href="#new_clock_cache-1">new_clock_cache/1</a></td><td>Similar to NewLRUCache, but create a cache based on CLOCK algorithm with
better concurrent performance in some cases.</td></tr><tr><td valign="top"><a href="#new_lru_cache-1">new_lru_cache/1</a></td><td>// Create a new cache with a fixed size capacity.</td></tr><tr><td valign="top"><a href="#new_rate_limiter-2">new_rate_limiter/2</a></td><td>create new Limiter.</td></tr><tr><td valign="top"><a href="#next_binary_update-1">next_binary_update/1</a></td><td>go to the last update as a binary in the transaction log, can be ussed with the write_binary_update function.</td></tr><tr><td valign="top"><a href="#next_update-1">next_update/1</a></td><td>like binary update but also return the batch as a list of operations.</td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td>Open RocksDB with the defalut column family.</td></tr><tr><td valign="top"><a href="#open_backup_engine-1">open_backup_engine/1</a></td><td>open a new backup engine.</td></tr><tr><td valign="top"><a href="#open_with_cf-3">open_with_cf/3</a></td><td>Open RocksDB with the specified column families.</td></tr><tr><td valign="top"><a href="#purge_old_backup-2">purge_old_backup/2</a></td><td>deletes old backups, keeping latest num_backups_to_keep alive.</td></tr><tr><td valign="top"><a href="#put-4">put/4</a></td><td>Put a key/value pair into the default column family.</td></tr><tr><td valign="top"><a href="#put-5">put/5</a></td><td>Put a key/value pair into the specified column family.</td></tr><tr><td valign="top"><a href="#release_cache-1">release_cache/1</a></td><td>release the cache.</td></tr><tr><td valign="top"><a href="#release_rate_limiter-1">release_rate_limiter/1</a></td><td>release the limiter.</td></tr><tr><td valign="top"><a href="#release_snapshot-1">release_snapshot/1</a></td><td>release a snapshot.</td></tr><tr><td valign="top"><a href="#repair-2">repair/2</a></td><td>Try to repair as much of the contents of the database as possible.</td></tr><tr><td valign="top"><a href="#restore_db_from_backup-3">restore_db_from_backup/3</a></td><td>restore from backup with backup_id.</td></tr><tr><td valign="top"><a href="#restore_db_from_backup-4">restore_db_from_backup/4</a></td><td>restore from backup with backup_id.</td></tr><tr><td valign="top"><a href="#restore_db_from_latest_backup-2">restore_db_from_latest_backup/2</a></td><td>restore from the latest backup.</td></tr><tr><td valign="top"><a href="#restore_db_from_latest_backup-3">restore_db_from_latest_backup/3</a></td><td>restore from the latest backup.</td></tr><tr><td valign="top"><a href="#set_capacity-2">set_capacity/2</a></td><td>sets the maximum configured capacity of the cache.</td></tr><tr><td valign="top"><a href="#set_db_background_threads-2">set_db_background_threads/2</a></td><td>set background threads of a database.</td></tr><tr><td valign="top"><a href="#set_db_background_threads-3">set_db_background_threads/3</a></td><td>set database background threads of low and high prioriry threads pool of an environment
Flush threads are in the HIGH priority pool, while compaction threads are in the
LOW priority pool.</td></tr><tr><td valign="top"><a href="#set_env_background_threads-2">set_env_background_threads/2</a></td><td>set background threads of an environment.</td></tr><tr><td valign="top"><a href="#set_env_background_threads-3">set_env_background_threads/3</a></td><td>set background threads of low and high prioriry threads pool of an environment
Flush threads are in the HIGH priority pool, while compaction threads are in the
LOW priority pool.</td></tr><tr><td valign="top"><a href="#single_delete-3">single_delete/3</a></td><td>Remove the database entry for "key".</td></tr><tr><td valign="top"><a href="#single_delete-4">single_delete/4</a></td><td>like <code>single_delete/3</code> but on the specified column family.</td></tr><tr><td valign="top"><a href="#snapshot-1">snapshot/1</a></td><td>return a database snapshot
Snapshots provide consistent read-only views over the entire state of the key-value store.</td></tr><tr><td valign="top"><a href="#stats-1">stats/1</a></td><td>Return the current stats of the default column family
Implemented by calling GetProperty with "rocksdb.stats".</td></tr><tr><td valign="top"><a href="#stats-2">stats/2</a></td><td>Return the current stats of the specified column family
Implemented by calling GetProperty with "rocksdb.stats".</td></tr><tr><td valign="top"><a href="#stop_backup-1">stop_backup/1</a></td><td></td></tr><tr><td valign="top"><a href="#sync_wal-1">sync_wal/1</a></td><td> Sync the wal.</td></tr><tr><td valign="top"><a href="#updates_iterator-2">updates_iterator/2</a></td><td>create a new iterator to retrive ethe transaction log since a sequce.</td></tr><tr><td valign="top"><a href="#verify_backup-2">verify_backup/2</a></td><td>checks that each file exists and that the size of the file matches
our expectations.</td></tr><tr><td valign="top"><a href="#write-3">write/3</a></td><td>Apply the specified updates to the database.</td></tr><tr><td valign="top"><a href="#write_batch-3">write_batch/3</a></td><td>write the batch to the database.</td></tr><tr><td valign="top"><a href="#write_binary_update-3">write_binary_update/3</a></td><td>apply a set of operation coming from a transaction log to another database.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="batch-0"></a>

### batch/0 ###

<pre><code>
batch() -&gt; {ok, Batch::<a href="#type-batch_handle">batch_handle()</a>}
</code></pre>
<br />

create a new batch in memory. A batch is a nif resource attached to the current process. Pay attention when you
share it with other processes as it may not been released. To force its release you will need to use the close_batch
function.

<a name="batch_clear-1"></a>

### batch_clear/1 ###

<pre><code>
batch_clear(Batch::<a href="#type-batch_handle">batch_handle()</a>) -&gt; ok
</code></pre>
<br />

reset the batch, clear all operations.

<a name="batch_count-1"></a>

### batch_count/1 ###

<pre><code>
batch_count(_Batch::<a href="#type-batch_handle">batch_handle()</a>) -&gt; Count::non_neg_integer()
</code></pre>
<br />

return the number of operations in the batch

<a name="batch_delete-2"></a>

### batch_delete/2 ###

<pre><code>
batch_delete(Batch::<a href="#type-batch_handle">batch_handle()</a>, Key::binary()) -&gt; ok
</code></pre>
<br />

batch implementation of delete operation to the batch

<a name="batch_delete-3"></a>

### batch_delete/3 ###

<pre><code>
batch_delete(Batch::<a href="#type-batch_handle">batch_handle()</a>, ColumnFamily::<a href="#type-cf_handle">cf_handle()</a>, Key::binary()) -&gt; ok
</code></pre>
<br />

like `batch_delete/2` but apply the operation to a column family

<a name="batch_put-3"></a>

### batch_put/3 ###

<pre><code>
batch_put(Batch::<a href="#type-batch_handle">batch_handle()</a>, Key::binary(), Value::binary()) -&gt; ok
</code></pre>
<br />

add a put operation to the batch

<a name="batch_put-4"></a>

### batch_put/4 ###

<pre><code>
batch_put(Batch::<a href="#type-batch_handle">batch_handle()</a>, ColumnFamily::<a href="#type-cf_handle">cf_handle()</a>, Key::binary(), Value::binary()) -&gt; ok
</code></pre>
<br />

like `put/3` but apply the operation to a column family

<a name="batch_rollback-1"></a>

### batch_rollback/1 ###

<pre><code>
batch_rollback(Batch::<a href="#type-batch_handle">batch_handle()</a>) -&gt; ok
</code></pre>
<br />

rollback the operations to the latest checkpoint

<a name="batch_savepoint-1"></a>

### batch_savepoint/1 ###

<pre><code>
batch_savepoint(Batch::<a href="#type-batch_handle">batch_handle()</a>) -&gt; ok
</code></pre>
<br />

store a checkpoint in the batch to which you can rollback later

<a name="batch_single_delete-2"></a>

### batch_single_delete/2 ###

<pre><code>
batch_single_delete(Batch::<a href="#type-batch_handle">batch_handle()</a>, Key::binary()) -&gt; ok
</code></pre>
<br />

batch implementation of single_delete operation to the batch

<a name="batch_single_delete-3"></a>

### batch_single_delete/3 ###

<pre><code>
batch_single_delete(Batch::<a href="#type-batch_handle">batch_handle()</a>, ColumnFamily::<a href="#type-cf_handle">cf_handle()</a>, Key::binary()) -&gt; ok
</code></pre>
<br />

like `batch_single_delete/2` but apply the operation to a column family

<a name="batch_tolist-1"></a>

### batch_tolist/1 ###

<pre><code>
batch_tolist(Batch::<a href="#type-batch_handle">batch_handle()</a>) -&gt; Ops::<a href="#type-write_actions">write_actions()</a>
</code></pre>
<br />

return all the operation sin the batch as a list of operations

<a name="checkpoint-2"></a>

### checkpoint/2 ###

<pre><code>
checkpoint(DbHandle::<a href="#type-db_handle">db_handle()</a>, Path::<a href="file.md#type-filename_all">file:filename_all()</a>) -&gt; ok | {error, any()}
</code></pre>
<br />

take a snapshot of a running RocksDB database in a separate directory
http://rocksdb.org/blog/2609/use-checkpoints-for-efficient-snapshots/

<a name="close-1"></a>

### close/1 ###

<pre><code>
close(DBHandle) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Close RocksDB

<a name="close_backup-1"></a>

### close_backup/1 ###

<pre><code>
close_backup(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>) -&gt; ok
</code></pre>
<br />

stop and close the backup
note: experimental for testing only

<a name="close_batch-1"></a>

### close_batch/1 ###

<pre><code>
close_batch(Batch::<a href="#type-batch_handle">batch_handle()</a>) -&gt; ok
</code></pre>
<br />

<a name="close_updates_iterator-1"></a>

### close_updates_iterator/1 ###

`close_updates_iterator(Iterator) -> any()`

close the transaction log

<a name="count-1"></a>

### count/1 ###

<pre><code>
count(DBHandle::<a href="#type-db_handle">db_handle()</a>) -&gt; non_neg_integer() | {error, any()}
</code></pre>
<br />

Return the approximate number of keys in the default column family.
Implemented by calling GetIntProperty with "rocksdb.estimate-num-keys"

<a name="count-2"></a>

### count/2 ###

<pre><code>
count(DBHandle::<a href="#type-db_handle">db_handle()</a>, CFHandle::<a href="#type-cf_handle">cf_handle()</a>) -&gt; non_neg_integer() | {error, any()}
</code></pre>
<br />

Return the approximate number of keys in the specified column family.

<a name="create_column_family-3"></a>

### create_column_family/3 ###

<pre><code>
create_column_family(DBHandle, Name, CFOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Name = string()</code></li><li><code>CFOpts = <a href="#type-cf_options">cf_options()</a></code></li><li><code>Res = {ok, <a href="#type-cf_handle">cf_handle()</a>} | {error, any()}</code></li></ul>

Create a new column family

<a name="create_new_backup-2"></a>

### create_new_backup/2 ###

<pre><code>
create_new_backup(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>, Db::<a href="#type-db_handle">db_handle()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

Captures the state of the database in the latest backup

<a name="default_env-0"></a>

### default_env/0 ###

<pre><code>
default_env() -&gt; {ok, <a href="#type-env_handle">env_handle()</a>}
</code></pre>
<br />

return a default db environment

<a name="delete-3"></a>

### delete/3 ###

<pre><code>
delete(DBHandle, Key, WriteOpts) -&gt; ok | {error, any()}
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li></ul>

Delete a key/value pair in the default column family

<a name="delete-4"></a>

### delete/4 ###

<pre><code>
delete(DBHandle, CFHandle, Key, WriteOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Delete a key/value pair in the specified column family

<a name="delete_backup-2"></a>

### delete_backup/2 ###

<pre><code>
delete_backup(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>, BackupId::non_neg_integer()) -&gt; ok | {error, any()}
</code></pre>
<br />

deletes a specific backup

<a name="delete_range-4"></a>

### delete_range/4 ###

<pre><code>
delete_range(DBHandle, BeginKey, EndKey, WriteOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>BeginKey = binary()</code></li><li><code>EndKey = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Removes the database entries in the range ["BeginKey", "EndKey"), i.e.,
including "BeginKey" and excluding "EndKey". Returns OK on success, and
a non-OK status on error. It is not an error if no keys exist in the range
["BeginKey", "EndKey").

This feature is currently an experimental performance optimization for
deleting very large ranges of contiguous keys. Invoking it many times or on
small ranges may severely degrade read performance; in particular, the
resulting performance can be worse than calling Delete() for each key in
the range. Note also the degraded read performance affects keys outside the
deleted ranges, and affects database operations involving scans, like flush
and compaction.

Consider setting ReadOptions::ignore_range_deletions = true to speed
up reads for key(s) that are known to be unaffected by range deletions.

<a name="delete_range-5"></a>

### delete_range/5 ###

<pre><code>
delete_range(DBHandle, CFHandle, BeginKey, EndKey, WriteOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>BeginKey = binary()</code></li><li><code>EndKey = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Removes the database entries in the range ["BeginKey", "EndKey").
like `delete_range/3` but for a column family

<a name="destroy-2"></a>

### destroy/2 ###

<pre><code>
destroy(Name::<a href="file.md#type-filename_all">file:filename_all()</a>, DBOpts::<a href="#type-db_options">db_options()</a>) -&gt; ok | {error, any()}
</code></pre>
<br />

Destroy the contents of the specified database.
Be very careful using this method.

<a name="destroy_column_family-1"></a>

### destroy_column_family/1 ###

<pre><code>
destroy_column_family(CFHandle) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Destroy a column family

<a name="destroy_env-1"></a>

### destroy_env/1 ###

<pre><code>
destroy_env(Env::<a href="#type-env_handle">env_handle()</a>) -&gt; ok
</code></pre>
<br />

destroy an environment

<a name="drop_column_family-1"></a>

### drop_column_family/1 ###

<pre><code>
drop_column_family(CFHandle) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Drop a column family

<a name="flush-1"></a>

### flush/1 ###

<pre><code>
flush(DbHandle::<a href="#type-db_handle">db_handle()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

Flush all mem-table data.

<a name="flush-2"></a>

### flush/2 ###

<pre><code>
flush(DbHandle::<a href="#type-db_handle">db_handle()</a>, CfHandle::<a href="#type-cf_handle">cf_handle()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

Flush all mem-table data.

<a name="fold-4"></a>

### fold/4 ###

<pre><code>
fold(DBHandle, Fun, AccIn, ReadOpts) -&gt; AccOut
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Fun = <a href="#type-fold_fun">fold_fun()</a></code></li><li><code>AccIn = any()</code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>AccOut = any()</code></li></ul>

Calls Fun(Elem, AccIn) on successive elements in the default column family
starting with AccIn == Acc0.
Fun/2 must return a new accumulator which is passed to the next call.
The function returns the final value of the accumulator.
Acc0 is returned if the default column family is empty.

<a name="fold-5"></a>

### fold/5 ###

<pre><code>
fold(DBHandle, CFHandle, Fun, AccIn, ReadOpts) -&gt; AccOut
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Fun = <a href="#type-fold_fun">fold_fun()</a></code></li><li><code>AccIn = any()</code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>AccOut = any()</code></li></ul>

Calls Fun(Elem, AccIn) on successive elements in the specified column family
Other specs are same with fold/4

<a name="fold_keys-4"></a>

### fold_keys/4 ###

<pre><code>
fold_keys(DBHandle, Fun, AccIn, ReadOpts) -&gt; AccOut
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Fun = <a href="#type-fold_keys_fun">fold_keys_fun()</a></code></li><li><code>AccIn = any()</code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>AccOut = any()</code></li></ul>

Calls Fun(Elem, AccIn) on successive elements in the default column family
starting with AccIn == Acc0.
Fun/2 must return a new accumulator which is passed to the next call.
The function returns the final value of the accumulator.
Acc0 is returned if the default column family is empty.

<a name="fold_keys-5"></a>

### fold_keys/5 ###

<pre><code>
fold_keys(DBHandle, CFHandle, Fun, AccIn, ReadOpts) -&gt; AccOut
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Fun = <a href="#type-fold_keys_fun">fold_keys_fun()</a></code></li><li><code>AccIn = any()</code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>AccOut = any()</code></li></ul>

Calls Fun(Elem, AccIn) on successive elements in the specified column family
Other specs are same with fold_keys/4

<a name="garbage_collect_backup-1"></a>

### garbage_collect_backup/1 ###

<pre><code>
garbage_collect_backup(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>) -&gt; ok
</code></pre>
<br />

Will delete all the files we don't need anymore
It will do the full scan of the files/ directory and delete all the
files that are not referenced.

<a name="get-3"></a>

### get/3 ###

<pre><code>
get(DBHandle, Key, ReadOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>Res = {ok, binary()} | not_found | {error, any()}</code></li></ul>

Retrieve a key/value pair in the default column family

<a name="get-4"></a>

### get/4 ###

<pre><code>
get(DBHandle, CFHandle, Key, ReadOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>Res = {ok, binary()} | not_found | {error, any()}</code></li></ul>

Retrieve a key/value pair in the specified column family

<a name="get_backup_info-1"></a>

### get_backup_info/1 ###

<pre><code>
get_backup_info(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>) -&gt; [<a href="#type-backup_info">backup_info()</a>]
</code></pre>
<br />

Returns info about backups in backup_info

<a name="get_capacity-1"></a>

### get_capacity/1 ###

<pre><code>
get_capacity(Cache::<a href="#type-cache_handle">cache_handle()</a>) -&gt; non_neg_integer()
</code></pre>
<br />

returns the maximum configured capacity of the cache.

<a name="get_latest_sequence_number-1"></a>

### get_latest_sequence_number/1 ###

<pre><code>
get_latest_sequence_number(Db::<a href="#type-db_handle">db_handle()</a>) -&gt; Seq::non_neg_integer()
</code></pre>
<br />

get latest sequence from the log

<a name="get_pinned_usage-1"></a>

### get_pinned_usage/1 ###

<pre><code>
get_pinned_usage(Cache::<a href="#type-cache_handle">cache_handle()</a>) -&gt; non_neg_integer()
</code></pre>
<br />

returns the memory size for the entries in use by the system

<a name="get_property-2"></a>

### get_property/2 ###

<pre><code>
get_property(DBHandle::<a href="#type-db_handle">db_handle()</a>, Property::binary()) -&gt; {ok, any()} | {error, any()}
</code></pre>
<br />

Return the RocksDB internal status of the default column family specified at Property

<a name="get_property-3"></a>

### get_property/3 ###

<pre><code>
get_property(DBHandle::<a href="#type-db_handle">db_handle()</a>, CFHandle::<a href="#type-cf_handle">cf_handle()</a>, Property::binary()) -&gt; string() | {error, any()}
</code></pre>
<br />

Return the RocksDB internal status of the specified column family specified at Property

<a name="get_snapshot_sequence-1"></a>

### get_snapshot_sequence/1 ###

<pre><code>
get_snapshot_sequence(SnapshotHandle::<a href="#type-snapshot_handle">snapshot_handle()</a>) -&gt; Sequence::non_neg_integer()
</code></pre>
<br />

returns Snapshot's sequence number

<a name="get_usage-1"></a>

### get_usage/1 ###

<pre><code>
get_usage(Cache::<a href="#type-cache_handle">cache_handle()</a>) -&gt; non_neg_integer()
</code></pre>
<br />

returns the memory size for a specific entry in the cache.

<a name="is_empty-1"></a>

### is_empty/1 ###

<pre><code>
is_empty(DBHandle::<a href="#type-db_handle">db_handle()</a>) -&gt; true | false
</code></pre>
<br />

is the database empty

<a name="iterator-2"></a>

### iterator/2 ###

<pre><code>
iterator(DBHandle, ReadOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>Res = {ok, <a href="#type-itr_handle">itr_handle()</a>} | {error, any()}</code></li></ul>

Return a iterator over the contents of the database.
The result of iterator() is initially invalid (caller must
call iterator_move function on the iterator before using it).

<a name="iterator-3"></a>

### iterator/3 ###

<pre><code>
iterator(DBHandle, CFHandle, ReadOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li><li><code>Res = {ok, <a href="#type-itr_handle">itr_handle()</a>} | {error, any()}</code></li></ul>

Return a iterator over the contents of the database.
The result of iterator() is initially invalid (caller must
call iterator_move function on the iterator before using it).

<a name="iterator_close-1"></a>

### iterator_close/1 ###

<pre><code>
iterator_close(ITRHandle) -&gt; ok
</code></pre>

<ul class="definitions"><li><code>ITRHandle = <a href="#type-itr_handle">itr_handle()</a></code></li></ul>

Close a iterator

<a name="iterator_move-2"></a>

### iterator_move/2 ###

<pre><code>
iterator_move(ITRHandle, ITRAction) -&gt; {ok, Key::binary(), Value::binary()} | {ok, Key::binary()} | {error, invalid_iterator} | {error, iterator_closed}
</code></pre>

<ul class="definitions"><li><code>ITRHandle = <a href="#type-itr_handle">itr_handle()</a></code></li><li><code>ITRAction = <a href="#type-iterator_action">iterator_action()</a></code></li></ul>

Move to the specified place

<a name="iterator_refresh-1"></a>

### iterator_refresh/1 ###

<pre><code>
iterator_refresh(ITRHandle) -&gt; ok
</code></pre>

<ul class="definitions"><li><code>ITRHandle = <a href="#type-itr_handle">itr_handle()</a></code></li></ul>

Refresh iterator

<a name="iterators-3"></a>

### iterators/3 ###

<pre><code>
iterators(DBHandle, CFHandle, ReadOpts) -&gt; {ok, <a href="#type-itr_handle">itr_handle()</a>} | {error, any()}
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>ReadOpts = <a href="#type-read_options">read_options()</a></code></li></ul>

Return a iterator over the contents of the specified column family.

<a name="list_column_families-2"></a>

### list_column_families/2 ###

<pre><code>
list_column_families(Name, DBOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>Name = <a href="file.md#type-filename_all">file:filename_all()</a></code></li><li><code>DBOpts = <a href="#type-db_options">db_options()</a></code></li><li><code>Res = {ok, [string()]} | {error, any()}</code></li></ul>

List column families

<a name="mem_env-0"></a>

### mem_env/0 ###

<pre><code>
mem_env() -&gt; {ok, <a href="#type-env_handle">env_handle()</a>}
</code></pre>
<br />

return a memory environment

<a name="new_clock_cache-1"></a>

### new_clock_cache/1 ###

<pre><code>
new_clock_cache(Capacity::non_neg_integer()) -&gt; {ok, <a href="#type-cache_handle">cache_handle()</a>}
</code></pre>
<br />

Similar to NewLRUCache, but create a cache based on CLOCK algorithm with
better concurrent performance in some cases. See util/clock_cache.cc for
more detail.

<a name="new_lru_cache-1"></a>

### new_lru_cache/1 ###

<pre><code>
new_lru_cache(Capacity::non_neg_integer()) -&gt; {ok, <a href="#type-cache_handle">cache_handle()</a>}
</code></pre>
<br />

// Create a new cache with a fixed size capacity. The cache is sharded
to 2^num_shard_bits shards, by hash of the key. The total capacity
is divided and evenly assigned to each shard.

<a name="new_rate_limiter-2"></a>

### new_rate_limiter/2 ###

`new_rate_limiter(RateBytesPerSec, Auto) -> any()`

create new Limiter

<a name="next_binary_update-1"></a>

### next_binary_update/1 ###

<pre><code>
next_binary_update(Iterator::term()) -&gt; {ok, LastSeq::non_neg_integer(), BinLog::binary()} | {error, term()}
</code></pre>
<br />

go to the last update as a binary in the transaction log, can be ussed with the write_binary_update function.

<a name="next_update-1"></a>

### next_update/1 ###

<pre><code>
next_update(Iterator::term()) -&gt; {ok, LastSeq::non_neg_integer(), Log::<a href="#type-write_actions">write_actions()</a>, BinLog::binary()} | {error, term()}
</code></pre>
<br />

like binary update but also return the batch as a list of operations

<a name="open-2"></a>

### open/2 ###

<pre><code>
open(Name, DBOpts) -&gt; Result
</code></pre>

<ul class="definitions"><li><code>Name = <a href="file.md#type-filename_all">file:filename_all()</a></code></li><li><code>DBOpts = <a href="#type-options">options()</a></code></li><li><code>Result = {ok, <a href="#type-db_handle">db_handle()</a>} | {error, any()}</code></li></ul>

Open RocksDB with the defalut column family

<a name="open_backup_engine-1"></a>

### open_backup_engine/1 ###

<pre><code>
open_backup_engine(Path::string) -&gt; {ok, <a href="#type-backup_engine">backup_engine()</a>} | {error, term()}
</code></pre>
<br />

open a new backup engine

<a name="open_with_cf-3"></a>

### open_with_cf/3 ###

<pre><code>
open_with_cf(Name, DBOpts, CFDescriptors) -&gt; {ok, <a href="#type-db_handle">db_handle()</a>, [<a href="#type-cf_handle">cf_handle()</a>]} | {error, any()}
</code></pre>

<ul class="definitions"><li><code>Name = <a href="file.md#type-filename_all">file:filename_all()</a></code></li><li><code>DBOpts = <a href="#type-db_options">db_options()</a></code></li><li><code>CFDescriptors = [#cf_descriptor{name = string(), options = <a href="#type-cf_options">cf_options()</a>}]</code></li></ul>

Open RocksDB with the specified column families

<a name="purge_old_backup-2"></a>

### purge_old_backup/2 ###

<pre><code>
purge_old_backup(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>, NumBackupToKeep::non_neg_integer()) -&gt; ok | {error, any()}
</code></pre>
<br />

deletes old backups, keeping latest num_backups_to_keep alive

<a name="put-4"></a>

### put/4 ###

<pre><code>
put(DBHandle, Key, Value, WriteOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>Value = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Put a key/value pair into the default column family

<a name="put-5"></a>

### put/5 ###

<pre><code>
put(DBHandle, CFHandle, Key, Value, WriteOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>Value = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Put a key/value pair into the specified column family

<a name="release_cache-1"></a>

### release_cache/1 ###

`release_cache(Cache) -> any()`

release the cache

<a name="release_rate_limiter-1"></a>

### release_rate_limiter/1 ###

`release_rate_limiter(Limiter) -> any()`

release the limiter

<a name="release_snapshot-1"></a>

### release_snapshot/1 ###

<pre><code>
release_snapshot(SnapshotHandle::<a href="#type-snapshot_handle">snapshot_handle()</a>) -&gt; ok | {error, any()}
</code></pre>
<br />

release a snapshot

<a name="repair-2"></a>

### repair/2 ###

<pre><code>
repair(Name::<a href="file.md#type-filename_all">file:filename_all()</a>, DBOpts::<a href="#type-db_options">db_options()</a>) -&gt; ok | {error, any()}
</code></pre>
<br />

Try to repair as much of the contents of the database as possible.
Some data may be lost, so be careful when calling this function

<a name="restore_db_from_backup-3"></a>

### restore_db_from_backup/3 ###

<pre><code>
restore_db_from_backup(BackupEngine, BackupId, DbDir) -&gt; Result
</code></pre>

<ul class="definitions"><li><code>BackupEngine = <a href="#type-backup_engine">backup_engine()</a></code></li><li><code>BackupId = non_neg_integer()</code></li><li><code>DbDir = string()</code></li><li><code>Result = ok | {error, any()}</code></li></ul>

restore from backup with backup_id

<a name="restore_db_from_backup-4"></a>

### restore_db_from_backup/4 ###

<pre><code>
restore_db_from_backup(BackupEngine, BackupId, DbDir, WalDir) -&gt; Result
</code></pre>

<ul class="definitions"><li><code>BackupEngine = <a href="#type-backup_engine">backup_engine()</a></code></li><li><code>BackupId = non_neg_integer()</code></li><li><code>DbDir = string()</code></li><li><code>WalDir = string()</code></li><li><code>Result = ok | {error, any()}</code></li></ul>

restore from backup with backup_id

<a name="restore_db_from_latest_backup-2"></a>

### restore_db_from_latest_backup/2 ###

<pre><code>
restore_db_from_latest_backup(BackupEngine, DbDir) -&gt; Result
</code></pre>

<ul class="definitions"><li><code>BackupEngine = <a href="#type-backup_engine">backup_engine()</a></code></li><li><code>DbDir = string()</code></li><li><code>Result = ok | {error, any()}</code></li></ul>

restore from the latest backup

<a name="restore_db_from_latest_backup-3"></a>

### restore_db_from_latest_backup/3 ###

<pre><code>
restore_db_from_latest_backup(BackupEngine, DbDir, WalDir) -&gt; Result
</code></pre>

<ul class="definitions"><li><code>BackupEngine = <a href="#type-backup_engine">backup_engine()</a></code></li><li><code>DbDir = string()</code></li><li><code>WalDir = string()</code></li><li><code>Result = ok | {error, any()}</code></li></ul>

restore from the latest backup

<a name="set_capacity-2"></a>

### set_capacity/2 ###

<pre><code>
set_capacity(Cache::<a href="#type-cache_handle">cache_handle()</a>, Capacity::non_neg_integer()) -&gt; ok
</code></pre>
<br />

sets the maximum configured capacity of the cache. When the new
capacity is less than the old capacity and the existing usage is
greater than new capacity, the implementation will do its best job to
purge the released entries from the cache in order to lower the usage

<a name="set_db_background_threads-2"></a>

### set_db_background_threads/2 ###

<pre><code>
set_db_background_threads(DB::<a href="#type-db_handle">db_handle()</a>, N::non_neg_integer()) -&gt; ok
</code></pre>
<br />

set background threads of a database

<a name="set_db_background_threads-3"></a>

### set_db_background_threads/3 ###

<pre><code>
set_db_background_threads(DB::<a href="#type-db_handle">db_handle()</a>, N::non_neg_integer(), Priority::<a href="#type-env_priority">env_priority()</a>) -&gt; ok
</code></pre>
<br />

set database background threads of low and high prioriry threads pool of an environment
Flush threads are in the HIGH priority pool, while compaction threads are in the
LOW priority pool. To increase the number of threads in each pool call:

<a name="set_env_background_threads-2"></a>

### set_env_background_threads/2 ###

<pre><code>
set_env_background_threads(Env::<a href="#type-env_handle">env_handle()</a>, N::non_neg_integer()) -&gt; ok
</code></pre>
<br />

set background threads of an environment

<a name="set_env_background_threads-3"></a>

### set_env_background_threads/3 ###

<pre><code>
set_env_background_threads(Env::<a href="#type-env_handle">env_handle()</a>, N::non_neg_integer(), Priority::<a href="#type-env_priority">env_priority()</a>) -&gt; ok
</code></pre>
<br />

set background threads of low and high prioriry threads pool of an environment
Flush threads are in the HIGH priority pool, while compaction threads are in the
LOW priority pool. To increase the number of threads in each pool call:

<a name="single_delete-3"></a>

### single_delete/3 ###

<pre><code>
single_delete(DBHandle, Key, WriteOpts) -&gt; ok | {error, any()}
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li></ul>

Remove the database entry for "key". Requires that the key exists
and was not overwritten. Returns OK on success, and a non-OK status
on error.  It is not an error if "key" did not exist in the database.

If a key is overwritten (by calling Put() multiple times), then the result
of calling SingleDelete() on this key is undefined.  SingleDelete() only
behaves correctly if there has been only one Put() for this key since the
previous call to SingleDelete() for this key.

This feature is currently an experimental performance optimization
for a very specific workload.  It is up to the caller to ensure that
SingleDelete is only used for a key that is not deleted using Delete() or
written using Merge().  Mixing SingleDelete operations with Deletes
can result in undefined behavior.

Note: consider setting options `{sync, true}`.

<a name="single_delete-4"></a>

### single_delete/4 ###

<pre><code>
single_delete(DBHandle, CFHandle, Key, WriteOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>CFHandle = <a href="#type-cf_handle">cf_handle()</a></code></li><li><code>Key = binary()</code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

like `single_delete/3` but on the specified column family

<a name="snapshot-1"></a>

### snapshot/1 ###

<pre><code>
snapshot(DbHandle::<a href="#type-db_handle">db_handle()</a>) -&gt; {ok, <a href="#type-snapshot_handle">snapshot_handle()</a>} | {error, any()}
</code></pre>
<br />

return a database snapshot
Snapshots provide consistent read-only views over the entire state of the key-value store

<a name="stats-1"></a>

### stats/1 ###

<pre><code>
stats(DBHandle::<a href="#type-db_handle">db_handle()</a>) -&gt; {ok, any()} | {error, any()}
</code></pre>
<br />

Return the current stats of the default column family
Implemented by calling GetProperty with "rocksdb.stats"

<a name="stats-2"></a>

### stats/2 ###

<pre><code>
stats(DBHandle::<a href="#type-db_handle">db_handle()</a>, CFHandle::<a href="#type-cf_handle">cf_handle()</a>) -&gt; {ok, any()} | {error, any()}
</code></pre>
<br />

Return the current stats of the specified column family
Implemented by calling GetProperty with "rocksdb.stats"

<a name="stop_backup-1"></a>

### stop_backup/1 ###

<pre><code>
stop_backup(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>) -&gt; ok
</code></pre>
<br />

<a name="sync_wal-1"></a>

### sync_wal/1 ###

<pre><code>
sync_wal(DbHandle::<a href="#type-db_handle">db_handle()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

Sync the wal. Note that Write() followed by SyncWAL() is not exactly the
same as Write() with sync=true: in the latter case the changes won't be
visible until the sync is done.
Currently only works if allow_mmap_writes = false in Options.

<a name="updates_iterator-2"></a>

### updates_iterator/2 ###

<pre><code>
updates_iterator(Db::<a href="#type-db_handle">db_handle()</a>, Since::non_neg_integer()) -&gt; {ok, Iterator::term()}
</code></pre>
<br />

create a new iterator to retrive ethe transaction log since a sequce

<a name="verify_backup-2"></a>

### verify_backup/2 ###

<pre><code>
verify_backup(BackupEngine::<a href="#type-backup_engine">backup_engine()</a>, BackupId::non_neg_integer()) -&gt; ok | {error, any()}
</code></pre>
<br />

checks that each file exists and that the size of the file matches
our expectations. it does not check file checksum.

<a name="write-3"></a>

### write/3 ###

<pre><code>
write(DBHandle, WriteActions, WriteOpts) -&gt; Res
</code></pre>

<ul class="definitions"><li><code>DBHandle = <a href="#type-db_handle">db_handle()</a></code></li><li><code>WriteActions = <a href="#type-write_actions">write_actions()</a></code></li><li><code>WriteOpts = <a href="#type-write_options">write_options()</a></code></li><li><code>Res = ok | {error, any()}</code></li></ul>

Apply the specified updates to the database.

<a name="write_batch-3"></a>

### write_batch/3 ###

<pre><code>
write_batch(Db::<a href="#type-db_handle">db_handle()</a>, Batch::<a href="#type-batch_handle">batch_handle()</a>, WriteOptions::<a href="#type-write_options">write_options()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

write the batch to the database

<a name="write_binary_update-3"></a>

### write_binary_update/3 ###

<pre><code>
write_binary_update(Iterator::term(), BinLog::binary(), WriteOptions::<a href="#type-write_options">write_options()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

apply a set of operation coming from a transaction log to another database. Can be useful to use it in slave
mode.

