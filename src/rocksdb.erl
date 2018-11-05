%%% -*- erlang -*-
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
%% Copyright (c) 2016-2018 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


%% @doc Erlang Wrapper for RocksDB
-module(rocksdb).

-export([
  open/2,
  open_with_cf/3,
  close/1,
  set_db_background_threads/2, set_db_background_threads/3,
  destroy/2,
  repair/2,
  is_empty/1,
  list_column_families/2,
  create_column_family/3,
  drop_column_family/1,
  destroy_column_family/1,
  checkpoint/2,
  flush/1, flush/2,
  sync_wal/1,
  count/1, count/2,
  stats/1, stats/2,
  get_property/2, get_property/3,
  get_approximate_sizes/3, get_approximate_sizes/4,
  get_approximate_memtable_stats/2, get_approximate_memtable_stats/3
]).


%% snapshot
-export([
  snapshot/1,
  release_snapshot/1,
  get_snapshot_sequence/1
]).

%% KV API
-export([
  put/4, put/5,
  merge/4, merge/5,
  delete/3, delete/4,
  single_delete/3, single_delete/4,
  write/3,
  get/3, get/4,
  delete_range/4, delete_range/5,
  compact_range/4, compact_range/5,
  iterator/2, iterator/3,
  iterators/3,
  iterator_move/2,
  iterator_refresh/1,
  iterator_close/1
]).


-export([fold/4, fold/5, fold_keys/4, fold_keys/5]).
%% Cache API
-export([
  new_lru_cache/1,
  new_clock_cache/1,
  get_usage/1,
  get_pinned_usage/1,
  set_capacity/2,
  get_capacity/1,
  set_strict_capacity_limit/2,
  release_cache/1
]).

%% Limiter API
-export([
    new_rate_limiter/2,
    release_rate_limiter/1
]).

%% env api
-export([
  default_env/0,
  mem_env/0,
  set_env_background_threads/2, set_env_background_threads/3,
  destroy_env/1
]).


-export([get_latest_sequence_number/1]).
-export([updates_iterator/2]).
-export([close_updates_iterator/1]).
-export([next_binary_update/1]).
-export([write_binary_update/3]).
-export([next_update/1]).

%% batch functions
-export([batch/0,
         release_batch/1,
         write_batch/3,
         batch_put/3, batch_put/4,
         batch_merge/3, batch_merge/4,
         batch_delete/2, batch_delete/3,
         batch_single_delete/2, batch_single_delete/3,
         batch_clear/1,
         batch_savepoint/1,
         batch_rollback/1,
         batch_count/1,
         batch_data_size/1,
         batch_tolist/1]).

-export([
  open_backup_engine/1,
  create_new_backup/2,
  stop_backup/1,
  get_backup_info/1,
  verify_backup/2,
  delete_backup/2,
  purge_old_backup/2,
  restore_db_from_backup/3, restore_db_from_backup/4,
  restore_db_from_latest_backup/2, restore_db_from_latest_backup/3,
  garbage_collect_backup/1,
  close_backup/1
]).


-export_type([
  env/0,
  env_handle/0,
  db_handle/0,
  cache_handle/0,
  cf_handle/0,
  itr_handle/0,
  snapshot_handle/0,
  batch_handle/0,
  rate_limiter_handle/0,
  compression_type/0,
  compaction_style/0,
  access_hint/0,
  wal_recovery_mode/0,
  backup_engine/0,
  backup_info/0
]).

-on_load(init/0).

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref),
    receive {Ref, Reply} ->
        Reply
    end).

-spec init() -> ok | {error, any()}.
init() ->
  SoName = case code:priv_dir(?MODULE) of
         {error, bad_name} ->
           case code:which(?MODULE) of
             Filename when is_list(Filename) ->
               filename:join([filename:dirname(Filename),"../priv", "rocksdb"]);
             _ ->
               filename:join("../priv", "rocksdb")
           end;
         Dir ->
           filename:join(Dir, "rocksdb")
       end,
  erlang:load_nif(SoName, application:get_all_env(rocksdb)).

-record(db_path, {path        :: file:filename_all(),
          target_size :: non_neg_integer()}).

-record(cf_descriptor, {name    :: string(),
                        options :: cf_options()}).

-type compression_type() :: snappy | zlib | bzip2 | lz4 | lz4h | none.
-type compaction_style() :: level | universal | fifo | none.
-type compaction_pri() :: compensated_size | oldest_largest_seq_first | oldest_smallest_seq_first.
-type access_hint() :: normal | sequential | willneed | none.
-type wal_recovery_mode() :: tolerate_corrupted_tail_records |
               absolute_consistency |
               point_in_time_recovery |
               skip_any_corrupted_records.



-opaque env_handle() :: reference() | binary().
-opaque db_handle() :: reference() | binary().
-opaque cf_handle() :: reference() | binary().
-opaque itr_handle() :: reference() | binary().
-opaque snapshot_handle() :: reference() | binary().
-opaque batch_handle() :: reference() | binary().
-opaque backup_engine() :: reference() | binary().
-opaque cache_handle() :: reference() | binary().
-opaque rate_limiter_handle() :: reference() | binary().

-opaque env() :: default | memenv | env_handle().

-type env_priority() :: priority_high | priority_low.

-type block_based_table_options() :: [{no_block_cache, boolean()} |
                                      {block_size, pos_integer()} |
                                      {block_cache, cache_handle()} |
                                      {block_cache_size, pos_integer()} |
                                      {bloom_filter_policy, BitsPerKey :: pos_integer()} |
                                      {format_version, 0 | 1 | 2} |
                                      {cache_index_and_filter_blocks, boolean()}].

-type merge_operator() :: erlang_merge_operator |
                          bitset_merge_operator |
                          {bitset_merge_operator, non_neg_integer()} |
                          counter_merge_operator.

-type cf_options() :: [{block_cache_size_mb_for_point_lookup, non_neg_integer()} |
                       {memtable_memory_budget, pos_integer()} |
                       {write_buffer_size,  pos_integer()} |
                       {max_write_buffer_number,  pos_integer()} |
                       {min_write_buffer_number_to_merge,  pos_integer()} |
                       {compression,  compression_type()} |
                       {num_levels,  pos_integer()} |
                       {level0_file_num_compaction_trigger,  integer()} |
                       {level0_slowdown_writes_trigger,  integer()} |
                       {level0_stop_writes_trigger,  integer()} |
                       {max_mem_compaction_level,  pos_integer()} |
                       {target_file_size_base,  pos_integer()} |
                       {target_file_size_multiplier,  pos_integer()} |
                       {max_bytes_for_level_base,  pos_integer()} |
                       {max_bytes_for_level_multiplier,  pos_integer()} |
                       {max_compaction_bytes,  pos_integer()} |
                       {soft_rate_limit,  float()} |
                       {hard_rate_limit,  float()} |
                       {arena_block_size,  integer()} |
                       {disable_auto_compactions,  boolean()} |
                       {purge_redundant_kvs_while_flush,  boolean()} |
                       {compaction_style,  compaction_style()} |
                       {compaction_pri,  compaction_pri()} |
                       {filter_deletes,  boolean()} |
                       {max_sequential_skip_in_iterations,  pos_integer()} |
                       {inplace_update_support,  boolean()} |
                       {inplace_update_num_locks,  pos_integer()} |
                       {table_factory_block_cache_size, pos_integer()} |
                       {in_memory_mode, boolean()} |
                       {block_based_table_options, block_based_table_options()} |
                       {level_compaction_dynamic_level_bytes, boolean()} |
                       {optimize_filters_for_hits, boolean()} |
                       {prefix_transform, [{fixed_prefix_transform, integer()} | 
                                           {capped_prefix_transform, integer()}]} |
                       {merge_operator, merge_operator()}
                      ].

-type db_options() :: [{env, env()} |
                       {total_threads, pos_integer()} |
                       {create_if_missing, boolean()} |
                       {create_missing_column_families, boolean()} |
                       {error_if_exists, boolean()} |
                       {paranoid_checks, boolean()} |
                       {max_open_files, integer()} |
                       {max_total_wal_size, non_neg_integer()} |
                       {use_fsync, boolean()} |
                       {db_paths, list(#db_path{})} |
                       {db_log_dir, file:filename_all()} |
                       {wal_dir, file:filename_all()} |
                       {delete_obsolete_files_period_micros, pos_integer()} |
                       {max_background_jobs, pos_integer()} |
                       {max_background_compactions, pos_integer()} |
                       {max_background_flushes, pos_integer()} |
                       {max_log_file_size, non_neg_integer()} |
                       {log_file_time_to_roll, non_neg_integer()} |
                       {keep_log_file_num, pos_integer()} |
                       {max_manifest_file_size, pos_integer()} |
                       {table_cache_numshardbits, pos_integer()} |
                       {wal_ttl_seconds, non_neg_integer()} |
                       {wal_size_limit_mb, non_neg_integer()} |
                       {manifest_preallocation_size, pos_integer()} |
                       {allow_mmap_reads, boolean()} |
                       {allow_mmap_writes, boolean()} |
                       {is_fd_close_on_exec, boolean()} |
                       {skip_log_error_on_recovery, boolean()} |
                       {stats_dump_period_sec, non_neg_integer()} |
                       {advise_random_on_open, boolean()} |
                       {access_hint, access_hint()} |
                       {compaction_readahead_size, non_neg_integer()} |
                       {new_table_reader_for_compaction_inputs, boolean()} |
                       {use_adaptive_mutex, boolean()} |
                       {bytes_per_sync, non_neg_integer()} |
                       {skip_stats_update_on_db_open, boolean()} |
                       {wal_recovery_mode, wal_recovery_mode()} |
                       {allow_concurrent_memtable_write, boolean()} |
                       {enable_write_thread_adaptive_yield, boolean()} |
                       {db_write_buffer_size, non_neg_integer()}  |
                       {in_memory, boolean()} |
                       {rate_limiter, rate_limiter_handle()} |
                       {max_subcompactions, non_neg_integer()}].

-type options() :: db_options() | cf_options().

-type read_options() :: [{verify_checksums, boolean()} |
                         {fill_cache, boolean()} |
                         {iterate_upper_bound, binary()} |
                         {tailing, boolean()} |
                         {total_order_seek, boolean()} |
                         {prefix_same_as_start, boolean()} |
                         {snapshot, snapshot_handle()}].

-type write_options() :: [{sync, boolean()} |
                          {disable_wal, boolean()} |
                          {ignore_missing_column_families, boolean()}].

-type write_actions() :: [{put, Key::binary(), Value::binary()} |
                          {put, ColumnFamilyHandle::cf_handle(), Key::binary(), Value::binary()} |
                          {delete, Key::binary()} |
                          {delete, ColumnFamilyHandle::cf_handle(), Key::binary()} |
                          {single_delete, Key::binary()} |
                          {single_delete, ColumnFamilyHandle::cf_handle(), Key::binary()} |
                          clear].

-type compact_range_options()  :: [{exclusive_manual_compaction, boolean()} |
                                   {change_level, boolean()} |
                                   {target_level, integer()} |
                                   {allow_write_stall, boolean()} |
                                   {max_subcompactions, non_neg_integer()}].

-type iterator_action() :: first | last | next | prev | binary() | {seek, binary()} | {seek_for_prev, binary()}.

-type backup_info() :: #{
  id := non_neg_integer(),
  timestamp := non_neg_integer(),
  size := non_neg_integer(),
  number_files := non_neg_integer()
}.


-type size_approximation_flag() :: none | include_memtables | include_files | include_both.
-type range() :: {Start::binary(), Limit::binary()}.


%% @doc Open RocksDB with the defalut column family
-spec open(Name, DBOpts) -> Result when
  Name :: file:filename_all(),
  DBOpts :: options(),
  Result :: {ok, db_handle()} | {error, any()}.
open(_Name, _DBOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Open RocksDB with the specified column families
-spec(open_with_cf(Name, DBOpts, CFDescriptors) ->
       {ok, db_handle(), list(cf_handle())} | {error, any()}
         when Name::file:filename_all(),
          DBOpts :: db_options(),
          CFDescriptors :: list(#cf_descriptor{})).
open_with_cf(_Name, _DBOpts, _CFDescriptors) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Close RocksDB
-spec close(DBHandle) -> Res when
  DBHandle :: db_handle(),
  Res :: ok | {error, any()}.
close(_DBHandle) ->
  erlang:nif_error({error, not_loaded}).

%% ===============================================
%% Column Families API
%% ===============================================

%% @doc List column families
-spec list_column_families(Name, DBOpts) -> Res when
  Name::file:filename_all(),
  DBOpts::db_options(),
  Res :: {ok, list(string())} | {error, any()}.
list_column_families(_Name, _DbOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Create a new column family
-spec create_column_family(DBHandle, Name, CFOpts) -> Res when
  DBHandle :: db_handle(),
  Name ::string(),
  CFOpts :: cf_options(),
  Res :: {ok, cf_handle()} | {error, any()}.
create_column_family(_DBHandle, _Name, _CFOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Drop a column family
-spec drop_column_family(CFHandle) -> Res when
  CFHandle::cf_handle(),
  Res :: ok | {error, any()}.
drop_column_family(_CFHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Destroy a column family
-spec destroy_column_family(CFHandle) -> Res when
  CFHandle::cf_handle(),
  Res :: ok | {error, any()}.
destroy_column_family(_CFHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc return a database snapshot
%% Snapshots provide consistent read-only views over the entire state of the key-value store
-spec snapshot(DbHandle::db_handle()) -> {ok, snapshot_handle()} | {error, any()}.
snapshot(_DbHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc release a snapshot
-spec release_snapshot(SnapshotHandle::snapshot_handle()) -> ok | {error, any()}.
release_snapshot(_SnapshotHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc returns Snapshot's sequence number
-spec get_snapshot_sequence(SnapshotHandle::snapshot_handle()) -> Sequence::non_neg_integer().
get_snapshot_sequence(_SnapshotHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Put a key/value pair into the default column family
-spec put(DBHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
put(_DBHandle, _Key, _Value, _WriteOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Put a key/value pair into the specified column family
-spec put(DBHandle, CFHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
put(_DBHandle, _CFHandle, _Key, _Value, _WriteOpts) ->
   erlang:nif_error({error, not_loaded}).

%% @doc Merge a key/value pair into the default column family
-spec merge(DBHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
merge(_DBHandle, _Key, _Value, _WriteOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Merge a key/value pair into the specified column family
-spec merge(DBHandle, CFHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
merge(_DBHandle, _CFHandle, _Key, _Value, _WriteOpts) ->
   erlang:nif_error({error, not_loaded}).

%% @doc Delete a key/value pair in the default column family
-spec(delete(DBHandle, Key, WriteOpts) ->
       ok | {error, any()} when DBHandle::db_handle(),
                    Key::binary(),
                    WriteOpts::write_options()).
delete(_DBHandle, _Key, _WriteOpts) ->
   erlang:nif_error({error, not_loaded}).

%% @doc Delete a key/value pair in the specified column family
-spec delete(DBHandle, CFHandle, Key, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  WriteOpts::write_options(),
  Res ::  ok | {error, any()}.
delete(_DBHandle, _CFHandle, _Key, _WriteOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Remove the database entry for "key". Requires that the key exists
%% and was not overwritten. Returns OK on success, and a non-OK status
%% on error.  It is not an error if "key" did not exist in the database.
%%
%% If a key is overwritten (by calling Put() multiple times), then the result
%% of calling SingleDelete() on this key is undefined.  SingleDelete() only
%% behaves correctly if there has been only one Put() for this key since the
%% previous call to SingleDelete() for this key.
%%
%%  This feature is currently an experimental performance optimization
%% for a very specific workload.  It is up to the caller to ensure that
%% SingleDelete is only used for a key that is not deleted using Delete() or
%% written using Merge().  Mixing SingleDelete operations with Deletes
%%  can result in undefined behavior.
%%
%% Note: consider setting options `{sync, true}'.
-spec(single_delete(DBHandle, Key, WriteOpts) ->
        ok | {error, any()} when DBHandle::db_handle(),
                    Key::binary(),
                    WriteOpts::write_options()).
single_delete(_DBHandle, _Key, _WriteOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc like `single_delete/3' but on the specified column family
-spec single_delete(DBHandle, CFHandle, Key, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  WriteOpts::write_options(),
  Res ::  ok | {error, any()}.
single_delete(_DBHandle, _CFHandle, _Key, _WriteOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Apply the specified updates to the database.
-spec write(DBHandle, WriteActions, WriteOpts) -> Res when
  DBHandle::db_handle(),
   WriteActions::write_actions(),
   WriteOpts::write_options(),
   Res :: ok | {error, any()}.
write(DBHandle, WriteOps, WriteOpts) ->
  {ok, Batch} = batch(),
  try write_1(WriteOps, Batch, DBHandle, WriteOpts)
  after release_batch(Batch)
  end.

write_1([{put, Key, Value} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_put(Batch, Key, Value),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([{put, CfHandle, Key, Value} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_put(Batch, CfHandle, Key, Value),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([{merge, Key, Value} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_merge(Batch, Key, Value),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([{merge, CfHandle, Key, Value} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_merge(Batch, CfHandle, Key, Value),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([{delete, Key} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_delete(Batch, Key),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([{delete, CfHandle, Key} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_delete(Batch, CfHandle, Key),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([{single_delete, Key} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_single_delete(Batch, Key),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([{single_delete, CfHandle, Key} | Rest], Batch, DbHandle, WriteOpts) ->
  batch_single_delete(Batch, CfHandle, Key),
  write_1(Rest, Batch, DbHandle, WriteOpts);
write_1([_ | _], _Batch, _DbHandle, _WriteOpts) ->
  erlang:error(badarg);
write_1([], Batch, DbHandle, WriteOpts) ->
  write_batch(DbHandle, Batch, WriteOpts).


%% @doc Retrieve a key/value pair in the default column family
-spec get(DBHandle, Key, ReadOpts) ->  Res when
  DBHandle::db_handle(),
  Key::binary(),
  ReadOpts::read_options(),
   Res :: {ok, binary()} | not_found | {error, any()}.
get(_DBHandle, _Key, _ReadOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Retrieve a key/value pair in the specified column family
-spec get(DBHandle, CFHandle, Key, ReadOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  ReadOpts::read_options(),
  Res :: {ok, binary()} | not_found | {error, any()}.
get(_DBHandle, _CFHandle, _Key, _ReadOpts) ->
  erlang:nif_error({error, not_loaded}).


%% @doc For each i in [0,n-1], store in "Sizes[i]", the approximate
%% file system space used by keys in "[range[i].start .. range[i].limit)".
%%
%% Note that the returned sizes measure file system space usage, so
%% if the user data compresses by a factor of ten, the returned
%% sizes will be one-tenth the size of the corresponding user data size.
%%
%% If `IncludeFlags' defines whether the returned size should include
%% the recently written data in the mem-tables (if
%% the mem-table type supports it), data serialized to disk, or both.
-spec get_approximate_sizes(DBHandle, Ranges, IncludeFlags) -> Sizes when
  DBHandle::db_handle(),
  Ranges::[range()],
  IncludeFlags::size_approximation_flag(),
  Sizes :: [non_neg_integer()].
get_approximate_sizes(_DBHandle, _Ranges, _IncludeFlags) ->
  erlang:nif_error({error, not_loaded}).

-spec get_approximate_sizes(DBHandle, CFHandle, Ranges, IncludeFlags) -> Sizes when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Ranges::[range()],
  IncludeFlags::size_approximation_flag(),
  Sizes :: [non_neg_integer()].
get_approximate_sizes(_DBHandle, _CFHandle, _Ranges, _IncludeFlags) ->
  erlang:nif_error({error, not_loaded}).

%% @doc The method is similar to GetApproximateSizes, except it
%% returns approximate number of records in memtables.
-spec get_approximate_memtable_stats(DBHandle, Range) -> Res when
  DBHandle::db_handle(),
  Range::range(),
  Res :: {ok, {Count::non_neg_integer(), Size::non_neg_integer()}}.
get_approximate_memtable_stats(_DBHandle, _Range) ->
  erlang:nif_error({error, not_loaded}).

-spec get_approximate_memtable_stats(DBHandle, CFHandle, Range) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Range::range(),
  Res :: {ok, {Count::non_neg_integer(), Size::non_neg_integer()}}.
get_approximate_memtable_stats(_DBHandle, _CFHandle, _Range) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Removes the database entries in the range ["BeginKey", "EndKey"), i.e.,
%% including "BeginKey" and excluding "EndKey". Returns OK on success, and
%% a non-OK status on error. It is not an error if no keys exist in the range
%% ["BeginKey", "EndKey").
%%
%% This feature is currently an experimental performance optimization for
%% deleting very large ranges of contiguous keys. Invoking it many times or on
%% small ranges may severely degrade read performance; in particular, the
%% resulting performance can be worse than calling Delete() for each key in
%% the range. Note also the degraded read performance affects keys outside the
%% deleted ranges, and affects database operations involving scans, like flush
%% and compaction.
%%
%% Consider setting ReadOptions::ignore_range_deletions = true to speed
%% up reads for key(s) that are known to be unaffected by range deletions.
-spec delete_range(DBHandle, BeginKey, EndKey, WriteOpts) -> Res when
  DBHandle::db_handle(),
  BeginKey::binary(),
  EndKey::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
delete_range(_DbHandle, _Start, _End, _WriteOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Removes the database entries in the range ["BeginKey", "EndKey").
%% like `delete_range/3' but for a column family
-spec delete_range(DBHandle, CFHandle, BeginKey, EndKey, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  BeginKey::binary(),
  EndKey::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
delete_range(_DbHandle, _CFHandle, _Start, _End, _WriteOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Compact the underlying storage for the key range [*begin,*end].
%% The actual compaction interval might be superset of [*begin, *end].
%% In particular, deleted and overwritten versions are discarded,
%% and the data is rearranged to reduce the cost of operations
%% needed to access the data.  This operation should typically only
%% be invoked by users who understand the underlying implementation.
%%
%% "begin==undefined" is treated as a key before all keys in the database.
%% "end==undefined" is treated as a key after all keys in the database.
%% Therefore the following call will compact the entire database:
%% rocksdb::compact_range(Options, undefined, undefined);
%% Note that after the entire database is compacted, all data are pushed
%% down to the last level containing any data. If the total data size after
%% compaction is reduced, that level might not be appropriate for hosting all
%% the files. In this case, client could set options.change_level to true, to
%% move the files back to the minimum level capable of holding the data set
%% or a given level (specified by non-negative target_level).
-spec compact_range(DBHandle, BeginKey, EndKey, CompactRangeOpts) -> Res when
  DBHandle::db_handle(),
  BeginKey::binary(),
  EndKey::binary(),
  CompactRangeOpts::compact_range_options(),
  Res :: ok | {error, any()}.
compact_range(_DbHandle, _Start, _End, _CompactRangeOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc  Compact the underlying storage for the key range ["BeginKey", "EndKey").
%% like `compact_range/3' but for a column family
-spec compact_range(DBHandle, CFHandle, BeginKey, EndKey, CompactRangeOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  BeginKey::binary(),
  EndKey::binary(),
  CompactRangeOpts::compact_range_options(),
  Res :: ok | {error, any()}.
compact_range(_DbHandle, _CFHandle, _Start, _End, _CompactRangeOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Return a iterator over the contents of the database.
%% The result of iterator() is initially invalid (caller must
%% call iterator_move function on the iterator before using it).
-spec iterator(DBHandle, ReadOpts) -> Res when
  DBHandle::db_handle(),
  ReadOpts::read_options(),
  Res :: {ok, itr_handle()} | {error, any()}.
iterator(_DBHandle, _ReadOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Return a iterator over the contents of the database.
%% The result of iterator() is initially invalid (caller must
%% call iterator_move function on the iterator before using it).
-spec iterator(DBHandle, CFHandle, ReadOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  ReadOpts::read_options(),
  Res :: {ok, itr_handle()} | {error, any()}.
iterator(_DBHandle, _CfHandle, _ReadOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc
%% Return a iterator over the contents of the specified column family.
-spec(iterators(DBHandle, CFHandle, ReadOpts) ->
             {ok, itr_handle()} | {error, any()} when DBHandle::db_handle(),
                                                      CFHandle::cf_handle(),
                                                      ReadOpts::read_options()).
iterators(_DBHandle, _CFHandle, _ReadOpts) ->
  erlang:nif_error({error, not_loaded}).


%% @doc
%% Move to the specified place
-spec(iterator_move(ITRHandle, ITRAction) ->
             {ok, Key::binary(), Value::binary()} |
             {ok, Key::binary()} |
             {error, invalid_iterator} |
             {error, iterator_closed} when ITRHandle::itr_handle(),
                                           ITRAction::iterator_action()).
iterator_move(_ITRHandle, _ITRAction) ->
  erlang:nif_error({error, not_loaded}).

%% @doc
%% Refresh iterator
-spec(iterator_refresh(ITRHandle) -> ok when ITRHandle::itr_handle()).
iterator_refresh(_ITRHandle) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Close a iterator
-spec(iterator_close(ITRHandle) -> ok when ITRHandle::itr_handle()).
iterator_close(_ITRHandle) ->
    erlang:nif_error({error, not_loaded}).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

%% @doc Calls Fun(Elem, AccIn) on successive elements in the default column family
%% starting with AccIn == Acc0.
%% Fun/2 must return a new accumulator which is passed to the next call.
%% The function returns the final value of the accumulator.
%% Acc0 is returned if the default column family is empty.
-spec fold(DBHandle, Fun, AccIn, ReadOpts) -> AccOut when
  DBHandle::db_handle(),
  Fun::fold_fun(),
  AccIn::any(),
  ReadOpts::read_options(),
  AccOut :: any().
fold(DBHandle, Fun, Acc0, ReadOpts) ->
  {ok, Itr} = iterator(DBHandle, ReadOpts),
  do_fold(Itr, Fun, Acc0).

%% @doc Calls Fun(Elem, AccIn) on successive elements in the specified column family
%% Other specs are same with fold/4
-spec fold(DBHandle, CFHandle, Fun, AccIn, ReadOpts) -> AccOut when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Fun::fold_fun(),
  AccIn::any(),
  ReadOpts::read_options(),
  AccOut :: any().
fold(DbHandle, CFHandle, Fun, Acc0, ReadOpts) ->
  {ok, Itr} = iterator(DbHandle, CFHandle, ReadOpts),
  do_fold(Itr, Fun, Acc0).

-type fold_keys_fun() :: fun((Key::binary(), any()) -> any()).

%% @doc Calls Fun(Elem, AccIn) on successive elements in the default column family
%% starting with AccIn == Acc0.
%% Fun/2 must return a new accumulator which is passed to the next call.
%% The function returns the final value of the accumulator.
%% Acc0 is returned if the default column family is empty.
-spec fold_keys(DBHandle, Fun, AccIn, ReadOpts) -> AccOut when
  DBHandle::db_handle(),
  Fun::fold_keys_fun(),
  AccIn::any(),
  ReadOpts::read_options(),
  AccOut :: any().
fold_keys(DBHandle, UserFun, Acc0, ReadOpts) ->
  WrapperFun = fun({K, _V}, Acc) -> UserFun(K, Acc);
                  (Else, Acc) -> UserFun(Else, Acc) end,
  {ok, Itr} = iterator(DBHandle, ReadOpts),
  do_fold(Itr, WrapperFun, Acc0).

%% @doc Calls Fun(Elem, AccIn) on successive elements in the specified column family
%% Other specs are same with fold_keys/4
-spec fold_keys(DBHandle, CFHandle, Fun, AccIn, ReadOpts) -> AccOut when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Fun::fold_keys_fun(),
  AccIn::any(),
  ReadOpts::read_options(),
  AccOut :: any().
fold_keys(DBHandle, CFHandle, UserFun, Acc0, ReadOpts) ->
  WrapperFun = fun({K, _V}, Acc) -> UserFun(K, Acc);
                  (Else, Acc) -> UserFun(Else, Acc) end,
  {ok, Itr} = iterator(DBHandle, CFHandle, ReadOpts),
  do_fold(Itr, WrapperFun, Acc0).

%% @doc is the database empty
-spec  is_empty(DBHandle::db_handle()) -> true | false.
is_empty(_DbHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Destroy the contents of the specified database.
%% Be very careful using this method.
-spec destroy(Name::file:filename_all(), DBOpts::db_options()) -> ok | {error, any()}.
destroy(_Name, _DBOpts) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Try to repair as much of the contents of the database as possible.
%% Some data may be lost, so be careful when calling this function
-spec repair(Name::file:filename_all(), DBOpts::db_options()) -> ok | {error, any()}.
repair(_Name, _DBOpts) ->
   erlang:nif_error({error, not_loaded}).

%% @doc take a snapshot of a running RocksDB database in a separate directory
%% http://rocksdb.org/blog/2609/use-checkpoints-for-efficient-snapshots/
-spec checkpoint(
  DbHandle::db_handle(), Path::file:filename_all()
) -> ok | {error, any()}.
checkpoint(_DbHandle, _Path) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Flush all mem-table data.
-spec flush(db_handle()) -> ok | {error, term()}.
flush(_DbHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Flush all mem-table data.
-spec flush(db_handle(), cf_handle()) -> ok | {error, term()}.
flush(_DbHandle, _CfHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc  Sync the wal. Note that Write() followed by SyncWAL() is not exactly the
%% same as Write() with sync=true: in the latter case the changes won't be
%% visible until the sync is done.
%% Currently only works if allow_mmap_writes = false in Options.
-spec sync_wal(db_handle()) -> ok | {error, term()}.
sync_wal(_DbHandle) ->
  erlang:nif_error({error, not_loaded}).



%% @doc Return the approximate number of keys in the default column family.
%% Implemented by calling GetIntProperty with "rocksdb.estimate-num-keys"
-spec count(DBHandle::db_handle()) ->  non_neg_integer() | {error, any()}.
count(DBHandle) ->
  count_1(get_property(DBHandle, <<"rocksdb.estimate-num-keys">>)).

%% @doc
%% Return the approximate number of keys in the specified column family.
%%
-spec count(DBHandle::db_handle(), CFHandle::cf_handle()) -> non_neg_integer() | {error, any()}.
count(DBHandle, CFHandle) ->
  count_1(get_property(DBHandle, CFHandle, <<"rocksdb.estimate-num-keys">>)).

count_1({ok, BinCount}) -> erlang:binary_to_integer(BinCount);
count_1(Error) -> Error.

%% @doc Return the current stats of the default column family
%% Implemented by calling GetProperty with "rocksdb.stats"
-spec stats(DBHandle::db_handle()) -> {ok, any()} | {error, any()}.
stats(DBHandle) ->
  get_property(DBHandle, <<"rocksdb.stats">>).

%% @doc Return the current stats of the specified column family
%% Implemented by calling GetProperty with "rocksdb.stats"
-spec stats(
  DBHandle::db_handle(), CFHandle::cf_handle()
) -> {ok, any()} | {error, any()}.
stats(DBHandle, CfHandle) ->
  get_property(DBHandle, CfHandle, <<"rocksdb.stats">>).

%% @doc Return the RocksDB internal status of the default column family specified at Property
-spec get_property(
  DBHandle::db_handle(), Property::binary()
) -> {ok, any()} | {error, any()}.
get_property(_DBHandle, _Property) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Return the RocksDB internal status of the specified column family specified at Property
-spec get_property(
  DBHandle::db_handle(), CFHandle::cf_handle(), Property::binary()
) -> string() | {error, any()}.
get_property(_DBHandle, _CFHandle, _Property) ->
  erlang:nif_error({error, not_loaded}).

%% @doc get latest sequence from the log
-spec get_latest_sequence_number(Db :: db_handle()) -> Seq :: non_neg_integer().
get_latest_sequence_number(_DbHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc create a new iterator to retrive ethe transaction log since a sequce
-spec updates_iterator(Db :: db_handle(),Since :: non_neg_integer()) -> {ok, Iterator :: term()}.
updates_iterator(_DbHandle, _Since) ->
  erlang:nif_error({error, not_loaded}).

%% @doc close the transaction log
close_updates_iterator(_Iterator) ->
  erlang:nif_error({error, not_loaded}).

%% @doc go to the last update as a binary in the transaction log, can be ussed with the write_binary_update function.
-spec next_binary_update(
        Iterator :: term()
       ) -> {ok, LastSeq :: non_neg_integer(), BinLog :: binary()} | {error, term()}.
next_binary_update(_Iterator) ->
  erlang:nif_error({error, not_loaded}).

%% @doc apply a set of operation coming from a transaction log to another database. Can be useful to use it in slave
%% mode.
%%
-spec write_binary_update(
        Iterator :: term(), BinLog :: binary(), WriteOptions :: write_options()
       ) -> ok | {error, term()}.
write_binary_update(_Iterator, _Update, _WriteOptions) ->
  erlang:nif_error({error, not_loaded}).


%% @doc like binary update but also return the batch as a list of operations
-spec next_update(
        Iterator :: term()
       ) -> {ok, LastSeq :: non_neg_integer(), Log :: write_actions(), BinLog :: binary()} | {error, term()}.
next_update(_Iterator) ->
  erlang:nif_error({error, not_loaded}).

%% @doc create a new batch in memory. A batch is a nif resource attached to the current process. Pay attention when you
%% share it with other processes as it may not been released. To force its release you will need to use the close_batch
%% function.
-spec batch() -> {ok, Batch :: batch_handle()}.
batch() ->
  erlang:nif_error({error, not_loaded}).

-spec release_batch(Batch :: batch_handle()) -> ok.
release_batch(_Batch) ->
  erlang:nif_error({error, not_loaded}).

%% @doc write the batch to the database
-spec write_batch(Db :: db_handle(), Batch :: batch_handle(), WriteOptions :: write_options()) -> ok | {error, term()}.
write_batch(_DbHandle, _Batch, _WriteOptions) ->
  erlang:nif_error({error, not_loaded}).

%% @doc add a put operation to the batch
-spec batch_put(Batch :: batch_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_put(_Batch, _Key, _Value) ->
  erlang:nif_error({error, not_loaded}).

%% @doc like `batch_put/3' but apply the operation to a column family
-spec batch_put(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_put(_Batch, _ColumnFamily, _Key, _Value) ->
  erlang:nif_error({error, not_loaded}).

%% @doc add a merge operation to the batch
-spec batch_merge(Batch :: batch_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_merge(_Batch, _Key, _Value) ->
  erlang:nif_error({error, not_loaded}).

%% @doc like `batch_mege/3' but apply the operation to a column family
-spec batch_merge(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_merge(_Batch, _ColumnFamily, _Key, _Value) ->
  erlang:nif_error({error, not_loaded}).

%% @doc batch implementation of delete operation to the batch
-spec batch_delete(Batch :: batch_handle(), Key :: binary()) -> ok.
batch_delete(_Batch, _Key) ->
  erlang:nif_error({error, not_loaded}).

%% @doc like `batch_delete/2' but apply the operation to a column family
-spec batch_delete(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary()) -> ok.
batch_delete(_Batch, _ColumnFamily, _Key) ->
  erlang:nif_error({error, not_loaded}).

%% @doc batch implementation of single_delete operation to the batch
-spec batch_single_delete(Batch :: batch_handle(), Key :: binary()) -> ok.
batch_single_delete(_Batch, _Key) ->
  erlang:nif_error({error, not_loaded}).

%% @doc like `batch_single_delete/2' but apply the operation to a column family
-spec batch_single_delete(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary()) -> ok.
batch_single_delete(_Batch, _ColumnFamily, _Key) ->
  erlang:nif_error({error, not_loaded}).

%% @doc return the number of operations in the batch
-spec batch_count(_Batch :: batch_handle()) -> Count :: non_neg_integer().
batch_count(_Batch) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Retrieve data size of the batch.
-spec batch_data_size(_Batch :: batch_handle()) -> BatchSize :: non_neg_integer().
batch_data_size(_Batch) ->
  erlang:nif_error({error, not_loaded}).

%% @doc reset the batch, clear all operations.
-spec batch_clear(Batch :: batch_handle()) -> ok.
batch_clear(_Batch) ->
  erlang:nif_error({error, not_loaded}).

%% @doc store a checkpoint in the batch to which you can rollback later
-spec batch_savepoint(Batch :: batch_handle()) -> ok.
batch_savepoint(_Batch) ->
  erlang:nif_error({error, not_loaded}).

%% @doc rollback the operations to the latest checkpoint
-spec batch_rollback(Batch :: batch_handle()) -> ok.
batch_rollback(_Batch) ->
  erlang:nif_error({error, not_loaded}).

%% @doc return all the operation sin the batch as a list of operations
-spec batch_tolist(Batch :: batch_handle()) -> Ops :: write_actions().
batch_tolist(_Batch) ->
  erlang:nif_error({error, not_loaded}).


%% ===================================================================
%% backup functions

%% @doc open a new backup engine
-spec open_backup_engine(Path :: string) -> {ok, backup_engine()} | {error, term()}.
open_backup_engine(_Path) ->
  erlang:nif_error({error, not_loaded}).

%% %% @doc Call this from another process if you want to stop the backup
%% that is currently happening. It will return immediatelly, will
%% not wait for the backup to stop.
%% The backup will stop ASAP and the call to CreateNewBackup will
%% return Status::Incomplete(). It will not clean up after itself, but
%% the state will remain consistent. The state will be cleaned up
%% next time you create BackupableDB or RestoreBackupableDB.
-spec stop_backup(backup_engine()) -> ok.
stop_backup(_BackupEngine) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Captures the state of the database in the latest backup
-spec create_new_backup(BackupEngine :: backup_engine(), Db :: db_handle()) -> ok | {error, term()}.
create_new_backup(_BackupEngine, _DbHandle) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Returns info about backups in backup_info
-spec get_backup_info(backup_engine()) -> [backup_info()].
get_backup_info(_BackupEngine) ->
  erlang:nif_error({error, not_loaded}).

%% @doc checks that each file exists and that the size of the file matches
%% our expectations. it does not check file checksum.
-spec verify_backup(BackupEngine :: backup_engine(), BackupId :: non_neg_integer()) -> ok | {error, any()}.
verify_backup(_BackupEngine, _BackupId) ->
  erlang:nif_error({error, not_loaded}).

%% @doc deletes a specific backup
-spec delete_backup(BackupEngine :: backup_engine(), BackupId :: non_neg_integer()) -> ok | {error, any()}.
delete_backup(_BackupEngine, _BackupId) ->
  erlang:nif_error({error, not_loaded}).

%% @doc deletes old backups, keeping latest num_backups_to_keep alive
-spec purge_old_backup(BackupEngine :: backup_engine(), NumBackupToKeep :: non_neg_integer()) -> ok | {error, any()}.
purge_old_backup(_BackupEngine, _NumBackupToKeep) ->
  erlang:nif_error({error, not_loaded}).

%% @doc restore from backup with backup_id
-spec restore_db_from_backup(BackupEngine, BackupId, DbDir) -> Result when
  BackupEngine :: backup_engine(),
  BackupId :: non_neg_integer(),
  DbDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_backup(_BackupEngine, _BackupId, _DbDir) ->
  erlang:nif_error({error, not_loaded}).

%% @doc restore from backup with backup_id
-spec restore_db_from_backup(BackupEngine, BackupId, DbDir, WalDir) -> Result when
  BackupEngine :: backup_engine(),
  BackupId :: non_neg_integer(),
  DbDir :: string(),
  WalDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_backup(_BackupEngine, _BackupId, _DbDir, _WalDir) ->
  erlang:nif_error({error, not_loaded}).

%% @doc restore from the latest backup
-spec restore_db_from_latest_backup(BackupEngine, DbDir) -> Result when
  BackupEngine :: backup_engine(),
  DbDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_latest_backup(_BackupEngine, _DbDir) ->
  erlang:nif_error({error, not_loaded}).

%% @doc restore from the latest backup
-spec restore_db_from_latest_backup(BackupEngine,  DbDir, WalDir) -> Result when
  BackupEngine :: backup_engine(),
  DbDir :: string(),
  WalDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_latest_backup(_BackupEngine,  _DbDir, _WalDir) ->
  erlang:nif_error({error, not_loaded}).

%% @doc  Will delete all the files we don't need anymore
%% It will do the full scan of the files/ directory and delete all the
%% files that are not referenced.
-spec garbage_collect_backup(backup_engine()) -> ok.
garbage_collect_backup(_BackupEngine) ->
  erlang:nif_error({error, not_loaded}).

%% @doc stop and close the backup
%% note: experimental for testing only
-spec close_backup(backup_engine()) -> ok.
close_backup(_BackupEngine) ->
  erlang:nif_error({error, not_loaded}).


%% ===================================================================
%% cache functions

%% @doc // Create a new cache with a fixed size capacity. The cache is sharded
%% to 2^num_shard_bits shards, by hash of the key. The total capacity
%% is divided and evenly assigned to each shard.
-spec new_lru_cache(Capacity :: non_neg_integer()) -> {ok, cache_handle()}.
new_lru_cache(_Capacity) ->
  erlang:nif_error({error, not_loaded}).

%% @doc Similar to NewLRUCache, but create a cache based on CLOCK algorithm with
%% better concurrent performance in some cases. See util/clock_cache.cc for
%% more detail.
-spec new_clock_cache(Capacity :: non_neg_integer()) -> {ok, cache_handle()}.
new_clock_cache(_Capacity) ->
  erlang:nif_error({error, not_loaded}).

%% @doc returns the memory size for a specific entry in the cache.
-spec get_usage(cache_handle()) -> non_neg_integer().
get_usage(_Cache) ->
  erlang:nif_error({error, not_loaded}).

%% @doc  returns the memory size for the entries in use by the system
-spec get_pinned_usage(cache_handle()) -> non_neg_integer().
get_pinned_usage(_Cache) ->
  erlang:nif_error({error, not_loaded}).

%% @doc  returns the maximum configured capacity of the cache.
-spec get_capacity(cache_handle()) -> non_neg_integer().
get_capacity(_Cache) ->
  erlang:nif_error({error, not_loaded}).

%% @doc sets the maximum configured capacity of the cache. When the new
%% capacity is less than the old capacity and the existing usage is
%% greater than new capacity, the implementation will do its best job to
%% purge the released entries from the cache in order to lower the usage
-spec set_capacity(Cache :: cache_handle(), Capacity :: non_neg_integer()) -> ok.
set_capacity(_Cache, _Capacity) ->
  erlang:nif_error({error, not_loaded}).

%% @doc sets strict_capacity_limit flag of the cache. If the flag is set
%% to true, insert to cache will fail if no enough capacity can be free.
-spec set_strict_capacity_limit(Cache :: cache_handle(), StrictCapacityLimit :: boolean()) -> ok.
set_strict_capacity_limit(_Cache, _StrictCapacityLimit) ->
    erlang:nif_error({error, not_loaded}).

%% @doc release the cache
release_cache(_Cache) ->
  erlang:nif_error({error, not_loaded}).

%% ===================================================================
%% Limiter functions

%% @doc create new Limiter
new_rate_limiter(_RateBytesPerSec, _Auto) ->
    erlang:nif_error({error, not_loaded}).

%% @doc release the limiter
release_rate_limiter(_Limiter) ->
    erlang:nif_error({error, not_loaded}).

%% ===================================================================
%% env functions

%% @doc return a default db environment
-spec default_env() -> {ok, env_handle()}.
default_env() ->
  erlang:nif_error({error, not_loaded}).

%% @doc return a memory environment
-spec mem_env() -> {ok, env_handle()}.
mem_env() ->
  erlang:nif_error({error, not_loaded}).

%% @doc set background threads of an environment
-spec set_env_background_threads(Env :: env_handle(), N :: non_neg_integer()) -> ok.
set_env_background_threads(_Env, _N) ->
  erlang:nif_error({error, not_loaded}).

%% @doc set background threads of low and high prioriry threads pool of an environment
%% Flush threads are in the HIGH priority pool, while compaction threads are in the
%% LOW priority pool. To increase the number of threads in each pool call:
-spec set_env_background_threads(Env :: env_handle(), N :: non_neg_integer(), Priority :: env_priority()) -> ok.
set_env_background_threads(_Env, _N, _PRIORITY) ->
  erlang:nif_error({error, not_loaded}).

%% @doc destroy an environment
-spec destroy_env(Env :: env_handle()) -> ok.
destroy_env(_Env) ->
  erlang:nif_error({error, not_loaded}).


%% @doc set background threads of a database
-spec set_db_background_threads(DB :: db_handle(), N :: non_neg_integer()) -> ok.
set_db_background_threads(_Db, _N) ->
  erlang:nif_error({error, not_loaded}).

%% @doc set database background threads of low and high prioriry threads pool of an environment
%% Flush threads are in the HIGH priority pool, while compaction threads are in the
%% LOW priority pool. To increase the number of threads in each pool call:
-spec set_db_background_threads(DB :: db_handle(), N :: non_neg_integer(), Priority :: env_priority()) -> ok.
set_db_background_threads(_Db, _N, _PRIORITY) ->
  erlang:nif_error({error, not_loaded}).



%% ===================================================================
%% Internal functions
%% ===================================================================
do_fold(Itr, Fun, Acc0) ->
  try
    fold_loop(iterator_move(Itr, first), Itr, Fun, Acc0)
  after
    iterator_close(Itr)
  end.

fold_loop({error, iterator_closed}, _Itr, _Fun, Acc0) ->
  throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0) ->
  Acc0;
fold_loop({ok, K}, Itr, Fun, Acc0) ->
  Acc = Fun(K, Acc0),
  fold_loop(iterator_move(Itr, next), Itr, Fun, Acc);
fold_loop({ok, K, V}, Itr, Fun, Acc0) ->
  Acc = Fun({K, V}, Acc0),
  fold_loop(iterator_move(Itr, next), Itr, Fun, Acc).
