%%% -*- erlang -*-
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
%% Copyright (c) 2016-2025 Benoit Chesneau
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
  open/2, open/3,
  open_readonly/2, open_readonly/3,
  open_optimistic_transaction_db/2, open_optimistic_transaction_db/3,
  open_with_ttl/4,
  close/1,
  set_db_background_threads/2, set_db_background_threads/3,
  destroy/2,
  repair/2,
  is_empty/1,
  list_column_families/2,
  create_column_family/3,
  drop_column_family/2,
  destroy_column_family/2,
  checkpoint/2,
  flush/2, flush/3,
  sync_wal/1,
  stats/1, stats/2,
  get_property/2, get_property/3,
  get_approximate_sizes/3, get_approximate_sizes/4,
  get_approximate_memtable_stats/3, get_approximate_memtable_stats/4
]).

-export([open_with_cf/3, open_with_cf_readonly/3]).
-export([drop_column_family/1]).
-export([destroy_column_family/1]).
-export([get_column_family_metadata/1, get_column_family_metadata/2]).

-export([get_latest_sequence_number/1]).

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
  get/3, get/4,
  put_entity/4, put_entity/5,
  get_entity/3, get_entity/4,
  delete_range/4, delete_range/5,
  compact_range/4, compact_range/5,
  iterator/2, iterator/3,
  iterators/3,
  coalescing_iterator/3,
  iterator_move/2,
  iterator_refresh/1,
  iterator_prepare_value/1,
  iterator_close/1,
  iterator_columns/1,
  delete_entity/3, delete_entity/4
]).

%% deprecated API

-export([write/3]).
-export([count/1, count/2]).
-export([fold/4, fold/5, fold_keys/4, fold_keys/5]).



%% Cache API
-export([new_cache/2,
         release_cache/1,
         cache_info/1,
         cache_info/2,
         set_capacity/2,
         set_strict_capacity_limit/2]).

-export([new_lru_cache/1, new_clock_cache/1]).
-export([get_usage/1]).
-export([get_pinned_usage/1]).
-export([get_capacity/1]).

%% Limiter API
-export([
    new_rate_limiter/2,
    release_rate_limiter/1
]).

%% sst file manager API
-export([
  new_sst_file_manager/1, new_sst_file_manager/2,
  release_sst_file_manager/1,
  sst_file_manager_flag/3,
  sst_file_manager_info/1, sst_file_manager_info/2
]).

%% write buffer manager API
-export([
  new_write_buffer_manager/1,
  new_write_buffer_manager/2,
  release_write_buffer_manager/1,
  write_buffer_manager_info/1, write_buffer_manager_info/2
]).

%% Statistics API
-export([
  new_statistics/0,
  set_stats_level/2,
  statistics_info/1,
  statistics_ticker/2,
  statistics_histogram/2,
  release_statistics/1
]).

%% Env API
-export([
  new_env/0, new_env/1,
  set_env_background_threads/2, set_env_background_threads/3,
  destroy_env/1
]).
-export([default_env/0, mem_env/0]).

%% Log Iterator API
-export([tlog_iterator/2,
         tlog_iterator_close/1,
         tlog_next_binary_update/1,
         tlog_next_update/1]).

-export([write_binary_update/3]).

-export([updates_iterator/2]).
-export([close_updates_iterator/1]).
-export([next_binary_update/1]).
-export([next_update/1]).

%% Batch API
-export([batch/0,
         release_batch/1,
         write_batch/3,
         batch_put/3, batch_put/4,
         batch_merge/3, batch_merge/4,
         batch_delete/2, batch_delete/3,
         batch_single_delete/2, batch_single_delete/3,
         batch_delete_range/3, batch_delete_range/4,
         batch_clear/1,
         batch_savepoint/1,
         batch_rollback/1,
         batch_count/1,
         batch_data_size/1,
         batch_tolist/1]).

%% Transaction API (Optimistic)
-export([
         transaction/2,
         release_transaction/1,
         transaction_put/3, transaction_put/4,
         transaction_get/3, transaction_get/4,
         %% see comment in c_src/transaction.cc
         %% transaction_merge/3, transaction_merge/4,
         transaction_delete/2, transaction_delete/3,
         transaction_iterator/2, transaction_iterator/3,
         transaction_commit/1,
         transaction_rollback/1
        ]).

%% Pessimistic Transaction API
-export([
         open_pessimistic_transaction_db/2, open_pessimistic_transaction_db/3,
         pessimistic_transaction/2, pessimistic_transaction/3,
         release_pessimistic_transaction/1,
         pessimistic_transaction_put/3, pessimistic_transaction_put/4,
         pessimistic_transaction_get/3, pessimistic_transaction_get/4,
         pessimistic_transaction_get_for_update/3, pessimistic_transaction_get_for_update/4,
         pessimistic_transaction_delete/2, pessimistic_transaction_delete/3,
         pessimistic_transaction_iterator/2, pessimistic_transaction_iterator/3,
         pessimistic_transaction_commit/1,
         pessimistic_transaction_rollback/1,
         pessimistic_transaction_set_savepoint/1,
         pessimistic_transaction_rollback_to_savepoint/1,
         pessimistic_transaction_pop_savepoint/1,
         pessimistic_transaction_get_id/1,
         pessimistic_transaction_get_waiting_txns/1
        ]).

%% Backup Engine
-export([
  open_backup_engine/1,
  close_backup_engine/1,
  gc_backup_engine/1,
  create_new_backup/2,
  stop_backup/1,
  get_backup_info/1,
  verify_backup/2,
  delete_backup/2,
  purge_old_backup/2,
  restore_db_from_backup/3, restore_db_from_backup/4,
  restore_db_from_latest_backup/2, restore_db_from_latest_backup/3
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
  transaction_handle/0,
  rate_limiter_handle/0,
  compression_type/0,
  compaction_style/0,
  access_hint/0,
  wal_recovery_mode/0,
  backup_engine/0,
  backup_info/0,
  sst_file_manager/0,
  write_buffer_manager/0,
  statistics_handle/0,
  stats_level/0
]).

-deprecated({count, 1, next_major_release}).
-deprecated({count, 2, next_major_release}).
-deprecated({fold, 4, next_major_release}).
-deprecated({fold, 5, next_major_release}).
-deprecated({fold_keys, 4, next_major_release}).
-deprecated({fold_keys, 5, next_major_release}).
-deprecated({write, 3, next_major_release}).
-deprecated({updates_iterator, 2, next_major_release}).
-deprecated({close_updates_iterator, 1, next_major_release}).
-deprecated({next_binary_update, 1, next_major_release}).
-deprecated({next_update, 1, next_major_release}).
-deprecated({default_env, 0, next_major_release}).
-deprecated({mem_env, 0, next_major_release}).
-deprecated({new_lru_cache, 1, next_major_release}).
-deprecated({new_clock_cache, 1, next_major_release}).
-deprecated({get_pinned_usage, 1, next_major_release}).
-deprecated({get_usage, 1, next_major_release}).
-deprecated({get_capacity, 1, next_major_release}).
-deprecated({drop_column_family, 1, next_major_release}).
-deprecated({destroy_column_family, 1, next_major_release}).
-deprecated({open_with_cf, 3, next_major_release}).

-record(db_path, {path        :: file:filename_all(),
          target_size :: non_neg_integer()}).

-type cf_descriptor() :: {string(), cf_options()}.
-type cache_type() :: lru | clock.
-type compression_type() :: snappy | zlib | bzip2 | lz4 | lz4h | zstd | none.
-type compaction_style() :: level | universal | fifo | none.
-type compaction_pri() :: compensated_size | oldest_largest_seq_first | oldest_smallest_seq_first.
-type access_hint() :: normal | sequential | willneed | none.
-type wal_recovery_mode() :: tolerate_corrupted_tail_records |
               absolute_consistency |
               point_in_time_recovery |
               skip_any_corrupted_records.



-opaque env_handle() :: reference() | binary().
-opaque sst_file_manager() :: reference() | binary().
-opaque db_handle() :: reference() | binary().
-opaque cf_handle() :: reference() | binary().
-opaque itr_handle() :: reference() | binary().
-opaque snapshot_handle() :: reference() | binary().
-opaque batch_handle() :: reference() | binary().
-opaque transaction_handle() :: reference() | binary().
-opaque backup_engine() :: reference() | binary().
-opaque cache_handle() :: reference() | binary().
-opaque rate_limiter_handle() :: reference() | binary().
-opaque write_buffer_manager() :: reference() | binary().
-opaque statistics_handle() :: reference() | binary().

-type column_family() :: cf_handle() | default_column_family.

-type env_type() :: default | memenv.
-opaque env() :: env_type() | env_handle().
-type env_priority() :: priority_high | priority_low.

-type compaction_options_fifo() :: [{max_table_file_size, pos_integer()} |
                                    {allow_compaction, boolean()}].


-type block_based_table_options() :: [{no_block_cache, boolean()} |
                                      {block_size, pos_integer()} |
                                      {block_cache, cache_handle()} |
                                      {block_cache_size, pos_integer()} |
                                      {bloom_filter_policy, BitsPerKey :: pos_integer()} |
                                      {format_version, 0 | 1 | 2 | 3 | 4 | 5} |
                                      {cache_index_and_filter_blocks, boolean()}].

-type merge_operator() :: erlang_merge_operator |
                          bitset_merge_operator |
                          {bitset_merge_operator, non_neg_integer()} |
                          counter_merge_operator.

-type read_tier() :: read_all_tier |
                     block_cache_tier |
                     persisted_tier |
                     memtable_tier.

-type prepopulate_blob_cache() :: disable | flush_only.

-type cf_options() :: [{block_cache_size_mb_for_point_lookup, non_neg_integer()} |
                       {memtable_memory_budget, pos_integer()} |
                       {write_buffer_size,  pos_integer()} |
                       {max_write_buffer_number,  pos_integer()} |
                       {min_write_buffer_number_to_merge,  pos_integer()} |
                       {enable_blob_files, boolean()} |
                       {min_blob_size, non_neg_integer()} |
                       {blob_file_size, non_neg_integer()} |
                       {blob_compression_type, compression_type()} |
                       {enable_blob_garbage_collection, boolean()} |
                       {blob_garbage_collection_age_cutoff, float()} |
                       {blob_garbage_collection_force_threshold, float()} |
                       {blob_compaction_readahead_size, non_neg_integer()} |
                       {blob_file_starting_level, non_neg_integer()} |
                       {blob_cache, cache_handle()} |
                       {prepopulate_blob_cache, prepopulate_blob_cache()} |
                       {compression,  compression_type()} |
                       {bottommost_compression,  compression_type()} |
                       {compression_opts, compression_opts()} |
                       {bottommost_compression_opts, compression_opts()} |
                       {num_levels,  pos_integer()} |
                       {ttl, pos_integer()} |
                       {level0_file_num_compaction_trigger,  integer()} |
                       {level0_slowdown_writes_trigger,  integer()} |
                       {level0_stop_writes_trigger,  integer()} |
                       {target_file_size_base,  pos_integer()} |
                       {target_file_size_multiplier,  pos_integer()} |
                       {max_bytes_for_level_base,  pos_integer()} |
                       {max_bytes_for_level_multiplier,  pos_integer()} |
                       {max_compaction_bytes,  pos_integer()} |
                       {arena_block_size,  integer()} |
                       {disable_auto_compactions,  boolean()} |
                       {compaction_style,  compaction_style()} |
                       {compaction_pri,  compaction_pri()} |
                       {compaction_options_fifo, compaction_options_fifo()} |
                       {filter_deletes,  boolean()} |
                       {max_sequential_skip_in_iterations,  pos_integer()} |
                       {inplace_update_support,  boolean()} |
                       {inplace_update_num_locks,  pos_integer()} |
                       {table_factory_block_cache_size, pos_integer()} |
                       {in_memory_mode, boolean()} |
                       {block_based_table_options, block_based_table_options()} |
                       {level_compaction_dynamic_level_bytes, boolean()} |
                       {optimize_filters_for_hits, boolean()} |
                       {prefix_extractor, {fixed_prefix_transform, integer()} | 
                                           {capped_prefix_transform, integer()}} |
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
                       {manual_wal_flush, boolean()} |
                       {wal_size_limit_mb, non_neg_integer()} |
                       {manifest_preallocation_size, pos_integer()} |
                       {allow_mmap_reads, boolean()} |
                       {allow_mmap_writes, boolean()} |
                       {is_fd_close_on_exec, boolean()} |
                       {stats_dump_period_sec, non_neg_integer()} |
                       {advise_random_on_open, boolean()} |
                       {access_hint, access_hint()} |
                       {compaction_readahead_size, non_neg_integer()} |
                       {use_adaptive_mutex, boolean()} |
                       {bytes_per_sync, non_neg_integer()} |
                       {skip_stats_update_on_db_open, boolean()} |
                       {wal_recovery_mode, wal_recovery_mode()} |
                       {allow_concurrent_memtable_write, boolean()} |
                       {enable_write_thread_adaptive_yield, boolean()} |
                       {db_write_buffer_size, non_neg_integer()}  |
                       {in_memory, boolean()} |
                       {rate_limiter, rate_limiter_handle()} |
                       {sst_file_manager, sst_file_manager()} |
                       {write_buffer_manager, write_buffer_manager()} |
                       {max_subcompactions, non_neg_integer()} |
                       {atomic_flush, boolean()} |
                       {use_direct_reads, boolean()} |
                       {use_direct_io_for_flush_and_compaction, boolean()} |
                       {enable_pipelined_write, boolean()} |
                       {unordered_write, boolean()} |
                       {two_write_queues, boolean()} |
                       {statistics, statistics_handle()}].

-type options() :: db_options() | cf_options().

-type read_options() :: [{read_tier, read_tier()} |
                         {verify_checksums, boolean()} |
                         {fill_cache, boolean()} |
                         {iterate_upper_bound, binary()} |
                         {iterate_lower_bound, binary()} |
                         {tailing, boolean()} |
                         {total_order_seek, boolean()} |
                         {prefix_same_as_start, boolean()} |
                         {snapshot, snapshot_handle()} |
                         {auto_refresh_iterator_with_snapshot, boolean()} |
                         {auto_readahead_size, boolean()} |
                         {allow_unprepared_value, boolean()}].

-type write_options() :: [{sync, boolean()} |
                          {disable_wal, boolean()} |
                          {ignore_missing_column_families, boolean()} |
                          {no_slowdown, boolean()} |
                          {low_pri, boolean()}].

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

-type flush_options() :: [{wait, boolean()} |
                          {allow_write_stall, boolean()}].

-type compression_opts() :: [{enabled, boolean()} |
                             {window_bits, pos_integer()} |
                             {level, non_neg_integer()} |
                             {strategy, integer()} |
                             {max_dict_bytes, non_neg_integer()} |
                             {zstd_max_train_bytes, non_neg_integer()}].

-type iterator_action() :: first | last | next | prev | binary() | {seek, binary()} | {seek_for_prev, binary()}.

-type backup_info() :: #{
  id := non_neg_integer(),
  timestamp := non_neg_integer(),
  size := non_neg_integer(),
  number_files := non_neg_integer()
}.


-type size_approximation_flag() :: none | include_memtables | include_files | include_both.
-type range() :: {Start::binary(), Limit::binary()}.

-type stats_level() :: stats_disable_all |
      stats_except_tickers |
      stats_except_histogram_or_timers |
      stats_except_timers |
      stats_except_detailed_timers |
      stats_except_time_for_mutex |
      stats_all.

-type blob_db_ticker() :: blob_db_num_put |
      blob_db_num_write |
      blob_db_num_get |
      blob_db_num_multiget |
      blob_db_num_seek |
      blob_db_num_next |
      blob_db_num_prev |
      blob_db_num_keys_written |
      blob_db_num_keys_read |
      blob_db_bytes_written |
      blob_db_bytes_read |
      blob_db_write_inlined |
      blob_db_write_inlined_ttl |
      blob_db_write_blob |
      blob_db_write_blob_ttl |
      blob_db_blob_file_bytes_written |
      blob_db_blob_file_bytes_read |
      blob_db_blob_file_synced |
      blob_db_blob_index_expired_count |
      blob_db_blob_index_expired_size |
      blob_db_blob_index_evicted_count |
      blob_db_blob_index_evicted_size |
      blob_db_gc_num_files |
      blob_db_gc_num_new_files |
      blob_db_gc_failures |
      blob_db_gc_num_keys_relocated |
      blob_db_gc_bytes_relocated |
      blob_db_fifo_num_files_evicted |
      blob_db_fifo_num_keys_evicted |
      blob_db_fifo_bytes_evicted |
      blob_db_cache_miss |
      blob_db_cache_hit |
      blob_db_cache_add |
      blob_db_cache_add_failures |
      blob_db_cache_bytes_read |
      blob_db_cache_bytes_write.

-type blob_db_histogram() :: blob_db_key_size |
      blob_db_value_size |
      blob_db_write_micros |
      blob_db_get_micros |
      blob_db_multiget_micros |
      blob_db_seek_micros |
      blob_db_next_micros |
      blob_db_prev_micros |
      blob_db_blob_file_write_micros |
      blob_db_blob_file_read_micros |
      blob_db_blob_file_sync_micros |
      blob_db_compression_micros |
      blob_db_decompression_micros.

-type histogram_info() :: #{median => float(),
                            percentile95 => float(),
                            percentile99 => float(),
                            average => float(),
                            standard_deviation => float(),
                            max => float(),
                            count => non_neg_integer(),
                            sum => non_neg_integer()}.

-type blob_metadata() :: #{blob_file_number => non_neg_integer(),
                           blob_file_name => binary(),
                           blob_file_path => binary(),
                           size => non_neg_integer(),
                           total_blob_count => non_neg_integer(),
                           total_blob_bytes => non_neg_integer(),
                           garbage_blob_count => non_neg_integer(),
                           garbage_blob_bytes => non_neg_integer()}.

-type cf_metadata() :: #{size => non_neg_integer(),
                         file_count => non_neg_integer(),
                         name => binary(),
                         blob_file_size => non_neg_integer(),
                         blob_files => [blob_metadata()]}.

-compile(no_native).
-on_load(on_load/0).

-define(nif_stub,nif_stub_error(?LINE)).
nif_stub_error(Line) ->
    erlang:nif_error({nif_not_loaded,module,?MODULE,line,Line}).

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref),
    receive {Ref, Reply} ->
        Reply
    end).

-spec on_load() -> ok | {error, any()}.
on_load() ->
  SoName = case code:priv_dir(?MODULE) of
         {error, bad_name} ->
           case code:which(?MODULE) of
             Filename when is_list(Filename) ->
               filename:join([filename:dirname(Filename),"../priv", "liberocksdb"]);
             _ ->
               filename:join("../priv", "liberocksdb")
           end;
         Dir ->
           filename:join(Dir, "liberocksdb")
       end,
  erlang:load_nif(SoName, application:get_all_env(rocksdb)).

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------

%% @doc Open RocksDB with the defalut column family
-spec open(Name, DBOpts) -> Result when
  Name :: file:filename_all(),
  DBOpts :: options(),
  Result :: {ok, db_handle()} | {error, any()}.
open(_Name, _DBOpts) ->
  ?nif_stub.

-spec open_readonly(Name, DBOpts) -> Result when
  Name :: file:filename_all(),
  DBOpts :: options(),
  Result :: {ok, db_handle()} | {error, any()}.
open_readonly(_Name, _DBOpts) ->
  ?nif_stub.

%% @doc Open RocksDB with the specified column families
-spec(open(Name, DBOpts, CFDescriptors) ->
       {ok, db_handle(), list(cf_handle())} | {error, any()}
         when Name::file:filename_all(),
          DBOpts :: db_options(),
          CFDescriptors :: list(cf_descriptor())).
open(_Name, _DBOpts, _CFDescriptors) ->
  ?nif_stub.

%% @doc Open read-only RocksDB with the specified column families
-spec(open_readonly(Name, DBOpts, CFDescriptors) ->
       {ok, db_handle(), list(cf_handle())} | {error, any()}
         when Name::file:filename_all(),
          DBOpts :: db_options(),
          CFDescriptors :: list(cf_descriptor())).
open_readonly(_Name, _DBOpts, _CFDescriptors) ->
  ?nif_stub.

open_with_cf(Name, DbOpts, CFDescriptors) ->
  open(Name, DbOpts, CFDescriptors).

open_with_cf_readonly(Name, DbOpts, CFDescriptors) ->
  open_readonly(Name, DbOpts, CFDescriptors).

open_optimistic_transaction_db(_Name, _DbOpts) ->
    open_optimistic_transaction_db(_Name, _DbOpts, [{"default", []}]).

open_optimistic_transaction_db(_Name, _DbOpts, _CFDescriptors) ->
    ?nif_stub.


%% @doc Open RocksDB with TTL support
%% This API should be used to open the db when key-values inserted are
%% meant to be removed from the db in a non-strict `TTL' amount of time
%% Therefore, this guarantees that key-values inserted will remain in the
%% db for >= TTL amount of time and the db will make efforts to remove the
%% key-values as soon as possible after ttl seconds of their insertion.
%%
%% BEHAVIOUR:
%% TTL is accepted in seconds
%% (int32_t)Timestamp(creation) is suffixed to values in Put internally
%% Expired TTL values deleted in compaction only:(`Timestamp+ttl<time_now')
%% Get/Iterator may return expired entries(compaction not run on them yet)
%% Different TTL may be used during different Opens
%% Example: Open1 at t=0 with TTL=4 and insert k1,k2, close at t=2
%%          Open2 at t=3 with TTL=5. Now k1,k2 should be deleted at t>=5
%% Readonly=true opens in the usual read-only mode. Compactions will not be
%% triggered(neither manual nor automatic), so no expired entries removed
-spec(open_with_ttl(Name, DBOpts, TTL, ReadOnly) ->
       {ok, db_handle()} | {error, any()}
         when Name::file:filename_all(),
          DBOpts :: db_options(),
          TTL :: integer(),
          ReadOnly :: boolean()).
open_with_ttl(_Name, _DBOpts, _TTL, _ReadOnly) ->
  ?nif_stub.


%% @doc Close RocksDB
-spec close(DBHandle) -> Res when
  DBHandle :: db_handle(),
  Res :: ok | {error, any()}.
close(_DBHandle) ->
  ?nif_stub.

%% ===============================================
%% Column Families API
%% ===============================================

%% @doc List column families
-spec list_column_families(Name, DBOpts) -> Res when
  Name::file:filename_all(),
  DBOpts::db_options(),
  Res :: {ok, list(string())} | {error, any()}.
list_column_families(_Name, _DbOpts) ->
  ?nif_stub.

%% @doc Create a new column family
-spec create_column_family(DBHandle, Name, CFOpts) -> Res when
  DBHandle :: db_handle(),
  Name ::string(),
  CFOpts :: cf_options(),
  Res :: {ok, cf_handle()} | {error, any()}.
create_column_family(_DBHandle, _Name, _CFOpts) ->
  ?nif_stub.

%% @doc Drop a column family
-spec drop_column_family(DBHandle, CFHandle) -> Res when
  DBHandle :: db_handle(),
  CFHandle :: cf_handle(),
  Res :: ok | {error, any()}.

drop_column_family(_DbHandle, _CFHandle) ->
  ?nif_stub.

%% @doc Destroy a column family
-spec destroy_column_family(DBHandle, CFHandle) -> Res when
  DBHandle :: db_handle(),
  CFHandle :: cf_handle(),
  Res :: ok | {error, any()}.
destroy_column_family(_DBHandle, _CFHandle) ->
  ?nif_stub.

drop_column_family(_CFHandle) ->
  ?nif_stub.

destroy_column_family(_CFHandle) ->
  ?nif_stub.

%% @doc Get column family metadata including blob file information.
-spec get_column_family_metadata(DBHandle) -> {ok, cf_metadata()} when
  DBHandle :: db_handle().
get_column_family_metadata(_DBHandle) ->
  ?nif_stub.

%% @doc Get column family metadata for a specific column family.
-spec get_column_family_metadata(DBHandle, CFHandle) -> {ok, cf_metadata()} when
  DBHandle :: db_handle(),
  CFHandle :: cf_handle().
get_column_family_metadata(_DBHandle, _CFHandle) ->
  ?nif_stub.

%% @doc return a database snapshot
%% Snapshots provide consistent read-only views over the entire state of the key-value store
-spec snapshot(DbHandle::db_handle()) -> {ok, snapshot_handle()} | {error, any()}.
snapshot(_DbHandle) ->
  ?nif_stub.

%% @doc release a snapshot
-spec release_snapshot(SnapshotHandle::snapshot_handle()) -> ok | {error, any()}.
release_snapshot(_SnapshotHandle) ->
  ?nif_stub.

%% @doc returns Snapshot's sequence number
-spec get_snapshot_sequence(SnapshotHandle::snapshot_handle()) -> Sequence::non_neg_integer().
get_snapshot_sequence(_SnapshotHandle) ->
  ?nif_stub.

%% @doc Put a key/value pair into the default column family
-spec put(DBHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
put(_DBHandle, _Key, _Value, _WriteOpts) ->
  ?nif_stub.

%% @doc Put a key/value pair into the specified column family
-spec put(DBHandle, CFHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
put(_DBHandle, _CFHandle, _Key, _Value, _WriteOpts) ->
   ?nif_stub.

%% @doc Merge a key/value pair into the default column family
-spec merge(DBHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
merge(_DBHandle, _Key, _Value, _WriteOpts) ->
  ?nif_stub.

%% @doc Merge a key/value pair into the specified column family
-spec merge(DBHandle, CFHandle, Key, Value, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  Value::binary(),
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
merge(_DBHandle, _CFHandle, _Key, _Value, _WriteOpts) ->
   ?nif_stub.

%% @doc Delete a key/value pair in the default column family
-spec(delete(DBHandle, Key, WriteOpts) ->
       ok | {error, any()} when DBHandle::db_handle(),
                    Key::binary(),
                    WriteOpts::write_options()).
delete(_DBHandle, _Key, _WriteOpts) ->
   ?nif_stub.

%% @doc Delete a key/value pair in the specified column family
-spec delete(DBHandle, CFHandle, Key, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  WriteOpts::write_options(),
  Res ::  ok | {error, any()}.
delete(_DBHandle, _CFHandle, _Key, _WriteOpts) ->
  ?nif_stub.

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
  ?nif_stub.

%% @doc like `single_delete/3' but on the specified column family
-spec single_delete(DBHandle, CFHandle, Key, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  WriteOpts::write_options(),
  Res ::  ok | {error, any()}.
single_delete(_DBHandle, _CFHandle, _Key, _WriteOpts) ->
  ?nif_stub.

%% @doc Apply the specified updates to the database.
%% this function will be removed on the next major release. You should use the `batch_*' API instead.
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
   Res :: {ok, binary()} | not_found | {error, {corruption, string()}} | {error, any()}.
get(_DBHandle, _Key, _ReadOpts) ->
  ?nif_stub.

%% @doc Retrieve a key/value pair in the specified column family
-spec get(DBHandle, CFHandle, Key, ReadOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  ReadOpts::read_options(),
  Res :: {ok, binary()} | not_found | {error, {corruption, string()}} | {error, any()}.
get(_DBHandle, _CFHandle, _Key, _ReadOpts) ->
  ?nif_stub.

%% @doc Put an entity (wide-column key) in the default column family.
%% An entity is a key with multiple named columns stored as a proplist.
-spec put_entity(DBHandle, Key, Columns, WriteOpts) -> Res when
  DBHandle::db_handle(),
  Key::binary(),
  Columns::[{binary(), binary()}],
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
put_entity(_DBHandle, _Key, _Columns, _WriteOpts) ->
  ?nif_stub.

%% @doc Put an entity (wide-column key) in the specified column family.
-spec put_entity(DBHandle, CFHandle, Key, Columns, WriteOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  Columns::[{binary(), binary()}],
  WriteOpts::write_options(),
  Res :: ok | {error, any()}.
put_entity(_DBHandle, _CFHandle, _Key, _Columns, _WriteOpts) ->
  ?nif_stub.

%% @doc Retrieve an entity (wide-column key) from the default column family.
%% Returns the columns as a proplist of {Name, Value} tuples.
-spec get_entity(DBHandle, Key, ReadOpts) -> Res when
  DBHandle::db_handle(),
  Key::binary(),
  ReadOpts::read_options(),
  Res :: {ok, [{binary(), binary()}]} | not_found | {error, any()}.
get_entity(_DBHandle, _Key, _ReadOpts) ->
  ?nif_stub.

%% @doc Retrieve an entity (wide-column key) from the specified column family.
-spec get_entity(DBHandle, CFHandle, Key, ReadOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Key::binary(),
  ReadOpts::read_options(),
  Res :: {ok, [{binary(), binary()}]} | not_found | {error, any()}.
get_entity(_DBHandle, _CFHandle, _Key, _ReadOpts) ->
  ?nif_stub.


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
  ?nif_stub.

-spec get_approximate_sizes(DBHandle, CFHandle, Ranges, IncludeFlags) -> Sizes when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  Ranges::[range()],
  IncludeFlags::size_approximation_flag(),
  Sizes :: [non_neg_integer()].
get_approximate_sizes(_DBHandle, _CFHandle, _Ranges, _IncludeFlags) ->
  ?nif_stub.

%% @doc The method is similar to GetApproximateSizes, except it
%% returns approximate number of records in memtables.
-spec get_approximate_memtable_stats(DBHandle, StartKey, LimitKey) -> Res when
  DBHandle::db_handle(),
  StartKey :: binary(),
  LimitKey :: binary(),
  Res :: {ok, {Count::non_neg_integer(), Size::non_neg_integer()}}.
get_approximate_memtable_stats(_DBHandle, _StartKey, _LimitKey) ->
  ?nif_stub.

-spec get_approximate_memtable_stats(DBHandle, CFHandle, StartKey, LimitKey) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  StartKey :: binary(),
  LimitKey :: binary(),
  Res :: {ok, {Count::non_neg_integer(), Size::non_neg_integer()}}.
get_approximate_memtable_stats(_DBHandle, _CFHandle, _StartKey, _LimitKey) ->
  ?nif_stub.

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
  ?nif_stub.

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
  ?nif_stub.

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
  BeginKey::binary() | undefined,
  EndKey::binary() | undefined,
  CompactRangeOpts::compact_range_options(),
  Res :: ok | {error, any()}.
compact_range(_DbHandle, _Start, _End, _CompactRangeOpts) ->
  ?nif_stub.

%% @doc  Compact the underlying storage for the key range ["BeginKey", "EndKey").
%% like `compact_range/3' but for a column family
-spec compact_range(DBHandle, CFHandle, BeginKey, EndKey, CompactRangeOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  BeginKey::binary() | undefined,
  EndKey::binary() | undefined,
  CompactRangeOpts::compact_range_options(),
  Res :: ok | {error, any()}.
compact_range(_DbHandle, _CFHandle, _Start, _End, _CompactRangeOpts) ->
  ?nif_stub.

%% @doc Return a iterator over the contents of the database.
%% The result of iterator() is initially invalid (caller must
%% call iterator_move function on the iterator before using it).
-spec iterator(DBHandle, ReadOpts) -> Res when
  DBHandle::db_handle(),
  ReadOpts::read_options(),
  Res :: {ok, itr_handle()} | {error, any()}.
iterator(_DBHandle, _ReadOpts) ->
  ?nif_stub.

%% @doc Return a iterator over the contents of the database.
%% The result of iterator() is initially invalid (caller must
%% call iterator_move function on the iterator before using it).
-spec iterator(DBHandle, CFHandle, ReadOpts) -> Res when
  DBHandle::db_handle(),
  CFHandle::cf_handle(),
  ReadOpts::read_options(),
  Res :: {ok, itr_handle()} | {error, any()}.
iterator(_DBHandle, _CfHandle, _ReadOpts) ->
  ?nif_stub.

%% @doc
%% Return a iterator over the contents of the specified column family.
-spec(iterators(DBHandle, CFHandle, ReadOpts) ->
             {ok, itr_handle()} | {error, any()} when DBHandle::db_handle(),
                                                      CFHandle::cf_handle(),
                                                      ReadOpts::read_options()).
iterators(_DBHandle, _CFHandle, _ReadOpts) ->
  ?nif_stub.

%% @doc
%% Return a coalescing iterator over multiple column families.
%% The iterator merges results from all column families and returns
%% keys in sorted order. When the same key exists in multiple column
%% families, only one value is returned (from the first CF in the list).
-spec(coalescing_iterator(DBHandle, CFHandles, ReadOpts) ->
             {ok, itr_handle()} | {error, any()} when DBHandle::db_handle(),
                                                      CFHandles::[cf_handle()],
                                                      ReadOpts::read_options()).
coalescing_iterator(_DBHandle, _CFHandles, _ReadOpts) ->
  ?nif_stub.

%% @doc
%% Move to the specified place
-spec(iterator_move(ITRHandle, ITRAction) ->
             {ok, Key::binary(), Value::binary()} |
             {ok, Key::binary()} |
             {error, invalid_iterator} |
             {error, iterator_closed} when ITRHandle::itr_handle(),
                                           ITRAction::iterator_action()).
iterator_move(_ITRHandle, _ITRAction) ->
  ?nif_stub.

%% @doc
%% Refresh iterator
-spec(iterator_refresh(ITRHandle) -> ok when ITRHandle::itr_handle()).
iterator_refresh(_ITRHandle) ->
    ?nif_stub.

%% @doc Load the blob value for the current iterator position.
%% Use with `{allow_unprepared_value, true}' to enable efficient key-only
%% scanning with selective value loading.
-spec(iterator_prepare_value(ITRHandle) -> ok | {error, any()} when ITRHandle::itr_handle()).
iterator_prepare_value(_ITRHandle) ->
    ?nif_stub.

%% @doc
%% Close a iterator
-spec(iterator_close(ITRHandle) -> ok | {error, _} when ITRHandle::itr_handle()).
iterator_close(_ITRHandle) ->
    ?nif_stub.

%% @doc Get the columns of the current iterator entry.
%% Returns the wide columns for the current entry. For entities, returns
%% all columns. For regular key-values, returns a single column with an
%% empty name (the default column) containing the value.
-spec iterator_columns(ITRHandle) -> Res when
    ITRHandle::itr_handle(),
    Res :: {ok, [{binary(), binary()}]} | {error, any()}.
iterator_columns(_ITRHandle) ->
    ?nif_stub.

%% @doc Delete an entity (same as regular delete).
%% Entities are deleted using the normal delete operation - all columns
%% are removed when the key is deleted.
-spec delete_entity(DBHandle, Key, WriteOpts) -> Res when
    DBHandle::db_handle(),
    Key::binary(),
    WriteOpts::write_options(),
    Res :: ok | {error, any()}.
delete_entity(DBHandle, Key, WriteOpts) ->
    delete(DBHandle, Key, WriteOpts).

%% @doc Delete an entity from a column family (same as regular delete).
-spec delete_entity(DBHandle, CFHandle, Key, WriteOpts) -> Res when
    DBHandle::db_handle(),
    CFHandle::cf_handle(),
    Key::binary(),
    WriteOpts::write_options(),
    Res :: ok | {error, any()}.
delete_entity(DBHandle, CFHandle, Key, WriteOpts) ->
    delete(DBHandle, CFHandle, Key, WriteOpts).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

%% @doc Calls Fun(Elem, AccIn) on successive elements in the default column family
%% starting with AccIn == Acc0.
%% Fun/2 must return a new accumulator which is passed to the next call.
%% The function returns the final value of the accumulator.
%% Acc0 is returned if the default column family is empty.
%%
%% this function is deprecated and will be removed in next major release.
%% You should use the `iterator' API instead.
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
%%
%% this function is deprecated and will be removed in next major release.
%% You should use the `iterator' API instead.
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
%%
%% this function is deprecated and will be removed in next major release.
%% You should use the `iterator' API instead.
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
%%
%% this function is deprecated and will be removed in next major release.
%% You should use the `iterator' API instead.
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
  ?nif_stub.

%% @doc Destroy the contents of the specified database.
%% Be very careful using this method.
-spec destroy(Name::file:filename_all(), DBOpts::db_options()) -> ok | {error, any()}.
destroy(_Name, _DBOpts) ->
  ?nif_stub.

%% @doc Try to repair as much of the contents of the database as possible.
%% Some data may be lost, so be careful when calling this function
-spec repair(Name::file:filename_all(), DBOpts::db_options()) -> ok | {error, any()}.
repair(_Name, _DBOpts) ->
   ?nif_stub.

%% @doc take a snapshot of a running RocksDB database in a separate directory
%% http://rocksdb.org/blog/2609/use-checkpoints-for-efficient-snapshots/
-spec checkpoint(
  DbHandle::db_handle(), Path::file:filename_all()
) -> ok | {error, any()}.
checkpoint(_DbHandle, _Path) ->
  ?nif_stub.

%% @doc Flush all mem-table data.
-spec flush(db_handle(), flush_options()) -> ok | {error, term()}.
flush(DbHandle, FlushOptions) ->
  flush(DbHandle, default_column_family, FlushOptions).

%% @doc Flush all mem-table data for a column family
-spec flush(db_handle(), column_family(), flush_options()) -> ok | {error, term()}.
flush(_DbHandle, _Cf, _FlushOptions) ->
  ?nif_stub.

%% @doc  Sync the wal. Note that Write() followed by SyncWAL() is not exactly the
%% same as Write() with sync=true: in the latter case the changes won't be
%% visible until the sync is done.
%% Currently only works if allow_mmap_writes = false in Options.
-spec sync_wal(db_handle()) -> ok | {error, term()}.
sync_wal(_DbHandle) ->
  ?nif_stub.



%% @doc Return the approximate number of keys in the default column family.
%% Implemented by calling GetIntProperty with "rocksdb.estimate-num-keys"
%%
%% this function is deprecated and will be removed in next major release.
-spec count(DBHandle::db_handle()) ->  non_neg_integer() | {error, any()}.
count(DBHandle) ->
  count_1(get_property(DBHandle, <<"rocksdb.estimate-num-keys">>)).

%% @doc
%% Return the approximate number of keys in the specified column family.
%%
%% this function is deprecated and will be removed in next major release.
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
  ?nif_stub.

%% @doc Return the RocksDB internal status of the specified column family specified at Property
-spec get_property(
  DBHandle::db_handle(), CFHandle::cf_handle(), Property::binary()
) -> {ok, binary()} | {error, any()}.
get_property(_DBHandle, _CFHandle, _Property) ->
  ?nif_stub.

%% @doc gThe sequence number of the most recent transaction.
-spec get_latest_sequence_number(Db :: db_handle()) -> Seq :: non_neg_integer().
get_latest_sequence_number(_DbHandle) ->
  ?nif_stub.

%% ===================================================================
%% Transaction Log API


%% @doc create a new iterator to retrive ethe transaction log since a sequce
-spec tlog_iterator(Db :: db_handle(),Since :: non_neg_integer()) -> {ok, Iterator :: term()}.
tlog_iterator(_DbHandle, _Since) ->
  ?nif_stub.

%% @doc close the transaction log
-spec tlog_iterator_close(term()) -> ok.
tlog_iterator_close(_Iterator) ->
  ?nif_stub.

%% @doc go to the last update as a binary in the transaction log, can be ussed with the write_binary_update function.
-spec tlog_next_binary_update(
        Iterator :: term()
       ) -> {ok, LastSeq :: non_neg_integer(), BinLog :: binary()} | {error, term()}.
tlog_next_binary_update(_Iterator) ->
  ?nif_stub.

%% @doc like `tlog_nex_binary_update/1' but also return the batch as a list of operations
-spec tlog_next_update(
        Iterator :: term()
       ) -> {ok, LastSeq :: non_neg_integer(), Log :: write_actions(), BinLog :: binary()} | {error, term()}.
tlog_next_update(_Iterator) ->
  ?nif_stub.

%% @doc apply a set of operation coming from a transaction log to another database. Can be useful to use it in slave
%% mode.
%%
-spec write_binary_update(
        DbHandle :: db_handle(), BinLog :: binary(), WriteOptions :: write_options()
       ) -> ok | {error, term()}.
write_binary_update(_DbHandle, _Update, _WriteOptions) ->
  ?nif_stub.



updates_iterator(DBH, Since) -> tlog_iterator(DBH, Since).
close_updates_iterator(Itr) -> tlog_iterator_close(Itr).
next_binary_update(Itr) -> tlog_next_binary_update(Itr).
next_update(Itr) -> tlog_next_update(Itr).

%% ===================================================================
%% Batch API

%% @doc create a new batch in memory. A batch is a nif resource attached to the current process. Pay attention when you
%% share it with other processes as it may not been released. To force its release you will need to use the close_batch
%% function.
-spec batch() -> {ok, Batch :: batch_handle()}.
batch() ->
  ?nif_stub.

-spec release_batch(Batch :: batch_handle()) -> ok.
release_batch(_Batch) ->
  ?nif_stub.

%% @doc write the batch to the database
-spec write_batch(Db :: db_handle(), Batch :: batch_handle(), WriteOptions :: write_options()) -> ok | {error, term()}.
write_batch(_DbHandle, _Batch, _WriteOptions) ->
  ?nif_stub.

%% @doc add a put operation to the batch
-spec batch_put(Batch :: batch_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_put(_Batch, _Key, _Value) ->
  ?nif_stub.

%% @doc like `batch_put/3' but apply the operation to a column family
-spec batch_put(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_put(_Batch, _ColumnFamily, _Key, _Value) ->
  ?nif_stub.

%% @doc add a merge operation to the batch
-spec batch_merge(Batch :: batch_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_merge(_Batch, _Key, _Value) ->
  ?nif_stub.

%% @doc like `batch_mege/3' but apply the operation to a column family
-spec batch_merge(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary(), Value :: binary()) -> ok.
batch_merge(_Batch, _ColumnFamily, _Key, _Value) ->
  ?nif_stub.

%% @doc batch implementation of delete operation to the batch
-spec batch_delete(Batch :: batch_handle(), Key :: binary()) -> ok.
batch_delete(_Batch, _Key) ->
  ?nif_stub.

%% @doc like `batch_delete/2' but apply the operation to a column family
-spec batch_delete(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary()) -> ok.
batch_delete(_Batch, _ColumnFamily, _Key) ->
  ?nif_stub.

%% @doc batch implementation of single_delete operation to the batch
-spec batch_single_delete(Batch :: batch_handle(), Key :: binary()) -> ok.
batch_single_delete(_Batch, _Key) ->
  ?nif_stub.

%% @doc like `batch_single_delete/2' but apply the operation to a column family
-spec batch_single_delete(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Key :: binary()) -> ok.
batch_single_delete(_Batch, _ColumnFamily, _Key) ->
  ?nif_stub.

%% @doc Batch implementation of `delete_range/5'
-spec batch_delete_range(Batch :: batch_handle(), Begin :: binary(), End :: binary()) -> ok.
batch_delete_range(_Batch, _Begin, _End) ->
  ?nif_stub.

%% @doc Like `batch_delete_range/3' but apply the operation to a column family
-spec batch_delete_range(Batch :: batch_handle(), ColumnFamily :: cf_handle(), Begin :: binary(), End :: binary()) -> ok.
batch_delete_range(_Batch, _ColumnFamily, _Begin, _End) ->
  ?nif_stub.

%% @doc return the number of operations in the batch
-spec batch_count(_Batch :: batch_handle()) -> Count :: non_neg_integer().
batch_count(_Batch) ->
  ?nif_stub.

%% @doc Retrieve data size of the batch.
-spec batch_data_size(_Batch :: batch_handle()) -> BatchSize :: non_neg_integer().
batch_data_size(_Batch) ->
  ?nif_stub.

%% @doc reset the batch, clear all operations.
-spec batch_clear(Batch :: batch_handle()) -> ok.
batch_clear(_Batch) ->
  ?nif_stub.

%% @doc store a checkpoint in the batch to which you can rollback later
-spec batch_savepoint(Batch :: batch_handle()) -> ok.
batch_savepoint(_Batch) ->
  ?nif_stub.

%% @doc rollback the operations to the latest checkpoint
-spec batch_rollback(Batch :: batch_handle()) -> ok.
batch_rollback(_Batch) ->
  ?nif_stub.

%% @doc return all the operation sin the batch as a list of operations
-spec batch_tolist(Batch :: batch_handle()) -> Ops :: write_actions().
batch_tolist(_Batch) ->
  ?nif_stub.

%% ===================================================================
%% Transaction API

%% @doc create a new transaction
%% When opened as a Transaction or Optimistic Transaction db,
%% a user can both read and write to a transaction without committing
%% anything to the disk until they decide to do so.
-spec transaction(TransactionDB :: db_handle(), WriteOptions :: write_options()) ->
                         {ok, transaction_handle()}.

transaction(_TransactionDB, _WriteOptions) ->
  ?nif_stub.

%% @doc release a transaction
-spec release_transaction(TransactionHandle::transaction_handle()) -> ok.
release_transaction(_TransactionHandle) ->
  ?nif_stub.

%% @doc add a put operation to the transaction
-spec transaction_put(Transaction :: transaction_handle(), Key :: binary(), Value :: binary()) -> ok | {error, any()}.
transaction_put(_Transaction, _Key, _Value) ->
  ?nif_stub.

%% @doc like `transaction_put/3' but apply the operation to a column family
-spec transaction_put(Transaction :: transaction_handle(), ColumnFamily :: cf_handle(), Key :: binary(),  Value :: binary()) -> ok | {error, any()}.
transaction_put(_Transaction, _ColumnFamily, _Key, _Value) ->
  ?nif_stub.

%% @doc do a get operation on the contents of the transaction
-spec transaction_get(Transaction :: transaction_handle(),
                      Key :: binary(),
                      Opts :: read_options()) ->
          Res :: {ok, binary()} |
                 not_found |
                 {error, {corruption, string()}} |
                 {error, any()}.
transaction_get(_Transaction, _Key, _Opts) ->
  ?nif_stub.

%% @doc like `transaction_get/3' but apply the operation to a column family
-spec transaction_get(Transaction :: transaction_handle(),
                      ColumnFamily :: cf_handle(),
                      Key :: binary(),
                      Opts :: read_options()) ->
          Res :: {ok, binary()} |
                 not_found |
                 {error, {corruption, string()}} |
                 {error, any()}.
transaction_get(_Transaction, _ColumnFamily, _Key, _Opts) ->
  ?nif_stub.

%% see comment in c_src/transaction.cc

%% %% @doc add a merge operation to the transaction
%% -spec transaction_merge(Transaction :: transaction_handle(), Key :: binary(), Value :: binary()) -> ok.
%% transaction_merge(_Transaction, _Key, _Value) ->
%%   ?nif_stub.

%% %% @doc like `transaction_merge/3' but apply the operation to a column family
%% -spec transaction_merge(Transaction :: transaction_handle(), ColumnFamily :: cf_handle(), Key :: binary(), Value :: binary()) -> ok.
%% transaction_merge(_Transaction, _ColumnFamily, _Key, _Value) ->
%%   ?nif_stub.

%% @doc transaction implementation of delete operation to the transaction
-spec transaction_delete(Transaction :: transaction_handle(), Key :: binary()) -> ok.
transaction_delete(_Transaction, _Key) ->
  ?nif_stub.

%% @doc like `transaction_delete/2' but apply the operation to a column family
-spec transaction_delete(Transaction :: transaction_handle(), ColumnFamily :: cf_handle(), Key :: binary()) -> ok.
transaction_delete(_Transaction, _ColumnFamily, _Key) ->
  ?nif_stub.

%% @doc Return a iterator over the contents of the database and
%% uncommited writes and deletes in the current transaction.
%% The result of iterator() is initially invalid (caller must
%% call iterator_move function on the iterator before using it).
-spec transaction_iterator(TransactionHandle, ReadOpts) -> Res when
  TransactionHandle::transaction_handle(),
  ReadOpts::read_options(),
  Res :: {ok, itr_handle()} | {error, any()}.
transaction_iterator(_TransactionHandle, _Ta_ReadOpts) ->
  ?nif_stub.

%% @doc Return a iterator over the contents of the database and
%% uncommited writes and deletes in the current transaction.
%% The result of iterator() is initially invalid (caller must
%% call iterator_move function on the iterator before using it).
-spec transaction_iterator(TransactionHandle, CFHandle, ReadOpts) -> Res when
  TransactionHandle::transaction_handle(),
  CFHandle::cf_handle(),
  ReadOpts::read_options(),
  Res :: {ok, itr_handle()} | {error, any()}.
transaction_iterator(_TransactionHandle, _CfHandle, _ReadOpts) ->
  ?nif_stub.

%% @doc commit a transaction to disk atomically (?)
-spec transaction_commit(Transaction :: transaction_handle()) -> ok | {error, term()}.
transaction_commit(_Transaction) ->
  ?nif_stub.

%% @doc rollback a transaction to disk atomically (?)
-spec transaction_rollback(Transaction :: transaction_handle()) -> ok | {error, term()}.
transaction_rollback(_Transaction) ->
  ?nif_stub.

%% ===================================================================
%% Pessimistic Transaction API

%% @doc open a database with pessimistic transaction support.
%% Pessimistic transactions acquire locks on keys when they are accessed,
%% providing strict serializability at the cost of potential lock contention.
-spec open_pessimistic_transaction_db(Name :: file:filename_all(), DbOpts :: db_options()) ->
    {ok, db_handle(), [cf_handle()]} | {error, any()}.
open_pessimistic_transaction_db(_Name, _DbOpts) ->
  open_pessimistic_transaction_db(_Name, _DbOpts, [{"default", []}]).

%% @doc open a database with pessimistic transaction support and column families.
-spec open_pessimistic_transaction_db(Name :: file:filename_all(),
                                       DbOpts :: db_options(),
                                       CfDescriptors :: [cf_descriptor()]) ->
    {ok, db_handle(), [cf_handle()]} | {error, any()}.
open_pessimistic_transaction_db(_Name, _DbOpts, _CFDescriptors) ->
  ?nif_stub.

%% @doc create a new pessimistic transaction.
%% Pessimistic transactions use row-level locking with deadlock detection.
-spec pessimistic_transaction(TransactionDB :: db_handle(), WriteOptions :: write_options()) ->
    {ok, transaction_handle()} | {error, any()}.
pessimistic_transaction(_TransactionDB, _WriteOptions) ->
  ?nif_stub.

%% @doc create a new pessimistic transaction with transaction options.
%% Transaction options include:
%%   {set_snapshot, boolean()} - acquire a snapshot at start
%%   {deadlock_detect, boolean()} - enable deadlock detection
%%   {lock_timeout, integer()} - lock wait timeout in ms
-spec pessimistic_transaction(TransactionDB :: db_handle(),
                               WriteOptions :: write_options(),
                               TxnOptions :: list()) ->
    {ok, transaction_handle()} | {error, any()}.
pessimistic_transaction(_TransactionDB, _WriteOptions, _TxnOptions) ->
  ?nif_stub.

%% @doc release a pessimistic transaction.
-spec release_pessimistic_transaction(TransactionHandle :: transaction_handle()) -> ok.
release_pessimistic_transaction(_TransactionHandle) ->
  ?nif_stub.

%% @doc put a key-value pair in the transaction.
-spec pessimistic_transaction_put(Transaction :: transaction_handle(),
                                   Key :: binary(),
                                   Value :: binary()) ->
    ok | {error, busy} | {error, timed_out} | {error, any()}.
pessimistic_transaction_put(_Transaction, _Key, _Value) ->
  ?nif_stub.

%% @doc put a key-value pair in a column family within the transaction.
-spec pessimistic_transaction_put(Transaction :: transaction_handle(),
                                   ColumnFamily :: cf_handle(),
                                   Key :: binary(),
                                   Value :: binary()) ->
    ok | {error, busy} | {error, timed_out} | {error, any()}.
pessimistic_transaction_put(_Transaction, _ColumnFamily, _Key, _Value) ->
  ?nif_stub.

%% @doc get a value from the transaction (read without acquiring lock).
-spec pessimistic_transaction_get(Transaction :: transaction_handle(),
                                   Key :: binary(),
                                   Opts :: read_options()) ->
    {ok, binary()} | not_found | {error, any()}.
pessimistic_transaction_get(_Transaction, _Key, _Opts) ->
  ?nif_stub.

%% @doc get a value from a column family within the transaction.
-spec pessimistic_transaction_get(Transaction :: transaction_handle(),
                                   ColumnFamily :: cf_handle(),
                                   Key :: binary(),
                                   Opts :: read_options()) ->
    {ok, binary()} | not_found | {error, any()}.
pessimistic_transaction_get(_Transaction, _ColumnFamily, _Key, _Opts) ->
  ?nif_stub.

%% @doc get a value and acquire an exclusive lock on the key.
%% This is useful for read-modify-write patterns.
-spec pessimistic_transaction_get_for_update(Transaction :: transaction_handle(),
                                              Key :: binary(),
                                              Opts :: read_options()) ->
    {ok, binary()} | not_found | {error, busy} | {error, timed_out} | {error, any()}.
pessimistic_transaction_get_for_update(_Transaction, _Key, _Opts) ->
  ?nif_stub.

%% @doc get a value from a column family and acquire an exclusive lock.
-spec pessimistic_transaction_get_for_update(Transaction :: transaction_handle(),
                                              ColumnFamily :: cf_handle(),
                                              Key :: binary(),
                                              Opts :: read_options()) ->
    {ok, binary()} | not_found | {error, busy} | {error, timed_out} | {error, any()}.
pessimistic_transaction_get_for_update(_Transaction, _ColumnFamily, _Key, _Opts) ->
  ?nif_stub.

%% @doc delete a key from the transaction.
-spec pessimistic_transaction_delete(Transaction :: transaction_handle(),
                                      Key :: binary()) ->
    ok | {error, busy} | {error, timed_out} | {error, any()}.
pessimistic_transaction_delete(_Transaction, _Key) ->
  ?nif_stub.

%% @doc delete a key from a column family within the transaction.
-spec pessimistic_transaction_delete(Transaction :: transaction_handle(),
                                      ColumnFamily :: cf_handle(),
                                      Key :: binary()) ->
    ok | {error, busy} | {error, timed_out} | {error, any()}.
pessimistic_transaction_delete(_Transaction, _ColumnFamily, _Key) ->
  ?nif_stub.

%% @doc create an iterator over the transaction's view of the database.
-spec pessimistic_transaction_iterator(TransactionHandle :: transaction_handle(),
                                        ReadOpts :: read_options()) ->
    {ok, itr_handle()} | {error, any()}.
pessimistic_transaction_iterator(_TransactionHandle, _ReadOpts) ->
  ?nif_stub.

%% @doc create an iterator over a column family within the transaction.
-spec pessimistic_transaction_iterator(TransactionHandle :: transaction_handle(),
                                        CFHandle :: cf_handle(),
                                        ReadOpts :: read_options()) ->
    {ok, itr_handle()} | {error, any()}.
pessimistic_transaction_iterator(_TransactionHandle, _CfHandle, _ReadOpts) ->
  ?nif_stub.

%% @doc commit the transaction atomically.
-spec pessimistic_transaction_commit(Transaction :: transaction_handle()) ->
    ok | {error, busy} | {error, expired} | {error, any()}.
pessimistic_transaction_commit(_Transaction) ->
  ?nif_stub.

%% @doc rollback the transaction, discarding all changes.
-spec pessimistic_transaction_rollback(Transaction :: transaction_handle()) ->
    ok | {error, any()}.
pessimistic_transaction_rollback(_Transaction) ->
  ?nif_stub.

%% @doc set a savepoint in a pessimistic transaction.
%% Use {@link pessimistic_transaction_rollback_to_savepoint/1} to rollback to this point.
-spec pessimistic_transaction_set_savepoint(Transaction :: transaction_handle()) -> ok.
pessimistic_transaction_set_savepoint(_Transaction) ->
  ?nif_stub.

%% @doc rollback a pessimistic transaction to the most recent savepoint.
%% All operations since the last call to {@link pessimistic_transaction_set_savepoint/1}
%% are undone and the savepoint is removed.
-spec pessimistic_transaction_rollback_to_savepoint(Transaction :: transaction_handle()) ->
    ok | {error, any()}.
pessimistic_transaction_rollback_to_savepoint(_Transaction) ->
  ?nif_stub.

%% @doc pop the most recent savepoint without rolling back.
%% The savepoint is simply discarded.
-spec pessimistic_transaction_pop_savepoint(Transaction :: transaction_handle()) ->
    ok | {error, any()}.
pessimistic_transaction_pop_savepoint(_Transaction) ->
  ?nif_stub.

%% @doc get the unique ID of a pessimistic transaction.
%% This ID can be used to identify the transaction in deadlock detection
%% and waiting transaction lists.
-spec pessimistic_transaction_get_id(Transaction :: transaction_handle()) ->
    {ok, non_neg_integer()}.
pessimistic_transaction_get_id(_Transaction) ->
  ?nif_stub.

%% @doc get information about transactions this transaction is waiting on.
%% Returns a map with:
%% - `column_family_id': The column family ID of the key being waited on
%% - `key': The key being waited on (binary)
%% - `waiting_txns': List of transaction IDs that hold locks this transaction needs
%%
%% If the transaction is not currently waiting, returns an empty waiting_txns list.
-spec pessimistic_transaction_get_waiting_txns(Transaction :: transaction_handle()) ->
    {ok, #{column_family_id := non_neg_integer(),
           key := binary(),
           waiting_txns := [non_neg_integer()]}}.
pessimistic_transaction_get_waiting_txns(_Transaction) ->
  ?nif_stub.

%% ===================================================================
%% Backup Engine API

%% @doc open a new backup engine for creating new backups.
-spec open_backup_engine(Path :: string()) -> {ok, backup_engine()} | {error, term()}.
open_backup_engine(_Path) ->
  ?nif_stub.


%% @doc stop and close the backup engine
%% note: experimental for testing only
-spec close_backup_engine(backup_engine()) -> ok.
close_backup_engine(_BackupEngine) ->
  ?nif_stub.

%% @doc  Will delete all the files we don't need anymore
%% It will do the full scan of the files/ directory and delete all the
%% files that are not referenced.
-spec gc_backup_engine(backup_engine()) -> ok.
gc_backup_engine(_BackupEngine) ->
  ?nif_stub.

%% %% @doc Call this from another process if you want to stop the backup
%% that is currently happening. It will return immediatelly, will
%% not wait for the backup to stop.
%% The backup will stop ASAP and the call to CreateNewBackup will
%% return Status::Incomplete(). It will not clean up after itself, but
%% the state will remain consistent. The state will be cleaned up
%% next time you create BackupableDB or RestoreBackupableDB.
-spec stop_backup(backup_engine()) -> ok.
stop_backup(_BackupEngine) ->
  ?nif_stub.

%% @doc Captures the state of the database in the latest backup
-spec create_new_backup(BackupEngine :: backup_engine(), Db :: db_handle()) -> ok | {error, term()}.
create_new_backup(_BackupEngine, _DbHandle) ->
  ?nif_stub.

%% @doc Returns info about backups in backup_info
-spec get_backup_info(backup_engine()) -> [backup_info()].
get_backup_info(_BackupEngine) ->
  ?nif_stub.

%% @doc checks that each file exists and that the size of the file matches
%% our expectations. it does not check file checksum.
-spec verify_backup(BackupEngine :: backup_engine(), BackupId :: non_neg_integer()) -> ok | {error, any()}.
verify_backup(_BackupEngine, _BackupId) ->
  ?nif_stub.

%% @doc deletes a specific backup
-spec delete_backup(BackupEngine :: backup_engine(), BackupId :: non_neg_integer()) -> ok | {error, any()}.
delete_backup(_BackupEngine, _BackupId) ->
  ?nif_stub.

%% @doc deletes old backups, keeping latest num_backups_to_keep alive
-spec purge_old_backup(BackupEngine :: backup_engine(), NumBackupToKeep :: non_neg_integer()) -> ok | {error, any()}.
purge_old_backup(_BackupEngine, _NumBackupToKeep) ->
  ?nif_stub.

%% @doc restore from backup with backup_id
-spec restore_db_from_backup(BackupEngine, BackupId, DbDir) -> Result when
  BackupEngine :: backup_engine(),
  BackupId :: non_neg_integer(),
  DbDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_backup(_BackupEngine, _BackupId, _DbDir) ->
  ?nif_stub.

%% @doc restore from backup with backup_id
-spec restore_db_from_backup(BackupEngine, BackupId, DbDir, WalDir) -> Result when
  BackupEngine :: backup_engine(),
  BackupId :: non_neg_integer(),
  DbDir :: string(),
  WalDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_backup(_BackupEngine, _BackupId, _DbDir, _WalDir) ->
  ?nif_stub.

%% @doc restore from the latest backup
-spec restore_db_from_latest_backup(BackupEngine, DbDir) -> Result when
  BackupEngine :: backup_engine(),
  DbDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_latest_backup(_BackupEngine, _DbDir) ->
  ?nif_stub.

%% @doc restore from the latest backup
-spec restore_db_from_latest_backup(BackupEngine,  DbDir, WalDir) -> Result when
  BackupEngine :: backup_engine(),
  DbDir :: string(),
  WalDir :: string(),
  Result :: ok | {error, any()}.
restore_db_from_latest_backup(_BackupEngine,  _DbDir, _WalDir) ->
  ?nif_stub.



%% ===================================================================
%% Cache API

%% @doc // Create a new cache.

%M Whi the type `lru' it create a cache  with a fixed size capacity. The cache is sharded
%% to 2^num_shard_bits shards, by hash of the key. The total capacity
%% is divided and evenly assigned to each shard. With the type `clock`, it creates a
%% cache based on CLOCK algorithm with better concurrent performance in some cases. See util/clock_cache.cc for
%% more detail.
-spec new_cache(Type :: cache_type(), Capacity :: non_neg_integer()) -> {ok, cache_handle()}.
new_cache(_Type, _Capacity) ->
  ?nif_stub.

%% @doc return informations of a cache as a list of tuples.
%% * `{capacity, integer >=0}'
%%      the maximum configured capacity of the cache.
%% * `{strict_capacity, boolean}'
%%      the flag whether to return error on insertion when cache reaches its full capacity.
%% * `{usage, integer >=0}'
%%      the memory size for the entries residing in the cache.
%% * `{pinned_usage, integer >= 0}'
%%      the memory size for the entries in use by the system
-spec cache_info(Cache) -> InfoList when
  Cache :: cache_handle(),
  InfoList :: [InfoTuple],
  InfoTuple :: {capacity, non_neg_integer()}
            |  {strict_capacity, boolean()}
            |  {usage, non_neg_integer()}
            |  {pinned_usage, non_neg_integer()}.     
cache_info(_Cache) ->
  ?nif_stub.

%% @doc return the information associated with Item for cache Cache
-spec cache_info(Cache, Item) -> Value when
  Cache :: cache_handle(),
  Item :: capacity | strict_capacity | usage | pinned_usage,
  Value :: term().
cache_info(_Cache, _Item) ->
  ?nif_stub.

%% @doc sets the maximum configured capacity of the cache. When the new
%% capacity is less than the old capacity and the existing usage is
%% greater than new capacity, the implementation will do its best job to
%% purge the released entries from the cache in order to lower the usage
-spec set_capacity(Cache :: cache_handle(), Capacity :: non_neg_integer()) -> ok.
set_capacity(_Cache, _Capacity) ->
  ?nif_stub.

%% @doc sets strict_capacity_limit flag of the cache. If the flag is set
%% to true, insert to cache will fail if no enough capacity can be free.
-spec set_strict_capacity_limit(Cache :: cache_handle(), StrictCapacityLimit :: boolean()) -> ok.
set_strict_capacity_limit(_Cache, _StrictCapacityLimit) ->
    ?nif_stub.

%% @doc release the cache
release_cache(_Cache) ->
  ?nif_stub.

new_lru_cache(Capacity) -> new_cache(lru, Capacity).
new_clock_cache(Capacity) -> new_cache(clock, Capacity).
get_usage(Cache) -> cache_info(Cache, usage).
get_pinned_usage(Cache) -> cache_info(Cache, pinned_usage).
get_capacity(Cache) -> cache_info(Cache, capacity).

%% ===================================================================
%% Limiter functions

%% @doc create new Limiter
new_rate_limiter(_RateBytesPerSec, _Auto) ->
    ?nif_stub.

%% @doc release the limiter
release_rate_limiter(_Limiter) ->
    ?nif_stub.



%% ===================================================================
%% Env API

%% @doc return a default db environment
-spec new_env() -> {ok, env_handle()}.
new_env() -> new_env(default).

%% @doc return a db environment
-spec new_env(EnvType :: env_type()) -> {ok, env_handle()}.
new_env(_EnvType) ->
  ?nif_stub.

%% @doc set background threads of an environment
-spec set_env_background_threads(Env :: env_handle(), N :: non_neg_integer()) -> ok.
set_env_background_threads(_Env, _N) ->
  ?nif_stub.

%% @doc set background threads of low and high prioriry threads pool of an environment
%% Flush threads are in the HIGH priority pool, while compaction threads are in the
%% LOW priority pool. To increase the number of threads in each pool call:
-spec set_env_background_threads(Env :: env_handle(), N :: non_neg_integer(), Priority :: env_priority()) -> ok.
set_env_background_threads(_Env, _N, _PRIORITY) ->
  ?nif_stub.

%% @doc destroy an environment
-spec destroy_env(Env :: env_handle()) -> ok.
destroy_env(_Env) ->
  ?nif_stub.


%% @doc set background threads of a database
-spec set_db_background_threads(DB :: db_handle(), N :: non_neg_integer()) -> ok.
set_db_background_threads(_Db, _N) ->
  ?nif_stub.

%% @doc set database background threads of low and high prioriry threads pool of an environment
%% Flush threads are in the HIGH priority pool, while compaction threads are in the
%% LOW priority pool. To increase the number of threads in each pool call:
-spec set_db_background_threads(DB :: db_handle(), N :: non_neg_integer(), Priority :: env_priority()) -> ok.
set_db_background_threads(_Db, _N, _PRIORITY) ->
  ?nif_stub.

default_env() -> new_env(default).

mem_env() -> new_env(memenv).

%% ===================================================================
%% SstFileManager functions

%% @doc create new SstFileManager with the default options:
%% RateBytesPerSec = 0, MaxTrashDbRatio = 0.25, BytesMaxDeleteChunk = 64 * 1024 * 1024.
-spec new_sst_file_manager(env_handle()) -> {ok, sst_file_manager()} | {error, any()}.
new_sst_file_manager(Env) ->
   ?MODULE:new_sst_file_manager(Env, []).

%% @doc create new SstFileManager that can be shared among multiple RocksDB
%% instances to track SST file and control there deletion rate.
%%
%%  * `Env' is an environment resource created using `rocksdb:new_env/{0,1}'.
%%  * `delete_rate_bytes_per_sec': How many bytes should be deleted per second, If
%%     this value is set to 1024 (1 Kb / sec) and we deleted a file of size 4 Kb
%%     in 1 second, we will wait for another 3 seconds before we delete other
%%     files, Set to 0 to disable deletion rate limiting.
%%  * `max_trash_db_ratio':  If the trash size constitutes for more than this
%%     fraction of the total DB size we will start deleting new files passed to
%%     DeleteScheduler immediately
%%  * `bytes_max_delete_chunk':  if a file to delete is larger than delete
%%     chunk, ftruncate the file by this size each time, rather than dropping the
%%     whole file. 0 means to always delete the whole file. If the file has more
%%     than one linked names, the file will be deleted as a whole. Either way,
%%     `delete_rate_bytes_per_sec' will be appreciated. NOTE that with this option,
%%     files already renamed as a trash may be partial, so users should not
%%     directly recover them without checking.
-spec new_sst_file_manager(Env, OptionsList) -> Result when
  Env :: env_handle(),
  OptionsList :: [OptionTuple],
  OptionTuple :: {delete_rate_bytes_per_sec, non_neg_integer()}
               | {max_trash_db_ratio, float()}
               | {bytes_max_delete_chunk, non_neg_integer()},
  Result :: {ok, sst_file_manager()} | {error, any()}.
new_sst_file_manager(_Env, _OptionsList) ->
    ?nif_stub.

%% @doc release the SstFileManager
-spec release_sst_file_manager(sst_file_manager()) -> ok.
release_sst_file_manager(_SstFileManager) ->
    ?nif_stub.

%% @doc set certains flags for the SST file manager
%% * `max_allowed_space_usage': Update the maximum allowed space that should be used by RocksDB, if
%%    the total size of the SST files exceeds MaxAllowedSpace, writes to
%%    RocksDB will fail.
%%
%%    Setting MaxAllowedSpace to 0 will disable this feature; maximum allowed
%%    pace will be infinite (Default value).
%% * `compaction_buffer_size': Set the amount of buffer room each compaction should be able to leave.
%%    In other words, at its maximum disk space consumption, the compaction
%%    should still leave compaction_buffer_size available on the disk so that
%%    other background functions may continue, such as logging and flushing.
%% * `delete_rate_bytes_per_sec': Update the delete rate limit in bytes per second.
%%    zero means disable delete rate limiting and delete files immediately
%% * `max_trash_db_ratio': Update trash/DB size ratio where new files will be deleted immediately (float)
-spec sst_file_manager_flag(SstFileManager, Flag, Value) -> Result when
  SstFileManager :: sst_file_manager(),
  Flag :: max_allowed_space_usage | compaction_buffer_size | delete_rate_bytes_per_sec | max_trash_db_ratio,
  Value :: non_neg_integer() | float(),
  Result :: ok.
sst_file_manager_flag(_SstFileManager, _Flag, _Val) ->
  ?nif_stub.

%% @doc return informations of a Sst File Manager as a list of tuples.
%%
%% * `{total_size, Int>0}': total size of all tracked files
%% * `{delete_rate_bytes_per_sec, Int > 0}': delete rate limit in bytes per second
%% * `{max_trash_db_ratio, Float>0}': trash/DB size ratio where new files will be deleted immediately
%% * `{total_trash_size, Int > 0}': total size of trash files
%% * `{is_max_allowed_space_reached, Boolean}' true if the total size of SST files exceeded the maximum allowed space usage
%% * `{max_allowed_space_reached_including_compactions, Boolean}': true if the total size of SST files as well as
%%   estimated size of ongoing compactions exceeds the maximums allowed space usage
-spec sst_file_manager_info(SstFileManager) -> InfoList when
  SstFileManager :: sst_file_manager(),
  InfoList :: [InfoTuple],
  InfoTuple :: {total_size, non_neg_integer()}
             | {delete_rate_bytes_per_sec, non_neg_integer()}
             | {max_trash_db_ratio, float()}
             | {total_trash_size, non_neg_integer()}
             | {is_max_allowed_space_reached, boolean()}
             | {max_allowed_space_reached_including_compactions, boolean()}.
sst_file_manager_info(_SstFileManager) ->
  ?nif_stub.

%% @doc return the information associated with Item for an SST File Manager SstFileManager
-spec sst_file_manager_info(SstFileManager, Item) -> Value when
    SstFileManager :: sst_file_manager(),
    Item :: total_size | delete_rate_bytes_per_sec
          | max_trash_db_ratio | total_trash_size
          | is_max_allowed_space_reached
          | max_allowed_space_reached_including_compactions,
    Value :: term().
sst_file_manager_info(_SstFileManager, _Item) ->
  ?nif_stub.


%% ===================================================================
%% WriteBufferManager functions

%% @doc  create a new WriteBufferManager.
-spec new_write_buffer_manager(BufferSize::non_neg_integer()) -> {ok, write_buffer_manager()}.
new_write_buffer_manager(_BufferSize) ->
  ?nif_stub.

%% @doc  create a new WriteBufferManager. a  WriteBufferManager is for managing memory
%% allocation for one or more MemTables.
%%
%% The memory usage of memtable will report to this object. The same object
%% can be passed into multiple DBs and it will track the sum of size of all
%% the DBs. If the total size of all live memtables of all the DBs exceeds
%% a limit, a flush will be triggered in the next DB to which the next write
%% is issued.
%%
%% If the object is only passed to on DB, the behavior is the same as
%% db_write_buffer_size. When write_buffer_manager is set, the value set will
%% override db_write_buffer_size.
-spec new_write_buffer_manager(BufferSize::non_neg_integer(), Cache::cache_handle()) -> {ok, write_buffer_manager()}.
new_write_buffer_manager(_BufferSize, _Cache) ->
  ?nif_stub.

-spec release_write_buffer_manager(write_buffer_manager()) -> ok.
release_write_buffer_manager(_WriteBufferManager) ->
  ?nif_stub.

%% @doc return informations of a Write Buffer Manager as a list of tuples.
-spec write_buffer_manager_info(WriteBufferManager) -> InfoList when
    WriteBufferManager :: write_buffer_manager(),
    InfoList :: [InfoTuple],
    InfoTuple :: {memory_usage, non_neg_integer()}
               | {mutable_memtable_memory_usage, non_neg_integer()}
               | {buffer_size, non_neg_integer()}
               | {enabled, boolean()}.
write_buffer_manager_info(_WriteBufferManager) ->
  ?nif_stub.

%% @doc return the information associated with Item for a Write Buffer Manager.
-spec write_buffer_manager_info(WriteBufferManager, Item) -> Value when
    WriteBufferManager :: write_buffer_manager(),
    Item :: memory_usage | mutable_memtable_memory_usage | buffer_size | enabled,
    Value :: term().
write_buffer_manager_info(_WriteBufferManager, _Item) ->
  ?nif_stub.

%% ===================================================================
%% Statistics API

-spec new_statistics() -> {ok, statistics_handle()}.
new_statistics() ->
  ?nif_stub.

-spec set_stats_level(statistics_handle(), stats_level()) -> ok.
set_stats_level(_StatisticsHandle, _StatsLevel) ->
  ?nif_stub.

-spec statistics_info(Statistics) -> InfoList when
  Statistics :: statistics_handle(),
  InfoList :: [InfoTuple],
  InfoTuple :: {stats_level, stats_level()}.
statistics_info(_Statistics) ->
  ?nif_stub.

%% @doc Get the count for a specific statistics ticker.
%% Returns the count for BlobDB related tickers such as blob_db_num_put,
%% blob_db_cache_hit, blob_db_gc_num_files, etc.
-spec statistics_ticker(statistics_handle(), blob_db_ticker()) -> {ok, non_neg_integer()}.
statistics_ticker(_Statistics, _Ticker) ->
  ?nif_stub.

%% @doc Get histogram data for a specific statistics histogram.
%% Returns histogram information including median, percentiles, average, etc.
%% For integrated BlobDB, relevant histograms are blob_db_blob_file_write_micros,
%% blob_db_blob_file_read_micros, blob_db_compression_micros, etc.
-spec statistics_histogram(statistics_handle(), blob_db_histogram()) -> {ok, histogram_info()}.
statistics_histogram(_Statistics, _Histogram) ->
  ?nif_stub.


%% @doc release the Statistics Handle
-spec release_statistics(statistics_handle()) -> ok.
release_statistics(_Statistics) ->
    ?nif_stub.

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
