// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
// Copyright (c) 2016-2017 Benoit Chesneau
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "atoms.h"
#include "erocksdb.h"
#include "refobjects.h"
#include "cache.h"
#include "rate_limiter.h"
#include "env.h"

static ErlNifFunc nif_funcs[] =
{


  {"open", 2, erocksdb::Open, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"open_with_cf", 3, erocksdb::OpenWithCf, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"close", 1, erocksdb::Close, ERL_NIF_DIRTY_JOB_IO_BOUND},


  // db management
  {"checkpoint", 2, erocksdb::Checkpoint, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"repair", 2, erocksdb::Repair, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"is_empty", 1, erocksdb::IsEmpty, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"destroy", 2, erocksdb::Destroy, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"get_property", 2, erocksdb::GetProperty},
  {"get_property", 3, erocksdb::GetProperty},
  {"flush", 1, erocksdb::Flush, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"flush", 2, erocksdb::Flush, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"sync_wal", 1, erocksdb::SyncWal, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"set_db_background_threads", 2, erocksdb::SetDBBackgroundThreads},


  {"delete_range", 4, erocksdb::DeleteRange, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"delete_range", 5, erocksdb::DeleteRange, ERL_NIF_DIRTY_JOB_IO_BOUND},

  // column families
  {"list_column_families", 2, erocksdb::ListColumnFamilies, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"create_column_family", 3, erocksdb::CreateColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"drop_column_family", 1, erocksdb::DropColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"destroy_column_family", 1, erocksdb::DestroyColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},

  // kv operations
  {"get", 3, erocksdb::Get, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"get", 4, erocksdb::Get, ERL_NIF_DIRTY_JOB_IO_BOUND},

  {"snapshot", 1, erocksdb::Snapshot},
  {"release_snapshot", 1, erocksdb::ReleaseSnapshot},
  {"get_snapshot_sequence", 1, erocksdb::GetSnapshotSequenceNumber},

  // iterator operations
  {"iterator", 2, erocksdb::Iterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"iterator", 3, erocksdb::Iterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"iterators", 3, erocksdb::Iterators, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"iterator_move", 2, erocksdb::IteratorMove, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"iterator_refresh", 1, erocksdb::IteratorRefresh, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"iterator_close", 1, erocksdb::IteratorClose, ERL_NIF_DIRTY_JOB_IO_BOUND},

  {"get_latest_sequence_number", 1, erocksdb::GetLatestSequenceNumber},

  // transactions
  {"updates_iterator", 2, erocksdb::UpdatesIterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"close_updates_iterator", 1, erocksdb::UpdatesIteratorClose, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"next_binary_update", 1, erocksdb::NextBinaryUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"write_binary_update", 3, erocksdb::WriteBinaryUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"next_update", 1, erocksdb::NextUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},

  // Batch
  {"batch", 0, erocksdb::NewBatch},
  {"close_batch", 1, erocksdb::CloseBatch},
  {"write_batch", 3, erocksdb::WriteBatch, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"batch_put", 3, erocksdb::PutBatch},
  {"batch_put", 4, erocksdb::PutBatch},
  {"batch_delete", 2, erocksdb::DeleteBatch},
  {"batch_delete", 3, erocksdb::DeleteBatch},
  {"batch_single_delete", 2, erocksdb::SingleDeleteBatch},
  {"batch_single_delete", 3, erocksdb::SingleDeleteBatch},
  {"batch_clear", 1, erocksdb::ClearBatch},
  {"batch_savepoint", 1, erocksdb::BatchSetSavePoint},
  {"batch_rollback", 1, erocksdb::BatchRollbackToSavePoint},
  {"batch_count", 1, erocksdb::BatchCount},
  {"batch_tolist", 1, erocksdb::BatchToList, ERL_NIF_DIRTY_JOB_CPU_BOUND},


  // backup engine
  {"open_backup_engine", 1, erocksdb::OpenBackupEngine, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"create_new_backup", 2, erocksdb::CreateNewBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"get_backup_info", 1, erocksdb::GetBackupInfo, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"verify_backup", 2, erocksdb::VerifyBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"stop_backup", 1, erocksdb::StopBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"delete_backup", 2, erocksdb::DeleteBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"purge_old_backup", 2, erocksdb::PurgeOldBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"restore_db_from_backup", 3, erocksdb::RestoreDBFromBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"restore_db_from_backup", 4, erocksdb::RestoreDBFromBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"restore_db_from_latest_backup", 2, erocksdb::RestoreDBFromLatestBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"restore_db_from_latest_backup", 3, erocksdb::RestoreDBFromLatestBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"garbage_collect_backup", 1, erocksdb::GarbageCollect, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"close_backup", 1, erocksdb::CloseBackup, ERL_NIF_DIRTY_JOB_IO_BOUND},

  // cache
  {"new_lru_cache", 1, erocksdb::NewLRUCache},
  {"new_clock_cache", 1, erocksdb::NewClockCache},
  {"get_usage", 1, erocksdb::GetUsage},
  {"get_pinned_usage", 1, erocksdb::GetPinnedUsage},
  {"set_capacity", 2, erocksdb::SetCapacity, ERL_NIF_DIRTY_JOB_CPU_BOUND},
  {"get_capacity", 1, erocksdb::GetCapacity},
  {"release_cache", 1, erocksdb::ReleaseCache},

  // rate limiter
  {"new_rate_limiter", 2, erocksdb::NewRateLimiter},
  {"release_rate_limiter", 1, erocksdb::ReleaseRateLimiter},

  // env
  {"default_env", 0, erocksdb::DefaultEnv},
  {"mem_env", 0, erocksdb::MemEnv},
  {"set_env_background_threads", 2, erocksdb::SetEnvBackgroundThreads},
  {"set_env_background_threads", 3, erocksdb::SetEnvBackgroundThreads},
  {"destroy_env", 1, erocksdb::DestroyEnv}


};

namespace erocksdb {

// Atoms (initialized in on_load)
// Related to Erlang
ERL_NIF_TERM ATOM_TRUE;
ERL_NIF_TERM ATOM_FALSE;
ERL_NIF_TERM ATOM_OK;
ERL_NIF_TERM ATOM_ERROR;
ERL_NIF_TERM ATOM_EINVAL;
ERL_NIF_TERM ATOM_BADARG;
ERL_NIF_TERM ATOM_NOT_FOUND;
ERL_NIF_TERM ATOM_INC;
ERL_NIF_TERM ATOM_DEC;

// related to envs
ERL_NIF_TERM ATOM_DEFAULT;
ERL_NIF_TERM ATOM_MEMENV;

// Related to CFOptions
ERL_NIF_TERM ATOM_BLOCK_CACHE_SIZE_MB_FOR_POINT_LOOKUP;
ERL_NIF_TERM ATOM_MEMTABLE_MEMORY_BUDGET;
ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
ERL_NIF_TERM ATOM_MAX_WRITE_BUFFER_NUMBER;
ERL_NIF_TERM ATOM_MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
ERL_NIF_TERM ATOM_COMPRESSION;
ERL_NIF_TERM ATOM_NUM_LEVELS;
ERL_NIF_TERM ATOM_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
ERL_NIF_TERM ATOM_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
ERL_NIF_TERM ATOM_LEVEL0_STOP_WRITES_TRIGGER;
ERL_NIF_TERM ATOM_MAX_MEM_COMPACTION_LEVEL;
ERL_NIF_TERM ATOM_TARGET_FILE_SIZE_BASE;
ERL_NIF_TERM ATOM_TARGET_FILE_SIZE_MULTIPLIER;
ERL_NIF_TERM ATOM_MAX_BYTES_FOR_LEVEL_BASE;
ERL_NIF_TERM ATOM_MAX_BYTES_FOR_LEVEL_MULTIPLIER;
ERL_NIF_TERM ATOM_MAX_COMPACTION_BYTES;
ERL_NIF_TERM ATOM_SOFT_RATE_LIMIT;
ERL_NIF_TERM ATOM_HARD_RATE_LIMIT;
ERL_NIF_TERM ATOM_ARENA_BLOCK_SIZE;
ERL_NIF_TERM ATOM_DISABLE_AUTO_COMPACTIONS;
ERL_NIF_TERM ATOM_PURGE_REDUNDANT_KVS_WHILE_FLUSH;
ERL_NIF_TERM ATOM_COMPACTION_STYLE;
ERL_NIF_TERM ATOM_FILTER_DELETES;
ERL_NIF_TERM ATOM_MAX_SEQUENTIAL_SKIP_IN_ITERATIONS;
ERL_NIF_TERM ATOM_INPLACE_UPDATE_SUPPORT;
ERL_NIF_TERM ATOM_INPLACE_UPDATE_NUM_LOCKS;
ERL_NIF_TERM ATOM_TABLE_FACTORY_BLOCK_CACHE_SIZE;
ERL_NIF_TERM ATOM_IN_MEMORY_MODE;
ERL_NIF_TERM ATOM_IN_MEMORY;
ERL_NIF_TERM ATOM_BLOCK_BASED_TABLE_OPTIONS;
ERL_NIF_TERM ATOM_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES;

// Related to DBOptions
ERL_NIF_TERM ATOM_TOTAL_THREADS;
ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
ERL_NIF_TERM ATOM_CREATE_MISSING_COLUMN_FAMILIES;
ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
ERL_NIF_TERM ATOM_PARANOID_CHECKS;
ERL_NIF_TERM ATOM_MAX_OPEN_FILES;
ERL_NIF_TERM ATOM_MAX_TOTAL_WAL_SIZE;
ERL_NIF_TERM ATOM_USE_FSYNC;
ERL_NIF_TERM ATOM_DB_PATHS;
ERL_NIF_TERM ATOM_DB_LOG_DIR;
ERL_NIF_TERM ATOM_WAL_DIR;
ERL_NIF_TERM ATOM_DELETE_OBSOLETE_FILES_PERIOD_MICROS;
ERL_NIF_TERM ATOM_MAX_BACKGROUND_JOBS;
ERL_NIF_TERM ATOM_MAX_BACKGROUND_COMPACTIONS;
ERL_NIF_TERM ATOM_MAX_BACKGROUND_FLUSHES;
ERL_NIF_TERM ATOM_MAX_LOG_FILE_SIZE;
ERL_NIF_TERM ATOM_LOG_FILE_TIME_TO_ROLL;
ERL_NIF_TERM ATOM_KEEP_LOG_FILE_NUM;
ERL_NIF_TERM ATOM_MAX_MANIFEST_FILE_SIZE;
ERL_NIF_TERM ATOM_TABLE_CACHE_NUMSHARDBITS;
ERL_NIF_TERM ATOM_WAL_TTL_SECONDS;
ERL_NIF_TERM ATOM_WAL_SIZE_LIMIT_MB;
ERL_NIF_TERM ATOM_MANIFEST_PREALLOCATION_SIZE;
ERL_NIF_TERM ATOM_ALLOW_MMAP_READS;
ERL_NIF_TERM ATOM_ALLOW_MMAP_WRITES;
ERL_NIF_TERM ATOM_IS_FD_CLOSE_ON_EXEC;
ERL_NIF_TERM ATOM_SKIP_LOG_ERROR_ON_RECOVERY;
ERL_NIF_TERM ATOM_STATS_DUMP_PERIOD_SEC;
ERL_NIF_TERM ATOM_ADVISE_RANDOM_ON_OPEN;
ERL_NIF_TERM ATOM_ACCESS_HINT;
ERL_NIF_TERM ATOM_COMPACTION_READAHEAD_SIZE;
ERL_NIF_TERM ATOM_USE_ADAPTIVE_MUTEX;
ERL_NIF_TERM ATOM_BYTES_PER_SYNC;
ERL_NIF_TERM ATOM_SKIP_STATS_UPDATE_ON_DB_OPEN;
ERL_NIF_TERM ATOM_WAL_RECOVERY_MODE;
ERL_NIF_TERM ATOM_ALLOW_CONCURRENT_MEMTABLE_WRITE;
ERL_NIF_TERM ATOM_ENABLE_WRITE_THREAD_ADAPTATIVE_YIELD;
ERL_NIF_TERM ATOM_DB_WRITE_BUFFER_SIZE;
ERL_NIF_TERM ATOM_RATE_LIMITER;


// Related to BlockBasedTable Options
ERL_NIF_TERM ATOM_NO_BLOCK_CACHE;
ERL_NIF_TERM ATOM_BLOCK_CACHE;
ERL_NIF_TERM ATOM_BLOCK_SIZE;
ERL_NIF_TERM ATOM_BLOCK_CACHE_SIZE;
ERL_NIF_TERM ATOM_BLOOM_FILTER_POLICY;
ERL_NIF_TERM ATOM_FORMAT_VERSION;
ERL_NIF_TERM ATOM_CACHE_INDEX_AND_FILTER_BLOCKS;

// Related to Read Options
ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
ERL_NIF_TERM ATOM_FILL_CACHE;
ERL_NIF_TERM ATOM_ITERATE_UPPER_BOUND;
ERL_NIF_TERM ATOM_TAILING;
ERL_NIF_TERM ATOM_TOTAL_ORDER_SEEK;
ERL_NIF_TERM ATOM_SNAPSHOT;
ERL_NIF_TERM ATOM_BAD_SNAPSHOT;

// Related to Write Options
ERL_NIF_TERM ATOM_SYNC;
ERL_NIF_TERM ATOM_DISABLE_WAL;
ERL_NIF_TERM ATOM_IGNORE_MISSING_COLUMN_FAMILIES;

// Related to Write Actions
ERL_NIF_TERM ATOM_CLEAR;
ERL_NIF_TERM ATOM_PUT;
ERL_NIF_TERM ATOM_DELETE;
ERL_NIF_TERM ATOM_SINGLE_DELETE;

// Related to Iterator Actions
ERL_NIF_TERM ATOM_FIRST;
ERL_NIF_TERM ATOM_LAST;
ERL_NIF_TERM ATOM_NEXT;
ERL_NIF_TERM ATOM_PREV;
ERL_NIF_TERM ATOM_SEEK_FOR_PREV;
ERL_NIF_TERM ATOM_SEEK;

// Related to Iterator Value to be retrieved
ERL_NIF_TERM ATOM_KEYS_ONLY;

// Related to Access Hint
ERL_NIF_TERM ATOM_ACCESS_HINT_NORMAL;
ERL_NIF_TERM ATOM_ACCESS_HINT_SEQUENTIAL;
ERL_NIF_TERM ATOM_ACCESS_HINT_WILLNEED;
ERL_NIF_TERM ATOM_ACCESS_HINT_NONE;

// Related to Compression Type
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_SNAPPY;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_ZLIB;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_BZIP2;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_LZ4;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_LZ4H;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_NONE;

// Related to Compaction Style
ERL_NIF_TERM ATOM_COMPACTION_STYLE_LEVEL;
ERL_NIF_TERM ATOM_COMPACTION_STYLE_UNIVERSAL;
ERL_NIF_TERM ATOM_COMPACTION_STYLE_FIFO;
ERL_NIF_TERM ATOM_COMPACTION_STYLE_NONE;

// Related to WAL Recovery Mode
ERL_NIF_TERM ATOM_WAL_TOLERATE_CORRUPTED_TAIL_RECORDS;
ERL_NIF_TERM ATOM_WAL_ABSOLUTE_CONSISTENCY;
ERL_NIF_TERM ATOM_WAL_POINT_IN_TIME_RECOVERY;
ERL_NIF_TERM ATOM_WAL_SKIP_ANY_CORRUPTED_RECORDS;

// Related to Error Codes
ERL_NIF_TERM ATOM_ERROR_DB_OPEN;
ERL_NIF_TERM ATOM_ERROR_DB_PUT;
ERL_NIF_TERM ATOM_ERROR_DB_DELETE;
ERL_NIF_TERM ATOM_ERROR_DB_WRITE;
ERL_NIF_TERM ATOM_ERROR_DB_DESTROY;
ERL_NIF_TERM ATOM_ERROR_DB_REPAIR;
ERL_NIF_TERM ATOM_BAD_WRITE_ACTION;
ERL_NIF_TERM ATOM_KEEP_RESOURCE_FAILED;
ERL_NIF_TERM ATOM_ITERATOR_CLOSED;
ERL_NIF_TERM ATOM_INVALID_ITERATOR;
ERL_NIF_TERM ATOM_ERROR_BACKUP_ENGINE_OPEN;

// Related to NIF initialize parameters
ERL_NIF_TERM ATOM_WRITE_THREADS;

ERL_NIF_TERM ATOM_ENV;
ERL_NIF_TERM ATOM_PRIORITY_HIGH;
ERL_NIF_TERM ATOM_PRIORITY_LOW;


// backup info
ERL_NIF_TERM ATOM_BACKUP_INFO_ID;
ERL_NIF_TERM ATOM_BACKUP_INFO_TIMESTAMP;
ERL_NIF_TERM ATOM_BACKUP_INFO_SIZE;
ERL_NIF_TERM ATOM_BACKUP_INFO_NUMBER_FILES;

}   // namespace erocksdb


using std::nothrow;

static void on_unload(ErlNifEnv *env, void *priv_data)
{
}

static int on_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
    /* Convert the private data to the new version. */
    *priv_data = *old_priv_data;
    return 0;
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
try
{
  rocksdb::Env::Default();

  // inform erlang of our two resource types
  erocksdb::ManagedEnv::CreateEnvType(env);
  erocksdb::DbObject::CreateDbObjectType(env);
  erocksdb::ColumnFamilyObject::CreateColumnFamilyObjectType(env);
  erocksdb::ItrObject::CreateItrObjectType(env);
  erocksdb::SnapshotObject::CreateSnapshotObjectType(env);
  erocksdb::CreateBatchType(env);
  erocksdb::TLogItrObject::CreateTLogItrObjectType(env);
  erocksdb::BackupEngineObject::CreateBackupEngineObjectType(env);
  erocksdb::Cache::CreateCacheType(env);
  erocksdb::RateLimiter::CreateRateLimiterType(env);


  // must initialize atoms before processing options
#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }
  // Related to Erlang
  ATOM(erocksdb::ATOM_TRUE, "true");
  ATOM(erocksdb::ATOM_FALSE, "false");
  ATOM(erocksdb::ATOM_OK, "ok");
  ATOM(erocksdb::ATOM_ERROR, "error");
  ATOM(erocksdb::ATOM_EINVAL, "einval");
  ATOM(erocksdb::ATOM_BADARG, "badarg");
  ATOM(erocksdb::ATOM_NOT_FOUND, "not_found");
  ATOM(erocksdb::ATOM_INC, "inc");
  ATOM(erocksdb::ATOM_DEC, "dec");


  ATOM(erocksdb::ATOM_DEFAULT, "default");
  ATOM(erocksdb::ATOM_MEMENV, "memenv");

  // Related to CFOptions
  ATOM(erocksdb::ATOM_BLOCK_CACHE_SIZE_MB_FOR_POINT_LOOKUP, "block_cache_size_mb_for_point_lookup");
  ATOM(erocksdb::ATOM_MEMTABLE_MEMORY_BUDGET, "memtable_memory_budget");
  ATOM(erocksdb::ATOM_WRITE_BUFFER_SIZE, "write_buffer_size");
  ATOM(erocksdb::ATOM_MAX_WRITE_BUFFER_NUMBER, "max_write_buffer_number");
  ATOM(erocksdb::ATOM_MIN_WRITE_BUFFER_NUMBER_TO_MERGE, "min_write_buffer_number_to_merge");
  ATOM(erocksdb::ATOM_COMPRESSION, "compression");
  ATOM(erocksdb::ATOM_NUM_LEVELS, "num_levels");
  ATOM(erocksdb::ATOM_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, "level0_file_num_compaction_trigger");
  ATOM(erocksdb::ATOM_LEVEL0_SLOWDOWN_WRITES_TRIGGER, "level0_slowdown_writes_trigger");
  ATOM(erocksdb::ATOM_LEVEL0_STOP_WRITES_TRIGGER, "level0_stop_writes_trigger");
  ATOM(erocksdb::ATOM_MAX_MEM_COMPACTION_LEVEL, "max_mem_compaction_level");
  ATOM(erocksdb::ATOM_TARGET_FILE_SIZE_BASE, "target_file_size_base");
  ATOM(erocksdb::ATOM_TARGET_FILE_SIZE_MULTIPLIER, "target_file_size_multiplier");
  ATOM(erocksdb::ATOM_MAX_BYTES_FOR_LEVEL_BASE, "max_bytes_for_level_base");
  ATOM(erocksdb::ATOM_MAX_BYTES_FOR_LEVEL_MULTIPLIER, "max_bytes_for_level_multiplier");
  ATOM(erocksdb::ATOM_MAX_COMPACTION_BYTES, "max_compaction_bytes");
  ATOM(erocksdb::ATOM_SOFT_RATE_LIMIT, "soft_rate_limit");
  ATOM(erocksdb::ATOM_HARD_RATE_LIMIT, "hard_rate_limit");
  ATOM(erocksdb::ATOM_ARENA_BLOCK_SIZE, "arena_block_size");
  ATOM(erocksdb::ATOM_DISABLE_AUTO_COMPACTIONS, "disable_auto_compactions");
  ATOM(erocksdb::ATOM_PURGE_REDUNDANT_KVS_WHILE_FLUSH, "purge_redundant_kvs_while_flush");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE, "compaction_style");
  ATOM(erocksdb::ATOM_FILTER_DELETES, "filter_deletes");
  ATOM(erocksdb::ATOM_MAX_SEQUENTIAL_SKIP_IN_ITERATIONS, "max_sequential_skip_in_iterations");
  ATOM(erocksdb::ATOM_INPLACE_UPDATE_SUPPORT, "inplace_update_support");
  ATOM(erocksdb::ATOM_INPLACE_UPDATE_NUM_LOCKS, "inplace_update_num_locks");
  ATOM(erocksdb::ATOM_TABLE_FACTORY_BLOCK_CACHE_SIZE, "table_factory_block_cache_size");
  ATOM(erocksdb::ATOM_IN_MEMORY_MODE, "in_memory_mode");
  ATOM(erocksdb::ATOM_IN_MEMORY, "in_memory");
  ATOM(erocksdb::ATOM_BLOCK_BASED_TABLE_OPTIONS, "block_based_table_options");
  ATOM(erocksdb::ATOM_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES, "level_compaction_dynamic_level_bytes");

  // Related to DBOptions
  ATOM(erocksdb::ATOM_TOTAL_THREADS, "total_threads");
  ATOM(erocksdb::ATOM_CREATE_IF_MISSING, "create_if_missing");
  ATOM(erocksdb::ATOM_CREATE_MISSING_COLUMN_FAMILIES, "create_missing_column_families");
  ATOM(erocksdb::ATOM_ERROR_IF_EXISTS, "error_if_exists");
  ATOM(erocksdb::ATOM_PARANOID_CHECKS, "paranoid_checks");
  ATOM(erocksdb::ATOM_MAX_OPEN_FILES, "max_open_files");
  ATOM(erocksdb::ATOM_MAX_TOTAL_WAL_SIZE, "max_total_wal_size");
  ATOM(erocksdb::ATOM_USE_FSYNC, "use_fsync");
  ATOM(erocksdb::ATOM_DB_PATHS, "db_paths");
  ATOM(erocksdb::ATOM_DB_LOG_DIR, "db_log_dir");
  ATOM(erocksdb::ATOM_WAL_DIR, "wal_dir");
  ATOM(erocksdb::ATOM_DELETE_OBSOLETE_FILES_PERIOD_MICROS, "delete_obsolete_files_period_micros");
  ATOM(erocksdb::ATOM_MAX_BACKGROUND_JOBS, "max_background_jobs");
  ATOM(erocksdb::ATOM_MAX_BACKGROUND_COMPACTIONS, "max_background_compactions");
  ATOM(erocksdb::ATOM_MAX_BACKGROUND_FLUSHES, "max_background_flushes");
  ATOM(erocksdb::ATOM_MAX_LOG_FILE_SIZE, "max_log_file_size");
  ATOM(erocksdb::ATOM_LOG_FILE_TIME_TO_ROLL, "log_file_time_to_roll");
  ATOM(erocksdb::ATOM_KEEP_LOG_FILE_NUM, "keep_log_file_num");
  ATOM(erocksdb::ATOM_MAX_MANIFEST_FILE_SIZE, "max_manifest_file_size");
  ATOM(erocksdb::ATOM_TABLE_CACHE_NUMSHARDBITS, "table_cache_numshardbits");
  ATOM(erocksdb::ATOM_WAL_TTL_SECONDS, "wal_ttl_seconds");
  ATOM(erocksdb::ATOM_WAL_SIZE_LIMIT_MB, "wal_size_limit_mb");
  ATOM(erocksdb::ATOM_MANIFEST_PREALLOCATION_SIZE, "manifest_preallocation_size");
  ATOM(erocksdb::ATOM_ALLOW_MMAP_READS, "allow_mmap_reads");
  ATOM(erocksdb::ATOM_ALLOW_MMAP_WRITES, "allow_mmap_writes");
  ATOM(erocksdb::ATOM_IS_FD_CLOSE_ON_EXEC, "is_fd_close_on_exec");
  ATOM(erocksdb::ATOM_SKIP_LOG_ERROR_ON_RECOVERY, "skip_log_error_on_recovery");
  ATOM(erocksdb::ATOM_STATS_DUMP_PERIOD_SEC, "stats_dump_period_sec");
  ATOM(erocksdb::ATOM_ADVISE_RANDOM_ON_OPEN, "advise_random_on_open");
  ATOM(erocksdb::ATOM_ACCESS_HINT, "access_hint");
  ATOM(erocksdb::ATOM_COMPACTION_READAHEAD_SIZE, "compaction_readahead_size");
  ATOM(erocksdb::ATOM_USE_ADAPTIVE_MUTEX, "use_adaptive_mutex");
  ATOM(erocksdb::ATOM_BYTES_PER_SYNC, "bytes_per_sync");
  ATOM(erocksdb::ATOM_SKIP_STATS_UPDATE_ON_DB_OPEN, "skip_stats_update_on_db_open");
  ATOM(erocksdb::ATOM_WAL_RECOVERY_MODE, "wal_recovery_mode");
  ATOM(erocksdb::ATOM_ALLOW_CONCURRENT_MEMTABLE_WRITE, "allow_concurrent_memtable_write");
  ATOM(erocksdb::ATOM_ENABLE_WRITE_THREAD_ADAPTATIVE_YIELD, "enable_write_thread_adaptive_yield");
  ATOM(erocksdb::ATOM_DB_WRITE_BUFFER_SIZE, "db_write_buffer_size");
  ATOM(erocksdb::ATOM_RATE_LIMITER, "rate_limiter");

  // Related to BlockBasedTable Options
  ATOM(erocksdb::ATOM_NO_BLOCK_CACHE, "no_block_cache");
  ATOM(erocksdb::ATOM_BLOCK_CACHE, "block_cache");

  ATOM(erocksdb::ATOM_BLOCK_SIZE, "block_size");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_SIZE, "block_cache_size");
  ATOM(erocksdb::ATOM_BLOOM_FILTER_POLICY, "bloom_filter_policy");
  ATOM(erocksdb::ATOM_FORMAT_VERSION, "format_version");
  ATOM(erocksdb::ATOM_CACHE_INDEX_AND_FILTER_BLOCKS, "cache_index_and_filter_blocks");

  // Related to Read Options
  ATOM(erocksdb::ATOM_VERIFY_CHECKSUMS, "verify_checksums");
  ATOM(erocksdb::ATOM_FILL_CACHE,"fill_cache");
  ATOM(erocksdb::ATOM_ITERATE_UPPER_BOUND,"iterate_upper_bound");
  ATOM(erocksdb::ATOM_TAILING,"tailing");
  ATOM(erocksdb::ATOM_TOTAL_ORDER_SEEK,"total_order_seek");
  ATOM(erocksdb::ATOM_SNAPSHOT, "snapshot");
  ATOM(erocksdb::ATOM_BAD_SNAPSHOT, "bad_snapshot");

  // Related to Write Options
  ATOM(erocksdb::ATOM_SYNC, "sync");
  ATOM(erocksdb::ATOM_DISABLE_WAL, "disable_wal");
  ATOM(erocksdb::ATOM_IGNORE_MISSING_COLUMN_FAMILIES, "ignore_missing_column_families");

  // Related to Write Options
  ATOM(erocksdb::ATOM_CLEAR, "clear");
  ATOM(erocksdb::ATOM_PUT, "put");
  ATOM(erocksdb::ATOM_DELETE, "delete");
  ATOM(erocksdb::ATOM_SINGLE_DELETE, "single_delete");

  // Related to Iterator Options
  ATOM(erocksdb::ATOM_FIRST, "first");
  ATOM(erocksdb::ATOM_LAST, "last");
  ATOM(erocksdb::ATOM_NEXT, "next");
  ATOM(erocksdb::ATOM_PREV, "prev");
  ATOM(erocksdb::ATOM_SEEK_FOR_PREV, "seek_for_prev");
  ATOM(erocksdb::ATOM_SEEK, "seek");

  // Related to Iterator Value to be retrieved
  ATOM(erocksdb::ATOM_KEYS_ONLY, "keys_only");

  // Related to Access Hint
  ATOM(erocksdb::ATOM_ACCESS_HINT_NORMAL,"normal");
  ATOM(erocksdb::ATOM_ACCESS_HINT_SEQUENTIAL,"sequential");
  ATOM(erocksdb::ATOM_ACCESS_HINT_WILLNEED,"willneed");
  ATOM(erocksdb::ATOM_ACCESS_HINT_NONE,"none");

  // Related to Compression Type
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_SNAPPY, "snappy");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_ZLIB, "zlib");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_BZIP2, "bzip2");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_LZ4, "lz4");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_LZ4H, "lz4h");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_NONE, "none");

  // Related to Compaction Style
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_LEVEL, "level");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_UNIVERSAL, "universal");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_FIFO, "fifo");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_NONE, "none");

  // Related to WAL Recovery Mode
  ATOM(erocksdb::ATOM_WAL_TOLERATE_CORRUPTED_TAIL_RECORDS, "tolerate_corrupted_tail_records");
  ATOM(erocksdb::ATOM_WAL_ABSOLUTE_CONSISTENCY, "absolute_consistency");
  ATOM(erocksdb::ATOM_WAL_POINT_IN_TIME_RECOVERY, "point_in_time_recovery");
  ATOM(erocksdb::ATOM_WAL_SKIP_ANY_CORRUPTED_RECORDS, "skip_any_corrupted_records");

  // Related to Error Codes
  ATOM(erocksdb::ATOM_ERROR_DB_OPEN,"db_open");
  ATOM(erocksdb::ATOM_ERROR_DB_PUT, "db_put");
  ATOM(erocksdb::ATOM_ERROR_DB_DELETE, "db_delete");
  ATOM(erocksdb::ATOM_ERROR_DB_WRITE, "db_write");
  ATOM(erocksdb::ATOM_ERROR_DB_DESTROY, "error_db_destroy");
  ATOM(erocksdb::ATOM_ERROR_DB_REPAIR, "error_db_repair");
  ATOM(erocksdb::ATOM_BAD_WRITE_ACTION, "bad_write_action");
  ATOM(erocksdb::ATOM_KEEP_RESOURCE_FAILED, "keep_resource_failed");
  ATOM(erocksdb::ATOM_ITERATOR_CLOSED, "iterator_closed");
  ATOM(erocksdb::ATOM_INVALID_ITERATOR, "invalid_iterator");
  ATOM(erocksdb::ATOM_ERROR_BACKUP_ENGINE_OPEN, "backup_engine_open");

  // Related to NIF initialize parameters
  ATOM(erocksdb::ATOM_WRITE_THREADS, "write_threads");

  ATOM(erocksdb::ATOM_PRIORITY_HIGH, "priority_high");
  ATOM(erocksdb::ATOM_PRIORITY_LOW, "priority_low");
  ATOM(erocksdb::ATOM_ENV, "env");

  // backup info
  ATOM(erocksdb::ATOM_BACKUP_INFO_ID, "backup_id");
  ATOM(erocksdb::ATOM_BACKUP_INFO_TIMESTAMP, "timestamp");
  ATOM(erocksdb::ATOM_BACKUP_INFO_SIZE, "size");
  ATOM(erocksdb::ATOM_BACKUP_INFO_NUMBER_FILES, "number_files");

#undef ATOM

return 0;
}
catch(std::exception& e)
{
    /* Refuse to load the NIF module (I see no way right now to return a more specific exception
    or log extra information): */
    return -1;
}
catch(...)
{
    return -1;
}

extern "C" {
    ERL_NIF_INIT(rocksdb, nif_funcs, &on_load, NULL, &on_upgrade, &on_unload);
}
