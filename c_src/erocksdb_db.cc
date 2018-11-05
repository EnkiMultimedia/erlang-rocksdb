// -------------------------------------------------------------------
// Copyright (c) 2016-2017 Benoit Chesneau. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-çàooàpç2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/checkpoint.h"

#include "atoms.h"
#include "refobjects.h"
#include "util.h"
#include "erocksdb_db.h"
#include "cache.h"
#include "rate_limiter.h"
#include "env.h"
#include "erlang_merge.h"
#include "bitset_merge_operator.h"
#include "counter_merge_operator.h"

ERL_NIF_TERM parse_bbt_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::BlockBasedTableOptions& opts) {
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == erocksdb::ATOM_NO_BLOCK_CACHE) {
            opts.no_block_cache = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_BLOCK_SIZE) {
            int block_size;
            if (enif_get_int(env, option[1], &block_size))
                opts.block_size = block_size;
        }
        else if (option[0] == erocksdb::ATOM_BLOCK_CACHE) {
            erocksdb::Cache* cache_ptr = erocksdb::Cache::RetrieveCacheResource(env,option[1]);
            if(NULL!=cache_ptr) {
                auto cache = cache_ptr->cache();
                opts.block_cache = cache;
            }
        }
        else if (option[0] == erocksdb::ATOM_BLOOM_FILTER_POLICY) {
            int bits_per_key;
            if (enif_get_int(env, option[1], &bits_per_key))
                opts.filter_policy = std::shared_ptr<const rocksdb::FilterPolicy>(rocksdb::NewBloomFilterPolicy(bits_per_key));
        }
        else if (option[0] == erocksdb::ATOM_FORMAT_VERSION) {
            int format_version;
            if (enif_get_int(env, option[1], &format_version))
                opts.format_version = format_version;
        }
        else if (option[0] == erocksdb::ATOM_CACHE_INDEX_AND_FILTER_BLOCKS) {
            opts.cache_index_and_filter_blocks = (option[1] == erocksdb::ATOM_TRUE);
        }
    }

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM parse_db_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::DBOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {

        if (option[0] == erocksdb::ATOM_ENV)
        {
            if (enif_is_atom(env, option[1]))
            {
                if(option[1] == erocksdb::ATOM_MEMENV)
                {
                    rocksdb::Env* memenv = rocksdb::NewMemEnv(rocksdb::Env::Default());
                    opts.env = memenv;
                    opts.create_if_missing = true;
                }
            } else {
                erocksdb::ManagedEnv* env_ptr = erocksdb::ManagedEnv::RetrieveEnvResource(env,option[1]);
                if(NULL!=env_ptr)
                    opts.env = (rocksdb::Env*)env_ptr->env();
            }
        }
        else if (option[0] == erocksdb::ATOM_TOTAL_THREADS)
        {
            int total_threads;
            if (enif_get_int(env, option[1], &total_threads))
                opts.IncreaseParallelism(total_threads);
        }
        else if (option[0] == erocksdb::ATOM_CREATE_IF_MISSING)
            opts.create_if_missing = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_CREATE_MISSING_COLUMN_FAMILIES)
            opts.create_missing_column_families = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_ERROR_IF_EXISTS)
            opts.error_if_exists = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_PARANOID_CHECKS)
            opts.paranoid_checks = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_MAX_OPEN_FILES)
        {
            int max_open_files;
            if (enif_get_int(env, option[1], &max_open_files))
                opts.max_open_files = max_open_files;
        }
        else if (option[0] == erocksdb::ATOM_MAX_TOTAL_WAL_SIZE)
        {
            ErlNifUInt64 max_total_wal_size;
            if (enif_get_uint64(env, option[1], &max_total_wal_size))
                opts.max_total_wal_size = max_total_wal_size;
        }
        else if (option[0] == erocksdb::ATOM_USE_FSYNC)
        {
            opts.use_fsync = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_DB_PATHS)
        {
            ERL_NIF_TERM head;
            ERL_NIF_TERM tail;
            char db_name[4096];
            while(enif_get_list_cell(env, option[1], &head, &tail)) {
                if (enif_get_string(env, head, db_name, sizeof(db_name), ERL_NIF_LATIN1))
                {
                    std::string str_db_name(db_name);
                    rocksdb::DbPath db_path(str_db_name, 0);
                    opts.db_paths.push_back(db_path);
                }
            }
        }
        else if (option[0] == erocksdb::ATOM_DB_LOG_DIR)
        {
            char db_log_dir[4096];
            if (enif_get_string(env, option[1], db_log_dir, sizeof(db_log_dir), ERL_NIF_LATIN1))
                opts.db_log_dir = std::string(db_log_dir);
        }
        else if (option[0] == erocksdb::ATOM_WAL_DIR)
        {
            char wal_dir[4096];
            if (enif_get_string(env, option[1], wal_dir, sizeof(wal_dir), ERL_NIF_LATIN1))
                opts.wal_dir = std::string(wal_dir);
        }
        else if (option[0] == erocksdb::ATOM_DELETE_OBSOLETE_FILES_PERIOD_MICROS)
        {
            ErlNifUInt64 delete_obsolete_files_period_micros;
            if (enif_get_uint64(env, option[1], &delete_obsolete_files_period_micros))
                opts.delete_obsolete_files_period_micros = delete_obsolete_files_period_micros;
        }
        else if (option[0] == erocksdb::ATOM_MAX_BACKGROUND_JOBS)
        {
            int max_background_jobs;
            if (enif_get_int(env, option[1], &max_background_jobs))
                opts.max_background_jobs = max_background_jobs;
        }
        else if (option[0] == erocksdb::ATOM_MAX_BACKGROUND_COMPACTIONS)
        {
            int max_background_compactions;
            if (enif_get_int(env, option[1], &max_background_compactions))
                opts.max_background_compactions = max_background_compactions;
        }
        else if (option[0] == erocksdb::ATOM_MAX_BACKGROUND_FLUSHES)
        {
            int max_background_flushes;
            if (enif_get_int(env, option[1], &max_background_flushes))
                opts.max_background_flushes = max_background_flushes;
        }
        else if (option[0] == erocksdb::ATOM_MAX_LOG_FILE_SIZE)
        {
            unsigned int max_log_file_size;
            if (enif_get_uint(env, option[1], &max_log_file_size))
                opts.max_log_file_size = max_log_file_size;
        }
        else if (option[0] == erocksdb::ATOM_LOG_FILE_TIME_TO_ROLL)
        {
            unsigned int log_file_time_to_roll;
            if (enif_get_uint(env, option[1], &log_file_time_to_roll))
                opts.log_file_time_to_roll = log_file_time_to_roll;
        }
        else if (option[0] == erocksdb::ATOM_KEEP_LOG_FILE_NUM)
        {
            unsigned int keep_log_file_num;
            if (enif_get_uint(env, option[1], &keep_log_file_num))
                opts.keep_log_file_num= keep_log_file_num;
        }
        else if (option[0] == erocksdb::ATOM_MAX_MANIFEST_FILE_SIZE)
        {
            ErlNifUInt64 max_manifest_file_size;
            if (enif_get_uint64(env, option[1], &max_manifest_file_size))
                opts.max_manifest_file_size = max_manifest_file_size;
        }
        else if (option[0] == erocksdb::ATOM_TABLE_CACHE_NUMSHARDBITS)
        {
            int table_cache_numshardbits;
            if (enif_get_int(env, option[1], &table_cache_numshardbits))
                opts.table_cache_numshardbits = table_cache_numshardbits;
        }
        else if (option[0] == erocksdb::ATOM_WAL_TTL_SECONDS)
        {
            ErlNifUInt64 WAL_ttl_seconds;
            if (enif_get_uint64(env, option[1], &WAL_ttl_seconds))
                opts.WAL_ttl_seconds = WAL_ttl_seconds;
        }
        else if (option[0] == erocksdb::ATOM_WAL_SIZE_LIMIT_MB)
        {
            ErlNifUInt64 WAL_size_limit_MB;
            if (enif_get_uint64(env, option[1], &WAL_size_limit_MB))
                opts.WAL_size_limit_MB = WAL_size_limit_MB;
        }
        else if (option[0] == erocksdb::ATOM_MANIFEST_PREALLOCATION_SIZE)
        {
            unsigned int manifest_preallocation_size;
            if (enif_get_uint(env, option[1], &manifest_preallocation_size))
                opts.manifest_preallocation_size = manifest_preallocation_size;
        }
        else if (option[0] == erocksdb::ATOM_ALLOW_MMAP_READS)
        {
            opts.allow_mmap_reads = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_ALLOW_MMAP_WRITES)
        {
            opts.allow_mmap_writes = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_IS_FD_CLOSE_ON_EXEC)
        {
            opts.is_fd_close_on_exec = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_SKIP_LOG_ERROR_ON_RECOVERY)
        {
            opts.skip_log_error_on_recovery = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_STATS_DUMP_PERIOD_SEC)
        {
            unsigned int stats_dump_period_sec;
            if (enif_get_uint(env, option[1], &stats_dump_period_sec))
                opts.stats_dump_period_sec = stats_dump_period_sec;
        }
        else if (option[0] == erocksdb::ATOM_ADVISE_RANDOM_ON_OPEN)
        {
            opts.advise_random_on_open = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_ACCESS_HINT)
        {
            if (option[1] == erocksdb::ATOM_ACCESS_HINT_NORMAL) {
                opts.access_hint_on_compaction_start = rocksdb::DBOptions::AccessHint::NORMAL;
            }
            else if (option[1] == erocksdb::ATOM_ACCESS_HINT_SEQUENTIAL) {
                opts.access_hint_on_compaction_start = rocksdb::DBOptions::AccessHint::SEQUENTIAL;
            }
            else if (option[1] == erocksdb::ATOM_ACCESS_HINT_WILLNEED) {
                opts.access_hint_on_compaction_start = rocksdb::DBOptions::AccessHint::WILLNEED;
            }
            else if (option[1] == erocksdb::ATOM_ACCESS_HINT_NONE) {
                opts.access_hint_on_compaction_start = rocksdb::DBOptions::AccessHint::NONE;
            }
        }
        else if (option[0] == erocksdb::ATOM_COMPACTION_READAHEAD_SIZE)
        {
            unsigned int compaction_readahead_size;
            if (enif_get_uint(env, option[1], &compaction_readahead_size))
                opts.compaction_readahead_size = compaction_readahead_size;
        }
        else if (option[0] == erocksdb::ATOM_NEW_TABLE_READER_FOR_COMPACTION_INPUTS)
        {
            opts.new_table_reader_for_compaction_inputs = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_USE_ADAPTIVE_MUTEX)
        {
            opts.use_adaptive_mutex = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_BYTES_PER_SYNC)
        {
            ErlNifUInt64 bytes_per_sync;
            if (enif_get_uint64(env, option[1], &bytes_per_sync))
                opts.bytes_per_sync = bytes_per_sync;
        }
        else if (option[0] == erocksdb::ATOM_SKIP_STATS_UPDATE_ON_DB_OPEN)
        {
            opts.skip_stats_update_on_db_open = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_WAL_RECOVERY_MODE)
        {
            if (option[1] == erocksdb::ATOM_WAL_TOLERATE_CORRUPTED_TAIL_RECORDS) {
                opts.wal_recovery_mode = rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
            }
            else if (option[1] == erocksdb::ATOM_WAL_ABSOLUTE_CONSISTENCY) {
                opts.wal_recovery_mode = rocksdb::WALRecoveryMode::kAbsoluteConsistency;
            }
            else if (option[1] == erocksdb::ATOM_WAL_POINT_IN_TIME_RECOVERY) {
                opts.wal_recovery_mode = rocksdb::WALRecoveryMode::kPointInTimeRecovery;
            }
            else if (option[1] == erocksdb::ATOM_WAL_SKIP_ANY_CORRUPTED_RECORDS) {
                opts.wal_recovery_mode = rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
            }
        }
        else if (option[0] == erocksdb::ATOM_ALLOW_CONCURRENT_MEMTABLE_WRITE)
        {
            opts.allow_concurrent_memtable_write = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_ENABLE_WRITE_THREAD_ADAPTATIVE_YIELD)
        {
            opts.enable_write_thread_adaptive_yield = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_DB_WRITE_BUFFER_SIZE)
        {
            unsigned int db_write_buffer_size;
            if (enif_get_uint(env, option[1], &db_write_buffer_size))
                opts.db_write_buffer_size = db_write_buffer_size;
        }
        else if (option[0] == erocksdb::ATOM_IN_MEMORY)
        {
            if (option[1] == erocksdb::ATOM_TRUE)
            {
                opts.env = rocksdb::NewMemEnv(rocksdb::Env::Default());
                opts.create_if_missing = true;
            }
        }
        else if (option[0] == erocksdb::ATOM_RATE_LIMITER) {
            erocksdb::RateLimiter* rate_limiter_ptr = erocksdb::RateLimiter::RetrieveRateLimiterResource(env,option[1]);
            if(NULL!=rate_limiter_ptr) {
                auto rate_limiter = rate_limiter_ptr->rate_limiter();
                opts.rate_limiter = rate_limiter;
            }
        }
        else if (option[0] == erocksdb::ATOM_MAX_SUBCOMPACTIONS)
        {
            unsigned int max_subcompactions;
            if (enif_get_uint(env, option[1], &max_subcompactions))
                opts.max_subcompactions = max_subcompactions;
        }
    }
    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM parse_cf_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::ColumnFamilyOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && arity == 2)
    {
        if (option[0] == erocksdb::ATOM_BLOCK_CACHE_SIZE_MB_FOR_POINT_LOOKUP)
            // @TODO ignored now
            ;
        else if (option[0] == erocksdb::ATOM_MEMTABLE_MEMORY_BUDGET)
        {
            ErlNifUInt64 memtable_memory_budget;
            if (enif_get_uint64(env, option[1], &memtable_memory_budget))
                opts.OptimizeLevelStyleCompaction(memtable_memory_budget);
        }
        else if (option[0] == erocksdb::ATOM_WRITE_BUFFER_SIZE)
        {
            unsigned int write_buffer_size;
            if (enif_get_uint(env, option[1], &write_buffer_size))
                opts.write_buffer_size = write_buffer_size;
        }
        else if (option[0] == erocksdb::ATOM_MAX_WRITE_BUFFER_NUMBER)
        {
            int max_write_buffer_number;
            if (enif_get_int(env, option[1], &max_write_buffer_number))
                opts.max_write_buffer_number = max_write_buffer_number;
        }
        else if (option[0] == erocksdb::ATOM_MIN_WRITE_BUFFER_NUMBER_TO_MERGE)
        {
            int min_write_buffer_number_to_merge;
            if (enif_get_int(env, option[1], &min_write_buffer_number_to_merge))
                opts.min_write_buffer_number_to_merge = min_write_buffer_number_to_merge;
        }
        else if (option[0] == erocksdb::ATOM_COMPRESSION)
        {
            if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_SNAPPY) {
                opts.compression = rocksdb::CompressionType::kSnappyCompression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_ZLIB) {
                opts.compression = rocksdb::CompressionType::kZlibCompression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_BZIP2) {
                opts.compression = rocksdb::CompressionType::kBZip2Compression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_LZ4) {
                opts.compression = rocksdb::CompressionType::kLZ4Compression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_LZ4H) {
                opts.compression = rocksdb::CompressionType::kLZ4HCCompression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_NONE) {
                opts.compression = rocksdb::CompressionType::kNoCompression;
            }
        }
        else if (option[0] == erocksdb::ATOM_NUM_LEVELS)
        {
            int num_levels;
            if (enif_get_int(env, option[1], &num_levels))
                opts.num_levels = num_levels;
        }
        else if (option[0] == erocksdb::ATOM_LEVEL0_FILE_NUM_COMPACTION_TRIGGER)
        {
            int level0_file_num_compaction_trigger;
            if (enif_get_int(env, option[1], &level0_file_num_compaction_trigger))
                opts.level0_file_num_compaction_trigger = level0_file_num_compaction_trigger;
        }
        else if (option[0] == erocksdb::ATOM_LEVEL0_SLOWDOWN_WRITES_TRIGGER)
        {
            int level0_slowdown_writes_trigger;
            if (enif_get_int(env, option[1], &level0_slowdown_writes_trigger))
                opts.level0_slowdown_writes_trigger = level0_slowdown_writes_trigger;
        }
        else if (option[0] == erocksdb::ATOM_LEVEL0_STOP_WRITES_TRIGGER)
        {
            int level0_stop_writes_trigger;
            if (enif_get_int(env, option[1], &level0_stop_writes_trigger))
                opts.level0_stop_writes_trigger = level0_stop_writes_trigger;
        }
        else if (option[0] == erocksdb::ATOM_MAX_MEM_COMPACTION_LEVEL)
        {
            int max_mem_compaction_level;
            if (enif_get_int(env, option[1], &max_mem_compaction_level))
                opts.max_mem_compaction_level = max_mem_compaction_level;
        }
        else if (option[0] == erocksdb::ATOM_TARGET_FILE_SIZE_BASE)
        {
            ErlNifUInt64 target_file_size_base;
            if (enif_get_uint64(env, option[1], &target_file_size_base))
                opts.target_file_size_base = target_file_size_base;
        }
        else if (option[0] == erocksdb::ATOM_TARGET_FILE_SIZE_MULTIPLIER)
        {
            int target_file_size_multiplier;
            if (enif_get_int(env, option[1], &target_file_size_multiplier))
                opts.target_file_size_multiplier = target_file_size_multiplier;
        }
        else if (option[0] == erocksdb::ATOM_MAX_BYTES_FOR_LEVEL_BASE)
        {
            ErlNifUInt64 max_bytes_for_level_base;
            if (enif_get_uint64(env, option[1], &max_bytes_for_level_base))
                opts.max_bytes_for_level_base = max_bytes_for_level_base;
        }
        else if (option[0] == erocksdb::ATOM_MAX_BYTES_FOR_LEVEL_MULTIPLIER)
        {
            int max_bytes_for_level_multiplier;
            if (enif_get_int(env, option[1], &max_bytes_for_level_multiplier))
                opts.max_bytes_for_level_multiplier = max_bytes_for_level_multiplier;
        }
        else if (option[0] == erocksdb::ATOM_MAX_COMPACTION_BYTES)
        {
            int max_compaction_bytes;
            if (enif_get_int(env, option[1], &max_compaction_bytes))
                opts.max_compaction_bytes = max_compaction_bytes;
        }
        else if (option[0] == erocksdb::ATOM_SOFT_RATE_LIMIT)
        {
            double soft_rate_limit;
            if (enif_get_double(env, option[1], &soft_rate_limit))
                opts.soft_rate_limit = soft_rate_limit;
        }
        else if (option[0] == erocksdb::ATOM_HARD_RATE_LIMIT)
        {
            double hard_rate_limit;
            if (enif_get_double(env, option[1], &hard_rate_limit))
                opts.hard_rate_limit = hard_rate_limit;
        }
        else if (option[0] == erocksdb::ATOM_ARENA_BLOCK_SIZE)
        {
            unsigned int arena_block_size;
            if (enif_get_uint(env, option[1], &arena_block_size))
                opts.arena_block_size = arena_block_size;
        }
        else if (option[0] == erocksdb::ATOM_DISABLE_AUTO_COMPACTIONS)
        {
            opts.disable_auto_compactions = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_PURGE_REDUNDANT_KVS_WHILE_FLUSH)
        {
            opts.purge_redundant_kvs_while_flush = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_COMPACTION_STYLE)
        {
            if (option[1] == erocksdb::ATOM_COMPACTION_STYLE_LEVEL) {
                opts.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
            }
            else if (option[1] == erocksdb::ATOM_COMPACTION_STYLE_UNIVERSAL) {
                opts.compaction_style = rocksdb::CompactionStyle::kCompactionStyleUniversal;
            }
            else if (option[1] == erocksdb::ATOM_COMPACTION_STYLE_FIFO) {
                opts.compaction_style = rocksdb::CompactionStyle::kCompactionStyleFIFO;
            }
            else if (option[1] == erocksdb::ATOM_COMPACTION_STYLE_NONE) {
                opts.compaction_style = rocksdb::CompactionStyle::kCompactionStyleNone;
            }
        }
        else if (option[0] == erocksdb::ATOM_COMPACTION_PRI)
        {
            if (option[1] == erocksdb::ATOM_COMPACTION_PRI_COMPENSATED_SIZE) {
                opts.compaction_pri = rocksdb::CompactionPri::kByCompensatedSize;
            }
            else if (option[1] == erocksdb::ATOM_COMPACTION_PRI_OLDEST_LARGEST_SEQ_FIRST) {
                opts.compaction_pri = rocksdb::CompactionPri::kOldestLargestSeqFirst;
            }
            else if (option[1] == erocksdb::ATOM_COMPACTION_PRI_OLDEST_SMALLEST_SEQ_FIRST) {
                opts.compaction_pri = rocksdb::CompactionPri::kOldestSmallestSeqFirst;
            }
        }
        else if (option[0] == erocksdb::ATOM_MAX_SEQUENTIAL_SKIP_IN_ITERATIONS)
        {
            ErlNifUInt64 max_sequential_skip_in_iterations;
            if (enif_get_uint64(env, option[1], &max_sequential_skip_in_iterations))
                opts.max_sequential_skip_in_iterations = max_sequential_skip_in_iterations;
        }
        else if (option[0] == erocksdb::ATOM_INPLACE_UPDATE_SUPPORT)
        {
            opts.inplace_update_support = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_INPLACE_UPDATE_NUM_LOCKS)
        {
            unsigned int inplace_update_num_locks;
            if (enif_get_uint(env, option[1], &inplace_update_num_locks))
                opts.inplace_update_num_locks= inplace_update_num_locks;
        }
        else if (option[0] == erocksdb::ATOM_BLOCK_BASED_TABLE_OPTIONS) {
            rocksdb::BlockBasedTableOptions bbtOpts;
            fold(env, option[1], parse_bbt_option, bbtOpts);
            opts.table_factory = std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewBlockBasedTableFactory(bbtOpts));
        }
        else if (option[0] == erocksdb::ATOM_IN_MEMORY_MODE)
        {
            if (option[1] == erocksdb::ATOM_TRUE)
            {
                // Set recommended defaults
                opts.prefix_extractor = std::shared_ptr<const rocksdb::SliceTransform>(rocksdb::NewFixedPrefixTransform(10));
                opts.table_factory = std::shared_ptr<rocksdb::TableFactory>(rocksdb::NewPlainTableFactory());
                opts.compression = rocksdb::CompressionType::kNoCompression;
                opts.memtable_prefix_bloom_size_ratio = 0.25;
                opts.compaction_style = rocksdb::CompactionStyle::kCompactionStyleUniversal;
                opts.compaction_options_universal.size_ratio = 10;
                opts.compaction_options_universal.min_merge_width = 2;
                opts.compaction_options_universal.max_size_amplification_percent = 50;
                opts.level0_file_num_compaction_trigger = 0;
                opts.level0_slowdown_writes_trigger = 8;
                opts.level0_stop_writes_trigger = 16;
                opts.bloom_locality = 1;
                opts.write_buffer_size = 32 << 20;
                opts.max_write_buffer_number = 2;
                opts.min_write_buffer_number_to_merge = 1;
            }
        }
        else if (option[0] == erocksdb::ATOM_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES)
        {
            opts.level_compaction_dynamic_level_bytes = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_OPTIMIZE_FILTERS_FOR_HITS)
        {
            opts.optimize_filters_for_hits = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_MERGE_OPERATOR)
        {
            int a;
            const ERL_NIF_TERM* merge_op;

            if (enif_is_atom(env, option[1])) {
                if (option[1] == erocksdb::ATOM_ERLANG_MERGE_OPERATOR) {
                    opts.merge_operator = erocksdb::CreateErlangMergeOperator();
                } else if (option[1] == erocksdb::ATOM_BITSET_MERGE_OPERATOR) {
                    opts.merge_operator = erocksdb::CreateBitsetMergeOperator(0x3E80);
                } else if (option[1] == erocksdb::ATOM_COUNTER_MERGE_OPERATOR) {
                    opts.merge_operator = erocksdb::CreateCounterMergeOperator();
                }
            } else if (enif_get_tuple(env, option[1], &a, &merge_op) && a >= 2) {
                if (merge_op[0] == erocksdb::ATOM_BITSET_MERGE_OPERATOR) {
                    unsigned int cap;
                    if (!enif_get_uint(env, merge_op[1], &cap))
                        return erocksdb::ATOM_BADARG;
                    opts.merge_operator = erocksdb::CreateBitsetMergeOperator(cap);
                }
            }
        }
        else if (option[0] == erocksdb::ATOM_PREFIX_EXTRACTOR)
        {
            int a;
            const ERL_NIF_TERM* prefix_extractor;

            if (enif_get_tuple(env, option[1], &a, &prefix_extractor) && a == 2) {
                if (prefix_extractor[0] == erocksdb::ATOM_FIXED_PREFIX_TRANSFORM)
                {
                    int len;
                    if (!enif_get_int(env, prefix_extractor[1], &len))
                        return erocksdb::ATOM_BADARG;
                    opts.prefix_extractor =
                        std::shared_ptr<const rocksdb::SliceTransform>(rocksdb::NewFixedPrefixTransform(len));
                }
                if (prefix_extractor[0] == erocksdb::ATOM_CAPPED_PREFIX_TRANSFORM)
                {
                    int cap_len;
                    if (!enif_get_int(env, prefix_extractor[1], &cap_len))
                        return erocksdb::ATOM_BADARG;
                    opts.prefix_extractor =
                        std::shared_ptr<const rocksdb::SliceTransform>(rocksdb::NewCappedPrefixTransform(cap_len));
                }
            }
        }
        else if (option[0] == erocksdb::ATOM_COMPARATOR)
        {
            if (option[1] == erocksdb::ATOM_BYTEWISE_COMPARATOR)
                opts.comparator = rocksdb::BytewiseComparator();
            else if (option[1] == erocksdb::ATOM_REVERSE_BYTEWISE_COMPARATOR)
                opts.comparator = rocksdb::ReverseBytewiseComparator();
        }

    }
    return erocksdb::ATOM_OK;
}


ERL_NIF_TERM parse_read_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::ReadOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == erocksdb::ATOM_VERIFY_CHECKSUMS)
            opts.verify_checksums = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_FILL_CACHE)
            opts.fill_cache = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_ITERATE_UPPER_BOUND)
            // @TODO Who should be the Slice owner?
            ;
        else if (option[0] == erocksdb::ATOM_TAILING)
            opts.tailing = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_TOTAL_ORDER_SEEK)
            opts.total_order_seek = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_SNAPSHOT)
        {
            erocksdb::ReferencePtr<erocksdb::SnapshotObject> snapshot_ptr;
            snapshot_ptr.assign(erocksdb::SnapshotObject::RetrieveSnapshotObject(env, option[1]));

            if(NULL==snapshot_ptr.get())
                return erocksdb::ATOM_BADARG;

            opts.snapshot = snapshot_ptr->m_Snapshot;
        }
    }

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM parse_write_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::WriteOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == erocksdb::ATOM_SYNC)
            opts.sync = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_DISABLE_WAL)
            opts.disableWAL = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_IGNORE_MISSING_COLUMN_FAMILIES)
            opts.ignore_missing_column_families = (option[1] == erocksdb::ATOM_TRUE);
    }

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM parse_compact_range_option(ErlNifEnv *env, ERL_NIF_TERM item, rocksdb::CompactRangeOptions &opts)
{
    int arity;
    const ERL_NIF_TERM *option;
    if (enif_get_tuple(env, item, &arity, &option) && 2 == arity)
    {
        if (option[0] == erocksdb::ATOM_EXCLUSIVE_MANUAL_COMPACTION)
        {
            opts.exclusive_manual_compaction = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_CHANGE_LEVEL)
        {
            opts.change_level = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_TARGET_LEVEL)
        {
            int target_level;
            if (enif_get_int(env, option[1], &target_level))
                opts.target_level = target_level;
        }
        else if (option[0] == erocksdb::ATOM_ALLOW_WRITE_STALL)
        {
            opts.allow_write_stall = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_MAX_SUBCOMPACTIONS)
        {
            unsigned int max_subcompactions;
            if (enif_get_uint(env, option[1], &max_subcompactions))
                opts.max_subcompactions = max_subcompactions;
        }
    }

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM
parse_cf_descriptor(ErlNifEnv* env, ERL_NIF_TERM item,
                    std::vector<rocksdb::ColumnFamilyDescriptor>& column_families)
{
    char cf_name[4096];
    int arity;
    const ERL_NIF_TERM *cf;

    if (enif_get_tuple(env, item, &arity, &cf) && 2 == arity) {
        if(!enif_get_string(env, cf[0], cf_name, sizeof(cf_name), ERL_NIF_LATIN1) ||
           !enif_is_list(env, cf[1]))
        {
            return enif_make_badarg(env);
        }
        rocksdb::ColumnFamilyOptions opts;
        ERL_NIF_TERM result = fold(env, cf[1], parse_cf_option, opts);
        if (result != erocksdb::ATOM_OK)
        {
            return result;
        }

        column_families.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, opts));
    }

    return erocksdb::ATOM_OK;
}


namespace erocksdb {

ERL_NIF_TERM
Open(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];
    DbObject * db_ptr;
    rocksdb::DB *db(0);


    if(!enif_get_string(env, argv[0], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[1]))
    {
        return enif_make_badarg(env);
    }

    // parse db options
    rocksdb::DBOptions *db_opts = new rocksdb::DBOptions;
    fold(env, argv[1], parse_db_option, *db_opts);

    // parse column family options
    rocksdb::ColumnFamilyOptions *cf_opts = new rocksdb::ColumnFamilyOptions;
    fold(env, argv[1], parse_cf_option, *cf_opts);

    // final options
    rocksdb::Options *opts = new rocksdb::Options(*db_opts, *cf_opts);
    rocksdb::Status status = rocksdb::DB::Open(*opts, db_name, &db);
    delete opts;
    delete db_opts;
    delete cf_opts;

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    db_ptr = DbObject::CreateDbObject(db);
    ERL_NIF_TERM result = enif_make_resource(env, db_ptr);
    enif_release_resource(db_ptr);
    return enif_make_tuple2(env, ATOM_OK, result);
}   // Open

ERL_NIF_TERM
OpenWithCf(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];
    DbObject * db_ptr;
    rocksdb::DB *db(0);


    if(!enif_get_string(env, argv[0], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[1]) || !enif_is_list(env, argv[2]))
    {
        return enif_make_badarg(env);
    }   // if

    // read db options
    rocksdb::DBOptions db_opts;
    fold(env, argv[1], parse_db_option, db_opts);

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    ERL_NIF_TERM head, tail = argv[2];
    while(enif_get_list_cell(env, tail, &head, &tail))
    {
        ERL_NIF_TERM result = parse_cf_descriptor(env, head, column_families);
        if (result != ATOM_OK)
        {
            return result;
        }
    }

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::Status status = rocksdb::DB::Open(db_opts, db_name, column_families, &handles, &db);

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    db_ptr = DbObject::CreateDbObject(db);

    ERL_NIF_TERM result = enif_make_resource(env, db_ptr);

    unsigned int num_cols;
    enif_get_list_length(env, argv[2], &num_cols);

    ERL_NIF_TERM cf_list = enif_make_list(env, 0);
    try {
        for (unsigned int i = 0; i < num_cols; ++i)
        {
            ColumnFamilyObject * handle_ptr;
            handle_ptr = ColumnFamilyObject::CreateColumnFamilyObject(db_ptr, handles[i]);
            ERL_NIF_TERM cf = enif_make_resource(env, handle_ptr);
            enif_release_resource(handle_ptr);
            handle_ptr = NULL;
            cf_list = enif_make_list_cell(env, cf, cf_list);
        }
    } catch (const std::exception& e) {
        // pass through
    }
    // clear the automatic reference from enif_alloc_resource in CreateDbObject
    enif_release_resource(db_ptr);

    ERL_NIF_TERM cf_list_out;
    enif_make_reverse_list(env, cf_list, &cf_list_out);

    return enif_make_tuple3(env, ATOM_OK, result, cf_list_out);
}   // async_open


ERL_NIF_TERM
Close(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    DbObject * db_ptr;
    db_ptr = DbObject::RetrieveDbObject(env, argv[0]);

    if (NULL==db_ptr)
        return enif_make_badarg(env);

    // set closing flag
    ErlRefObject::InitiateCloseRequest(db_ptr);
    db_ptr=NULL;
    return ATOM_OK;
}  // erocksdb::Close

ERL_NIF_TERM
GetProperty(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary name_bin;
    ERL_NIF_TERM name_ref;

    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    db_ptr.assign(DbObject::RetrieveDbObject(env, argv[0]));
    if(NULL==db_ptr.get())
        return enif_make_badarg(env);

    if(argc  == 3)
    {
      name_ref = argv[2];
      // we use a column family assign the value
      cf_ptr.assign(ColumnFamilyObject::RetrieveColumnFamilyObject(env, argv[1]));
    }
    else
    {
      name_ref = argv[1];
    }

    if (!enif_inspect_binary(env, name_ref, &name_bin))
        return enif_make_badarg(env);


    rocksdb::Slice name(reinterpret_cast<char*>(name_bin.data), name_bin.size);
    std::string value;
    if (db_ptr->m_Db->GetProperty(name, &value))
    {
        ERL_NIF_TERM result;
        memcpy(enif_make_new_binary(env, value.size(), &result), value.c_str(), value.size());
        return enif_make_tuple2(env, erocksdb::ATOM_OK, result);
    }
    return erocksdb::ATOM_ERROR;
}   // erocksdb_status

ERL_NIF_TERM
Get(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    int i = 1;
    if(argc == 4)
        i = 2;

    rocksdb::Slice key;
    if(!binary_to_slice(env, argv[i], &key))
    {
        return enif_make_badarg(env);
    }

    rocksdb::ReadOptions *opts = new rocksdb::ReadOptions();
    fold(env, argv[i+1], parse_read_option, *opts);

    rocksdb::Status status;
    rocksdb::PinnableSlice pvalue;
    if(argc==4)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr)) {
            delete opts;
            return enif_make_badarg(env);
        }
        status = db_ptr->m_Db->Get(*opts, cf_ptr->m_ColumnFamily, key, &pvalue);
    }
    else
    {
        status = db_ptr->m_Db->Get(*opts, db_ptr->m_Db->DefaultColumnFamily(), key, &pvalue);
    }

    delete opts;

    if (!status.ok())
    {

        if (status.IsNotFound())
            return ATOM_NOT_FOUND;

        if (status.IsCorruption())
            return ATOM_CORRUPTION;

        return error_tuple(env, ATOM_UNKNOWN_STATUS_ERROR, status);

    }

    ERL_NIF_TERM value_bin;
    memcpy(enif_make_new_binary(env, pvalue.size(), &value_bin), pvalue.data(), pvalue.size());
    pvalue.Reset();
    return enif_make_tuple2(env, ATOM_OK, value_bin);
}   // erocksdb::Get

ERL_NIF_TERM
Put(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key, value;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    rocksdb::Status status;
    rocksdb::ColumnFamilyHandle * cfh;
    if (argc > 4)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key) ||
                !enif_inspect_binary(env, argv[3], &value))
            return enif_make_badarg(env);
        cfh = cf_ptr->m_ColumnFamily;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key) ||
                !enif_inspect_binary(env, argv[2], &value))
            return enif_make_badarg(env);
        cfh = db_ptr->m_Db->DefaultColumnFamily();
    }
    rocksdb::WriteOptions *opts = new rocksdb::WriteOptions;
    fold(env, argv[3], parse_write_option, *opts);
    rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
    rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
    status = db_ptr->m_Db->Put(*opts, cfh, key_slice, value_slice);

    delete opts;
    opts = NULL;

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    return ATOM_OK;
}

ERL_NIF_TERM
Merge(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;

    ErlNifBinary key, value;

    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);
    rocksdb::Status status;
    rocksdb::ColumnFamilyHandle *cfh;
    if (argc > 4)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key) ||
                !enif_inspect_binary(env, argv[3], &value))
            return enif_make_badarg(env);
        cfh = cf_ptr->m_ColumnFamily;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key) ||
                !enif_inspect_binary(env, argv[2], &value))
            return enif_make_badarg(env);
        cfh = db_ptr->m_Db->DefaultColumnFamily();
    }
    rocksdb::WriteOptions *opts = new rocksdb::WriteOptions;
    fold(env, argv[3], parse_write_option, *opts);
    rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
    rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
    status = db_ptr->m_Db->Merge(*opts, cfh, key_slice, value_slice);
    delete opts;
    opts = NULL;
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    return ATOM_OK;
}

ERL_NIF_TERM
Delete(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);
    rocksdb::Status status;
    rocksdb::ColumnFamilyHandle *cfh;
    if (argc > 3)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key))
            return enif_make_badarg(env);
        cfh = cf_ptr->m_ColumnFamily;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key))
            return enif_make_badarg(env);
        cfh = db_ptr->m_Db->DefaultColumnFamily();
    }
    rocksdb::WriteOptions *opts = new rocksdb::WriteOptions;
    fold(env, argv[2], parse_write_option, *opts);
    rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
    status = db_ptr->m_Db->Delete(*opts, cfh, key_slice);
    delete opts;
    opts = NULL;
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    return ATOM_OK;
}

ERL_NIF_TERM
SingleDelete(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);
    rocksdb::Status status;
    rocksdb::ColumnFamilyHandle *cfh;
    if (argc > 3)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key))
            return enif_make_badarg(env);
        cfh = cf_ptr->m_ColumnFamily;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key))
            return enif_make_badarg(env);
        cfh = db_ptr->m_Db->DefaultColumnFamily();
    }
    rocksdb::WriteOptions *opts = new rocksdb::WriteOptions;
    fold(env, argv[2], parse_write_option, *opts);
    rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
    status = db_ptr->m_Db->SingleDelete(*opts, cfh, key_slice);
    delete opts;
    opts = NULL;
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    return ATOM_OK;
}

ERL_NIF_TERM
Checkpoint(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char path[4096];
    rocksdb::Checkpoint* checkpoint;
    rocksdb::Status status;

    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if(!enif_get_string(env, argv[1], path, sizeof(path), ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    status = rocksdb::Checkpoint::Create(db_ptr->m_Db, &checkpoint);
    if (status.ok())
    {
        status = checkpoint->CreateCheckpoint(path);
        if (status.ok())
        {
            delete checkpoint;
            return ATOM_OK;
        }
    }
    delete checkpoint;

    return error_tuple(env, ATOM_ERROR, status);

}   // Checkpoint

ERL_NIF_TERM
Destroy(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char name[4096];
    if (!enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1) ||
            !enif_is_list(env, argv[1]))
        return enif_make_badarg(env);

    // Parse out the options
    rocksdb::DBOptions db_opts;
    rocksdb::ColumnFamilyOptions cf_opts;
    fold(env, argv[1], parse_db_option, db_opts);
    fold(env, argv[1], parse_cf_option, cf_opts);
    rocksdb::Options *opts = new rocksdb::Options(db_opts, cf_opts);

    rocksdb::Status status = rocksdb::DestroyDB(name, *opts);

    delete opts;

    if (!status.ok())
    {
        return error_tuple(env, ATOM_ERROR_DB_DESTROY, status);
    }
    return ATOM_OK;
}   // Destroy

ERL_NIF_TERM
Repair(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char name[4096];
    if (!enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1) ||
            !enif_is_list(env, argv[1]))
        return enif_make_badarg(env);

    // Parse out the options
    rocksdb::DBOptions db_opts;
    rocksdb::ColumnFamilyOptions cf_opts;
    fold(env, argv[1], parse_db_option, db_opts);
    fold(env, argv[1], parse_cf_option, cf_opts);
    rocksdb::Options *opts = new rocksdb::Options(db_opts, cf_opts);

    rocksdb::Status status = rocksdb::RepairDB(name, *opts);
    if (!status.ok())
    {
        return error_tuple(env, erocksdb::ATOM_ERROR_DB_REPAIR, status);
    }
    return erocksdb::ATOM_OK;
}   // erocksdb_repair

ERL_NIF_TERM
IsEmpty(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    rocksdb::ReadOptions opts;
    rocksdb::Iterator* itr = db_ptr->m_Db->NewIterator(opts);
    itr->SeekToFirst();
    ERL_NIF_TERM result;
    if (itr->Valid())
    {
        result = erocksdb::ATOM_FALSE;
    }
    else
    {
        result = erocksdb::ATOM_TRUE;
    }
    delete itr;

    return result;
}   // erocksdb_is_empty

ERL_NIF_TERM
GetLatestSequenceNumber(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    rocksdb::SequenceNumber seq = db_ptr->m_Db->GetLatestSequenceNumber();

    return enif_make_uint64(env, seq);
}

ERL_NIF_TERM
DeleteRange(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    rocksdb::ColumnFamilyHandle *column_family;
    rocksdb::Slice begin;
    rocksdb::Slice end;
    rocksdb::Status status;
    ReferencePtr<ColumnFamilyObject> cf_ptr;
    int i = 1;

    if (!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if (argc == 5)
    {
        if (!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);

        column_family = cf_ptr->m_ColumnFamily;
        i = 2;
    }
    else
    {
        column_family = db_ptr->m_Db->DefaultColumnFamily();
    }

    if (!binary_to_slice(env, argv[i], &begin))
        return enif_make_badarg(env);

    if (!binary_to_slice(env, argv[i + 1], &end))
        return enif_make_badarg(env);

    // parse read_options
    rocksdb::WriteOptions *opts = new rocksdb::WriteOptions;
    fold(env, argv[i + 2], parse_write_option, *opts);

    status = db_ptr->m_Db->DeleteRange(*opts, column_family, begin, end);
    delete opts;
    opts = NULL;
    if (!status.ok())
        return error_tuple(env, erocksdb::ATOM_ERROR, status);

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM
GetApproximateSizes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    rocksdb::ColumnFamilyHandle *column_family;
    rocksdb::Slice start;
    rocksdb::Slice limit;
    rocksdb::Status status;
    ReferencePtr<ColumnFamilyObject> cf_ptr;
    int i = 1;
    
    if (!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if (argc == 5)
    {
        if (!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);

        i = 2;
    } else {
        column_family = db_ptr->m_Db->DefaultColumnFamily();
    }

    uint8_t flag;
    ERL_NIF_TERM flag_term = argv[i + 1];
    if (flag_term == erocksdb::ATOM_NONE)
        flag = rocksdb::DB::SizeApproximationFlags::NONE;
    else if (flag_term == erocksdb::ATOM_INCLUDE_MEMTABLES)
        flag = rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES;
    else if (flag_term == erocksdb::ATOM_INCLUDE_FILES)
        flag = rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES;
    else if (flag_term == erocksdb::ATOM_INCLUDE_BOTH)
        flag = rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES | rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES;
    else
        return enif_make_badarg(env);

    unsigned int num_ranges;
    if (!enif_get_list_length(env, argv[i], &num_ranges))
        return enif_make_badarg(env);

    ERL_NIF_TERM head, tail = argv[i];
    int j = 0;
    int arity;
    const ERL_NIF_TERM *rterm;
    
    rocksdb::Range *ranges = new rocksdb::Range[num_ranges];
    while (enif_get_list_cell(env, tail, &head, &tail))
    {
        if (enif_get_tuple(env, head, &arity, &rterm) && 2 == arity)
        {
            if (!binary_to_slice(env, rterm[0], &start) || !binary_to_slice(env, rterm[1], &limit)) 
            {
                delete[] ranges;
                return enif_make_badarg(env);
            }
                
            ranges[j].start = start;
            ranges[j].limit = limit;
            j++;
        }
        else
        {
            delete[] ranges;
            return enif_make_badarg(env);
        }
    }

    uint64_t sizes[num_ranges];
    db_ptr->m_Db->GetApproximateSizes(column_family, ranges, num_ranges, sizes, flag);
    ERL_NIF_TERM result = enif_make_list(env, 0);
    for (int k = 0; k < num_ranges; k++)
    {
        result = enif_make_list_cell(env, enif_make_uint64(env, sizes[k]), result);
    }
    ERL_NIF_TERM result_out;
    enif_make_reverse_list(env, result, &result_out);
    
    delete[] ranges;
    return result_out;
}

ERL_NIF_TERM
GetApproximateMemTableStats(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    rocksdb::ColumnFamilyHandle *column_family;
    rocksdb::Slice start;
    rocksdb::Slice limit;
    rocksdb::Status status;
    ReferencePtr<ColumnFamilyObject> cf_ptr;
    int i = 1;

    if (!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if (argc == 4)
    {
        if (!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);

        i = 2;
    }
    else
    {
        column_family = db_ptr->m_Db->DefaultColumnFamily();
    }

    int arity;
    const ERL_NIF_TERM *rterm;

    if (!enif_get_tuple(env, argv[i], &arity, &rterm) && 2 == arity)
        return enif_make_badarg(env);

    if (!binary_to_slice(env, rterm[0], &start) || !binary_to_slice(env, rterm[1], &limit))
            return enif_make_badarg(env);

    auto range = rocksdb::Range(start, limit);

    uint64_t size, count;
    db_ptr->m_Db->GetApproximateMemTableStats(column_family, range, &count, &size);
    return enif_make_tuple2(
        env,
        ATOM_OK,
        enif_make_tuple2(env, enif_make_uint64(env, count),  enif_make_uint64(env, size))
    );
}

ERL_NIF_TERM
CompactRange(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    rocksdb::ColumnFamilyHandle *column_family;
    rocksdb::Slice begin;
    rocksdb::Slice end;
    rocksdb::Status status;
    ReferencePtr<ColumnFamilyObject> cf_ptr;
    int i = 1;

    if (!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if (argc == 5)
    {
        if (!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        column_family = cf_ptr->m_ColumnFamily;
        i = 2;
    }
    else
    {
        column_family = db_ptr->m_Db->DefaultColumnFamily();
    }

    if (argv[i] == erocksdb::ATOM_UNDEFINED)
    {
        begin = nullptr;
    }
    else if (!binary_to_slice(env, argv[i], &begin))
    {
        return enif_make_badarg(env);
    }

    if (argv[i + 1] == erocksdb::ATOM_UNDEFINED)
    {
        end = nullptr;
    }
    else if (!binary_to_slice(env, argv[i + 1], &end))
    {
        return enif_make_badarg(env);
    }

    // parse read_options
    rocksdb::CompactRangeOptions *opts = new rocksdb::CompactRangeOptions;
    fold(env, argv[i + 2], parse_compact_range_option, *opts);

    status = db_ptr->m_Db->CompactRange(*opts, column_family, &begin, &end);
    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM
Flush(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);


    rocksdb::Status status;
    rocksdb::FlushOptions flush_opts;

    if(argc==2)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        status = db_ptr->m_Db->Flush(flush_opts, cf_ptr->m_ColumnFamily);
    }
    else
    {
        status = db_ptr->m_Db->Flush(flush_opts);
    }

    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return ATOM_OK;

}   // erocksdb::Flush

ERL_NIF_TERM
SyncWal(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    rocksdb::Status status;
    status = db_ptr->m_Db->SyncWAL();

    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return ATOM_OK;

}   // erocksdb::Flush

ERL_NIF_TERM
SetDBBackgroundThreads(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{

    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    rocksdb::Options options = db_ptr->m_Db->GetOptions();

    int n;
    if(!enif_get_int(env, argv[1], &n))
        return enif_make_badarg(env);

    if(argc==3)
    {
        if(argv[2] == ATOM_PRIORITY_HIGH)
             options.env->SetBackgroundThreads(n, rocksdb::Env::Priority::HIGH);
        else if((argv[2] == ATOM_PRIORITY_LOW))
             options.env->SetBackgroundThreads(n, rocksdb::Env::Priority::LOW);
        else
            return enif_make_badarg(env);
    }
    else
    {
        options.env->SetBackgroundThreads(n);
    }

    return ATOM_OK;
}   // erocksdb::SetDBBackgroundThreads



}

