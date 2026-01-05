// -------------------------------------------------------------------
// Copyright (c) 2016-2026 Benoit Chesneau. All Rights Reserved.
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
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/slice.h"
#include "rocksdb/wide_columns.h"
#include "rocksdb/cache.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/metadata.h"

#include "atoms.h"
#include "refobjects.h"
#include "util.h"
#include "erocksdb_db.h"
#include "cache.h"
#include "statistics.h"
#include "rate_limiter.h"
#include "sst_file_manager.h"
#include "write_buffer_manager.h"
#include "env.h"
#include "erlang_merge.h"
#include "bitset_merge_operator.h"
#include "counter_merge_operator.h"
#include "posting_list_merge_operator.h"
#include "compaction_filter.h"

ERL_NIF_TERM 
parse_bbt_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::BlockBasedTableOptions& opts) {
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

ERL_NIF_TERM 
parse_compaction_options_fifo(ErlNifEnv *env, ERL_NIF_TERM item, rocksdb::CompactionOptionsFIFO &opts)
{
    int arity;
    const ERL_NIF_TERM *option;
    if (enif_get_tuple(env, item, &arity, &option) && 2 == arity)
    {
        if (option[0] == erocksdb::ATOM_MAX_TABLE_FILE_SIZE)
        {
          ErlNifUInt64 max_table_file_size;
          if (enif_get_uint64(env, option[1], &max_table_file_size))
            opts.max_table_files_size = max_table_file_size;
        }
        else if (option[0] == erocksdb::ATOM_ALLOW_COMPACTION)
        {
            opts.allow_compaction = (option[1] == erocksdb::ATOM_TRUE);
        }
    }
    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM 
parse_db_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::DBOptions& opts)
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
                    auto memenv = rocksdb::NewMemEnv(rocksdb::Env::Default());
                    memenv->CreateDir("test");
                    opts.env = memenv;
                    opts.create_if_missing = true;
                }
            } else {
                erocksdb::ManagedEnv* env_ptr = erocksdb::ManagedEnv::RetrieveEnvResource(env, option[1]);
                if(NULL!=env_ptr)
                    opts.env = (rocksdb::Env*)env_ptr->env();
            }
        }
        else if (option[0] == erocksdb::ATOM_STATISTICS)
        {
            erocksdb::Statistics* statistics_ptr = erocksdb::Statistics::RetrieveStatisticsResource(env, option[1]);
            if (statistics_ptr != nullptr)
                opts.statistics = statistics_ptr->statistics();
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
            ERL_NIF_TERM head, tail;
            tail = option[1];
            char db_name[4096];
            while(enif_get_list_cell(env, tail, &head, &tail)) {
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
        else if (option[0] == erocksdb::ATOM_COMPACTION_READAHEAD_SIZE)
        {
            unsigned int compaction_readahead_size;
            if (enif_get_uint(env, option[1], &compaction_readahead_size))
                opts.compaction_readahead_size = compaction_readahead_size;
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
                auto memenv = rocksdb::NewMemEnv(rocksdb::Env::Default());
                memenv->CreateDir("test");
                opts.env = memenv;
                opts.create_if_missing = true;
            }
        }
        else if (option[0] == erocksdb::ATOM_RATE_LIMITER)
        {
            erocksdb::RateLimiter* rate_limiter_ptr = erocksdb::RateLimiter::RetrieveRateLimiterResource(env,option[1]);
            if(NULL!=rate_limiter_ptr) {
                auto rate_limiter = rate_limiter_ptr->rate_limiter();
                opts.rate_limiter = rate_limiter;
            }
        }
        else if (option[0] == erocksdb::ATOM_SST_FILE_MANAGER)
        {
            erocksdb::SstFileManager* ptr = erocksdb::SstFileManager::RetrieveSstFileManagerResource(env,option[1]);;
            if (NULL!=ptr) {
                opts.sst_file_manager = ptr->sst_file_manager();
            }
        }
        else if (option[0] == erocksdb::ATOM_WRITE_BUFFER_MANAGER)
        {
            erocksdb::WriteBufferManager* ptr = erocksdb::WriteBufferManager::RetrieveWriteBufferManagerResource(env,option[1]);;
            if (NULL!=ptr) {
                opts.write_buffer_manager = ptr->write_buffer_manager();
            }
        }
        else if (option[0] == erocksdb::ATOM_MAX_SUBCOMPACTIONS)
        {
            unsigned int max_subcompactions;
            if (enif_get_uint(env, option[1], &max_subcompactions))
                opts.max_subcompactions = max_subcompactions;
        }
        else if (option[0] == erocksdb::ATOM_ATOMIC_FLUSH)
        {
            opts.atomic_flush = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_MANUAL_WAL_FLUSH)
        {
            opts.manual_wal_flush = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_USE_DIRECT_READS)
        {
            opts.use_direct_reads = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION)
        {
            opts.use_direct_io_for_flush_and_compaction = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_ENABLE_PIPELINED_WRITE)
        {
            opts.enable_pipelined_write = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_UNORDERED_WRITE)
        {
            opts.unordered_write = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if (option[0] == erocksdb::ATOM_TWO_WRITE_QUEUES)
        {
            opts.two_write_queues = (option[1] == erocksdb::ATOM_TRUE);
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
        else if (option[0] == erocksdb::ATOM_COMPRESSION ||
                 option[0] == erocksdb::ATOM_BOTTOMMOST_COMPRESSION)
        {
            rocksdb::CompressionType compression;
            if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_SNAPPY) {
                compression = rocksdb::CompressionType::kSnappyCompression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_ZLIB) {
                compression = rocksdb::CompressionType::kZlibCompression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_BZIP2) {
                compression = rocksdb::CompressionType::kBZip2Compression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_LZ4) {
                compression = rocksdb::CompressionType::kLZ4Compression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_LZ4H) {
                compression = rocksdb::CompressionType::kLZ4HCCompression;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_ZSTD)
            {
                compression = rocksdb::CompressionType::kZSTD;
            }
            else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_NONE) {
                compression = rocksdb::CompressionType::kNoCompression;
            }

            if (option[0] == erocksdb::ATOM_COMPRESSION)
                opts.compression = compression;
            else
                opts.bottommost_compression = compression;
        }
        else if (option[0] == erocksdb::ATOM_COMPRESSION_OPTS ||
                 option[0] == erocksdb::ATOM_BOTTOMMOST_COMPRESSION_OPTS)
        {
            ERL_NIF_TERM head, tail;
            tail = option[1];
            rocksdb::CompressionOptions compression_opts = rocksdb::CompressionOptions();
            while(enif_get_list_cell(env, tail, &head, &tail)) {
                int arity2;
                const ERL_NIF_TERM* compression_opt;
                if (enif_get_tuple(env, head, &arity2, &compression_opt) && arity2 == 2)
                {
                    if (compression_opt[0] == erocksdb::ATOM_ENABLED)
                    {
                        compression_opts.enabled = enif_compare(enif_make_atom(env, "true"),
                                                                compression_opt[1]) == 0;
                    }
                    else if (compression_opt[0] == erocksdb::ATOM_WINDOW_BITS)
                    {
                        int window_bits;
                        if (enif_get_int(env, compression_opt[1], &window_bits))
                            compression_opts.window_bits = window_bits;
                    }
                    else if (compression_opt[0] == erocksdb::ATOM_LEVEL)
                    {
                        int compression_level;
                        if (enif_get_int(env, compression_opt[1], &compression_level))
                            compression_opts.level = compression_level;
                    }
                    else if (compression_opt[0] == erocksdb::ATOM_STRATEGY)
                    {
                        int strategy;
                        if (enif_get_int(env, compression_opt[1], &strategy))
                            compression_opts.strategy = strategy;
                    }
                    else if (compression_opt[0] == erocksdb::ATOM_MAX_DICT_BYTES)
                    {
                        uint32_t max_dict_bytes;
                        if (enif_get_uint(env, compression_opt[1], &max_dict_bytes))
                            compression_opts.max_dict_bytes = max_dict_bytes;
                    }
                    else if (compression_opt[0] == erocksdb::ATOM_ZSTD_MAX_TRAIN_BYTES)
                    {
                        uint32_t zstd_max_train_bytes;
                        if (enif_get_uint(env, compression_opt[1], &zstd_max_train_bytes))
                            compression_opts.zstd_max_train_bytes = zstd_max_train_bytes;
                    }
                }
            }
            if (option[0] == erocksdb::ATOM_COMPRESSION_OPTS)
                opts.compression_opts = compression_opts;
            else
                opts.bottommost_compression_opts = compression_opts;
        }
        else if (option[0] == erocksdb::ATOM_NUM_LEVELS)
        {
            int num_levels;
            if (enif_get_int(env, option[1], &num_levels))
                opts.num_levels = num_levels;
        }
        else if (option[0] == erocksdb::ATOM_TTL) {
            ErlNifUInt64 ttl;
            if (enif_get_uint64(env, option[1], &ttl))
                opts.ttl = ttl;
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
        else if (option[0] == erocksdb::ATOM_COMPACTION_OPTIONS_FIFO) {
            rocksdb::CompactionOptionsFIFO fifoOpts;
            fold(env, option[1], parse_compaction_options_fifo, fifoOpts);
            opts.compaction_options_fifo = fifoOpts;
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
                } else if (option[1] == erocksdb::ATOM_POSTING_LIST_MERGE_OPERATOR) {
                    opts.merge_operator = erocksdb::CreatePostingListMergeOperator();
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
        else if (option[0] == erocksdb::ATOM_COMPACTION_FILTER)
        {
            // Parse compaction filter options from a map
            // Expected format:
            // {compaction_filter, #{rules => [...], handler => Pid, ...}}
            if (enif_is_map(env, option[1])) {
                ERL_NIF_TERM rules_term, handler_term, batch_term, timeout_term;

                // Check for rules-based mode
                if (enif_get_map_value(env, option[1],
                    erocksdb::ATOM_RULES, &rules_term)) {

                    std::vector<erocksdb::FilterRule> rules;
                    ERL_NIF_TERM head, tail = rules_term;

                    while (enif_get_list_cell(env, tail, &head, &tail)) {
                        int rule_arity;
                        const ERL_NIF_TERM* tuple;

                        if (!enif_get_tuple(env, head, &rule_arity, &tuple) || rule_arity < 1) {
                            continue;
                        }

                        erocksdb::FilterRule rule;

                        if (tuple[0] == erocksdb::ATOM_KEY_PREFIX && rule_arity == 2) {
                            rule.type = erocksdb::RuleType::KeyPrefix;
                            ErlNifBinary bin;
                            if (enif_inspect_binary(env, tuple[1], &bin)) {
                                rule.pattern = std::string((char*)bin.data, bin.size);
                                rules.push_back(rule);
                            }
                        }
                        else if (tuple[0] == erocksdb::ATOM_KEY_SUFFIX && rule_arity == 2) {
                            rule.type = erocksdb::RuleType::KeySuffix;
                            ErlNifBinary bin;
                            if (enif_inspect_binary(env, tuple[1], &bin)) {
                                rule.pattern = std::string((char*)bin.data, bin.size);
                                rules.push_back(rule);
                            }
                        }
                        else if (tuple[0] == erocksdb::ATOM_KEY_CONTAINS && rule_arity == 2) {
                            rule.type = erocksdb::RuleType::KeyContains;
                            ErlNifBinary bin;
                            if (enif_inspect_binary(env, tuple[1], &bin)) {
                                rule.pattern = std::string((char*)bin.data, bin.size);
                                rules.push_back(rule);
                            }
                        }
                        else if (tuple[0] == erocksdb::ATOM_VALUE_EMPTY && rule_arity == 1) {
                            rule.type = erocksdb::RuleType::ValueEmpty;
                            rules.push_back(rule);
                        }
                        else if (tuple[0] == erocksdb::ATOM_VALUE_PREFIX && rule_arity == 2) {
                            rule.type = erocksdb::RuleType::ValuePrefix;
                            ErlNifBinary bin;
                            if (enif_inspect_binary(env, tuple[1], &bin)) {
                                rule.pattern = std::string((char*)bin.data, bin.size);
                                rules.push_back(rule);
                            }
                        }
                        else if (tuple[0] == erocksdb::ATOM_TTL_FROM_KEY && rule_arity == 4) {
                            rule.type = erocksdb::RuleType::TTLFromKey;
                            unsigned int offset, length;
                            ErlNifUInt64 ttl;
                            if (enif_get_uint(env, tuple[1], &offset) &&
                                enif_get_uint(env, tuple[2], &length) &&
                                enif_get_uint64(env, tuple[3], &ttl)) {
                                rule.offset = offset;
                                rule.length = length;
                                rule.ttl_seconds = ttl;
                                rules.push_back(rule);
                            }
                        }
                        else if (tuple[0] == erocksdb::ATOM_ALWAYS_DELETE && rule_arity == 1) {
                            rule.type = erocksdb::RuleType::Always;
                            rules.push_back(rule);
                        }
                    }

                    if (!rules.empty()) {
                        opts.compaction_filter_factory =
                            erocksdb::CreateCompactionFilterFactory(rules);
                    }
                }
                // Check for handler-based mode
                else if (enif_get_map_value(env, option[1],
                    erocksdb::ATOM_HANDLER, &handler_term)) {

                    ErlNifPid handler_pid;
                    if (!enif_get_local_pid(env, handler_term, &handler_pid)) {
                        return erocksdb::ATOM_BADARG;
                    }

                    unsigned int batch_size = 100;  // default
                    unsigned int timeout_ms = 5000; // default

                    if (enif_get_map_value(env, option[1],
                        erocksdb::ATOM_BATCH_SIZE, &batch_term)) {
                        enif_get_uint(env, batch_term, &batch_size);
                    }

                    if (enif_get_map_value(env, option[1],
                        erocksdb::ATOM_TIMEOUT, &timeout_term)) {
                        enif_get_uint(env, timeout_term, &timeout_ms);
                    }

                    opts.compaction_filter_factory =
                        erocksdb::CreateCompactionFilterFactory(
                            handler_pid, batch_size, timeout_ms);
                }
            }
        }
        else if (option[0] == erocksdb::ATOM_ENABLE_BLOB_FILES)
        {
          opts.enable_blob_files = (option[1] ==erocksdb::ATOM_TRUE); 
        }
        else if (option[0] == erocksdb::ATOM_MIN_BLOB_SIZE)
        {

          ErlNifUInt64 min_blob_size;
          if (enif_get_uint64(env, option[1], &min_blob_size))
            opts.min_blob_size = min_blob_size;
        }
        else if (option[0] == erocksdb::ATOM_BLOB_FILE_SIZE)
        {
          ErlNifUInt64 blob_file_size;
          if (enif_get_uint64(env, option[1], &blob_file_size))
            opts.min_blob_size = blob_file_size;
        }
        else if(option[0] == erocksdb::ATOM_BLOB_COMPRESSION_TYPE)
        {
          rocksdb::CompressionType compression = rocksdb::CompressionType::kNoCompression;
          if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_SNAPPY)
          {
            compression = rocksdb::CompressionType::kSnappyCompression;
          }
          else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_ZLIB)
          {
            compression = rocksdb::CompressionType::kZlibCompression;
          }
          else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_BZIP2)
          {
            compression = rocksdb::CompressionType::kBZip2Compression;
          }
          else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_LZ4)
          {
            compression = rocksdb::CompressionType::kLZ4Compression;
          }
          else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_LZ4H)
          {
            compression = rocksdb::CompressionType::kLZ4HCCompression;
          }
          else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_ZSTD)
          {
            compression = rocksdb::CompressionType::kZSTD;
          }
          else if (option[1] == erocksdb::ATOM_COMPRESSION_TYPE_NONE)
          {
            compression = rocksdb::CompressionType::kNoCompression;
          }

          opts.blob_compression_type = compression;
        }
        else if(option[0] == erocksdb::ATOM_ENABLE_BLOB_GC)
        {
          opts.enable_blob_garbage_collection = (option[1] == erocksdb::ATOM_TRUE);
        }
        else if(option[0] == erocksdb::ATOM_BLOB_GC_AGE_CUTOFF)
        {
          double cutoff;
          if (enif_get_double(env, option[1], &cutoff))
            opts.blob_garbage_collection_age_cutoff = cutoff;
        }
        if(option[0] == erocksdb::ATOM_BLOB_GC_FORCE_THRESHOLD)
        {
          double threshold;
          if (enif_get_double(env, option[1], &threshold))
            opts.blob_garbage_collection_force_threshold = threshold;
        }
        if(option[0] == erocksdb::ATOM_BLOB_COMPACTION_READAHEAD_SIZE)
        {
          ErlNifUInt64 readahead_size;
          if (enif_get_uint64(env, option[1], &readahead_size))
            opts.blob_compaction_readahead_size = readahead_size;
        }
        if(option[0] == erocksdb::ATOM_BLOB_FILE_STARTING_LEVEL)
        {
          int starting_level;
          if (enif_get_int(env, option[1], &starting_level))
            opts.blob_file_starting_level = starting_level;
        }
        if(option[0] == erocksdb::ATOM_BLOB_CACHE)
        {
          erocksdb::Cache* cache_ptr = erocksdb::Cache::RetrieveCacheResource(env,option[1]);
          if(NULL!=cache_ptr) {
            auto cache = cache_ptr->cache();
            opts.blob_cache = cache;
          }
        }
        if(option[0] == erocksdb::ATOM_PREPOPULATE_BLOB_CACHE)
        {
          if (option[1] == erocksdb::ATOM_DISABLE)
          {
            opts.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kDisable;
          }
          else if (option[1] == erocksdb::ATOM_FLUSH_ONLY)
          {
            opts.prepopulate_blob_cache = rocksdb::PrepopulateBlobCache::kFlushOnly;
          }
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
        if (option[0] == erocksdb::ATOM_READ_TIER)
        {
            if (option[1] == erocksdb::ATOM_READ_ALL_TIER)
            {
                opts.read_tier = rocksdb::kReadAllTier;
            }
            else if (option[1] == erocksdb::ATOM_BLOCK_CACHE_TIER) {
                opts.read_tier = rocksdb::kBlockCacheTier;
            }
            else if (option[1] == erocksdb::ATOM_PERSISTED_TIER) {
                opts.read_tier = rocksdb::kPersistedTier;
            }
            else if (option[1] == erocksdb::ATOM_MEMTABLE_TIER) {
                opts.read_tier = rocksdb::kMemtableTier;
            }
        }
        else if (option[0] == erocksdb::ATOM_VERIFY_CHECKSUMS)
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
        else if (option[0] == erocksdb::ATOM_AUTO_REFRESH_ITERATOR_WITH_SNAPSHOT)
            opts.auto_refresh_iterator_with_snapshot = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_READAHEAD_SIZE)
        {
            ErlNifUInt64 readahead_size;
            if (enif_get_uint64(env, option[1], &readahead_size))
                opts.readahead_size = static_cast<size_t>(readahead_size);
        }
        else if (option[0] == erocksdb::ATOM_ASYNC_IO)
            opts.async_io = (option[1] == erocksdb::ATOM_TRUE);
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
        else if (option[0] == erocksdb::ATOM_NO_SLOWDOWN)
            opts.no_slowdown = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_LOW_PRI)
            opts.low_pri = (option[1] == erocksdb::ATOM_TRUE);
    }

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM parse_flush_option(ErlNifEnv *env, ERL_NIF_TERM item, rocksdb::FlushOptions &opts)
{
    int arity;
    const ERL_NIF_TERM *option;
    if (enif_get_tuple(env, item, &arity, &option) && 2 == arity)
    {
        if (option[0] == erocksdb::ATOM_WAIT)
            opts.wait = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_ALLOW_WRITE_STALL)
            opts.allow_write_stall = (option[1] == erocksdb::ATOM_TRUE);
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
        else if (option[0] == erocksdb::ATOM_BOTTOMMOST_LEVEL_COMPACTION)
        {
            if (option[1] == erocksdb::ATOM_SKIP)
                opts.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kSkip;
            else if (option[1] == erocksdb::ATOM_IF_HAVE_COMPACTION_FILTER)
                opts.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter;
            else if (option[1] == erocksdb::ATOM_FORCE)
                opts.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
            else if (option[1] == erocksdb::ATOM_FORCE_OPTIMIZED)
                opts.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
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

// Base Open function.
//
// This `Open` function is not called by directly the VM due to the
// extra `read_only` argument. Instead, it is called by `Open` or
// `OpenReadOnly` below.
ERL_NIF_TERM
Open(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[],
    bool read_only)
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
    rocksdb::Status status;
    if (read_only) {
        status = rocksdb::DB::OpenForReadOnly(*opts, db_name, &db);
    } else {
        status = rocksdb::DB::Open(*opts, db_name, &db);
    }
    delete opts;
    delete db_opts;
    delete cf_opts;

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    db_ptr = DbObject::CreateDbObject(std::move(db));
    ERL_NIF_TERM result = enif_make_resource(env, db_ptr);
    enif_release_resource(db_ptr);
    return enif_make_tuple2(env, ATOM_OK, result);
}   // Open

ERL_NIF_TERM
Open(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    return Open(env, argc, argv, false);
} // Open

ERL_NIF_TERM
OpenReadOnly(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[]) {
    return Open(env, argc, argv, true);
} // OpenReadOnly

// Base OpenWithCf function.
//
// This `OpenWithCf` function is not called by directly the VM due to
// the extra `read_only` argument. Instead, it is called by
// `OpenWithCf` or `OpenWithCfReadOnly` below.
ERL_NIF_TERM
OpenWithCf(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[],
    bool read_only)
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
    rocksdb::Status status;
    if (read_only) {
        status = rocksdb::DB::OpenForReadOnly(db_opts, db_name, column_families, &handles, &db);
    } else {
        status = rocksdb::DB::Open(db_opts, db_name, column_families, &handles, &db);
    }

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    db_ptr = DbObject::CreateDbObject(std::move(db));

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
    } catch (const std::exception&) {
        // pass through
    }
    // clear the automatic reference from enif_alloc_resource in CreateDbObject
    enif_release_resource(db_ptr);

    ERL_NIF_TERM cf_list_out;
    enif_make_reverse_list(env, cf_list, &cf_list_out);

    return enif_make_tuple3(env, ATOM_OK, result, cf_list_out);
}   // async_open

ERL_NIF_TERM
OpenWithCf(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    return OpenWithCf(env, argc, argv, false);
} // OpenWithCf

ERL_NIF_TERM
OpenWithCfReadOnly(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    return OpenWithCf(env, argc, argv, true);
}

ERL_NIF_TERM
OpenWithTTL(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];
    int ttl;
    bool read_only;
    DbObject * db_ptr;
    rocksdb::DBWithTTL *db(0);


    if(!enif_get_string(env, argv[0], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[1]) || !enif_is_number(env, argv[2]) || !enif_is_atom(env, argv[3]))
    {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[2], &ttl))
    {
        return enif_make_badarg(env);
    }

    read_only = (argv[3] == erocksdb::ATOM_TRUE);

    // parse db options
    rocksdb::DBOptions *db_opts = new rocksdb::DBOptions;
    fold(env, argv[1], parse_db_option, *db_opts);

    // parse column family options
    rocksdb::ColumnFamilyOptions *cf_opts = new rocksdb::ColumnFamilyOptions;
    fold(env, argv[1], parse_cf_option, *cf_opts);

    // final options
    rocksdb::Options *opts = new rocksdb::Options(*db_opts, *cf_opts);
    rocksdb::Status status = rocksdb::DBWithTTL::Open(*opts, db_name, &db, ttl, read_only);
    delete opts;
    delete db_opts;
    delete cf_opts;

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    db_ptr = DbObject::CreateDbObject(std::move(db), false, true);  // IsPessimistic=false, IsTTL=true
    ERL_NIF_TERM result = enif_make_resource(env, db_ptr);
    enif_release_resource(db_ptr);
    return enif_make_tuple2(env, ATOM_OK, result);
}   // OpenWithTTL

ERL_NIF_TERM
OpenOptimisticTransactionDB(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];
    DbObject * db_ptr;
    rocksdb::OptimisticTransactionDB *db;


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
    rocksdb::Status status =
        rocksdb::OptimisticTransactionDB::Open(db_opts, db_name, column_families, &handles, &db);

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    db_ptr = DbObject::CreateDbObject(std::move(db));

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
    } catch (const std::exception&) {
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
    int /*argc*/,
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
            return error_tuple(env, ATOM_CORRUPTION, status);

        return error_tuple(env, ATOM_UNKNOWN_STATUS_ERROR, status);
    }

    ERL_NIF_TERM value_bin;
    memcpy(enif_make_new_binary(env, pvalue.size(), &value_bin), pvalue.data(), pvalue.size());
    pvalue.Reset();
    return enif_make_tuple2(env, ATOM_OK, value_bin);
}   // erocksdb::Get

ERL_NIF_TERM
MultiGet(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    // Determine argument positions based on arity
    // argc == 3: multi_get(Db, Keys, ReadOpts)
    // argc == 4: multi_get(Db, CF, Keys, ReadOpts)
    int keys_idx = (argc == 4) ? 2 : 1;
    int opts_idx = keys_idx + 1;

    // Get column family handle if provided
    rocksdb::ColumnFamilyHandle* cfh = db_ptr->m_Db->DefaultColumnFamily();
    ReferencePtr<ColumnFamilyObject> cf_ptr;
    if (argc == 4)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        cfh = cf_ptr->m_ColumnFamily;
    }

    // Parse keys list
    ERL_NIF_TERM keys_list = argv[keys_idx];
    unsigned int num_keys;
    if (!enif_get_list_length(env, keys_list, &num_keys))
        return enif_make_badarg(env);

    // Handle empty list case
    if (num_keys == 0)
        return enif_make_list(env, 0);

    // Allocate arrays for keys, values, and statuses
    std::vector<rocksdb::Slice> keys(num_keys);
    std::vector<ErlNifBinary> key_binaries(num_keys);
    std::vector<rocksdb::PinnableSlice> values(num_keys);
    std::vector<rocksdb::Status> statuses(num_keys);

    // Convert Erlang binaries to Slices
    ERL_NIF_TERM head, tail = keys_list;
    for (unsigned int i = 0; i < num_keys; i++)
    {
        if (!enif_get_list_cell(env, tail, &head, &tail))
            return enif_make_badarg(env);

        if (!enif_inspect_binary(env, head, &key_binaries[i]))
            return enif_make_badarg(env);

        keys[i] = rocksdb::Slice(reinterpret_cast<const char*>(key_binaries[i].data),
                                  key_binaries[i].size);
    }

    // Parse read options
    rocksdb::ReadOptions opts;
    fold(env, argv[opts_idx], parse_read_option, opts);

    // Call MultiGet
    db_ptr->m_Db->MultiGet(opts, cfh, num_keys, keys.data(), values.data(), statuses.data());

    // Build result list (from tail to head for efficiency)
    ERL_NIF_TERM result = enif_make_list(env, 0);
    for (int i = num_keys - 1; i >= 0; i--)
    {
        ERL_NIF_TERM item;
        if (statuses[i].ok())
        {
            ERL_NIF_TERM value_bin;
            memcpy(enif_make_new_binary(env, values[i].size(), &value_bin),
                   values[i].data(), values[i].size());
            item = enif_make_tuple2(env, ATOM_OK, value_bin);
        }
        else if (statuses[i].IsNotFound())
        {
            item = ATOM_NOT_FOUND;
        }
        else if (statuses[i].IsCorruption())
        {
            item = error_tuple(env, ATOM_CORRUPTION, statuses[i]);
        }
        else
        {
            item = error_tuple(env, ATOM_UNKNOWN_STATUS_ERROR, statuses[i]);
        }
        result = enif_make_list_cell(env, item, result);
    }

    return result;
}   // erocksdb::MultiGet

ERL_NIF_TERM
Put(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key, value;
    ERL_NIF_TERM arg_opts;

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
        arg_opts = argv[4];
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key) ||
                !enif_inspect_binary(env, argv[2], &value))
            return enif_make_badarg(env);
        cfh = db_ptr->m_Db->DefaultColumnFamily();
        arg_opts = argv[3];
    }
    rocksdb::WriteOptions *opts = new rocksdb::WriteOptions;
    fold(env, arg_opts, parse_write_option, *opts);
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
    int /*argc*/,
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
    int /*argc*/,
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
    int /*argc*/,
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
    rocksdb::Options opts(db_opts, cf_opts);

    rocksdb::Status status = rocksdb::RepairDB(name, opts);
    if (!status.ok())
    {
        return error_tuple(env, erocksdb::ATOM_ERROR_DB_REPAIR, status);
    }
    return erocksdb::ATOM_OK;
}   // erocksdb_repair

ERL_NIF_TERM
IsEmpty(
    ErlNifEnv* env,
    int /*argc*/,
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
GetLatestSequenceNumber(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
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
        column_family = cf_ptr->m_ColumnFamily;
        i = 2;
    } else {
        column_family = db_ptr->m_Db->DefaultColumnFamily();
    }

    rocksdb::DB::SizeApproximationFlags flag = rocksdb::DB::SizeApproximationFlags::NONE;
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

    uint64_t *sizes = new uint64_t[num_ranges];
    db_ptr->m_Db->GetApproximateSizes(column_family, ranges, num_ranges, sizes, flag);
    ERL_NIF_TERM result = enif_make_list(env, 0);
    for (auto k = 0U; k < num_ranges; k++)
    {
        result = enif_make_list_cell(env, enif_make_uint64(env, sizes[k]), result);
    }
    ERL_NIF_TERM result_out;
    enif_make_reverse_list(env, result, &result_out);
    delete[] sizes;
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

    if (!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if (argc == 4)
    {
        if (!enif_get_cf(env, argv[1], &cf_ptr) ||
            !binary_to_slice(env, argv[2], &start) ||
            !binary_to_slice(env, argv[3], &limit))
                return enif_make_badarg(env);

        column_family = cf_ptr->m_ColumnFamily;
    }
    else
    {
        if (!binary_to_slice(env, argv[1], &start) ||
            !binary_to_slice(env, argv[2], &limit))
            return enif_make_badarg(env);

        column_family = db_ptr->m_Db->DefaultColumnFamily();
    }

    rocksdb::Range r(start, limit);
    uint64_t size, count;
    db_ptr->m_Db->GetApproximateMemTableStats(column_family, r, &count, &size);
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
    rocksdb::CompactRangeOptions opts;
    fold(env, argv[i + 2], parse_compact_range_option, opts);

    status = db_ptr->m_Db->CompactRange(opts, column_family, &begin, &end);
    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM
Flush(
  ErlNifEnv* env,
  int /*argc*/,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    // parse flush options
    rocksdb::FlushOptions *opts = new rocksdb::FlushOptions;
    fold(env, argv[2], parse_flush_option, *opts);

    ReferencePtr<ColumnFamilyObject> cf_ptr;
    rocksdb::Status status;
    if(argv[1] == erocksdb::ATOM_DEFAULT_COLUMN_FAMILY)
    {
        status = db_ptr->m_Db->Flush(*opts);
    }
    else if (enif_get_cf(env, argv[1], &cf_ptr))
    {
        status = db_ptr->m_Db->Flush(*opts, cf_ptr->m_ColumnFamily);
    }
    else
    {
        delete opts;
        opts = NULL;
        return enif_make_badarg(env);
    }

    delete opts;
    opts = NULL;

    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return ATOM_OK;

}   // erocksdb::Flush

ERL_NIF_TERM
SyncWal(
  ErlNifEnv* env,
  int /*argc*/,
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

} // erocksdb::SyncWal

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

ERL_NIF_TERM
PutEntity(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key;
    ERL_NIF_TERM columns_list, arg_opts;

    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    rocksdb::ColumnFamilyHandle* cfh;
    int columns_idx, opts_idx;

    if (argc > 4)
    {
        // With column family: db, cf, key, columns, opts
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key) ||
                !enif_is_list(env, argv[3]))
            return enif_make_badarg(env);
        cfh = cf_ptr->m_ColumnFamily;
        columns_idx = 3;
        opts_idx = 4;
    }
    else
    {
        // Without column family: db, key, columns, opts
        if(!enif_inspect_binary(env, argv[1], &key) ||
                !enif_is_list(env, argv[2]))
            return enif_make_badarg(env);
        cfh = db_ptr->m_Db->DefaultColumnFamily();
        columns_idx = 2;
        opts_idx = 3;
    }

    columns_list = argv[columns_idx];
    arg_opts = argv[opts_idx];

    // Parse columns list: [{Name, Value}, ...]
    rocksdb::WideColumns columns;
    ERL_NIF_TERM head, tail = columns_list;
    while(enif_get_list_cell(env, tail, &head, &tail))
    {
        const ERL_NIF_TERM* tuple;
        int arity;
        if(!enif_get_tuple(env, head, &arity, &tuple) || arity != 2)
            return enif_make_badarg(env);

        ErlNifBinary col_name, col_value;
        if(!enif_inspect_binary(env, tuple[0], &col_name) ||
           !enif_inspect_binary(env, tuple[1], &col_value))
            return enif_make_badarg(env);

        columns.emplace_back(
            rocksdb::Slice(reinterpret_cast<char*>(col_name.data), col_name.size),
            rocksdb::Slice(reinterpret_cast<char*>(col_value.data), col_value.size));
    }

    rocksdb::WriteOptions opts;
    fold(env, arg_opts, parse_write_option, opts);

    rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
    rocksdb::Status status = db_ptr->m_Db->PutEntity(opts, cfh, key_slice, columns);

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    return ATOM_OK;
}   // erocksdb::PutEntity

ERL_NIF_TERM
GetEntity(
  ErlNifEnv* env,
  int argc,
  const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    int key_idx = 1;
    int opts_idx = 2;
    if(argc == 4)
    {
        key_idx = 2;
        opts_idx = 3;
    }

    rocksdb::Slice key;
    if(!binary_to_slice(env, argv[key_idx], &key))
        return enif_make_badarg(env);

    rocksdb::ReadOptions opts;
    fold(env, argv[opts_idx], parse_read_option, opts);

    rocksdb::PinnableWideColumns columns;
    rocksdb::Status status;

    if(argc == 4)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        status = db_ptr->m_Db->GetEntity(opts, cf_ptr->m_ColumnFamily, key, &columns);
    }
    else
    {
        status = db_ptr->m_Db->GetEntity(opts, db_ptr->m_Db->DefaultColumnFamily(), key, &columns);
    }

    if (!status.ok())
    {
        if (status.IsNotFound())
            return ATOM_NOT_FOUND;

        if (status.IsCorruption())
            return error_tuple(env, ATOM_CORRUPTION, status);

        return error_tuple(env, ATOM_UNKNOWN_STATUS_ERROR, status);
    }

    // Build result list: [{Name, Value}, ...]
    const rocksdb::WideColumns& cols = columns.columns();
    ERL_NIF_TERM result_list = enif_make_list(env, 0);

    // Build in reverse order, then reverse
    for (auto it = cols.rbegin(); it != cols.rend(); ++it)
    {
        ERL_NIF_TERM name_bin, value_bin;
        memcpy(enif_make_new_binary(env, it->name().size(), &name_bin),
               it->name().data(), it->name().size());
        memcpy(enif_make_new_binary(env, it->value().size(), &value_bin),
               it->value().data(), it->value().size());
        ERL_NIF_TERM tuple = enif_make_tuple2(env, name_bin, value_bin);
        result_list = enif_make_list_cell(env, tuple, result_list);
    }

    return enif_make_tuple2(env, ATOM_OK, result_list);
}   // erocksdb::GetEntity


ERL_NIF_TERM
parse_txn_db_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::TransactionDBOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == ATOM_MAX_NUM_LOCKS)
        {
            ErlNifSInt64 max_num_locks;
            if (enif_get_int64(env, option[1], &max_num_locks))
                opts.max_num_locks = max_num_locks;
        }
        else if (option[0] == ATOM_NUM_STRIPES)
        {
            ErlNifUInt64 num_stripes;
            if (enif_get_uint64(env, option[1], &num_stripes))
                opts.num_stripes = static_cast<size_t>(num_stripes);
        }
        else if (option[0] == ATOM_TRANSACTION_LOCK_TIMEOUT)
        {
            ErlNifSInt64 lock_timeout;
            if (enif_get_int64(env, option[1], &lock_timeout))
                opts.transaction_lock_timeout = lock_timeout;
        }
        else if (option[0] == ATOM_DEFAULT_LOCK_TIMEOUT)
        {
            ErlNifSInt64 lock_timeout;
            if (enif_get_int64(env, option[1], &lock_timeout))
                opts.default_lock_timeout = lock_timeout;
        }
    }
    return ATOM_OK;
}

ERL_NIF_TERM
OpenPessimisticTransactionDB(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];
    DbObject * db_ptr;
    rocksdb::TransactionDB *db;

    // argv[0] = db_name
    // argv[1] = db_options (merged with txn_db_options)
    // argv[2] = column_family_descriptors (optional for 2-arity version)

    if(!enif_get_string(env, argv[0], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[1]))
    {
        return enif_make_badarg(env);
    }

    // Read DB options
    rocksdb::DBOptions db_opts;
    fold(env, argv[1], parse_db_option, db_opts);

    // Read TransactionDB options from the same list
    rocksdb::TransactionDBOptions txn_db_opts;
    fold(env, argv[1], parse_txn_db_option, txn_db_opts);

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

    if (argc == 3)
    {
        // With column families
        if (!enif_is_list(env, argv[2]))
            return enif_make_badarg(env);

        ERL_NIF_TERM head, tail = argv[2];
        while(enif_get_list_cell(env, tail, &head, &tail))
        {
            ERL_NIF_TERM result = parse_cf_descriptor(env, head, column_families);
            if (result != ATOM_OK)
            {
                return result;
            }
        }
    }
    else
    {
        // Default column family only
        column_families.push_back(rocksdb::ColumnFamilyDescriptor(
            rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    }

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::Status status = rocksdb::TransactionDB::Open(
        db_opts, txn_db_opts, db_name, column_families, &handles, &db);

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    // Create DbObject with IsPessimistic = true
    db_ptr = DbObject::CreateDbObject(db, true);

    ERL_NIF_TERM result = enif_make_resource(env, db_ptr);

    unsigned int num_cols = column_families.size();

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
    } catch (const std::exception&) {
        // pass through
    }

    // Clear the automatic reference from enif_alloc_resource in CreateDbObject
    enif_release_resource(db_ptr);

    ERL_NIF_TERM cf_list_out;
    enif_make_reverse_list(env, cf_list, &cf_list_out);

    return enif_make_tuple3(env, ATOM_OK, result, cf_list_out);
}   // OpenPessimisticTransactionDB

ERL_NIF_TERM
GetColumnFamilyMetaData(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    rocksdb::ColumnFamilyMetaData cf_meta;

    if (argc == 2)
    {
        // With column family handle
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        db_ptr->m_Db->GetColumnFamilyMetaData(cf_ptr->m_ColumnFamily, &cf_meta);
    }
    else
    {
        // Default column family
        db_ptr->m_Db->GetColumnFamilyMetaData(&cf_meta);
    }

    // Build blob_files list
    ERL_NIF_TERM blob_files_list = enif_make_list(env, 0);
    for (auto it = cf_meta.blob_files.rbegin(); it != cf_meta.blob_files.rend(); ++it)
    {
        const rocksdb::BlobMetaData& blob = *it;

        // Create binary for file_name and file_path
        ERL_NIF_TERM file_name_bin;
        unsigned char* name_data = enif_make_new_binary(env, blob.blob_file_name.size(), &file_name_bin);
        memcpy(name_data, blob.blob_file_name.data(), blob.blob_file_name.size());

        ERL_NIF_TERM file_path_bin;
        unsigned char* path_data = enif_make_new_binary(env, blob.blob_file_path.size(), &file_path_bin);
        memcpy(path_data, blob.blob_file_path.data(), blob.blob_file_path.size());

        // Build blob metadata map
        ERL_NIF_TERM blob_keys[8];
        ERL_NIF_TERM blob_values[8];

        blob_keys[0] = ATOM_BLOB_FILE_NUMBER;
        blob_values[0] = enif_make_uint64(env, blob.blob_file_number);

        blob_keys[1] = ATOM_BLOB_FILE_NAME;
        blob_values[1] = file_name_bin;

        blob_keys[2] = ATOM_BLOB_FILE_PATH;
        blob_values[2] = file_path_bin;

        blob_keys[3] = ATOM_SIZE;
        blob_values[3] = enif_make_uint64(env, blob.blob_file_size);

        blob_keys[4] = ATOM_TOTAL_BLOB_COUNT;
        blob_values[4] = enif_make_uint64(env, blob.total_blob_count);

        blob_keys[5] = ATOM_TOTAL_BLOB_BYTES;
        blob_values[5] = enif_make_uint64(env, blob.total_blob_bytes);

        blob_keys[6] = ATOM_GARBAGE_BLOB_COUNT;
        blob_values[6] = enif_make_uint64(env, blob.garbage_blob_count);

        blob_keys[7] = ATOM_GARBAGE_BLOB_BYTES;
        blob_values[7] = enif_make_uint64(env, blob.garbage_blob_bytes);

        ERL_NIF_TERM blob_map;
        enif_make_map_from_arrays(env, blob_keys, blob_values, 8, &blob_map);

        blob_files_list = enif_make_list_cell(env, blob_map, blob_files_list);
    }

    // Reverse to get correct order
    ERL_NIF_TERM blob_files_out;
    enif_make_reverse_list(env, blob_files_list, &blob_files_out);

    // Create binary for name
    ERL_NIF_TERM name_bin;
    unsigned char* cf_name_data = enif_make_new_binary(env, cf_meta.name.size(), &name_bin);
    memcpy(cf_name_data, cf_meta.name.data(), cf_meta.name.size());

    // Build result map
    ERL_NIF_TERM keys[5];
    ERL_NIF_TERM values[5];

    keys[0] = ATOM_SIZE;
    values[0] = enif_make_uint64(env, cf_meta.size);

    keys[1] = ATOM_FILE_COUNT;
    values[1] = enif_make_uint64(env, cf_meta.file_count);

    keys[2] = ATOM_NAME;
    values[2] = name_bin;

    keys[3] = ATOM_BLOB_FILE_SIZE;
    values[3] = enif_make_uint64(env, cf_meta.blob_file_size);

    keys[4] = ATOM_BLOB_FILES;
    values[4] = blob_files_out;

    ERL_NIF_TERM result_map;
    enif_make_map_from_arrays(env, keys, values, 5, &result_map);

    return enif_make_tuple2(env, ATOM_OK, result_map);
}   // GetColumnFamilyMetaData


ERL_NIF_TERM
parse_ingest_external_file_option(ErlNifEnv* env, ERL_NIF_TERM item,
                                   rocksdb::IngestExternalFileOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2 == arity)
    {
        if (option[0] == ATOM_MOVE_FILES)
        {
            opts.move_files = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_FAILED_MOVE_FALL_BACK_TO_COPY)
        {
            opts.failed_move_fall_back_to_copy = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_SNAPSHOT_CONSISTENCY)
        {
            opts.snapshot_consistency = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_ALLOW_GLOBAL_SEQNO)
        {
            opts.allow_global_seqno = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_ALLOW_BLOCKING_FLUSH)
        {
            opts.allow_blocking_flush = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_INGEST_BEHIND)
        {
            opts.ingest_behind = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_VERIFY_CHECKSUMS_BEFORE_INGEST)
        {
            opts.verify_checksums_before_ingest = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_VERIFY_CHECKSUMS_READAHEAD_SIZE)
        {
            ErlNifUInt64 readahead_size;
            if (enif_get_uint64(env, option[1], &readahead_size))
                opts.verify_checksums_readahead_size = static_cast<size_t>(readahead_size);
        }
        else if (option[0] == ATOM_VERIFY_FILE_CHECKSUM)
        {
            opts.verify_file_checksum = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_FAIL_IF_NOT_BOTTOMMOST_LEVEL)
        {
            opts.fail_if_not_bottommost_level = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_ALLOW_DB_GENERATED_FILES)
        {
            opts.allow_db_generated_files = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_FILL_CACHE)
        {
            opts.fill_cache = (option[1] == ATOM_TRUE);
        }
    }
    return ATOM_OK;
}

ERL_NIF_TERM
IngestExternalFile(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;
    rocksdb::ColumnFamilyHandle* column_family;
    int i = 1;

    if (!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    // Check if column family is provided
    if (argc == 4)
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

    // argv[i] is the list of file paths
    ERL_NIF_TERM file_list = argv[i];
    ERL_NIF_TERM head, tail;
    std::vector<std::string> external_files;

    // Get the length of the file list
    unsigned int list_length;
    if (!enif_get_list_length(env, file_list, &list_length))
        return enif_make_badarg(env);

    if (list_length == 0)
        return enif_make_badarg(env);

    // Parse the list of file paths
    tail = file_list;
    while (enif_get_list_cell(env, tail, &head, &tail))
    {
        ErlNifBinary path_bin;
        char path_buffer[4096];

        if (enif_inspect_binary(env, head, &path_bin))
        {
            external_files.push_back(
                std::string(reinterpret_cast<const char*>(path_bin.data), path_bin.size));
        }
        else if (enif_get_string(env, head, path_buffer, sizeof(path_buffer), ERL_NIF_LATIN1) > 0)
        {
            external_files.push_back(std::string(path_buffer));
        }
        else
        {
            return enif_make_badarg(env);
        }
    }

    // Parse ingest options
    rocksdb::IngestExternalFileOptions opts;
    fold(env, argv[i + 1], parse_ingest_external_file_option, opts);

    // Call IngestExternalFile
    rocksdb::Status status = db_ptr->m_Db->IngestExternalFile(
        column_family, external_files, opts);

    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return ATOM_OK;
}   // IngestExternalFile


// TTL Functions

ERL_NIF_TERM
GetTtl(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    DbObject* db_ptr;
    ColumnFamilyObject* cf_ptr;

    db_ptr = DbObject::RetrieveDbObject(env, argv[0]);
    if (nullptr == db_ptr)
        return enif_make_badarg(env);

    if (!db_ptr->m_IsTTL)
        return error_tuple(env, ATOM_ERROR, "not a TTL database");

    cf_ptr = ColumnFamilyObject::RetrieveColumnFamilyObject(env, argv[1]);
    if (nullptr == cf_ptr)
        return enif_make_badarg(env);

    rocksdb::DBWithTTL* ttl_db = static_cast<rocksdb::DBWithTTL*>(db_ptr->m_Db);
    int32_t ttl;
    rocksdb::Status status = ttl_db->GetTtl(cf_ptr->m_ColumnFamily, &ttl);

    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, ttl));
}   // GetTtl


ERL_NIF_TERM
SetTtl(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    DbObject* db_ptr;
    int ttl;

    db_ptr = DbObject::RetrieveDbObject(env, argv[0]);
    if (nullptr == db_ptr)
        return enif_make_badarg(env);

    if (!db_ptr->m_IsTTL)
        return error_tuple(env, ATOM_ERROR, "not a TTL database");

    rocksdb::DBWithTTL* ttl_db = static_cast<rocksdb::DBWithTTL*>(db_ptr->m_Db);

    if (argc == 2) {
        // set_ttl(Db, TTL) - set default TTL
        if (!enif_get_int(env, argv[1], &ttl))
            return enif_make_badarg(env);

        ttl_db->SetTtl(ttl);
    } else {
        // set_ttl(Db, CF, TTL) - set TTL for column family
        ColumnFamilyObject* cf_ptr = ColumnFamilyObject::RetrieveColumnFamilyObject(env, argv[1]);
        if (nullptr == cf_ptr)
            return enif_make_badarg(env);

        if (!enif_get_int(env, argv[2], &ttl))
            return enif_make_badarg(env);

        ttl_db->SetTtl(cf_ptr->m_ColumnFamily, ttl);
    }

    return ATOM_OK;
}   // SetTtl


ERL_NIF_TERM
OpenWithTTLCf(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];
    bool read_only;
    DbObject* db_ptr;
    rocksdb::DBWithTTL* db;

    if (!enif_get_string(env, argv[0], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
        !enif_is_list(env, argv[1]) || !enif_is_list(env, argv[2]) || !enif_is_atom(env, argv[3]))
    {
        return enif_make_badarg(env);
    }

    read_only = (argv[3] == erocksdb::ATOM_TRUE);

    // parse db options
    rocksdb::DBOptions db_opts;
    fold(env, argv[1], parse_db_option, db_opts);

    // parse column families with TTLs
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    std::vector<int32_t> ttls;
    ERL_NIF_TERM head, tail;
    tail = argv[2];

    while (enif_get_list_cell(env, tail, &head, &tail))
    {
        const ERL_NIF_TERM* tuple;
        int arity;

        if (!enif_get_tuple(env, head, &arity, &tuple) || arity != 3)
            return enif_make_badarg(env);

        // Parse column family name
        char cf_name[4096];
        if (!enif_get_string(env, tuple[0], cf_name, sizeof(cf_name), ERL_NIF_LATIN1))
            return enif_make_badarg(env);

        // Parse column family options
        rocksdb::ColumnFamilyOptions cf_opts;
        if (!enif_is_list(env, tuple[1]))
            return enif_make_badarg(env);
        fold(env, tuple[1], parse_cf_option, cf_opts);

        // Parse TTL
        int ttl;
        if (!enif_get_int(env, tuple[2], &ttl))
            return enif_make_badarg(env);

        column_families.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, cf_opts));
        ttls.push_back(ttl);
    }

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::Status status = rocksdb::DBWithTTL::Open(
        db_opts, db_name, column_families, &handles, &db, ttls, read_only);

    if (!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    db_ptr = DbObject::CreateDbObject(db, false, true);  // IsPessimistic=false, IsTTL=true

    // Create column family handles list
    ERL_NIF_TERM cf_list = enif_make_list(env, 0);
    for (auto it = handles.rbegin(); it != handles.rend(); ++it)
    {
        ColumnFamilyObject* cf_ptr = ColumnFamilyObject::CreateColumnFamilyObject(db_ptr, *it);
        ERL_NIF_TERM cf_term = enif_make_resource(env, cf_ptr);
        enif_release_resource(cf_ptr);
        cf_list = enif_make_list_cell(env, cf_term, cf_list);
    }

    ERL_NIF_TERM db_term = enif_make_resource(env, db_ptr);
    enif_release_resource(db_ptr);

    return enif_make_tuple3(env, ATOM_OK, db_term, cf_list);
}   // OpenWithTTLCf


ERL_NIF_TERM
CreateColumnFamilyWithTtl(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    DbObject* db_ptr;
    char cf_name[4096];
    int ttl;

    db_ptr = DbObject::RetrieveDbObject(env, argv[0]);
    if (nullptr == db_ptr)
        return enif_make_badarg(env);

    if (!db_ptr->m_IsTTL)
        return error_tuple(env, ATOM_ERROR, "not a TTL database");

    if (!enif_get_string(env, argv[1], cf_name, sizeof(cf_name), ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    if (!enif_is_list(env, argv[2]))
        return enif_make_badarg(env);

    if (!enif_get_int(env, argv[3], &ttl))
        return enif_make_badarg(env);

    rocksdb::ColumnFamilyOptions cf_opts;
    fold(env, argv[2], parse_cf_option, cf_opts);

    rocksdb::DBWithTTL* ttl_db = static_cast<rocksdb::DBWithTTL*>(db_ptr->m_Db);
    rocksdb::ColumnFamilyHandle* handle;

    rocksdb::Status status = ttl_db->CreateColumnFamilyWithTtl(cf_opts, cf_name, &handle, ttl);

    if (!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    ColumnFamilyObject* cf_ptr = ColumnFamilyObject::CreateColumnFamilyObject(db_ptr, handle);
    ERL_NIF_TERM cf_term = enif_make_resource(env, cf_ptr);
    enif_release_resource(cf_ptr);

    return enif_make_tuple2(env, ATOM_OK, cf_term);
}   // CreateColumnFamilyWithTtl


}
