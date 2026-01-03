// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
// Copyright (c) 2016-2025 Benoit Chesneau
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
#include "erocksdb_db.h"
#include "refobjects.h"
#include "cache.h"
#include "statistics.h"
#include "rate_limiter.h"
#include "env.h"
#include "sst_file_manager.h"
#include "sst_file_writer.h"
#include "sst_file_reader.h"
#include "write_buffer_manager.h"
#include "pessimistic_transaction.h"

// See erl_nif(3) Data Types sections for ErlNifFunc for more deails
#define ERL_NIF_REGULAR_BOUND 0

static ErlNifFunc nif_funcs[] =
    {

        {"open", 2, erocksdb::Open, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"open_readonly", 2, erocksdb::OpenReadOnly, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"open", 3, erocksdb::OpenWithCf, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"open_readonly", 3, erocksdb::OpenWithCfReadOnly, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"open_with_ttl", 4, erocksdb::OpenWithTTL, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"open_optimistic_transaction_db", 3,
         erocksdb::OpenOptimisticTransactionDB, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"close", 1, erocksdb::Close, ERL_NIF_DIRTY_JOB_IO_BOUND},

        // db management
        {"checkpoint", 2, erocksdb::Checkpoint, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"repair", 2, erocksdb::Repair, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"is_empty", 1, erocksdb::IsEmpty, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"destroy", 2, erocksdb::Destroy, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_property", 2, erocksdb::GetProperty, ERL_NIF_REGULAR_BOUND},
        {"get_property", 3, erocksdb::GetProperty, ERL_NIF_REGULAR_BOUND},
        {"flush", 3, erocksdb::Flush, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sync_wal", 1, erocksdb::SyncWal, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"set_db_background_threads", 2, erocksdb::SetDBBackgroundThreads, ERL_NIF_REGULAR_BOUND},

        {"get_approximate_sizes", 3, erocksdb::GetApproximateSizes, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_approximate_sizes", 4, erocksdb::GetApproximateSizes, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_approximate_memtable_stats", 3, erocksdb::GetApproximateMemTableStats, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_approximate_memtable_stats", 4, erocksdb::GetApproximateMemTableStats, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"delete_range", 4, erocksdb::DeleteRange, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"delete_range", 5, erocksdb::DeleteRange, ERL_NIF_DIRTY_JOB_IO_BOUND},

        {"compact_range", 4, erocksdb::CompactRange, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"compact_range", 5, erocksdb::CompactRange, ERL_NIF_DIRTY_JOB_IO_BOUND},

        // column families
        {"list_column_families", 2, erocksdb::ListColumnFamilies, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"create_column_family", 3, erocksdb::CreateColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"drop_column_family", 1, erocksdb::DropColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"drop_column_family", 2, erocksdb::DropColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"destroy_column_family", 1, erocksdb::DestroyColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"destroy_column_family", 2, erocksdb::DestroyColumnFamily, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_column_family_metadata", 1, erocksdb::GetColumnFamilyMetaData, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_column_family_metadata", 2, erocksdb::GetColumnFamilyMetaData, ERL_NIF_DIRTY_JOB_IO_BOUND},

        // kv operations
        {"get", 3, erocksdb::Get, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get", 4, erocksdb::Get, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"multi_get", 3, erocksdb::MultiGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"multi_get", 4, erocksdb::MultiGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"put", 4, erocksdb::Put, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"put", 5, erocksdb::Put, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"merge", 4, erocksdb::Merge, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"merge", 5, erocksdb::Merge, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"delete", 3, erocksdb::Delete, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"delete", 4, erocksdb::Delete, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"single_delete", 3, erocksdb::SingleDelete, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"single_delete", 4, erocksdb::SingleDelete, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"put_entity", 4, erocksdb::PutEntity, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"put_entity", 5, erocksdb::PutEntity, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_entity", 3, erocksdb::GetEntity, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"get_entity", 4, erocksdb::GetEntity, ERL_NIF_DIRTY_JOB_IO_BOUND},

        {"snapshot", 1, erocksdb::Snapshot, ERL_NIF_REGULAR_BOUND},
        {"release_snapshot", 1, erocksdb::ReleaseSnapshot, ERL_NIF_REGULAR_BOUND},
        {"get_snapshot_sequence", 1, erocksdb::GetSnapshotSequenceNumber, ERL_NIF_REGULAR_BOUND},

        // iterator operations
        {"iterator", 2, erocksdb::Iterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"iterator", 3, erocksdb::Iterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"iterators", 3, erocksdb::Iterators, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"coalescing_iterator", 3, erocksdb::CoalescingIterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"iterator_move", 2, erocksdb::IteratorMove, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"iterator_refresh", 1, erocksdb::IteratorRefresh, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"iterator_prepare_value", 1, erocksdb::IteratorPrepareValue, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"iterator_close", 1, erocksdb::IteratorClose, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"iterator_columns", 1, erocksdb::IteratorColumns, ERL_NIF_DIRTY_JOB_IO_BOUND},

        {"get_latest_sequence_number", 1, erocksdb::GetLatestSequenceNumber, ERL_NIF_REGULAR_BOUND},

        // transactions
        {"tlog_iterator", 2, erocksdb::TransactionLogIterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"tlog_iterator_close", 1, erocksdb::TransactionLogIteratorClose, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"tlog_next_binary_update", 1, erocksdb::TransactionLogNextBinaryUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"tlog_next_update", 1, erocksdb::TransactionLogNextUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"write_binary_update", 3, erocksdb::WriteBinaryUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},

        // optimistic transaction db

        {"transaction", 2, erocksdb::NewTransaction, ERL_NIF_REGULAR_BOUND},
        {"transaction_put", 3, erocksdb::PutTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_put", 4, erocksdb::PutTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_get", 3, erocksdb::GetTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_get", 4, erocksdb::GetTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_delete", 2, erocksdb::DelTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_delete", 3, erocksdb::DelTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_iterator", 2, erocksdb::IteratorTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_iterator", 3, erocksdb::IteratorTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_commit", 1, erocksdb::CommitTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"transaction_rollback", 1, erocksdb::RollbackTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"release_transaction", 1, erocksdb::ReleaseTransaction, ERL_NIF_REGULAR_BOUND},

        // pessimistic transaction db

        {"open_pessimistic_transaction_db", 2, erocksdb::OpenPessimisticTransactionDB, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"open_pessimistic_transaction_db", 3, erocksdb::OpenPessimisticTransactionDB, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction", 2, erocksdb::NewPessimisticTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction", 3, erocksdb::NewPessimisticTransaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_put", 3, erocksdb::PessimisticTransactionPut, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_put", 4, erocksdb::PessimisticTransactionPut, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_get", 3, erocksdb::PessimisticTransactionGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_get", 4, erocksdb::PessimisticTransactionGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_get_for_update", 3, erocksdb::PessimisticTransactionGetForUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_get_for_update", 4, erocksdb::PessimisticTransactionGetForUpdate, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_delete", 2, erocksdb::PessimisticTransactionDelete, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_delete", 3, erocksdb::PessimisticTransactionDelete, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_iterator", 2, erocksdb::PessimisticTransactionIterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_iterator", 3, erocksdb::PessimisticTransactionIterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_commit", 1, erocksdb::PessimisticTransactionCommit, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_rollback", 1, erocksdb::PessimisticTransactionRollback, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"release_pessimistic_transaction", 1, erocksdb::ReleasePessimisticTransaction, ERL_NIF_REGULAR_BOUND},
        {"pessimistic_transaction_set_savepoint", 1, erocksdb::PessimisticTransactionSetSavepoint, ERL_NIF_REGULAR_BOUND},
        {"pessimistic_transaction_rollback_to_savepoint", 1, erocksdb::PessimisticTransactionRollbackToSavepoint, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"pessimistic_transaction_pop_savepoint", 1, erocksdb::PessimisticTransactionPopSavepoint, ERL_NIF_REGULAR_BOUND},
        {"pessimistic_transaction_get_id", 1, erocksdb::PessimisticTransactionGetId, ERL_NIF_REGULAR_BOUND},
        {"pessimistic_transaction_get_waiting_txns", 1, erocksdb::PessimisticTransactionGetWaitingTxns, ERL_NIF_REGULAR_BOUND},

        // Batch
        {"batch", 0, erocksdb::NewBatch, ERL_NIF_REGULAR_BOUND},
        {"release_batch", 1, erocksdb::ReleaseBatch, ERL_NIF_REGULAR_BOUND},
        {"write_batch", 3, erocksdb::WriteBatch, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"batch_put", 3, erocksdb::PutBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_put", 4, erocksdb::PutBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_merge", 3, erocksdb::MergeBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_merge", 4, erocksdb::MergeBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_delete", 2, erocksdb::DeleteBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_delete", 3, erocksdb::DeleteBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_single_delete", 2, erocksdb::SingleDeleteBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_single_delete", 3, erocksdb::SingleDeleteBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_delete_range", 3, erocksdb::DeleteRangeBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_delete_range", 4, erocksdb::DeleteRangeBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_clear", 1, erocksdb::ClearBatch, ERL_NIF_REGULAR_BOUND},
        {"batch_savepoint", 1, erocksdb::BatchSetSavePoint, ERL_NIF_REGULAR_BOUND},
        {"batch_rollback", 1, erocksdb::BatchRollbackToSavePoint, ERL_NIF_REGULAR_BOUND},
        {"batch_count", 1, erocksdb::BatchCount, ERL_NIF_REGULAR_BOUND},
        {"batch_data_size", 1, erocksdb::BatchDataSize, ERL_NIF_REGULAR_BOUND},
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
        {"gc_backup_engine", 1, erocksdb::GCBackupEngine, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"close_backup_engine", 1, erocksdb::CloseBackupEngine, ERL_NIF_DIRTY_JOB_IO_BOUND},

        // cache
        {"new_cache", 2, erocksdb::NewCache, ERL_NIF_REGULAR_BOUND},
        {"cache_info", 1, erocksdb::CacheInfo, ERL_NIF_REGULAR_BOUND},
        {"cache_info", 2, erocksdb::CacheInfo, ERL_NIF_REGULAR_BOUND},
        {"release_cache", 1, erocksdb::ReleaseCache, ERL_NIF_REGULAR_BOUND},
        {"set_strict_capacity_limit", 2, erocksdb::SetStrictCapacityLimit, ERL_NIF_REGULAR_BOUND},
        {"set_capacity", 2, erocksdb::SetCapacity, ERL_NIF_DIRTY_JOB_CPU_BOUND},

        // rate limiter
        {"new_rate_limiter", 2, erocksdb::NewRateLimiter, ERL_NIF_REGULAR_BOUND},
        {"release_rate_limiter", 1, erocksdb::ReleaseRateLimiter, ERL_NIF_REGULAR_BOUND},

        // env
        {"new_env", 1, erocksdb::NewEnv, ERL_NIF_REGULAR_BOUND},
        {"set_env_background_threads", 2, erocksdb::SetEnvBackgroundThreads, ERL_NIF_REGULAR_BOUND},
        {"set_env_background_threads", 3, erocksdb::SetEnvBackgroundThreads, ERL_NIF_REGULAR_BOUND},
        {"destroy_env", 1, erocksdb::DestroyEnv, ERL_NIF_REGULAR_BOUND},

        // SST File Manager
        {"new_sst_file_manager", 2, erocksdb::NewSstFileManager, ERL_NIF_REGULAR_BOUND},
        {"release_sst_file_manager", 1, erocksdb::ReleaseSstFileManager, ERL_NIF_REGULAR_BOUND},
        {"sst_file_manager_flag", 3, erocksdb::SstFileManagerFlag, ERL_NIF_REGULAR_BOUND},
        {"sst_file_manager_info", 1, erocksdb::SstFileManagerInfo, ERL_NIF_REGULAR_BOUND},
        {"sst_file_manager_info", 2, erocksdb::SstFileManagerInfo, ERL_NIF_REGULAR_BOUND},
        {"sst_file_manager_tracked_files", 1, erocksdb::SstFileManagerTrackedFiles, ERL_NIF_DIRTY_JOB_IO_BOUND},

        // SST File Writer
        {"sst_file_writer_open", 2, erocksdb::SstFileWriterOpen, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_put", 3, erocksdb::SstFileWriterPut, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_put_entity", 3, erocksdb::SstFileWriterPutEntity, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_merge", 3, erocksdb::SstFileWriterMerge, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_delete", 2, erocksdb::SstFileWriterDelete, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_delete_range", 3, erocksdb::SstFileWriterDeleteRange, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_finish", 1, erocksdb::SstFileWriterFinish, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_finish", 2, erocksdb::SstFileWriterFinish, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_writer_file_size", 1, erocksdb::SstFileWriterFileSize, ERL_NIF_REGULAR_BOUND},
        {"release_sst_file_writer", 1, erocksdb::ReleaseSstFileWriter, ERL_NIF_REGULAR_BOUND},

        // Ingest External File
        {"ingest_external_file", 3, erocksdb::IngestExternalFile, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"ingest_external_file", 4, erocksdb::IngestExternalFile, ERL_NIF_DIRTY_JOB_IO_BOUND},

        // SST File Reader
        {"sst_file_reader_open", 2, erocksdb::SstFileReaderOpen, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_reader_iterator", 2, erocksdb::SstFileReaderIterator, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_reader_get_table_properties", 1, erocksdb::SstFileReaderGetTableProperties, ERL_NIF_REGULAR_BOUND},
        {"sst_file_reader_verify_checksum", 1, erocksdb::SstFileReaderVerifyChecksum, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_reader_verify_checksum", 2, erocksdb::SstFileReaderVerifyChecksum, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_reader_iterator_move", 2, erocksdb::SstFileReaderIteratorMove, ERL_NIF_DIRTY_JOB_IO_BOUND},
        {"sst_file_reader_iterator_close", 1, erocksdb::SstFileReaderIteratorClose, ERL_NIF_REGULAR_BOUND},
        {"release_sst_file_reader", 1, erocksdb::ReleaseSstFileReader, ERL_NIF_REGULAR_BOUND},

        // Write Buffer Manager
        {"new_write_buffer_manager", 1, erocksdb::NewWriteBufferManager, ERL_NIF_REGULAR_BOUND},
        {"new_write_buffer_manager", 2, erocksdb::NewWriteBufferManager, ERL_NIF_REGULAR_BOUND},
        {"release_write_buffer_manager", 1, erocksdb::ReleaseWriteBufferManager, ERL_NIF_REGULAR_BOUND},
        {"write_buffer_manager_info", 1, erocksdb::WriteBufferManagerInfo, ERL_NIF_REGULAR_BOUND},
        {"write_buffer_manager_info", 2, erocksdb::WriteBufferManagerInfo, ERL_NIF_REGULAR_BOUND},

        // Statistics
        {"new_statistics", 0, erocksdb::NewStatistics, ERL_NIF_REGULAR_BOUND},
        {"set_stats_level", 2, erocksdb::SetStatsLevel, ERL_NIF_REGULAR_BOUND},
        {"statistics_info", 1, erocksdb::StatisticsInfo, ERL_NIF_REGULAR_BOUND},
        {"statistics_ticker", 2, erocksdb::StatisticsTicker, ERL_NIF_REGULAR_BOUND},
        {"statistics_histogram", 2, erocksdb::StatisticsHistogram, ERL_NIF_REGULAR_BOUND},
        {"release_statistics", 1, erocksdb::ReleaseStatistics, ERL_NIF_REGULAR_BOUND},
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
ERL_NIF_TERM ATOM_CORRUPTION;
ERL_NIF_TERM ATOM_INC;
ERL_NIF_TERM ATOM_DEC;
ERL_NIF_TERM ATOM_UNKNOWN_STATUS_ERROR;
ERL_NIF_TERM ATOM_UNDEFINED;

// related to envs
ERL_NIF_TERM ATOM_DEFAULT;
ERL_NIF_TERM ATOM_MEMENV;

// related to cache
ERL_NIF_TERM ATOM_LRU;
ERL_NIF_TERM ATOM_CLOCK;
ERL_NIF_TERM ATOM_USAGE;
ERL_NIF_TERM ATOM_PINNED_USAGE;
ERL_NIF_TERM ATOM_CAPACITY;
ERL_NIF_TERM ATOM_STRICT_CAPACITY;
ERL_NIF_TERM ATOM_FLUSH_ONLY;
ERL_NIF_TERM ATOM_DISABLE;

// generic
ERL_NIF_TERM ATOM_DEFAULT_COLUMN_FAMILY;

// Related to CFOptions
ERL_NIF_TERM ATOM_BLOCK_CACHE_SIZE_MB_FOR_POINT_LOOKUP;
ERL_NIF_TERM ATOM_MEMTABLE_MEMORY_BUDGET;
ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
ERL_NIF_TERM ATOM_MAX_WRITE_BUFFER_NUMBER;
ERL_NIF_TERM ATOM_MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
ERL_NIF_TERM ATOM_COMPRESSION;

// CFOptions blob
ERL_NIF_TERM ATOM_ENABLE_BLOB_FILES;
ERL_NIF_TERM ATOM_MIN_BLOB_SIZE;
ERL_NIF_TERM ATOM_BLOB_FILE_SIZE;
ERL_NIF_TERM ATOM_BLOB_COMPRESSION_TYPE;
ERL_NIF_TERM ATOM_ENABLE_BLOB_GC;
ERL_NIF_TERM ATOM_BLOB_GC_AGE_CUTOFF;
ERL_NIF_TERM ATOM_BLOB_GC_FORCE_THRESHOLD;
ERL_NIF_TERM ATOM_BLOB_COMPACTION_READAHEAD_SIZE;
ERL_NIF_TERM ATOM_BLOB_FILE_STARTING_LEVEL;
ERL_NIF_TERM ATOM_BLOB_CACHE;
ERL_NIF_TERM ATOM_PREPOPULATE_BLOB_CACHE;

// Related to CFOpCompressionOptions
ERL_NIF_TERM ATOM_BOTTOMMOST_COMPRESSION;
ERL_NIF_TERM ATOM_BOTTOMMOST_COMPRESSION_OPTS;
ERL_NIF_TERM ATOM_COMPRESSION_OPTS;
ERL_NIF_TERM ATOM_WINDOW_BITS;
ERL_NIF_TERM ATOM_LEVEL;
ERL_NIF_TERM ATOM_STRATEGY;
ERL_NIF_TERM ATOM_MAX_DICT_BYTES;
ERL_NIF_TERM ATOM_ZSTD_MAX_TRAIN_BYTES;

ERL_NIF_TERM ATOM_NUM_LEVELS;
ERL_NIF_TERM ATOM_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
ERL_NIF_TERM ATOM_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
ERL_NIF_TERM ATOM_LEVEL0_STOP_WRITES_TRIGGER;
ERL_NIF_TERM ATOM_TARGET_FILE_SIZE_BASE;
ERL_NIF_TERM ATOM_TARGET_FILE_SIZE_MULTIPLIER;
ERL_NIF_TERM ATOM_MAX_BYTES_FOR_LEVEL_BASE;
ERL_NIF_TERM ATOM_MAX_BYTES_FOR_LEVEL_MULTIPLIER;
ERL_NIF_TERM ATOM_MAX_COMPACTION_BYTES;
ERL_NIF_TERM ATOM_ARENA_BLOCK_SIZE;
ERL_NIF_TERM ATOM_DISABLE_AUTO_COMPACTIONS;
ERL_NIF_TERM ATOM_COMPACTION_STYLE;
ERL_NIF_TERM ATOM_COMPACTION_PRI;
ERL_NIF_TERM ATOM_FILTER_DELETES;
ERL_NIF_TERM ATOM_MAX_SEQUENTIAL_SKIP_IN_ITERATIONS;
ERL_NIF_TERM ATOM_INPLACE_UPDATE_SUPPORT;
ERL_NIF_TERM ATOM_INPLACE_UPDATE_NUM_LOCKS;
ERL_NIF_TERM ATOM_TABLE_FACTORY_BLOCK_CACHE_SIZE;
ERL_NIF_TERM ATOM_IN_MEMORY_MODE;
ERL_NIF_TERM ATOM_IN_MEMORY;
ERL_NIF_TERM ATOM_BLOCK_BASED_TABLE_OPTIONS;
ERL_NIF_TERM ATOM_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES;
ERL_NIF_TERM ATOM_OPTIMIZE_FILTERS_FOR_HITS;
ERL_NIF_TERM ATOM_PREFIX_EXTRACTOR;

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
ERL_NIF_TERM ATOM_STATS_DUMP_PERIOD_SEC;
ERL_NIF_TERM ATOM_ADVISE_RANDOM_ON_OPEN;
ERL_NIF_TERM ATOM_COMPACTION_READAHEAD_SIZE;
ERL_NIF_TERM ATOM_USE_ADAPTIVE_MUTEX;
ERL_NIF_TERM ATOM_BYTES_PER_SYNC;
ERL_NIF_TERM ATOM_SKIP_STATS_UPDATE_ON_DB_OPEN;
ERL_NIF_TERM ATOM_WAL_RECOVERY_MODE;
ERL_NIF_TERM ATOM_ALLOW_CONCURRENT_MEMTABLE_WRITE;
ERL_NIF_TERM ATOM_ENABLE_WRITE_THREAD_ADAPTATIVE_YIELD;
ERL_NIF_TERM ATOM_DB_WRITE_BUFFER_SIZE;
ERL_NIF_TERM ATOM_RATE_LIMITER;
ERL_NIF_TERM ATOM_SST_FILE_MANAGER;
ERL_NIF_TERM ATOM_WRITE_BUFFER_MANAGER;
ERL_NIF_TERM ATOM_MAX_SUBCOMPACTIONS;
ERL_NIF_TERM ATOM_MANUAL_WAL_FLUSH;
ERL_NIF_TERM ATOM_ATOMIC_FLUSH;
ERL_NIF_TERM ATOM_USE_DIRECT_READS;
ERL_NIF_TERM ATOM_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION;
ERL_NIF_TERM ATOM_ENABLE_PIPELINED_WRITE;
ERL_NIF_TERM ATOM_UNORDERED_WRITE;
ERL_NIF_TERM ATOM_TWO_WRITE_QUEUES;

// Related to BlockBasedTable Options
ERL_NIF_TERM ATOM_NO_BLOCK_CACHE;
ERL_NIF_TERM ATOM_BLOCK_CACHE;
ERL_NIF_TERM ATOM_BLOCK_SIZE;
ERL_NIF_TERM ATOM_BLOCK_CACHE_SIZE;
ERL_NIF_TERM ATOM_BLOOM_FILTER_POLICY;
ERL_NIF_TERM ATOM_FORMAT_VERSION;
ERL_NIF_TERM ATOM_CACHE_INDEX_AND_FILTER_BLOCKS;

// Related to ReadTier
ERL_NIF_TERM ATOM_READ_TIER;
ERL_NIF_TERM ATOM_READ_ALL_TIER;
ERL_NIF_TERM ATOM_BLOCK_CACHE_TIER;
ERL_NIF_TERM ATOM_PERSISTED_TIER;
ERL_NIF_TERM ATOM_MEMTABLE_TIER;

// Related to Read Options
ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
ERL_NIF_TERM ATOM_FILL_CACHE;
ERL_NIF_TERM ATOM_ITERATE_UPPER_BOUND;
ERL_NIF_TERM ATOM_ITERATE_LOWER_BOUND;
ERL_NIF_TERM ATOM_TAILING;
ERL_NIF_TERM ATOM_TOTAL_ORDER_SEEK;
ERL_NIF_TERM ATOM_PREFIX_SAME_AS_START;
ERL_NIF_TERM ATOM_SNAPSHOT;
ERL_NIF_TERM ATOM_BAD_SNAPSHOT;
ERL_NIF_TERM ATOM_AUTO_REFRESH_ITERATOR_WITH_SNAPSHOT;
ERL_NIF_TERM ATOM_AUTO_READAHEAD_SIZE;
ERL_NIF_TERM ATOM_ALLOW_UNPREPARED_VALUE;

// Related to Write Options
ERL_NIF_TERM ATOM_SYNC;
ERL_NIF_TERM ATOM_DISABLE_WAL;
ERL_NIF_TERM ATOM_IGNORE_MISSING_COLUMN_FAMILIES;
ERL_NIF_TERM ATOM_NO_SLOWDOWN;
ERL_NIF_TERM ATOM_LOW_PRI;

// Related to Write Actions
ERL_NIF_TERM ATOM_CLEAR;
ERL_NIF_TERM ATOM_PUT;
ERL_NIF_TERM ATOM_MERGE;
ERL_NIF_TERM ATOM_DELETE;
ERL_NIF_TERM ATOM_SINGLE_DELETE;

// Related to CompactRangeOptions
ERL_NIF_TERM ATOM_EXCLUSIVE_MANUAL_COMPACTION;
ERL_NIF_TERM ATOM_CHANGE_LEVEL;
ERL_NIF_TERM ATOM_TARGET_LEVEL;
ERL_NIF_TERM ATOM_ALLOW_WRITE_STALL;

// Related to CompactionOptionsFIFO
ERL_NIF_TERM ATOM_COMPACTION_OPTIONS_FIFO;
ERL_NIF_TERM ATOM_MAX_TABLE_FILE_SIZE;
ERL_NIF_TERM ATOM_ALLOW_COMPACTION;

ERL_NIF_TERM ATOM_TTL;


// Related to FlushOptions
ERL_NIF_TERM ATOM_WAIT;

// Related to Iterator Actions
ERL_NIF_TERM ATOM_FIRST;
ERL_NIF_TERM ATOM_LAST;
ERL_NIF_TERM ATOM_NEXT;
ERL_NIF_TERM ATOM_PREV;
ERL_NIF_TERM ATOM_SEEK_FOR_PREV;
ERL_NIF_TERM ATOM_SEEK;

// Related to Iterator Value to be retrieved
ERL_NIF_TERM ATOM_KEYS_ONLY;

// Related to Compression Type
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_SNAPPY;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_ZLIB;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_BZIP2;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_LZ4;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_LZ4H;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_ZSTD;
ERL_NIF_TERM ATOM_COMPRESSION_TYPE_NONE;

// Related to Compaction Style
ERL_NIF_TERM ATOM_COMPACTION_STYLE_LEVEL;
ERL_NIF_TERM ATOM_COMPACTION_STYLE_UNIVERSAL;
ERL_NIF_TERM ATOM_COMPACTION_STYLE_FIFO;
ERL_NIF_TERM ATOM_COMPACTION_STYLE_NONE;

// Related to Compaction Priority
ERL_NIF_TERM ATOM_COMPACTION_PRI_COMPENSATED_SIZE;
ERL_NIF_TERM ATOM_COMPACTION_PRI_OLDEST_LARGEST_SEQ_FIRST;
ERL_NIF_TERM ATOM_COMPACTION_PRI_OLDEST_SMALLEST_SEQ_FIRST;

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
ERL_NIF_TERM ATOM_ERROR_INCOMPLETE;

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


ERL_NIF_TERM ATOM_MERGE_OPERATOR;
ERL_NIF_TERM ATOM_ERLANG_MERGE_OPERATOR;
ERL_NIF_TERM ATOM_BITSET_MERGE_OPERATOR;
ERL_NIF_TERM ATOM_COUNTER_MERGE_OPERATOR;

ERL_NIF_TERM ATOM_MERGE_INT_ADD;
ERL_NIF_TERM ATOM_MERGE_LIST_APPEND;
ERL_NIF_TERM ATOM_MERGE_LIST_SUBSTRACT;
ERL_NIF_TERM ATOM_MERGE_LIST_SET;
ERL_NIF_TERM ATOM_MERGE_LIST_DELETE;
ERL_NIF_TERM ATOM_MERGE_LIST_INSERT;
ERL_NIF_TERM ATOM_MERGE_BINARY_APPEND;
ERL_NIF_TERM ATOM_MERGE_BINARY_REPLACE;
ERL_NIF_TERM ATOM_MERGE_BINARY_INSERT;
ERL_NIF_TERM ATOM_MERGE_BINARY_ERASE;

ERL_NIF_TERM ATOM_FIXED_PREFIX_TRANSFORM;
ERL_NIF_TERM ATOM_CAPPED_PREFIX_TRANSFORM;

ERL_NIF_TERM ATOM_COMPARATOR;
ERL_NIF_TERM ATOM_BYTEWISE_COMPARATOR;
ERL_NIF_TERM ATOM_REVERSE_BYTEWISE_COMPARATOR;

// range

ERL_NIF_TERM ATOM_NONE;
ERL_NIF_TERM ATOM_INCLUDE_MEMTABLES;
ERL_NIF_TERM ATOM_INCLUDE_FILES;
ERL_NIF_TERM ATOM_INCLUDE_BOTH;

// write buffer manager
ERL_NIF_TERM ATOM_ENABLED;
ERL_NIF_TERM ATOM_BUFFER_SIZE;
ERL_NIF_TERM ATOM_MUTABLE_MEMTABLE_MEMORY_USAGE;
ERL_NIF_TERM ATOM_MEMORY_USAGE;

// sst file manager

ERL_NIF_TERM ATOM_DELETE_RATE_BYTES_PER_SEC;
ERL_NIF_TERM ATOM_MAX_TRASH_DB_RATIO;
ERL_NIF_TERM ATOM_BYTES_MAX_DELETE_CHUNK;
ERL_NIF_TERM ATOM_MAX_ALLOWED_SPACE_USAGE;
ERL_NIF_TERM ATOM_COMPACTION_BUFFER_SIZE;
ERL_NIF_TERM ATOM_IS_MAX_ALLOWED_SPACE_REACHED;
ERL_NIF_TERM ATOM_MAX_ALLOWED_SPACE_REACHED_INCLUDING_COMPACTIONS;
ERL_NIF_TERM ATOM_TOTAL_SIZE;
ERL_NIF_TERM ATOM_TOTAL_TRASH_SIZE;

// sst file writer
ERL_NIF_TERM ATOM_WITH_FILE_INFO;
ERL_NIF_TERM ATOM_FILE_PATH;
ERL_NIF_TERM ATOM_SMALLEST_KEY;
ERL_NIF_TERM ATOM_LARGEST_KEY;
ERL_NIF_TERM ATOM_SMALLEST_RANGE_DEL_KEY;
ERL_NIF_TERM ATOM_LARGEST_RANGE_DEL_KEY;
ERL_NIF_TERM ATOM_FILE_SIZE;
ERL_NIF_TERM ATOM_NUM_ENTRIES;
ERL_NIF_TERM ATOM_NUM_RANGE_DEL_ENTRIES;
ERL_NIF_TERM ATOM_SEQUENCE_NUMBER;

// ingest external file
ERL_NIF_TERM ATOM_MOVE_FILES;
ERL_NIF_TERM ATOM_FAILED_MOVE_FALL_BACK_TO_COPY;
ERL_NIF_TERM ATOM_SNAPSHOT_CONSISTENCY;
ERL_NIF_TERM ATOM_ALLOW_GLOBAL_SEQNO;
ERL_NIF_TERM ATOM_ALLOW_BLOCKING_FLUSH;
ERL_NIF_TERM ATOM_INGEST_BEHIND;
ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS_BEFORE_INGEST;
ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS_READAHEAD_SIZE;
ERL_NIF_TERM ATOM_VERIFY_FILE_CHECKSUM;
ERL_NIF_TERM ATOM_FAIL_IF_NOT_BOTTOMMOST_LEVEL;
ERL_NIF_TERM ATOM_ALLOW_DB_GENERATED_FILES;

// sst file reader / table properties
ERL_NIF_TERM ATOM_DATA_SIZE;
ERL_NIF_TERM ATOM_INDEX_SIZE;
ERL_NIF_TERM ATOM_INDEX_PARTITIONS;
ERL_NIF_TERM ATOM_TOP_LEVEL_INDEX_SIZE;
ERL_NIF_TERM ATOM_FILTER_SIZE;
ERL_NIF_TERM ATOM_RAW_KEY_SIZE;
ERL_NIF_TERM ATOM_RAW_VALUE_SIZE;
ERL_NIF_TERM ATOM_NUM_DATA_BLOCKS;
ERL_NIF_TERM ATOM_NUM_DELETIONS;
ERL_NIF_TERM ATOM_NUM_MERGE_OPERANDS;
ERL_NIF_TERM ATOM_NUM_RANGE_DELETIONS;
ERL_NIF_TERM ATOM_FIXED_KEY_LEN;
ERL_NIF_TERM ATOM_COLUMN_FAMILY_ID;
ERL_NIF_TERM ATOM_COLUMN_FAMILY_NAME;
ERL_NIF_TERM ATOM_FILTER_POLICY_NAME;
ERL_NIF_TERM ATOM_COMPARATOR_NAME;
ERL_NIF_TERM ATOM_MERGE_OPERATOR_NAME;
ERL_NIF_TERM ATOM_PREFIX_EXTRACTOR_NAME;
ERL_NIF_TERM ATOM_PROPERTY_COLLECTORS_NAMES;
ERL_NIF_TERM ATOM_COMPRESSION_NAME;
ERL_NIF_TERM ATOM_COMPRESSION_OPTIONS;
ERL_NIF_TERM ATOM_CREATION_TIME;
ERL_NIF_TERM ATOM_OLDEST_KEY_TIME;
ERL_NIF_TERM ATOM_FILE_CREATION_TIME;
ERL_NIF_TERM ATOM_SLOW_COMPRESSION_ESTIMATED_DATA_SIZE;
ERL_NIF_TERM ATOM_FAST_COMPRESSION_ESTIMATED_DATA_SIZE;
ERL_NIF_TERM ATOM_EXTERNAL_SST_FILE_GLOBAL_SEQNO_OFFSET;

// statistics
ERL_NIF_TERM ATOM_STATISTICS;
ERL_NIF_TERM ATOM_STATS_DISABLE_ALL;
ERL_NIF_TERM ATOM_STATS_EXCEPT_TICKERS;
ERL_NIF_TERM ATOM_STATS_EXCEPT_HISTOGRAM_OR_TIMERS;
ERL_NIF_TERM ATOM_STATS_EXCEPT_TIMERS;
ERL_NIF_TERM ATOM_STATS_EXCEPT_DETAILED_TIMERS;
ERL_NIF_TERM ATOM_STATS_EXCEPT_TIME_FOR_MUTEX;
ERL_NIF_TERM ATOM_STATS_ALL;
ERL_NIF_TERM ATOM_STATS_LEVEL;

// BlobDB Statistics Tickers
ERL_NIF_TERM ATOM_BLOB_DB_NUM_PUT;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_WRITE;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_GET;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_MULTIGET;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_SEEK;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_NEXT;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_PREV;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_KEYS_WRITTEN;
ERL_NIF_TERM ATOM_BLOB_DB_NUM_KEYS_READ;
ERL_NIF_TERM ATOM_BLOB_DB_BYTES_WRITTEN;
ERL_NIF_TERM ATOM_BLOB_DB_BYTES_READ;
ERL_NIF_TERM ATOM_BLOB_DB_WRITE_INLINED;
ERL_NIF_TERM ATOM_BLOB_DB_WRITE_INLINED_TTL;
ERL_NIF_TERM ATOM_BLOB_DB_WRITE_BLOB;
ERL_NIF_TERM ATOM_BLOB_DB_WRITE_BLOB_TTL;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_FILE_BYTES_WRITTEN;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_FILE_BYTES_READ;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_FILE_SYNCED;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_INDEX_EXPIRED_COUNT;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_INDEX_EXPIRED_SIZE;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_INDEX_EVICTED_COUNT;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_INDEX_EVICTED_SIZE;
ERL_NIF_TERM ATOM_BLOB_DB_GC_NUM_FILES;
ERL_NIF_TERM ATOM_BLOB_DB_GC_NUM_NEW_FILES;
ERL_NIF_TERM ATOM_BLOB_DB_GC_FAILURES;
ERL_NIF_TERM ATOM_BLOB_DB_GC_NUM_KEYS_RELOCATED;
ERL_NIF_TERM ATOM_BLOB_DB_GC_BYTES_RELOCATED;
ERL_NIF_TERM ATOM_BLOB_DB_FIFO_NUM_FILES_EVICTED;
ERL_NIF_TERM ATOM_BLOB_DB_FIFO_NUM_KEYS_EVICTED;
ERL_NIF_TERM ATOM_BLOB_DB_FIFO_BYTES_EVICTED;
ERL_NIF_TERM ATOM_BLOB_DB_CACHE_MISS;
ERL_NIF_TERM ATOM_BLOB_DB_CACHE_HIT;
ERL_NIF_TERM ATOM_BLOB_DB_CACHE_ADD;
ERL_NIF_TERM ATOM_BLOB_DB_CACHE_ADD_FAILURES;
ERL_NIF_TERM ATOM_BLOB_DB_CACHE_BYTES_READ;
ERL_NIF_TERM ATOM_BLOB_DB_CACHE_BYTES_WRITE;

// BlobDB Statistics Histograms
ERL_NIF_TERM ATOM_BLOB_DB_KEY_SIZE;
ERL_NIF_TERM ATOM_BLOB_DB_VALUE_SIZE;
ERL_NIF_TERM ATOM_BLOB_DB_WRITE_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_GET_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_MULTIGET_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_SEEK_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_NEXT_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_PREV_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_FILE_WRITE_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_FILE_READ_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_BLOB_FILE_SYNC_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_COMPRESSION_MICROS;
ERL_NIF_TERM ATOM_BLOB_DB_DECOMPRESSION_MICROS;

// Core Operation Histograms
ERL_NIF_TERM ATOM_DB_GET;
ERL_NIF_TERM ATOM_DB_WRITE;
ERL_NIF_TERM ATOM_DB_MULTIGET;
ERL_NIF_TERM ATOM_DB_SEEK;
ERL_NIF_TERM ATOM_COMPACTION_TIME;
ERL_NIF_TERM ATOM_FLUSH_TIME;

// I/O and Sync Histograms
ERL_NIF_TERM ATOM_SST_READ_MICROS;
ERL_NIF_TERM ATOM_SST_WRITE_MICROS;
ERL_NIF_TERM ATOM_TABLE_SYNC_MICROS;
ERL_NIF_TERM ATOM_WAL_FILE_SYNC_MICROS;
ERL_NIF_TERM ATOM_BYTES_PER_READ;
ERL_NIF_TERM ATOM_BYTES_PER_WRITE;

// Transaction Histogram
ERL_NIF_TERM ATOM_NUM_OP_PER_TRANSACTION;

// Compaction Statistics Tickers
ERL_NIF_TERM ATOM_COMPACT_READ_BYTES;
ERL_NIF_TERM ATOM_COMPACT_WRITE_BYTES;
ERL_NIF_TERM ATOM_FLUSH_WRITE_BYTES;
ERL_NIF_TERM ATOM_COMPACTION_KEY_DROP_NEWER_ENTRY;
ERL_NIF_TERM ATOM_COMPACTION_KEY_DROP_OBSOLETE;
ERL_NIF_TERM ATOM_COMPACTION_KEY_DROP_RANGE_DEL;
ERL_NIF_TERM ATOM_COMPACTION_KEY_DROP_USER;
ERL_NIF_TERM ATOM_COMPACTION_CANCELLED;
ERL_NIF_TERM ATOM_NUMBER_SUPERVERSION_ACQUIRES;
ERL_NIF_TERM ATOM_NUMBER_SUPERVERSION_RELEASES;

// Read/Write Operation Tickers
ERL_NIF_TERM ATOM_NUMBER_KEYS_WRITTEN;
ERL_NIF_TERM ATOM_NUMBER_KEYS_READ;
ERL_NIF_TERM ATOM_NUMBER_KEYS_UPDATED;
ERL_NIF_TERM ATOM_BYTES_WRITTEN;
ERL_NIF_TERM ATOM_BYTES_READ;
ERL_NIF_TERM ATOM_ITER_BYTES_READ;
ERL_NIF_TERM ATOM_NUMBER_DB_SEEK;
ERL_NIF_TERM ATOM_NUMBER_DB_NEXT;
ERL_NIF_TERM ATOM_NUMBER_DB_PREV;
ERL_NIF_TERM ATOM_NUMBER_DB_SEEK_FOUND;
ERL_NIF_TERM ATOM_NUMBER_DB_NEXT_FOUND;
ERL_NIF_TERM ATOM_NUMBER_DB_PREV_FOUND;

// Block Cache Statistics Tickers
ERL_NIF_TERM ATOM_BLOCK_CACHE_MISS;
ERL_NIF_TERM ATOM_BLOCK_CACHE_HIT;
ERL_NIF_TERM ATOM_BLOCK_CACHE_ADD;
ERL_NIF_TERM ATOM_BLOCK_CACHE_ADD_FAILURES;
ERL_NIF_TERM ATOM_BLOCK_CACHE_INDEX_MISS;
ERL_NIF_TERM ATOM_BLOCK_CACHE_INDEX_HIT;
ERL_NIF_TERM ATOM_BLOCK_CACHE_FILTER_MISS;
ERL_NIF_TERM ATOM_BLOCK_CACHE_FILTER_HIT;
ERL_NIF_TERM ATOM_BLOCK_CACHE_DATA_MISS;
ERL_NIF_TERM ATOM_BLOCK_CACHE_DATA_HIT;
ERL_NIF_TERM ATOM_BLOCK_CACHE_BYTES_READ;
ERL_NIF_TERM ATOM_BLOCK_CACHE_BYTES_WRITE;

// Memtable and Stall Statistics Tickers
ERL_NIF_TERM ATOM_MEMTABLE_HIT;
ERL_NIF_TERM ATOM_MEMTABLE_MISS;
ERL_NIF_TERM ATOM_STALL_MICROS;
ERL_NIF_TERM ATOM_WRITE_DONE_BY_SELF;
ERL_NIF_TERM ATOM_WRITE_DONE_BY_OTHER;
ERL_NIF_TERM ATOM_WAL_FILE_SYNCED;

// Transaction Statistics Tickers
ERL_NIF_TERM ATOM_TXN_PREPARE_MUTEX_OVERHEAD;
ERL_NIF_TERM ATOM_TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD;
ERL_NIF_TERM ATOM_TXN_DUPLICATE_KEY_OVERHEAD;
ERL_NIF_TERM ATOM_TXN_SNAPSHOT_MUTEX_OVERHEAD;
ERL_NIF_TERM ATOM_TXN_GET_TRY_AGAIN;

// Histogram result keys
ERL_NIF_TERM ATOM_MEDIAN;
ERL_NIF_TERM ATOM_PERCENTILE95;
ERL_NIF_TERM ATOM_PERCENTILE99;
ERL_NIF_TERM ATOM_AVERAGE;
ERL_NIF_TERM ATOM_STANDARD_DEVIATION;
ERL_NIF_TERM ATOM_MAX;
ERL_NIF_TERM ATOM_COUNT;
ERL_NIF_TERM ATOM_SUM;

// Pessimistic Transaction DB Options
ERL_NIF_TERM ATOM_MAX_NUM_LOCKS;
ERL_NIF_TERM ATOM_NUM_STRIPES;
ERL_NIF_TERM ATOM_TRANSACTION_LOCK_TIMEOUT;
ERL_NIF_TERM ATOM_DEFAULT_LOCK_TIMEOUT;

// Pessimistic Transaction Options
ERL_NIF_TERM ATOM_SET_SNAPSHOT;
ERL_NIF_TERM ATOM_DEADLOCK_DETECT;
ERL_NIF_TERM ATOM_LOCK_TIMEOUT;

// Pessimistic Transaction Error Codes
ERL_NIF_TERM ATOM_BUSY;
ERL_NIF_TERM ATOM_TIMED_OUT;
ERL_NIF_TERM ATOM_EXPIRED;
ERL_NIF_TERM ATOM_TRY_AGAIN;

// Column Family/Blob Metadata
ERL_NIF_TERM ATOM_SIZE;
ERL_NIF_TERM ATOM_FILE_COUNT;
ERL_NIF_TERM ATOM_NAME;
// ATOM_BLOB_FILE_SIZE already defined above in CFOptions blob section
ERL_NIF_TERM ATOM_BLOB_FILES;
ERL_NIF_TERM ATOM_BLOB_FILE_NUMBER;
ERL_NIF_TERM ATOM_BLOB_FILE_NAME;
ERL_NIF_TERM ATOM_BLOB_FILE_PATH;
ERL_NIF_TERM ATOM_TOTAL_BLOB_COUNT;
ERL_NIF_TERM ATOM_TOTAL_BLOB_BYTES;
ERL_NIF_TERM ATOM_GARBAGE_BLOB_COUNT;
ERL_NIF_TERM ATOM_GARBAGE_BLOB_BYTES;

}   // namespace erocksdb


using std::nothrow;

static void on_unload(ErlNifEnv * /*env*/, void * /*priv_data*/)
{
}

static int on_upgrade(ErlNifEnv* /*env*/, void** priv_data, void** old_priv_data, ERL_NIF_TERM /*load_info*/)
{
    /* Convert the private data to the new version. */
    *priv_data = *old_priv_data;
    return 0;
}

static int on_load(ErlNifEnv* env, void** /*priv_data*/, ERL_NIF_TERM /*load_info*/)
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
  erocksdb::TransactionObject::CreateTransactionObjectType(env);
  erocksdb::TLogItrObject::CreateTLogItrObjectType(env);
  erocksdb::BackupEngineObject::CreateBackupEngineObjectType(env);
  erocksdb::Cache::CreateCacheType(env);
  erocksdb::Statistics::CreateStatisticsType(env);
  erocksdb::RateLimiter::CreateRateLimiterType(env);
  erocksdb::SstFileManager::CreateSstFileManagerType(env);
  erocksdb::SstFileWriterObject::CreateSstFileWriterType(env);
  erocksdb::SstFileReaderObject::CreateSstFileReaderType(env);
  erocksdb::WriteBufferManager::CreateWriteBufferManagerType(env);

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
  ATOM(erocksdb::ATOM_CORRUPTION, "corruption");
  ATOM(erocksdb::ATOM_INC, "inc");
  ATOM(erocksdb::ATOM_DEC, "dec");
  ATOM(erocksdb::ATOM_UNKNOWN_STATUS_ERROR, "unknown_status");
  ATOM(erocksdb::ATOM_UNDEFINED, "undefined");

  ATOM(erocksdb::ATOM_DEFAULT, "default");
  ATOM(erocksdb::ATOM_MEMENV, "memenv");

  ATOM(erocksdb::ATOM_LRU, "lru");
  ATOM(erocksdb::ATOM_CLOCK, "clock");
  ATOM(erocksdb::ATOM_USAGE, "usage");
  ATOM(erocksdb::ATOM_PINNED_USAGE, "pinned_usage");
  ATOM(erocksdb::ATOM_CAPACITY, "capacity");
  ATOM(erocksdb::ATOM_STRICT_CAPACITY, "strict_capacity");
  ATOM(erocksdb::ATOM_FLUSH_ONLY, "flush_only");
  ATOM(erocksdb::ATOM_DISABLE, "disable");

  ATOM(erocksdb::ATOM_DEFAULT_COLUMN_FAMILY, "default_column_family");

  // Related to CFOptions
  ATOM(erocksdb::ATOM_BLOCK_CACHE_SIZE_MB_FOR_POINT_LOOKUP, "block_cache_size_mb_for_point_lookup");
  ATOM(erocksdb::ATOM_MEMTABLE_MEMORY_BUDGET, "memtable_memory_budget");
  ATOM(erocksdb::ATOM_WRITE_BUFFER_SIZE, "write_buffer_size");
  ATOM(erocksdb::ATOM_MAX_WRITE_BUFFER_NUMBER, "max_write_buffer_number");
  ATOM(erocksdb::ATOM_MIN_WRITE_BUFFER_NUMBER_TO_MERGE, "min_write_buffer_number_to_merge");
  ATOM(erocksdb::ATOM_COMPRESSION, "compression");

  ATOM(erocksdb::ATOM_ENABLE_BLOB_FILES, "enable_blob_files");
  ATOM(erocksdb::ATOM_MIN_BLOB_SIZE, "min_blob_size");
  ATOM(erocksdb::ATOM_BLOB_FILE_SIZE, "blob_file_size");
  ATOM(erocksdb::ATOM_BLOB_COMPRESSION_TYPE, "blob_compression_type");
  ATOM(erocksdb::ATOM_ENABLE_BLOB_GC, "enable_blob_garbage_collection");
  ATOM(erocksdb::ATOM_BLOB_GC_AGE_CUTOFF, "blob_garbage_collection_age_cutoff");
  ATOM(erocksdb::ATOM_BLOB_GC_FORCE_THRESHOLD, "blob_garbage_collection_force_threshold");
  ATOM(erocksdb::ATOM_BLOB_COMPACTION_READAHEAD_SIZE, "blob_compaction_readahead_size");
  ATOM(erocksdb::ATOM_BLOB_FILE_STARTING_LEVEL, "blob_file_starting_level");
  ATOM(erocksdb::ATOM_BLOB_CACHE, "blob_cache");
  ATOM(erocksdb::ATOM_PREPOPULATE_BLOB_CACHE, "prepopulate_blob_cache");
  ATOM(erocksdb::ATOM_BOTTOMMOST_COMPRESSION, "bottommost_compression");
  ATOM(erocksdb::ATOM_BOTTOMMOST_COMPRESSION_OPTS, "bottommost_compression_opts");
  ATOM(erocksdb::ATOM_COMPRESSION_OPTS, "compression_opts");
  ATOM(erocksdb::ATOM_WINDOW_BITS, "window_bits");
  ATOM(erocksdb::ATOM_LEVEL, "level");
  ATOM(erocksdb::ATOM_STRATEGY, "strategy");
  ATOM(erocksdb::ATOM_MAX_DICT_BYTES, "max_dict_bytes");
  ATOM(erocksdb::ATOM_ZSTD_MAX_TRAIN_BYTES, "zstd_max_train_bytes");

  ATOM(erocksdb::ATOM_NUM_LEVELS, "num_levels");
  ATOM(erocksdb::ATOM_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, "level0_file_num_compaction_trigger");
  ATOM(erocksdb::ATOM_LEVEL0_SLOWDOWN_WRITES_TRIGGER, "level0_slowdown_writes_trigger");
  ATOM(erocksdb::ATOM_LEVEL0_STOP_WRITES_TRIGGER, "level0_stop_writes_trigger");
  ATOM(erocksdb::ATOM_TARGET_FILE_SIZE_BASE, "target_file_size_base");
  ATOM(erocksdb::ATOM_TARGET_FILE_SIZE_MULTIPLIER, "target_file_size_multiplier");
  ATOM(erocksdb::ATOM_MAX_BYTES_FOR_LEVEL_BASE, "max_bytes_for_level_base");
  ATOM(erocksdb::ATOM_MAX_BYTES_FOR_LEVEL_MULTIPLIER, "max_bytes_for_level_multiplier");
  ATOM(erocksdb::ATOM_MAX_COMPACTION_BYTES, "max_compaction_bytes");
  ATOM(erocksdb::ATOM_ARENA_BLOCK_SIZE, "arena_block_size");
  ATOM(erocksdb::ATOM_DISABLE_AUTO_COMPACTIONS, "disable_auto_compactions");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE, "compaction_style");
  ATOM(erocksdb::ATOM_COMPACTION_PRI, "compaction_pri");
  ATOM(erocksdb::ATOM_FILTER_DELETES, "filter_deletes");
  ATOM(erocksdb::ATOM_MAX_SEQUENTIAL_SKIP_IN_ITERATIONS, "max_sequential_skip_in_iterations");
  ATOM(erocksdb::ATOM_INPLACE_UPDATE_SUPPORT, "inplace_update_support");
  ATOM(erocksdb::ATOM_INPLACE_UPDATE_NUM_LOCKS, "inplace_update_num_locks");
  ATOM(erocksdb::ATOM_TABLE_FACTORY_BLOCK_CACHE_SIZE, "table_factory_block_cache_size");
  ATOM(erocksdb::ATOM_IN_MEMORY_MODE, "in_memory_mode");
  ATOM(erocksdb::ATOM_IN_MEMORY, "in_memory");
  ATOM(erocksdb::ATOM_BLOCK_BASED_TABLE_OPTIONS, "block_based_table_options");
  ATOM(erocksdb::ATOM_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES, "level_compaction_dynamic_level_bytes");
  ATOM(erocksdb::ATOM_OPTIMIZE_FILTERS_FOR_HITS, "optimize_filters_for_hits");
  ATOM(erocksdb::ATOM_PREFIX_EXTRACTOR, "prefix_extractor");

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
  ATOM(erocksdb::ATOM_STATS_DUMP_PERIOD_SEC, "stats_dump_period_sec");
  ATOM(erocksdb::ATOM_ADVISE_RANDOM_ON_OPEN, "advise_random_on_open");
  ATOM(erocksdb::ATOM_COMPACTION_READAHEAD_SIZE, "compaction_readahead_size");
  ATOM(erocksdb::ATOM_USE_ADAPTIVE_MUTEX, "use_adaptive_mutex");
  ATOM(erocksdb::ATOM_BYTES_PER_SYNC, "bytes_per_sync");
  ATOM(erocksdb::ATOM_SKIP_STATS_UPDATE_ON_DB_OPEN, "skip_stats_update_on_db_open");
  ATOM(erocksdb::ATOM_WAL_RECOVERY_MODE, "wal_recovery_mode");
  ATOM(erocksdb::ATOM_ALLOW_CONCURRENT_MEMTABLE_WRITE, "allow_concurrent_memtable_write");
  ATOM(erocksdb::ATOM_ENABLE_WRITE_THREAD_ADAPTATIVE_YIELD, "enable_write_thread_adaptive_yield");
  ATOM(erocksdb::ATOM_DB_WRITE_BUFFER_SIZE, "db_write_buffer_size");
  ATOM(erocksdb::ATOM_RATE_LIMITER, "rate_limiter");
  ATOM(erocksdb::ATOM_SST_FILE_MANAGER, "sst_file_manager");
  ATOM(erocksdb::ATOM_WRITE_BUFFER_MANAGER, "write_buffer_manager");
  ATOM(erocksdb::ATOM_MAX_SUBCOMPACTIONS, "max_subcompactions");
  ATOM(erocksdb::ATOM_MANUAL_WAL_FLUSH, "manual_wal_flush");
  ATOM(erocksdb::ATOM_ATOMIC_FLUSH, "atomic_flush");
  ATOM(erocksdb::ATOM_USE_DIRECT_READS, "use_direct_reads");
  ATOM(erocksdb::ATOM_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION, "use_direct_io_for_flush_and_compaction");
  ATOM(erocksdb::ATOM_ENABLE_PIPELINED_WRITE, "enable_pipelined_write");
  ATOM(erocksdb::ATOM_UNORDERED_WRITE, "unordered_write");
  ATOM(erocksdb::ATOM_TWO_WRITE_QUEUES, "two_write_queues");

  // Related to BlockBasedTable Options
  ATOM(erocksdb::ATOM_NO_BLOCK_CACHE, "no_block_cache");
  ATOM(erocksdb::ATOM_BLOCK_CACHE, "block_cache");

  ATOM(erocksdb::ATOM_BLOCK_SIZE, "block_size");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_SIZE, "block_cache_size");
  ATOM(erocksdb::ATOM_BLOOM_FILTER_POLICY, "bloom_filter_policy");
  ATOM(erocksdb::ATOM_FORMAT_VERSION, "format_version");
  ATOM(erocksdb::ATOM_CACHE_INDEX_AND_FILTER_BLOCKS, "cache_index_and_filter_blocks");

  // Related to ReadTier
  ATOM(erocksdb::ATOM_READ_TIER, "read_tier");
  ATOM(erocksdb::ATOM_READ_ALL_TIER, "read_all_tier");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_TIER, "block_cache_tier");
  ATOM(erocksdb::ATOM_PERSISTED_TIER, "persisted_tier");
  ATOM(erocksdb::ATOM_MEMTABLE_TIER, "memtable_tier");

  // Related to Read Options
  ATOM(erocksdb::ATOM_VERIFY_CHECKSUMS, "verify_checksums");
  ATOM(erocksdb::ATOM_FILL_CACHE,"fill_cache");
  ATOM(erocksdb::ATOM_ITERATE_UPPER_BOUND,"iterate_upper_bound");
  ATOM(erocksdb::ATOM_ITERATE_LOWER_BOUND,"iterate_lower_bound");
  ATOM(erocksdb::ATOM_TAILING,"tailing");
  ATOM(erocksdb::ATOM_TOTAL_ORDER_SEEK,"total_order_seek");
  ATOM(erocksdb::ATOM_PREFIX_SAME_AS_START,"prefix_same_as_start");
  ATOM(erocksdb::ATOM_SNAPSHOT, "snapshot");
  ATOM(erocksdb::ATOM_BAD_SNAPSHOT, "bad_snapshot");
  ATOM(erocksdb::ATOM_AUTO_REFRESH_ITERATOR_WITH_SNAPSHOT, "auto_refresh_iterator_with_snapshot");
  ATOM(erocksdb::ATOM_AUTO_READAHEAD_SIZE, "auto_readahead_size");
  ATOM(erocksdb::ATOM_ALLOW_UNPREPARED_VALUE, "allow_unprepared_value");

  // Related to Write Options
  ATOM(erocksdb::ATOM_SYNC, "sync");
  ATOM(erocksdb::ATOM_DISABLE_WAL, "disable_wal");
  ATOM(erocksdb::ATOM_IGNORE_MISSING_COLUMN_FAMILIES, "ignore_missing_column_families");
  ATOM(erocksdb::ATOM_NO_SLOWDOWN, "no_slowdown");
  ATOM(erocksdb::ATOM_LOW_PRI, "low_pri");

  // Related to Write Options
  ATOM(erocksdb::ATOM_CLEAR, "clear");
  ATOM(erocksdb::ATOM_PUT, "put");
  ATOM(erocksdb::ATOM_MERGE, "merge");
  ATOM(erocksdb::ATOM_DELETE, "delete");
  ATOM(erocksdb::ATOM_SINGLE_DELETE, "single_delete");

  // Related to CompactRangeOptions
  ATOM(erocksdb::ATOM_EXCLUSIVE_MANUAL_COMPACTION, "exclusive_manual_compaction");
  ATOM(erocksdb::ATOM_CHANGE_LEVEL, "change_level");
  ATOM(erocksdb::ATOM_TARGET_LEVEL, "target_level");
  ATOM(erocksdb::ATOM_ALLOW_WRITE_STALL, "allow_write_stall");

  // FIFO options
  ATOM(erocksdb::ATOM_COMPACTION_OPTIONS_FIFO, "compaction_options_fifo");
  ATOM(erocksdb::ATOM_MAX_TABLE_FILE_SIZE, "max_table_files_size");
  ATOM(erocksdb::ATOM_ALLOW_COMPACTION, "allow_compaction");

  ATOM(erocksdb::ATOM_TTL, "ttl");

  // related to FlushOptions
  ATOM(erocksdb::ATOM_WAIT, "wait");

  // Related to Iterator Options
  ATOM(erocksdb::ATOM_FIRST, "first");
  ATOM(erocksdb::ATOM_LAST, "last");
  ATOM(erocksdb::ATOM_NEXT, "next");
  ATOM(erocksdb::ATOM_PREV, "prev");
  ATOM(erocksdb::ATOM_SEEK_FOR_PREV, "seek_for_prev");
  ATOM(erocksdb::ATOM_SEEK, "seek");

  // Related to Iterator Value to be retrieved
  ATOM(erocksdb::ATOM_KEYS_ONLY, "keys_only");

  // Related to Compression Type
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_SNAPPY, "snappy");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_ZLIB, "zlib");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_BZIP2, "bzip2");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_LZ4, "lz4");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_LZ4H, "lz4h");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_ZSTD, "zstd");
  ATOM(erocksdb::ATOM_COMPRESSION_TYPE_NONE, "none");

  // Related to Compaction Style
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_LEVEL, "level");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_UNIVERSAL, "universal");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_FIFO, "fifo");
  ATOM(erocksdb::ATOM_COMPACTION_STYLE_NONE, "none");

  // Related to Compaction Priority
  ATOM(erocksdb::ATOM_COMPACTION_PRI_COMPENSATED_SIZE, "compensated_size");
  ATOM(erocksdb::ATOM_COMPACTION_PRI_OLDEST_LARGEST_SEQ_FIRST, "oldest_largest_seq_first");
  ATOM(erocksdb::ATOM_COMPACTION_PRI_OLDEST_SMALLEST_SEQ_FIRST, "oldest_smallest_seq_first");

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
  ATOM(erocksdb::ATOM_ERROR_INCOMPLETE, "incomplete");

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

    // Related to Merge OPs
  ATOM(erocksdb::ATOM_MERGE_OPERATOR, "merge_operator");
  ATOM(erocksdb::ATOM_ERLANG_MERGE_OPERATOR, "erlang_merge_operator");
  ATOM(erocksdb::ATOM_BITSET_MERGE_OPERATOR, "bitset_merge_operator");
  ATOM(erocksdb::ATOM_COUNTER_MERGE_OPERATOR, "counter_merge_operator");

  // erlang merge ops
  ATOM(erocksdb::ATOM_MERGE_INT_ADD, "int_add");
  ATOM(erocksdb::ATOM_MERGE_LIST_APPEND, "list_append");
  ATOM(erocksdb::ATOM_MERGE_LIST_SUBSTRACT, "list_substract");
  ATOM(erocksdb::ATOM_MERGE_LIST_SET, "list_set");
  ATOM(erocksdb::ATOM_MERGE_LIST_DELETE, "list_delete");
  ATOM(erocksdb::ATOM_MERGE_LIST_INSERT, "list_insert");
  ATOM(erocksdb::ATOM_MERGE_BINARY_APPEND, "binary_append");
  ATOM(erocksdb::ATOM_MERGE_BINARY_REPLACE, "binary_replace");
  ATOM(erocksdb::ATOM_MERGE_BINARY_INSERT, "binary_insert");
  ATOM(erocksdb::ATOM_MERGE_BINARY_ERASE, "binary_erase");

  // prefix extractor
  ATOM(erocksdb::ATOM_FIXED_PREFIX_TRANSFORM, "fixed_prefix_transform");
  ATOM(erocksdb::ATOM_CAPPED_PREFIX_TRANSFORM, "capped_prefix_transform");

  // comparator
  ATOM(erocksdb::ATOM_COMPARATOR, "comparator");
  ATOM(erocksdb::ATOM_BYTEWISE_COMPARATOR, "bytewise_comparator");
  ATOM(erocksdb::ATOM_REVERSE_BYTEWISE_COMPARATOR, "reverse_bytewise_comparator");

  // range
  ATOM(erocksdb::ATOM_NONE, "none");
  ATOM(erocksdb::ATOM_INCLUDE_MEMTABLES, "include_memtables");
  ATOM(erocksdb::ATOM_INCLUDE_FILES, "include_files");
  ATOM(erocksdb::ATOM_INCLUDE_BOTH, "include_both");

  // write buffer manager
  ATOM(erocksdb::ATOM_ENABLED, "enabled");
  ATOM(erocksdb::ATOM_BUFFER_SIZE, "buffer_size");
  ATOM(erocksdb::ATOM_MUTABLE_MEMTABLE_MEMORY_USAGE, "mutable_memtable_memory_usage");
  ATOM(erocksdb::ATOM_MEMORY_USAGE, "memory_usage");

  // sst file manager
  ATOM(erocksdb::ATOM_DELETE_RATE_BYTES_PER_SEC, "delete_rate_bytes_per_sec");
  ATOM(erocksdb::ATOM_MAX_TRASH_DB_RATIO, "max_trash_db_ratio");
  ATOM(erocksdb::ATOM_BYTES_MAX_DELETE_CHUNK, "bytes_max_delete_chunk");
  ATOM(erocksdb::ATOM_MAX_ALLOWED_SPACE_USAGE, "max_allowed_space_usage");
  ATOM(erocksdb::ATOM_COMPACTION_BUFFER_SIZE, "compaction_buffer_size");
  ATOM(erocksdb::ATOM_IS_MAX_ALLOWED_SPACE_REACHED, "is_max_allowed_space_reached");
  ATOM(erocksdb::ATOM_MAX_ALLOWED_SPACE_REACHED_INCLUDING_COMPACTIONS, "max_allowed_space_reached_including_compactions");
  ATOM(erocksdb::ATOM_TOTAL_SIZE, "total_size");
  ATOM(erocksdb::ATOM_TOTAL_TRASH_SIZE, "total_trash_size");

  // sst file writer
  ATOM(erocksdb::ATOM_WITH_FILE_INFO, "with_file_info");
  ATOM(erocksdb::ATOM_FILE_PATH, "file_path");
  ATOM(erocksdb::ATOM_SMALLEST_KEY, "smallest_key");
  ATOM(erocksdb::ATOM_LARGEST_KEY, "largest_key");
  ATOM(erocksdb::ATOM_SMALLEST_RANGE_DEL_KEY, "smallest_range_del_key");
  ATOM(erocksdb::ATOM_LARGEST_RANGE_DEL_KEY, "largest_range_del_key");
  ATOM(erocksdb::ATOM_FILE_SIZE, "file_size");
  ATOM(erocksdb::ATOM_NUM_ENTRIES, "num_entries");
  ATOM(erocksdb::ATOM_NUM_RANGE_DEL_ENTRIES, "num_range_del_entries");
  ATOM(erocksdb::ATOM_SEQUENCE_NUMBER, "sequence_number");

  // ingest external file
  ATOM(erocksdb::ATOM_MOVE_FILES, "move_files");
  ATOM(erocksdb::ATOM_FAILED_MOVE_FALL_BACK_TO_COPY, "failed_move_fall_back_to_copy");
  ATOM(erocksdb::ATOM_SNAPSHOT_CONSISTENCY, "snapshot_consistency");
  ATOM(erocksdb::ATOM_ALLOW_GLOBAL_SEQNO, "allow_global_seqno");
  ATOM(erocksdb::ATOM_ALLOW_BLOCKING_FLUSH, "allow_blocking_flush");
  ATOM(erocksdb::ATOM_INGEST_BEHIND, "ingest_behind");
  ATOM(erocksdb::ATOM_VERIFY_CHECKSUMS_BEFORE_INGEST, "verify_checksums_before_ingest");
  ATOM(erocksdb::ATOM_VERIFY_CHECKSUMS_READAHEAD_SIZE, "verify_checksums_readahead_size");
  ATOM(erocksdb::ATOM_VERIFY_FILE_CHECKSUM, "verify_file_checksum");
  ATOM(erocksdb::ATOM_FAIL_IF_NOT_BOTTOMMOST_LEVEL, "fail_if_not_bottommost_level");
  ATOM(erocksdb::ATOM_ALLOW_DB_GENERATED_FILES, "allow_db_generated_files");

  // sst file reader / table properties
  ATOM(erocksdb::ATOM_DATA_SIZE, "data_size");
  ATOM(erocksdb::ATOM_INDEX_SIZE, "index_size");
  ATOM(erocksdb::ATOM_INDEX_PARTITIONS, "index_partitions");
  ATOM(erocksdb::ATOM_TOP_LEVEL_INDEX_SIZE, "top_level_index_size");
  ATOM(erocksdb::ATOM_FILTER_SIZE, "filter_size");
  ATOM(erocksdb::ATOM_RAW_KEY_SIZE, "raw_key_size");
  ATOM(erocksdb::ATOM_RAW_VALUE_SIZE, "raw_value_size");
  ATOM(erocksdb::ATOM_NUM_DATA_BLOCKS, "num_data_blocks");
  ATOM(erocksdb::ATOM_NUM_DELETIONS, "num_deletions");
  ATOM(erocksdb::ATOM_NUM_MERGE_OPERANDS, "num_merge_operands");
  ATOM(erocksdb::ATOM_NUM_RANGE_DELETIONS, "num_range_deletions");
  ATOM(erocksdb::ATOM_FIXED_KEY_LEN, "fixed_key_len");
  ATOM(erocksdb::ATOM_COLUMN_FAMILY_ID, "column_family_id");
  ATOM(erocksdb::ATOM_COLUMN_FAMILY_NAME, "column_family_name");
  ATOM(erocksdb::ATOM_FILTER_POLICY_NAME, "filter_policy_name");
  ATOM(erocksdb::ATOM_COMPARATOR_NAME, "comparator_name");
  ATOM(erocksdb::ATOM_MERGE_OPERATOR_NAME, "merge_operator_name");
  ATOM(erocksdb::ATOM_PREFIX_EXTRACTOR_NAME, "prefix_extractor_name");
  ATOM(erocksdb::ATOM_PROPERTY_COLLECTORS_NAMES, "property_collectors_names");
  ATOM(erocksdb::ATOM_COMPRESSION_NAME, "compression_name");
  ATOM(erocksdb::ATOM_COMPRESSION_OPTIONS, "compression_options");
  ATOM(erocksdb::ATOM_CREATION_TIME, "creation_time");
  ATOM(erocksdb::ATOM_OLDEST_KEY_TIME, "oldest_key_time");
  ATOM(erocksdb::ATOM_FILE_CREATION_TIME, "file_creation_time");
  ATOM(erocksdb::ATOM_SLOW_COMPRESSION_ESTIMATED_DATA_SIZE, "slow_compression_estimated_data_size");
  ATOM(erocksdb::ATOM_FAST_COMPRESSION_ESTIMATED_DATA_SIZE, "fast_compression_estimated_data_size");
  ATOM(erocksdb::ATOM_EXTERNAL_SST_FILE_GLOBAL_SEQNO_OFFSET, "external_sst_file_global_seqno_offset");

  // statistics
  ATOM(erocksdb::ATOM_STATISTICS, "statistics");
  ATOM(erocksdb::ATOM_STATS_DISABLE_ALL, "stats_disable_all");
  ATOM(erocksdb::ATOM_STATS_EXCEPT_TICKERS, "stats_except_tickers");
  ATOM(erocksdb::ATOM_STATS_EXCEPT_HISTOGRAM_OR_TIMERS, "stats_except_histogram_or_timers");
  ATOM(erocksdb::ATOM_STATS_EXCEPT_TIMERS, "stats_except_timers");
  ATOM(erocksdb::ATOM_STATS_EXCEPT_DETAILED_TIMERS, "stats_except_detailed_timers");
  ATOM(erocksdb::ATOM_STATS_EXCEPT_TIME_FOR_MUTEX, "stats_except_time_for_mutex");
  ATOM(erocksdb::ATOM_STATS_ALL, "stats_all");
  ATOM(erocksdb::ATOM_STATS_LEVEL, "stats_level");

  // BlobDB Statistics Tickers
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_PUT, "blob_db_num_put");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_WRITE, "blob_db_num_write");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_GET, "blob_db_num_get");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_MULTIGET, "blob_db_num_multiget");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_SEEK, "blob_db_num_seek");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_NEXT, "blob_db_num_next");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_PREV, "blob_db_num_prev");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_KEYS_WRITTEN, "blob_db_num_keys_written");
  ATOM(erocksdb::ATOM_BLOB_DB_NUM_KEYS_READ, "blob_db_num_keys_read");
  ATOM(erocksdb::ATOM_BLOB_DB_BYTES_WRITTEN, "blob_db_bytes_written");
  ATOM(erocksdb::ATOM_BLOB_DB_BYTES_READ, "blob_db_bytes_read");
  ATOM(erocksdb::ATOM_BLOB_DB_WRITE_INLINED, "blob_db_write_inlined");
  ATOM(erocksdb::ATOM_BLOB_DB_WRITE_INLINED_TTL, "blob_db_write_inlined_ttl");
  ATOM(erocksdb::ATOM_BLOB_DB_WRITE_BLOB, "blob_db_write_blob");
  ATOM(erocksdb::ATOM_BLOB_DB_WRITE_BLOB_TTL, "blob_db_write_blob_ttl");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_FILE_BYTES_WRITTEN, "blob_db_blob_file_bytes_written");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_FILE_BYTES_READ, "blob_db_blob_file_bytes_read");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_FILE_SYNCED, "blob_db_blob_file_synced");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_INDEX_EXPIRED_COUNT, "blob_db_blob_index_expired_count");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, "blob_db_blob_index_expired_size");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_INDEX_EVICTED_COUNT, "blob_db_blob_index_evicted_count");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_INDEX_EVICTED_SIZE, "blob_db_blob_index_evicted_size");
  ATOM(erocksdb::ATOM_BLOB_DB_GC_NUM_FILES, "blob_db_gc_num_files");
  ATOM(erocksdb::ATOM_BLOB_DB_GC_NUM_NEW_FILES, "blob_db_gc_num_new_files");
  ATOM(erocksdb::ATOM_BLOB_DB_GC_FAILURES, "blob_db_gc_failures");
  ATOM(erocksdb::ATOM_BLOB_DB_GC_NUM_KEYS_RELOCATED, "blob_db_gc_num_keys_relocated");
  ATOM(erocksdb::ATOM_BLOB_DB_GC_BYTES_RELOCATED, "blob_db_gc_bytes_relocated");
  ATOM(erocksdb::ATOM_BLOB_DB_FIFO_NUM_FILES_EVICTED, "blob_db_fifo_num_files_evicted");
  ATOM(erocksdb::ATOM_BLOB_DB_FIFO_NUM_KEYS_EVICTED, "blob_db_fifo_num_keys_evicted");
  ATOM(erocksdb::ATOM_BLOB_DB_FIFO_BYTES_EVICTED, "blob_db_fifo_bytes_evicted");
  ATOM(erocksdb::ATOM_BLOB_DB_CACHE_MISS, "blob_db_cache_miss");
  ATOM(erocksdb::ATOM_BLOB_DB_CACHE_HIT, "blob_db_cache_hit");
  ATOM(erocksdb::ATOM_BLOB_DB_CACHE_ADD, "blob_db_cache_add");
  ATOM(erocksdb::ATOM_BLOB_DB_CACHE_ADD_FAILURES, "blob_db_cache_add_failures");
  ATOM(erocksdb::ATOM_BLOB_DB_CACHE_BYTES_READ, "blob_db_cache_bytes_read");
  ATOM(erocksdb::ATOM_BLOB_DB_CACHE_BYTES_WRITE, "blob_db_cache_bytes_write");

  // BlobDB Statistics Histograms
  ATOM(erocksdb::ATOM_BLOB_DB_KEY_SIZE, "blob_db_key_size");
  ATOM(erocksdb::ATOM_BLOB_DB_VALUE_SIZE, "blob_db_value_size");
  ATOM(erocksdb::ATOM_BLOB_DB_WRITE_MICROS, "blob_db_write_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_GET_MICROS, "blob_db_get_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_MULTIGET_MICROS, "blob_db_multiget_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_SEEK_MICROS, "blob_db_seek_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_NEXT_MICROS, "blob_db_next_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_PREV_MICROS, "blob_db_prev_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_FILE_WRITE_MICROS, "blob_db_blob_file_write_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_FILE_READ_MICROS, "blob_db_blob_file_read_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_BLOB_FILE_SYNC_MICROS, "blob_db_blob_file_sync_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_COMPRESSION_MICROS, "blob_db_compression_micros");
  ATOM(erocksdb::ATOM_BLOB_DB_DECOMPRESSION_MICROS, "blob_db_decompression_micros");

  // Core Operation Histograms
  ATOM(erocksdb::ATOM_DB_GET, "db_get");
  ATOM(erocksdb::ATOM_DB_WRITE, "db_write");
  ATOM(erocksdb::ATOM_DB_MULTIGET, "db_multiget");
  ATOM(erocksdb::ATOM_DB_SEEK, "db_seek");
  ATOM(erocksdb::ATOM_COMPACTION_TIME, "compaction_time");
  ATOM(erocksdb::ATOM_FLUSH_TIME, "flush_time");

  // I/O and Sync Histograms
  ATOM(erocksdb::ATOM_SST_READ_MICROS, "sst_read_micros");
  ATOM(erocksdb::ATOM_SST_WRITE_MICROS, "sst_write_micros");
  ATOM(erocksdb::ATOM_TABLE_SYNC_MICROS, "table_sync_micros");
  ATOM(erocksdb::ATOM_WAL_FILE_SYNC_MICROS, "wal_file_sync_micros");
  ATOM(erocksdb::ATOM_BYTES_PER_READ, "bytes_per_read");
  ATOM(erocksdb::ATOM_BYTES_PER_WRITE, "bytes_per_write");

  // Transaction Histogram
  ATOM(erocksdb::ATOM_NUM_OP_PER_TRANSACTION, "num_op_per_transaction");

  // Compaction Statistics Tickers
  ATOM(erocksdb::ATOM_COMPACT_READ_BYTES, "compact_read_bytes");
  ATOM(erocksdb::ATOM_COMPACT_WRITE_BYTES, "compact_write_bytes");
  ATOM(erocksdb::ATOM_FLUSH_WRITE_BYTES, "flush_write_bytes");
  ATOM(erocksdb::ATOM_COMPACTION_KEY_DROP_NEWER_ENTRY, "compaction_key_drop_newer_entry");
  ATOM(erocksdb::ATOM_COMPACTION_KEY_DROP_OBSOLETE, "compaction_key_drop_obsolete");
  ATOM(erocksdb::ATOM_COMPACTION_KEY_DROP_RANGE_DEL, "compaction_key_drop_range_del");
  ATOM(erocksdb::ATOM_COMPACTION_KEY_DROP_USER, "compaction_key_drop_user");
  ATOM(erocksdb::ATOM_COMPACTION_CANCELLED, "compaction_cancelled");
  ATOM(erocksdb::ATOM_NUMBER_SUPERVERSION_ACQUIRES, "number_superversion_acquires");
  ATOM(erocksdb::ATOM_NUMBER_SUPERVERSION_RELEASES, "number_superversion_releases");

  // Read/Write Operation Tickers
  ATOM(erocksdb::ATOM_NUMBER_KEYS_WRITTEN, "number_keys_written");
  ATOM(erocksdb::ATOM_NUMBER_KEYS_READ, "number_keys_read");
  ATOM(erocksdb::ATOM_NUMBER_KEYS_UPDATED, "number_keys_updated");
  ATOM(erocksdb::ATOM_BYTES_WRITTEN, "bytes_written");
  ATOM(erocksdb::ATOM_BYTES_READ, "bytes_read");
  ATOM(erocksdb::ATOM_ITER_BYTES_READ, "iter_bytes_read");
  ATOM(erocksdb::ATOM_NUMBER_DB_SEEK, "number_db_seek");
  ATOM(erocksdb::ATOM_NUMBER_DB_NEXT, "number_db_next");
  ATOM(erocksdb::ATOM_NUMBER_DB_PREV, "number_db_prev");
  ATOM(erocksdb::ATOM_NUMBER_DB_SEEK_FOUND, "number_db_seek_found");
  ATOM(erocksdb::ATOM_NUMBER_DB_NEXT_FOUND, "number_db_next_found");
  ATOM(erocksdb::ATOM_NUMBER_DB_PREV_FOUND, "number_db_prev_found");

  // Block Cache Statistics Tickers
  ATOM(erocksdb::ATOM_BLOCK_CACHE_MISS, "block_cache_miss");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_HIT, "block_cache_hit");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_ADD, "block_cache_add");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_ADD_FAILURES, "block_cache_add_failures");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_INDEX_MISS, "block_cache_index_miss");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_INDEX_HIT, "block_cache_index_hit");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_FILTER_MISS, "block_cache_filter_miss");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_FILTER_HIT, "block_cache_filter_hit");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_DATA_MISS, "block_cache_data_miss");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_DATA_HIT, "block_cache_data_hit");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_BYTES_READ, "block_cache_bytes_read");
  ATOM(erocksdb::ATOM_BLOCK_CACHE_BYTES_WRITE, "block_cache_bytes_write");

  // Memtable and Stall Statistics Tickers
  ATOM(erocksdb::ATOM_MEMTABLE_HIT, "memtable_hit");
  ATOM(erocksdb::ATOM_MEMTABLE_MISS, "memtable_miss");
  ATOM(erocksdb::ATOM_STALL_MICROS, "stall_micros");
  ATOM(erocksdb::ATOM_WRITE_DONE_BY_SELF, "write_done_by_self");
  ATOM(erocksdb::ATOM_WRITE_DONE_BY_OTHER, "write_done_by_other");
  ATOM(erocksdb::ATOM_WAL_FILE_SYNCED, "wal_file_synced");

  // Transaction Statistics Tickers
  ATOM(erocksdb::ATOM_TXN_PREPARE_MUTEX_OVERHEAD, "txn_prepare_mutex_overhead");
  ATOM(erocksdb::ATOM_TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD, "txn_old_commit_map_mutex_overhead");
  ATOM(erocksdb::ATOM_TXN_DUPLICATE_KEY_OVERHEAD, "txn_duplicate_key_overhead");
  ATOM(erocksdb::ATOM_TXN_SNAPSHOT_MUTEX_OVERHEAD, "txn_snapshot_mutex_overhead");
  ATOM(erocksdb::ATOM_TXN_GET_TRY_AGAIN, "txn_get_try_again");

  // Histogram result keys
  ATOM(erocksdb::ATOM_MEDIAN, "median");
  ATOM(erocksdb::ATOM_PERCENTILE95, "percentile95");
  ATOM(erocksdb::ATOM_PERCENTILE99, "percentile99");
  ATOM(erocksdb::ATOM_AVERAGE, "average");
  ATOM(erocksdb::ATOM_STANDARD_DEVIATION, "standard_deviation");
  ATOM(erocksdb::ATOM_MAX, "max");
  ATOM(erocksdb::ATOM_COUNT, "count");
  ATOM(erocksdb::ATOM_SUM, "sum");

  // Pessimistic Transaction DB Options
  ATOM(erocksdb::ATOM_MAX_NUM_LOCKS, "max_num_locks");
  ATOM(erocksdb::ATOM_NUM_STRIPES, "num_stripes");
  ATOM(erocksdb::ATOM_TRANSACTION_LOCK_TIMEOUT, "transaction_lock_timeout");
  ATOM(erocksdb::ATOM_DEFAULT_LOCK_TIMEOUT, "default_lock_timeout");

  // Pessimistic Transaction Options
  ATOM(erocksdb::ATOM_SET_SNAPSHOT, "set_snapshot");
  ATOM(erocksdb::ATOM_DEADLOCK_DETECT, "deadlock_detect");
  ATOM(erocksdb::ATOM_LOCK_TIMEOUT, "lock_timeout");

  // Pessimistic Transaction Error Codes
  ATOM(erocksdb::ATOM_BUSY, "busy");
  ATOM(erocksdb::ATOM_TIMED_OUT, "timed_out");
  ATOM(erocksdb::ATOM_EXPIRED, "expired");
  ATOM(erocksdb::ATOM_TRY_AGAIN, "try_again");

  // Column Family/Blob Metadata
  ATOM(erocksdb::ATOM_SIZE, "size");
  ATOM(erocksdb::ATOM_FILE_COUNT, "file_count");
  ATOM(erocksdb::ATOM_NAME, "name");
  // ATOM_BLOB_FILE_SIZE already initialized above
  ATOM(erocksdb::ATOM_BLOB_FILES, "blob_files");
  ATOM(erocksdb::ATOM_BLOB_FILE_NUMBER, "blob_file_number");
  ATOM(erocksdb::ATOM_BLOB_FILE_NAME, "blob_file_name");
  ATOM(erocksdb::ATOM_BLOB_FILE_PATH, "blob_file_path");
  ATOM(erocksdb::ATOM_TOTAL_BLOB_COUNT, "total_blob_count");
  ATOM(erocksdb::ATOM_TOTAL_BLOB_BYTES, "total_blob_bytes");
  ATOM(erocksdb::ATOM_GARBAGE_BLOB_COUNT, "garbage_blob_count");
  ATOM(erocksdb::ATOM_GARBAGE_BLOB_BYTES, "garbage_blob_bytes");

#undef ATOM

return 0;
}
catch(std::exception& )
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
    ERL_NIF_INIT(rocksdb, nif_funcs, &on_load, NULL, &on_upgrade, &on_unload)
}
