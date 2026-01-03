// -------------------------------------------------------------------
// Copyright (c) 2016-2025 Benoit Chesneau. All Rights Reserved.
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
//
// -------------------------------------------------------------------

#include <iostream>

#include "erl_nif.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include "atoms.h"
#include "refobjects.h"
#include "util.h"

#include "erocksdb_db.h"
#include "erocksdb_iter.h"
#include "pessimistic_transaction.h"

namespace erocksdb {

ERL_NIF_TERM
parse_txn_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::TransactionOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == ATOM_SET_SNAPSHOT)
        {
            opts.set_snapshot = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_DEADLOCK_DETECT)
        {
            opts.deadlock_detect = (option[1] == ATOM_TRUE);
        }
        else if (option[0] == ATOM_LOCK_TIMEOUT)
        {
            ErlNifSInt64 lock_timeout;
            if (enif_get_int64(env, option[1], &lock_timeout))
                opts.lock_timeout = lock_timeout;
        }
    }
    return ATOM_OK;
}

ERL_NIF_TERM
NewPessimisticTransaction(ErlNifEnv* env,
                          int argc,
                          const ERL_NIF_TERM argv[])
{
    if(argc < 2)
        return enif_make_badarg(env);

    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    // Verify this is a pessimistic transaction DB
    if(!db_ptr->m_IsPessimistic)
        return error_tuple(env, ATOM_ERROR, "not a pessimistic transaction db");

    // Initialize options
    rocksdb::TransactionOptions txn_options;
    rocksdb::WriteOptions write_options;
    fold(env, argv[1], parse_write_option, write_options);

    // Parse transaction-specific options from the same list
    if (argc >= 3)
    {
        fold(env, argv[2], parse_txn_option, txn_options);
    }

    rocksdb::TransactionDB* txn_db =
        reinterpret_cast<rocksdb::TransactionDB*>(db_ptr->m_Db);

    rocksdb::Transaction* tx = txn_db->BeginTransaction(write_options, txn_options);

    TransactionObject* tx_ptr = TransactionObject::CreateTransactionObject(db_ptr.get(), tx);

    ERL_NIF_TERM result = enif_make_resource(env, tx_ptr);
    enif_release_resource(tx_ptr);

    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
PessimisticTransactionPut(ErlNifEnv* env,
                          int argc,
                          const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    ErlNifBinary key, value;
    rocksdb::Status status;

    if(argc > 3)
    {
        // With column family: tx, cf, key, value
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
           !enif_inspect_binary(env, argv[2], &key) ||
           !enif_inspect_binary(env, argv[3], &value))
        {
            return enif_make_badarg(env);
        }

        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
        status = tx_ptr->m_Tx->Put(cf_ptr->m_ColumnFamily, key_slice, value_slice);
    }
    else
    {
        // Default column family: tx, key, value
        if(!enif_inspect_binary(env, argv[1], &key) ||
           !enif_inspect_binary(env, argv[2], &value))
        {
            return enif_make_badarg(env);
        }

        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
        status = tx_ptr->m_Tx->Put(key_slice, value_slice);
    }

    if(!status.ok())
    {
        if(status.IsBusy())
            return error_tuple(env, ATOM_BUSY, status);
        if(status.IsTimedOut())
            return error_tuple(env, ATOM_TIMED_OUT, status);
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
PessimisticTransactionGet(ErlNifEnv* env,
                          int argc,
                          const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    if(!enif_get_transaction(env, argv[0], &tx_ptr))
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

    rocksdb::PinnableSlice pvalue;
    rocksdb::Status status;

    if(argc == 4)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        // Use Get (not GetForUpdate) - reads without locking
        status = tx_ptr->m_Tx->Get(opts, cf_ptr->m_ColumnFamily, key, &pvalue);
    }
    else
    {
        status = tx_ptr->m_Tx->Get(opts, tx_ptr->m_DbPtr->m_Db->DefaultColumnFamily(), key, &pvalue);
    }

    if(!status.ok())
    {
        if(status.IsNotFound())
            return ATOM_NOT_FOUND;
        if(status.IsCorruption())
            return error_tuple(env, ATOM_CORRUPTION, status);
        return error_tuple(env, ATOM_UNKNOWN_STATUS_ERROR, status);
    }

    ERL_NIF_TERM value_bin;
    memcpy(enif_make_new_binary(env, pvalue.size(), &value_bin), pvalue.data(), pvalue.size());
    pvalue.Reset();
    return enif_make_tuple2(env, ATOM_OK, value_bin);
}

ERL_NIF_TERM
PessimisticTransactionGetForUpdate(ErlNifEnv* env,
                                   int argc,
                                   const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    if(!enif_get_transaction(env, argv[0], &tx_ptr))
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

    rocksdb::PinnableSlice pvalue;
    rocksdb::Status status;

    if(argc == 4)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        // Use GetForUpdate - acquires exclusive lock
        status = tx_ptr->m_Tx->GetForUpdate(opts, cf_ptr->m_ColumnFamily, key, &pvalue);
    }
    else
    {
        status = tx_ptr->m_Tx->GetForUpdate(opts, tx_ptr->m_DbPtr->m_Db->DefaultColumnFamily(), key, &pvalue);
    }

    if(!status.ok())
    {
        if(status.IsNotFound())
            return ATOM_NOT_FOUND;
        if(status.IsBusy())
            return error_tuple(env, ATOM_BUSY, status);
        if(status.IsTimedOut())
            return error_tuple(env, ATOM_TIMED_OUT, status);
        if(status.IsCorruption())
            return error_tuple(env, ATOM_CORRUPTION, status);
        if(status.IsTryAgain())
            return error_tuple(env, ATOM_TRY_AGAIN, status);
        return error_tuple(env, ATOM_UNKNOWN_STATUS_ERROR, status);
    }

    ERL_NIF_TERM value_bin;
    memcpy(enif_make_new_binary(env, pvalue.size(), &value_bin), pvalue.data(), pvalue.size());
    pvalue.Reset();
    return enif_make_tuple2(env, ATOM_OK, value_bin);
}

ERL_NIF_TERM
PessimisticTransactionMultiGet(ErlNifEnv* env,
                                int argc,
                                const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    // Determine argument positions based on arity
    // argc == 3: pessimistic_transaction_multi_get(Txn, Keys, ReadOpts)
    // argc == 4: pessimistic_transaction_multi_get(Txn, CF, Keys, ReadOpts)
    int keys_idx = (argc == 4) ? 2 : 1;
    int opts_idx = keys_idx + 1;

    // Get column family handle if provided
    rocksdb::ColumnFamilyHandle* cfh = tx_ptr->m_DbPtr->m_Db->DefaultColumnFamily();
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

    // Allocate arrays for keys and values
    std::vector<rocksdb::Slice> keys(num_keys);
    std::vector<ErlNifBinary> key_binaries(num_keys);
    std::vector<std::string> values(num_keys);

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

    // Call MultiGet on transaction (without locking)
    std::vector<rocksdb::ColumnFamilyHandle*> cfs(num_keys, cfh);
    std::vector<rocksdb::Status> statuses = tx_ptr->m_Tx->MultiGet(opts, cfs, keys, &values);

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
}

ERL_NIF_TERM
PessimisticTransactionMultiGetForUpdate(ErlNifEnv* env,
                                         int argc,
                                         const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    // Determine argument positions based on arity
    // argc == 3: pessimistic_transaction_multi_get_for_update(Txn, Keys, ReadOpts)
    // argc == 4: pessimistic_transaction_multi_get_for_update(Txn, CF, Keys, ReadOpts)
    int keys_idx = (argc == 4) ? 2 : 1;
    int opts_idx = keys_idx + 1;

    // Get column family handle if provided
    rocksdb::ColumnFamilyHandle* cfh = tx_ptr->m_DbPtr->m_Db->DefaultColumnFamily();
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

    // Allocate arrays for keys and values
    std::vector<rocksdb::Slice> keys(num_keys);
    std::vector<ErlNifBinary> key_binaries(num_keys);
    std::vector<std::string> values(num_keys);

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

    // Call MultiGetForUpdate on transaction - acquires exclusive locks
    std::vector<rocksdb::ColumnFamilyHandle*> cfs(num_keys, cfh);
    std::vector<rocksdb::Status> statuses = tx_ptr->m_Tx->MultiGetForUpdate(opts, cfs, keys, &values);

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
        else if (statuses[i].IsBusy())
        {
            item = error_tuple(env, ATOM_BUSY, statuses[i]);
        }
        else if (statuses[i].IsTimedOut())
        {
            item = error_tuple(env, ATOM_TIMED_OUT, statuses[i]);
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
}

ERL_NIF_TERM
PessimisticTransactionDelete(ErlNifEnv* env,
                             int argc,
                             const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    ErlNifBinary key;
    rocksdb::Status status;
    rocksdb::ColumnFamilyHandle* cfh;

    if(argc > 2)
    {
        // With column family: tx, cf, key
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
           !enif_inspect_binary(env, argv[2], &key))
            return enif_make_badarg(env);
        cfh = cf_ptr->m_ColumnFamily;
    }
    else
    {
        // Default column family: tx, key
        if(!enif_inspect_binary(env, argv[1], &key))
            return enif_make_badarg(env);
        cfh = tx_ptr->m_DbPtr->m_Db->DefaultColumnFamily();
    }

    rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
    status = tx_ptr->m_Tx->Delete(cfh, key_slice);

    if(!status.ok())
    {
        if(status.IsBusy())
            return error_tuple(env, ATOM_BUSY, status);
        if(status.IsTimedOut())
            return error_tuple(env, ATOM_TIMED_OUT, status);
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
PessimisticTransactionCommit(ErlNifEnv* env,
                             int /*argc*/,
                             const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    rocksdb::Status status = tx_ptr->m_Tx->Commit();

    if(!status.ok())
    {
        if(status.IsBusy())
            return error_tuple(env, ATOM_BUSY, status);
        if(status.IsExpired())
            return error_tuple(env, ATOM_EXPIRED, status);
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
PessimisticTransactionRollback(ErlNifEnv* env,
                               int /*argc*/,
                               const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    rocksdb::Status status = tx_ptr->m_Tx->Rollback();

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return ATOM_OK;
}

ERL_NIF_TERM
ReleasePessimisticTransaction(ErlNifEnv* env,
                              int /*argc*/,
                              const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& handle_ref = argv[0];
    ReferencePtr<TransactionObject> tx_ptr;
    tx_ptr.assign(TransactionObject::RetrieveTransactionObject(env, handle_ref));

    if(NULL == tx_ptr.get())
        return ATOM_OK;

    TransactionObject* tx = tx_ptr.get();
    ErlRefObject::InitiateCloseRequest(tx);
    return ATOM_OK;
}

ERL_NIF_TERM
PessimisticTransactionIterator(ErlNifEnv* env,
                               int argc,
                               const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;

    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    int opts_idx = argc - 1;

    if(!enif_is_list(env, argv[opts_idx]))
        return enif_make_badarg(env);

    rocksdb::ReadOptions* opts = new rocksdb::ReadOptions;
    ItrBounds bounds;
    auto itr_env = std::make_shared<ErlEnvCtr>();
    if(!parse_iterator_options(env, itr_env->env, argv[opts_idx], *opts, bounds))
    {
        delete opts;
        return enif_make_badarg(env);
    }

    ItrObject* itr_ptr;
    rocksdb::Iterator* iterator;

    if(argc == 3)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
        {
            delete opts;
            return enif_make_badarg(env);
        }
        iterator = tx_ptr->m_Tx->GetIterator(*opts, cf_ptr->m_ColumnFamily);
    }
    else
    {
        iterator = tx_ptr->m_Tx->GetIterator(*opts);
    }

    itr_ptr = ItrObject::CreateItrObject(tx_ptr->m_DbPtr.get(), itr_env, iterator);

    if(bounds.upper_bound_slice != nullptr)
        itr_ptr->SetUpperBoundSlice(bounds.upper_bound_slice);

    if(bounds.lower_bound_slice != nullptr)
        itr_ptr->SetLowerBoundSlice(bounds.lower_bound_slice);

    ERL_NIF_TERM result = enif_make_resource(env, itr_ptr);

    enif_release_resource(itr_ptr);
    delete opts;
    iterator = NULL;
    return enif_make_tuple2(env, ATOM_OK, result);
}

// Savepoint operations

ERL_NIF_TERM
PessimisticTransactionSetSavepoint(ErlNifEnv* env,
                                   int /*argc*/,
                                   const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    tx_ptr->m_Tx->SetSavePoint();

    return ATOM_OK;
}

ERL_NIF_TERM
PessimisticTransactionRollbackToSavepoint(ErlNifEnv* env,
                                          int /*argc*/,
                                          const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    rocksdb::Status status = tx_ptr->m_Tx->RollbackToSavePoint();

    if(!status.ok())
    {
        if(status.IsNotFound())
            return error_tuple(env, ATOM_ERROR, "no savepoint set");
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
PessimisticTransactionPopSavepoint(ErlNifEnv* env,
                                   int /*argc*/,
                                   const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    rocksdb::Status status = tx_ptr->m_Tx->PopSavePoint();

    if(!status.ok())
    {
        if(status.IsNotFound())
            return error_tuple(env, ATOM_ERROR, "no savepoint set");
        return error_tuple(env, ATOM_ERROR, status);
    }

    return ATOM_OK;
}

// Transaction information

ERL_NIF_TERM
PessimisticTransactionGetId(ErlNifEnv* env,
                            int /*argc*/,
                            const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    uint64_t id = tx_ptr->m_Tx->GetID();

    return enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, id));
}

ERL_NIF_TERM
PessimisticTransactionGetWaitingTxns(ErlNifEnv* env,
                                     int /*argc*/,
                                     const ERL_NIF_TERM argv[])
{
    ReferencePtr<TransactionObject> tx_ptr;
    if(!enif_get_transaction(env, argv[0], &tx_ptr))
        return enif_make_badarg(env);

    uint32_t column_family_id;
    std::string key;

    std::vector<rocksdb::TransactionID> waiting_txns =
        tx_ptr->m_Tx->GetWaitingTxns(&column_family_id, &key);

    // Build list of waiting transaction IDs
    ERL_NIF_TERM txn_list = enif_make_list(env, 0);
    for (auto it = waiting_txns.rbegin(); it != waiting_txns.rend(); ++it)
    {
        txn_list = enif_make_list_cell(env, enif_make_uint64(env, *it), txn_list);
    }

    // Build key binary
    ERL_NIF_TERM key_bin;
    memcpy(enif_make_new_binary(env, key.size(), &key_bin), key.data(), key.size());

    // Return {ok, #{column_family_id => CfId, key => Key, waiting_txns => TxnList}}
    ERL_NIF_TERM keys[3];
    ERL_NIF_TERM values[3];

    keys[0] = enif_make_atom(env, "column_family_id");
    values[0] = enif_make_uint(env, column_family_id);

    keys[1] = enif_make_atom(env, "key");
    values[1] = key_bin;

    keys[2] = enif_make_atom(env, "waiting_txns");
    values[2] = txn_list;

    ERL_NIF_TERM result_map;
    enif_make_map_from_arrays(env, keys, values, 3, &result_map);

    return enif_make_tuple2(env, ATOM_OK, result_map);
}

}  // namespace erocksdb
