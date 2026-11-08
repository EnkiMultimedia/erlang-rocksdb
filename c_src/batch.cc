// -------------------------------------------------------------------
// Copyright (c) 2016-2026 Benoit Chesneau. All Rights Reserved.
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


#include "erl_nif.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"

#include <mutex>

#include "atoms.h"
#include "refobjects.h"
#include "util.h"

#include "erocksdb_db.h"
#include "transaction_log.h"


struct Batch
{
    std::mutex mutex;           // serializes all use of wb and env
    rocksdb::WriteBatch* wb;    // NULL once released
    ErlNifEnv* env;
    // Database of the column families used in the batch, 0 when none.
    // RocksDB stores column families by id, so a batch must not mix them
    // across databases nor be written to another one. An id, not a
    // reference, so a batch never keeps a database open.
    uint64_t db_id = 0;
};

// A column family usable in this batch: live, and of the same database as
// the column families already in it.
static bool
batch_cf(ErlNifEnv* env, ERL_NIF_TERM term, Batch* batch,
         erocksdb::ReferencePtr<erocksdb::ColumnFamilyObject>* cf_ptr)
{
    if(!enif_get_cf(env, term, cf_ptr) || NULL == (*cf_ptr)->m_ColumnFamily)
        return false;
    uint64_t id = (*cf_ptr)->m_DbPtr->m_Id;
    return batch->db_id == 0 || batch->db_id == id;
}

static void cleanup_batch(Batch* batch)
{
    if(batch->env != nullptr) {
        enif_free_env(batch->env);
        batch->env = nullptr;
    }
    cleanup_obj_ptr(batch->wb);
}


namespace erocksdb {

ErlNifResourceType *m_Batch_RESOURCE;

void
batch_resource_cleanup(ErlNifEnv * /*env*/, void *arg)
{
    Batch* batch = reinterpret_cast<Batch*>(arg);
    cleanup_batch(batch);
    batch->~Batch();
}

// Get a batch that was not released and lock it for the rest of the call.
static bool
lock_batch(ErlNifEnv* env, ERL_NIF_TERM term, Batch** batch_ptr,
           std::unique_lock<std::mutex>& lock)
{
    Batch* batch = nullptr;
    if(!enif_get_resource(env, term, m_Batch_RESOURCE, (void **) &batch))
        return false;
    lock = std::unique_lock<std::mutex>(batch->mutex);
    if(batch->wb == nullptr)
    {
        lock.unlock();
        return false;
    }
    *batch_ptr = batch;
    return true;
}

void
CreateBatchType(ErlNifEnv *env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_Batch_RESOURCE = enif_open_resource_type(env, NULL, "rocksdb_WriteBatch", batch_resource_cleanup, flags, NULL);
    return;
}

ERL_NIF_TERM
NewBatch(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM[] /*argv*/)
{
    rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*>(enif_alloc(sizeof(rocksdb::WriteBatch)));
    if (wb == nullptr)
        return error_tuple(env, ATOM_ERROR, "out of memory");

    Batch* batch = reinterpret_cast<Batch*>(enif_alloc_resource(m_Batch_RESOURCE, sizeof(Batch)));
    if (batch == nullptr)
    {
        enif_free(wb);
        return error_tuple(env, ATOM_ERROR, "out of memory");
    }

    new(batch) Batch();
    batch->wb = new(wb) rocksdb::WriteBatch();
    batch->env = enif_alloc_env();
    if (batch->env == nullptr)
    {
        cleanup_obj_ptr(batch->wb);
        enif_release_resource(batch);
        return error_tuple(env, ATOM_ERROR, "out of memory");
    }
    ERL_NIF_TERM result = enif_make_resource(env, batch);
    enif_release_resource(batch);
    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
ReleaseBatch(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    Batch* batch_ptr = nullptr;
    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    // releasing twice is fine
    std::lock_guard<std::mutex> lock(batch_ptr->mutex);
    cleanup_batch(batch_ptr);
    batch_ptr = NULL;
    return ATOM_OK;
}

ERL_NIF_TERM
WriteBatch(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* wb = nullptr;
    Batch* batch_ptr = nullptr;
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[1], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    wb = batch_ptr->wb;
    rocksdb::WriteOptions* opts = new rocksdb::WriteOptions;
    fold(env, argv[2], parse_write_option, *opts);
    if(batch_ptr->db_id != 0 && batch_ptr->db_id != db_ptr->m_Id)
    {
        // column families of another database
        delete opts;
        return enif_make_badarg(env);
    }
    rocksdb::Status status = db_ptr->m_Db->Write(*opts, wb);
    if(batch_ptr->wb) {
        batch_ptr->wb->Clear();
        batch_ptr->db_id = 0;
    }
    enif_clear_env(batch_ptr->env);

    delete opts;
    opts = NULL;
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    return ATOM_OK;
}

ERL_NIF_TERM
PutBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* wb = nullptr;
    Batch* batch_ptr = nullptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key, value;

    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    wb = batch_ptr->wb;
    if (argc > 3)
    {
        if(!batch_cf(env, argv[1], batch_ptr, &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key) ||
                !enif_inspect_binary(env, argv[3], &value))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[1]);
        enif_make_copy(batch_ptr->env, argv[2]);
        enif_make_copy(batch_ptr->env, argv[3]);

        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();
        wb->Put(cf->m_ColumnFamily, key_slice, value_slice);
        batch_ptr->db_id = cf->m_DbPtr->m_Id;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key) ||
                !enif_inspect_binary(env, argv[2], &value))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[1]);
        enif_make_copy(batch_ptr->env, argv[2]);


        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
        wb->Put(key_slice, value_slice);
    }
    batch_ptr = nullptr;
    return ATOM_OK;
}

ERL_NIF_TERM
MergeBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* wb = nullptr;
    Batch* batch_ptr = nullptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key, value;

    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    wb = batch_ptr->wb;
    if (argc > 3)
    {
        if(!batch_cf(env, argv[1], batch_ptr, &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key) ||
                !enif_inspect_binary(env, argv[3], &value))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[1]);
        enif_make_copy(batch_ptr->env, argv[2]);
        enif_make_copy(batch_ptr->env, argv[3]);

        rocksdb::Slice key_slice((const char*)key.data, key.size);
        rocksdb::Slice value_slice((const char*)value.data, value.size);
        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();
        wb->Merge(cf->m_ColumnFamily, key_slice, value_slice);
        batch_ptr->db_id = cf->m_DbPtr->m_Id;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key) ||
                !enif_inspect_binary(env, argv[2], &value))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[1]);
        enif_make_copy(batch_ptr->env, argv[2]);

        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
        wb->Merge(key_slice, value_slice);
    }
    batch_ptr = nullptr;
    return ATOM_OK;
}

ERL_NIF_TERM
DeleteBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* wb = nullptr;
    Batch* batch_ptr = nullptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    wb = batch_ptr->wb;
    if (argc > 2)
    {
        if(!batch_cf(env, argv[1], batch_ptr, &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[1]);
        enif_make_copy(batch_ptr->env, argv[2]);

        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();
        wb->Delete(cf->m_ColumnFamily, key_slice);
        batch_ptr->db_id = cf->m_DbPtr->m_Id;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[1]);

        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        wb->Delete(key_slice);
    }
    return ATOM_OK;
}

ERL_NIF_TERM
SingleDeleteBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* wb = nullptr;
    Batch* batch_ptr = nullptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    wb = batch_ptr->wb;
    if (argc > 2)
    {
        if(!batch_cf(env, argv[1], batch_ptr, &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key))
            return enif_make_badarg(env);
        enif_make_copy(batch_ptr->env, argv[1]);
        enif_make_copy(batch_ptr->env, argv[2]);
        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();
        wb->SingleDelete(cf->m_ColumnFamily, key_slice);
        batch_ptr->db_id = cf->m_DbPtr->m_Id;
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key))
            return enif_make_badarg(env);
        enif_make_copy(batch_ptr->env, argv[1]);
        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        wb->SingleDelete(key_slice);
    }
    return ATOM_OK;
}

ERL_NIF_TERM
DeleteRangeBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* wb = nullptr;
    Batch* batch_ptr = nullptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    rocksdb::Slice begin;
    rocksdb::Slice end;

    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    wb = batch_ptr->wb;

    if (argc > 3)
    {
        if(!batch_cf(env, argv[1], batch_ptr, &cf_ptr))
            return enif_make_badarg(env);

        if (!binary_to_slice(env, argv[2], &begin))
            return enif_make_badarg(env);

        if (!binary_to_slice(env, argv[3], &end))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[2]);
        enif_make_copy(batch_ptr->env, argv[3]);

        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();
        wb->DeleteRange(cf->m_ColumnFamily, begin, end);
        batch_ptr->db_id = cf->m_DbPtr->m_Id;
    }
    else
    {
        if (!binary_to_slice(env, argv[1], &begin))
            return enif_make_badarg(env);

        if (!binary_to_slice(env, argv[2], &end))
            return enif_make_badarg(env);

        enif_make_copy(batch_ptr->env, argv[1]);
        enif_make_copy(batch_ptr->env, argv[2]);

        wb->DeleteRange(begin, end);
    }

    return ATOM_OK;
}


ERL_NIF_TERM
ClearBatch(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    Batch* batch_ptr = nullptr;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    batch_ptr->wb->Clear();
    batch_ptr->db_id = 0;
    enif_clear_env(batch_ptr->env);
    return ATOM_OK;
}


ERL_NIF_TERM
BatchSetSavePoint(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    Batch* batch_ptr = nullptr;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    batch_ptr->wb->SetSavePoint();
    return ATOM_OK;
}

ERL_NIF_TERM
BatchRollbackToSavePoint(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    Batch* batch_ptr = nullptr;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    rocksdb::Status status = batch_ptr->wb->RollbackToSavePoint();
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    return ATOM_OK;
}

ERL_NIF_TERM
BatchCount(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    Batch* batch_ptr = nullptr;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    int count = (int) batch_ptr->wb->Count();
    return enif_make_int(env, count);
}

ERL_NIF_TERM
BatchDataSize(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    Batch* batch_ptr = nullptr;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    int count = batch_ptr->wb->GetDataSize();
    return enif_make_int(env, count);
}

ERL_NIF_TERM
BatchToList(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{
    Batch* batch_ptr = nullptr;
    std::unique_lock<std::mutex> batch_lock;
    if(!lock_batch(env, argv[0], &batch_ptr, batch_lock))
        return enif_make_badarg(env);
    TransactionLogHandler handler = TransactionLogHandler(env);
    batch_ptr->wb->Iterate(&handler);
    ERL_NIF_TERM log;
    enif_make_reverse_list(env, handler.t_List, &log);
    return log;
}

}
