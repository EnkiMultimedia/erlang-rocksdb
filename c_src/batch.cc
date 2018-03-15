// -------------------------------------------------------------------
// Copyright (c) 2016-2017 Benoit Chesneau. All Rights Reserved.
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

#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"

#include "atoms.h"
#include "refobjects.h"
#include "util.h"

#include "erocksdb_db.h"
#include "transactions.h"

namespace erocksdb {

ErlNifResourceType *m_Batch_RESOURCE;

void
batch_resource_cleanup(ErlNifEnv *env, void *res)
{
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
        int argc,
        const ERL_NIF_TERM argv[])
{

    rocksdb::WriteBatch * batch;
    void *alloc_ptr;
    alloc_ptr = enif_alloc_resource(m_Batch_RESOURCE, sizeof(rocksdb::WriteBatch));
    batch = new(alloc_ptr) rocksdb::WriteBatch();
    ERL_NIF_TERM result = enif_make_resource(env, batch);
    enif_release_resource(batch);
    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
PutBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key, value;


    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    if (argc > 3)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key) ||
                !enif_inspect_binary(env, argv[3], &value))
            return enif_make_badarg(env);

        rocksdb::Slice key_slice((const char*)key.data, key.size);
        rocksdb::Slice value_slice((const char*)value.data, value.size);
        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();

        batch_ptr->Put(cf->m_ColumnFamily, key_slice, value_slice);
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key) ||
                !enif_inspect_binary(env, argv[2], &value))
            return enif_make_badarg(env);

        rocksdb::Slice key_slice((const char*)key.data, key.size);
        rocksdb::Slice value_slice((const char*)value.data, value.size);
        batch_ptr->Put(key_slice, value_slice);
    }
    return ATOM_OK;
}

ERL_NIF_TERM
DeleteBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key;

    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    if (argc > 2)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key))
            return enif_make_badarg(env);

        rocksdb::Slice key_slice((const char*)key.data, key.size);
        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();

        batch_ptr->Delete(cf->m_ColumnFamily, key_slice);
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key))
            return enif_make_badarg(env);

        rocksdb::Slice key_slice((const char*)key.data, key.size);
        batch_ptr->Delete(key_slice);
    }
    return ATOM_OK;
}

ERL_NIF_TERM
SingleDeleteBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ErlNifBinary key;

    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    if (argc > 2)
    {
        if(!enif_get_cf(env, argv[1], &cf_ptr) ||
                !enif_inspect_binary(env, argv[2], &key))
            return enif_make_badarg(env);

        rocksdb::Slice key_slice((const char*)key.data, key.size);
        erocksdb::ColumnFamilyObject* cf = cf_ptr.get();

        batch_ptr->SingleDelete(cf->m_ColumnFamily, key_slice);
    }
    else
    {
        if(!enif_inspect_binary(env, argv[1], &key))
            return enif_make_badarg(env);

        rocksdb::Slice key_slice((const char*)key.data, key.size);
        batch_ptr->SingleDelete(key_slice);
    }
    return ATOM_OK;
}

ERL_NIF_TERM
WriteBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;
    ReferencePtr<DbObject> db_ptr;

    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[1], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    rocksdb::WriteOptions* opts = new rocksdb::WriteOptions;
    fold(env, argv[2], parse_write_option, *opts);

    rocksdb::Status status = db_ptr->m_Db->Write(*opts, batch_ptr);
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);
    opts = NULL;
    return ATOM_OK;
}

ERL_NIF_TERM
ClearBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;

    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    batch_ptr->Clear();
    return ATOM_OK;
}


ERL_NIF_TERM
BatchSetSavePoint(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;

    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    batch_ptr->SetSavePoint();
    return ATOM_OK;
}

ERL_NIF_TERM
BatchRollbackToSavePoint(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;

    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    rocksdb::Status status = batch_ptr->RollbackToSavePoint();
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    return ATOM_OK;
}

ERL_NIF_TERM
BatchCount(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;

    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);


    int count = batch_ptr->Count();
    return enif_make_int(env, count);
}

ERL_NIF_TERM
BatchToList(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;

    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);

    TransactionLogHandler handler = TransactionLogHandler(env);
    batch_ptr->Iterate(&handler);
    ERL_NIF_TERM log;
    enif_make_reverse_list(env, handler.t_List, &log);

    return log;
}

ERL_NIF_TERM
CloseBatch(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    rocksdb::WriteBatch* batch_ptr;
    if(!enif_get_resource(env, argv[0], m_Batch_RESOURCE, (void **) &batch_ptr))
        return enif_make_badarg(env);
    batch_ptr->Clear();
    return ATOM_OK;
}

}
