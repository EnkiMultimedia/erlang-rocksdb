// -------------------------------------------------------------------
// Copyright (c) 2016-2020 Benoit Chesneau. All Rights Reserved.
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
#include "rocksdb/utilities/optimistic_transaction_db.h"

#include "atoms.h"
#include "refobjects.h"
#include "util.h"

#include "erocksdb_db.h"
#include "erocksdb_iter.h"

using namespace std;

struct TransactionResource
{
    rocksdb::Transaction *transaction;
    rocksdb::DB *base_db;
    ErlNifEnv *env;
};

static void cleanup_transaction(TransactionResource* transaction)
{
    if(transaction->env != nullptr) {
        enif_free_env(transaction->env);
        transaction->env = nullptr;
    }
    transaction->base_db = nullptr;
    delete transaction->transaction;
}

namespace erocksdb {

    ErlNifResourceType *m_Transaction_RESOURCE;

    void
    transaction_resource_cleanup(ErlNifEnv * /*env*/, void *arg)
    {
        TransactionResource* transaction = reinterpret_cast<TransactionResource*>(arg);
        cleanup_transaction(transaction);
    }

    void
    CreateTransactionType(ErlNifEnv *env)
    {
        ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
        m_Transaction_RESOURCE = enif_open_resource_type(env, NULL, "rocksdb_Transaction",
                                                 transaction_resource_cleanup, flags, NULL);
        return;
    }

    ERL_NIF_TERM
    NewTransaction(ErlNifEnv* env,
           int argc,
           const ERL_NIF_TERM argv[])
    {
        if(argc != 2) {
            return enif_make_badarg(env);
        }
        TransactionResource* transaction =
            reinterpret_cast<TransactionResource*>(enif_alloc_resource(m_Transaction_RESOURCE,
                                                                       sizeof(TransactionResource)));

        // not sure that we need this, since there are multiple usable arities
        rocksdb::OptimisticTransactionOptions transaction_options;
        rocksdb::WriteOptions write_options;
        fold(env, argv[1], parse_write_option, write_options);
        ReferencePtr<DbObject> db_ptr;
        if(!enif_get_db(env, argv[0], &db_ptr)) {
            return enif_make_badarg(env);
        }

        rocksdb::OptimisticTransactionDB* db =
            reinterpret_cast<rocksdb::OptimisticTransactionDB*>(db_ptr->m_Db);
        // transaction_options.set_snapshot = true;
        transaction->transaction = db->BeginTransaction(write_options, transaction_options);
        transaction->base_db = db->GetBaseDB();
        transaction->env = enif_alloc_env();

        ERL_NIF_TERM result = enif_make_resource(env, transaction);
        enif_release_resource(transaction);
        return enif_make_tuple2(env, ATOM_OK, result);
    }

    ERL_NIF_TERM
    PutTransaction(ErlNifEnv* env,
                   int argc,
                   const ERL_NIF_TERM argv[])
    {
        ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;

        ErlNifBinary key, value;
        rocksdb::Transaction *t = nullptr;
        TransactionResource *transaction = nullptr;

        if(!enif_get_resource(env, argv[0], m_Transaction_RESOURCE, (void **) &transaction)) {
            return enif_make_badarg(env);
        }
        t = transaction->transaction;
        if(t == nullptr ) {
            return enif_make_badarg(env);
        }

        if( argc == 3 ) { // default
            if(!enif_inspect_binary(env, argv[1], &key) ||
               !enif_inspect_iolist_as_binary(env, argv[2], &value)) {
                return enif_make_badarg(env);
            }

            enif_make_copy(transaction->env, argv[1]);
            enif_make_copy(transaction->env, argv[2]);

            rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
            rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
            t->Put(key_slice, value_slice);

        } else if( argc == 4 ) { // cf
            if(!enif_get_cf(env, argv[1], &cf_ptr) ||
               !enif_inspect_binary(env, argv[2], &key) ||
               !enif_inspect_iolist_as_binary(env, argv[3], &value)) {
                return enif_make_badarg(env);
            }

            enif_make_copy(transaction->env, argv[1]);
            enif_make_copy(transaction->env, argv[2]);
            enif_make_copy(transaction->env, argv[3]);

            rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
            rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
            erocksdb::ColumnFamilyObject* cf = cf_ptr.get();
            t->Put(cf->m_ColumnFamily, key_slice, value_slice);

        } else {
            // not sure this is possible with modern nifs, but
            return enif_make_badarg(env);
        }
        return ATOM_OK;
    }

    ERL_NIF_TERM
    GetTransaction(ErlNifEnv* env,
                   int argc,
                   const ERL_NIF_TERM argv[])
    {
        ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
        rocksdb::Transaction *t = nullptr;
        TransactionResource *transaction = nullptr;

        if(!enif_get_resource(env, argv[0], m_Transaction_RESOURCE, (void **) &transaction)) {
            return enif_make_badarg(env);
        }
        t = transaction->transaction;
        if(t == nullptr ) {
            return enif_make_badarg(env);
        }

        int i = 1;
        if(argc == 3) {
            i = 2;
        }

        rocksdb::Slice key;
        if(!binary_to_slice(env, argv[i], &key)) {
            return enif_make_badarg(env);
        }

        rocksdb::ReadOptions opts;
        fold(env, argv[i+1], parse_read_option, opts);

        rocksdb::Status status;
        rocksdb::PinnableSlice pvalue;
        if(argc == 3) {
            if(!enif_get_cf(env, argv[1], &cf_ptr)) {
                return enif_make_badarg(env);
            }
            status = t->GetForUpdate(opts, cf_ptr->m_ColumnFamily, key, &pvalue);
        } else {
            status = t->GetForUpdate(opts, transaction->base_db->DefaultColumnFamily(), key, &pvalue);
        }

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
    }

    /*
     * technically the below should work, but there are apparently
     * some issues with regards to using merges within transactions.
     * I'm commenting this out until such a time as rocks better
     * supports merges within transactions.

    ERL_NIF_TERM
    MergeTransaction(ErlNifEnv* env,
                     int argc,
                     const ERL_NIF_TERM argv[])
    {
        ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
        rocksdb::Transaction *t = nullptr;
        TransactionResource *transaction = nullptr;

        if(!enif_get_resource(env, argv[0], m_Transaction_RESOURCE, (void **) &transaction)) {
            return enif_make_badarg(env);
        }
        t = transaction->transaction;
        if(t == nullptr ) {
            return enif_make_badarg(env);
        }

        ErlNifBinary key, value;

        rocksdb::Status status;
        rocksdb::ColumnFamilyHandle *cfh;
        if (argc == 4) {
            if(!enif_get_cf(env, argv[1], &cf_ptr) ||
               !enif_inspect_binary(env, argv[2], &key) ||
               !enif_inspect_iolist_as_binary(env, argv[3], &value)) {
                return enif_make_badarg(env);
            }
            cfh = cf_ptr->m_ColumnFamily;
        } else if (argc == 3) {
            if(!enif_inspect_binary(env, argv[1], &key) ||
               !enif_inspect_iolist_as_binary(env, argv[2], &value)) {
                return enif_make_badarg(env);
            }
            cfh = transaction->base_db->DefaultColumnFamily();
        } else {
            return enif_make_badarg(env);
        }

        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        rocksdb::Slice value_slice(reinterpret_cast<char*>(value.data), value.size);
        status = t->Merge(cfh, key_slice, value_slice);

        if(status.ok()) {
            return ATOM_OK;
        } else {
            return error_tuple(env, ATOM_ERROR, status);
        }
    }

    */

    ERL_NIF_TERM
    DelTransaction(ErlNifEnv* env,
                   int argc,
                   const ERL_NIF_TERM argv[])
    {
        ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
        rocksdb::Transaction *t = nullptr;
        TransactionResource *transaction = nullptr;

        if(!enif_get_resource(env, argv[0], m_Transaction_RESOURCE, (void **) &transaction)) {
            return enif_make_badarg(env);
        }
        t = transaction->transaction;
        if(t == nullptr ) {
            return enif_make_badarg(env);
        }

        ErlNifBinary key;
        rocksdb::Status status;
        rocksdb::ColumnFamilyHandle *cfh;
        if (argc > 2)
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
            cfh = transaction->base_db->DefaultColumnFamily();
        }
        rocksdb::Slice key_slice(reinterpret_cast<char*>(key.data), key.size);
        status = t->Delete(cfh, key_slice);

        if(status.ok()) {
            return ATOM_OK;
        } else {
            return error_tuple(env, ATOM_ERROR, status);
        }
    }

    ERL_NIF_TERM
    IteratorTransaction(ErlNifEnv* env,
                        int argc,
                        const ERL_NIF_TERM argv[]) {
        ReferencePtr<DbObject> db_ptr;
        if(!enif_get_db(env, argv[0], &db_ptr)) {
            return enif_make_badarg(env);
        }

        rocksdb::Transaction *t = nullptr;
        TransactionResource *transaction = nullptr;

        if(!enif_get_resource(env, argv[1], m_Transaction_RESOURCE, (void **) &transaction)) {
            return enif_make_badarg(env);
        }

        t = transaction->transaction;
        if(t == nullptr ) {
            return enif_make_badarg(env);
        }

        int i = argc - 1;

        if(!enif_is_list(env, argv[i])) {
            return enif_make_badarg(env);
        }

        rocksdb::ReadOptions opts;
        ItrBounds bounds;
        auto itr_env = std::make_shared<ErlEnvCtr>();
        if (!parse_iterator_options(env, itr_env->env, argv[i], opts, bounds)) {
            return enif_make_badarg(env);
        }

        ItrObject * itr_ptr;
        rocksdb::Iterator * iterator;

        if (argc == 4) {
            ReferencePtr<ColumnFamilyObject> cf_ptr;
            if(!enif_get_cf(env, argv[2], &cf_ptr)) {
                return enif_make_badarg(env);
            }
            iterator = t->GetIterator(opts, cf_ptr->m_ColumnFamily);
        } else {
            iterator = t->GetIterator(opts);
        }

        itr_ptr = ItrObject::CreateItrObject(db_ptr.get(), itr_env, iterator);

        if(bounds.upper_bound_slice != nullptr) {
            itr_ptr->SetUpperBoundSlice(bounds.upper_bound_slice);
        }

        if(bounds.lower_bound_slice != nullptr) {
            itr_ptr->SetLowerBoundSlice(bounds.lower_bound_slice);
        }

        ERL_NIF_TERM result = enif_make_resource(env, itr_ptr);

        // release reference created during CreateItrObject()
        enif_release_resource(itr_ptr);

        iterator = NULL;
        return enif_make_tuple2(env, ATOM_OK, result);

    }

    ERL_NIF_TERM
    CommitTransaction(ErlNifEnv* env,
                      int /*argc*/,
                      const ERL_NIF_TERM argv[])
    {

        ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
        rocksdb::Transaction *t = nullptr;
        TransactionResource *transaction = nullptr;

        if(!enif_get_resource(env, argv[0], m_Transaction_RESOURCE, (void **) &transaction)) {
            return enif_make_badarg(env);
        }
        t = transaction->transaction;
        if(t == nullptr ) {
            return enif_make_badarg(env);
        }

        rocksdb::Status s = t->Commit();
        delete t;
        transaction->transaction = nullptr;

        if( s.ok() ) {
            return ATOM_OK;
        } else {
            return error_tuple(env, ATOM_ERROR, s);
        }
    }


}
