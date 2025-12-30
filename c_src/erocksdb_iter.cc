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
//
#include <mutex>
#include <vector>
#include <memory>


#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/wide_columns.h"

#include "atoms.h"
#include "erl_nif.h"
#include "erocksdb_db.h"
#include "refobjects.h"
#include "util.h"

#include "erocksdb_iter.h"



ItrBounds::ItrBounds()
    : upper_bound_slice(nullptr),
      lower_bound_slice(nullptr) {}

int
parse_iterator_options(
        ErlNifEnv* env,
        ErlNifEnv* itr_env,
        ERL_NIF_TERM term,
        rocksdb::ReadOptions& opts,
        ItrBounds& bounds)
{
    const ERL_NIF_TERM* option;
    ERL_NIF_TERM head, tail;
    int arity;

    if(!enif_is_list(env, term))
        return 0;

    tail = term;

    while(enif_get_list_cell(env, tail, &head, &tail))
    {
        if (enif_get_tuple(env, head, &arity, &option) && 2==arity)
        {
            if (option[0] == erocksdb::ATOM_VERIFY_CHECKSUMS)
                opts.verify_checksums = (option[1] == erocksdb::ATOM_TRUE);
            else if (option[0] == erocksdb::ATOM_FILL_CACHE)
                opts.fill_cache = (option[1] == erocksdb::ATOM_TRUE);
            else if (option[0] == erocksdb::ATOM_ITERATE_UPPER_BOUND)
            {
                ERL_NIF_TERM upper_bound = enif_make_copy(itr_env, option[1]);
                ErlNifBinary upper_bound_bin;
                if(!enif_inspect_binary(itr_env, upper_bound, &upper_bound_bin))
                    return 0;
                bounds.upper_bound_slice = new rocksdb::Slice(
                        reinterpret_cast<char*>(upper_bound_bin.data),
                        upper_bound_bin.size);
                opts.iterate_upper_bound = bounds.upper_bound_slice;
            }
            else if (option[0] == erocksdb::ATOM_ITERATE_LOWER_BOUND)
            {
                ERL_NIF_TERM lower_bound = enif_make_copy(itr_env, option[1]);
                ErlNifBinary lower_bound_bin;
                if(!enif_inspect_binary(itr_env, lower_bound, &lower_bound_bin))
                {
                    // Clean up already allocated upper_bound_slice
                    delete bounds.upper_bound_slice;
                    bounds.upper_bound_slice = nullptr;
                    return 0;
                }
                bounds.lower_bound_slice = new rocksdb::Slice(
                        reinterpret_cast<char*>(lower_bound_bin.data),
                        lower_bound_bin.size);
                opts.iterate_lower_bound = bounds.lower_bound_slice;
            }
            else if (option[0] == erocksdb::ATOM_TAILING)
                opts.tailing = (option[1] == erocksdb::ATOM_TRUE);
            else if (option[0] == erocksdb::ATOM_TOTAL_ORDER_SEEK)
                opts.total_order_seek = (option[1] == erocksdb::ATOM_TRUE);
            else if (option[0] == erocksdb::ATOM_PREFIX_SAME_AS_START)
                opts.prefix_same_as_start = (option[1] == erocksdb::ATOM_TRUE);
            else if (option[0] == erocksdb::ATOM_SNAPSHOT)
            {
                erocksdb::ReferencePtr<erocksdb::SnapshotObject> snapshot_ptr;
                snapshot_ptr.assign(erocksdb::SnapshotObject::RetrieveSnapshotObject(env, option[1]));

                if(NULL==snapshot_ptr.get())
                {
                    // Clean up already allocated bound slices
                    delete bounds.upper_bound_slice;
                    delete bounds.lower_bound_slice;
                    bounds.upper_bound_slice = nullptr;
                    bounds.lower_bound_slice = nullptr;
                    return 0;
                }

                opts.snapshot = snapshot_ptr->m_Snapshot;
            }
            else if (option[0] == erocksdb::ATOM_AUTO_REFRESH_ITERATOR_WITH_SNAPSHOT)
                opts.auto_refresh_iterator_with_snapshot = (option[1] == erocksdb::ATOM_TRUE);
            else if (option[0] == erocksdb::ATOM_AUTO_READAHEAD_SIZE)
                opts.auto_readahead_size = (option[1] == erocksdb::ATOM_TRUE);
            else if (option[0] == erocksdb::ATOM_ALLOW_UNPREPARED_VALUE)
                opts.allow_unprepared_value = (option[1] == erocksdb::ATOM_TRUE);
        }
    }
    return 1;
}

namespace erocksdb {
  ERL_NIF_TERM
Iterator(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    int i = 1;
    if(argc==3) i = 2;

    if(!enif_is_list(env, argv[i]))
        return enif_make_badarg(env);

    rocksdb::ReadOptions *opts = new rocksdb::ReadOptions;
    ItrBounds bounds;
    auto itr_env = std::make_shared<ErlEnvCtr>();
    if (!parse_iterator_options(env, itr_env->env, argv[i], *opts, bounds))
    {
        delete opts;
        return enif_make_badarg(env);
    }


    ItrObject * itr_ptr;
    rocksdb::Iterator * iterator;

    if(argc==3)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
        {
            delete opts;
            delete bounds.upper_bound_slice;
            delete bounds.lower_bound_slice;
            return enif_make_badarg(env);
        }
        iterator = db_ptr->m_Db->NewIterator(*opts, cf_ptr->m_ColumnFamily);
    }
    else
    {
        iterator = db_ptr->m_Db->NewIterator(*opts);
    }

    itr_ptr = ItrObject::CreateItrObject(db_ptr.get(), itr_env, iterator);

    if(bounds.upper_bound_slice != nullptr)
    {
        itr_ptr->SetUpperBoundSlice(bounds.upper_bound_slice);
    }

    if(bounds.lower_bound_slice != nullptr)
    {
        itr_ptr->SetLowerBoundSlice(bounds.lower_bound_slice);
    }

    ERL_NIF_TERM result = enif_make_resource(env, itr_ptr);

    // release reference created during CreateItrObject()
    enif_release_resource(itr_ptr);
    delete opts;
    iterator = NULL;
    return enif_make_tuple2(env, ATOM_OK, result);

}   // erocksdb::Iterator

ERL_NIF_TERM
Iterators(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if(!enif_is_list(env, argv[1]) || !enif_is_list(env, argv[2]))
       return enif_make_badarg(env);

    rocksdb::ReadOptions opts;
    ItrBounds bounds;
    auto itr_env = std::make_shared<ErlEnvCtr>();
    if (!parse_iterator_options(env, itr_env->env, argv[2], opts, bounds))
    {
        return enif_make_badarg(env);
    }

    std::vector<rocksdb::ColumnFamilyHandle*> column_families;
    ERL_NIF_TERM head, tail = argv[1];
    while(enif_get_list_cell(env, tail, &head, &tail))
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        cf_ptr.assign(ColumnFamilyObject::RetrieveColumnFamilyObject(env, head));
        ColumnFamilyObject* cf = cf_ptr.get();
        if(NULL == cf)
        {
            delete bounds.upper_bound_slice;
            delete bounds.lower_bound_slice;
            return enif_make_badarg(env);
        }
        column_families.push_back(cf->m_ColumnFamily);
    }

    std::vector<rocksdb::Iterator*> iterators;
    db_ptr->m_Db->NewIterators(opts, column_families, &iterators);


    ERL_NIF_TERM result = enif_make_list(env, 0);
    ItrObject * itr_ptr;
    try {
        for (size_t i = 0; i < iterators.size(); i++) {
            itr_ptr = ItrObject::CreateItrObject(db_ptr.get(), itr_env, iterators[i]);

            if(bounds.upper_bound_slice != nullptr)
            {
                itr_ptr->SetUpperBoundSlice(bounds.upper_bound_slice);
            }

            if(bounds.lower_bound_slice != nullptr)
            {
                itr_ptr->SetLowerBoundSlice(bounds.lower_bound_slice);
            }
            ERL_NIF_TERM itr_res = enif_make_resource(env, itr_ptr);
            result = enif_make_list_cell(env, itr_res, result);
            enif_release_resource(itr_ptr);
        }
    } catch (const std::exception&) {
        // pass through and return nullptr
    }
    ERL_NIF_TERM result_out;
    enif_make_reverse_list(env, result, &result_out);

    return enif_make_tuple2(env, erocksdb::ATOM_OK, result_out);
}

ERL_NIF_TERM
CoalescingIterator(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if(!enif_is_list(env, argv[1]) || !enif_is_list(env, argv[2]))
       return enif_make_badarg(env);

    rocksdb::ReadOptions opts;
    ItrBounds bounds;
    auto itr_env = std::make_shared<ErlEnvCtr>();
    if (!parse_iterator_options(env, itr_env->env, argv[2], opts, bounds))
    {
        return enif_make_badarg(env);
    }

    std::vector<rocksdb::ColumnFamilyHandle*> column_families;
    ERL_NIF_TERM head, tail = argv[1];
    while(enif_get_list_cell(env, tail, &head, &tail))
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        cf_ptr.assign(ColumnFamilyObject::RetrieveColumnFamilyObject(env, head));
        if(NULL == cf_ptr.get())
            return enif_make_badarg(env);
        column_families.push_back(cf_ptr->m_ColumnFamily);
    }

    std::unique_ptr<rocksdb::Iterator> iterator =
        db_ptr->m_Db->NewCoalescingIterator(opts, column_families);

    if (!iterator) {
        rocksdb::Status status = rocksdb::Status::NotSupported("CoalescingIterator not available");
        return error_tuple(env, ATOM_ERROR, status);
    }

    ItrObject* itr_ptr = ItrObject::CreateItrObject(db_ptr.get(), itr_env, iterator.release());

    if(bounds.upper_bound_slice != nullptr)
    {
        itr_ptr->SetUpperBoundSlice(bounds.upper_bound_slice);
    }

    if(bounds.lower_bound_slice != nullptr)
    {
        itr_ptr->SetLowerBoundSlice(bounds.lower_bound_slice);
    }

    ERL_NIF_TERM result = enif_make_resource(env, itr_ptr);
    enif_release_resource(itr_ptr);
    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
IteratorMove(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& itr_handle_ref   = argv[0];
    const ERL_NIF_TERM& action_or_target = argv[1];


    ReferencePtr<ItrObject> itr_ptr;
    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_handle_ref));

    if(NULL==itr_ptr.get())
    {
        return enif_make_badarg(env);
    }

    rocksdb::Iterator* itr = itr_ptr->m_Iterator;
    rocksdb::Slice key;

    if(enif_is_atom(env, action_or_target))
    {
        if(ATOM_FIRST == action_or_target) itr->SeekToFirst();
        if(ATOM_LAST == action_or_target) itr->SeekToLast();

        if(!itr->Valid())
        {
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
        }

        if(ATOM_NEXT == action_or_target) itr->Next();
        if(ATOM_PREV == action_or_target) itr->Prev();
    }
    else if(enif_is_tuple(env, action_or_target))
    {
        int arity;
        const ERL_NIF_TERM* seek;
        if(enif_get_tuple(env, action_or_target, &arity, &seek) && 2==arity)
        {
            if(seek[0] == erocksdb::ATOM_SEEK_FOR_PREV)
            {
                if(!binary_to_slice(env, seek[1], &key))
                    return error_einval(env);
                itr->SeekForPrev(key);
            }
            else if(seek[0] == erocksdb::ATOM_SEEK)
            {
                if(!binary_to_slice(env, seek[1], &key))
                    return error_einval(env);
                itr->Seek(key);
            }
            else
            {
                return enif_make_badarg(env);
            }
        }
        else
        {
            return enif_make_badarg(env);
        }
    }
    else
    {
        if(!binary_to_slice(env, action_or_target, &key))
            return error_einval(env);
        itr->Seek(key);
    }

    if(!itr->Valid())
    {
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
    }

    rocksdb::Status status = itr->status();

    if(!status.ok())
    {
        return error_tuple(env, ATOM_ERROR, status);
    }


    return enif_make_tuple3(env, ATOM_OK, slice_to_binary(env, itr->key()), slice_to_binary(env, itr->value()));

}   // erocksdb::IteratorMove

ERL_NIF_TERM
IteratorRefresh(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& itr_handle_ref = argv[0];

    ReferencePtr<ItrObject> itr_ptr;
    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_handle_ref));

    if(NULL==itr_ptr.get())
    {
        return enif_make_badarg(env);
    }

    rocksdb::Iterator* itr = itr_ptr->m_Iterator;
    rocksdb::Status status = itr->Refresh();
    if(!status.ok())
    {
        return error_tuple(env, ATOM_ERROR, status);
    }

    return(ATOM_OK);

}   // erocksdb::IteratorRefresh

ERL_NIF_TERM
IteratorPrepareValue(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& itr_handle_ref = argv[0];

    ReferencePtr<ItrObject> itr_ptr;
    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_handle_ref));

    if(NULL==itr_ptr.get())
    {
        return enif_make_badarg(env);
    }

    rocksdb::Iterator* itr = itr_ptr->m_Iterator;

    if(!itr->Valid())
    {
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
    }

    if(!itr->PrepareValue())
    {
        rocksdb::Status status = itr->status();
        return error_tuple(env, ATOM_ERROR, status);
    }

    return(ATOM_OK);

}   // erocksdb::IteratorPrepareValue

ERL_NIF_TERM
IteratorClose(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    ItrObject * itr_ptr;

    itr_ptr=ItrObject::RetrieveItrObject(env, argv[0], true);
    if (NULL!=itr_ptr)
    {
        // set closing flag ... atomic likely unnecessary (but safer)
        ErlRefObject::InitiateCloseRequest(itr_ptr);
        itr_ptr=NULL;
        return(ATOM_OK);
    }   // if
    else
    {
        ERL_NIF_TERM bad_arg=enif_make_badarg(env);
        return(bad_arg);
    }   // else

}   // erocksdb:IteratorClose

ERL_NIF_TERM
IteratorColumns(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& itr_handle_ref = argv[0];

    ReferencePtr<ItrObject> itr_ptr;
    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_handle_ref));

    if(NULL==itr_ptr.get())
    {
        return enif_make_badarg(env);
    }

    rocksdb::Iterator* itr = itr_ptr->m_Iterator;

    if(!itr->Valid())
    {
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
    }

    rocksdb::Status status = itr->status();
    if(!status.ok())
    {
        return error_tuple(env, ATOM_ERROR, status);
    }

    // Get columns from iterator
    const rocksdb::WideColumns& cols = itr->columns();

    // Build result list: [{Name, Value}, ...]
    ERL_NIF_TERM result_list = enif_make_list(env, 0);

    // Build in reverse order (since we prepend)
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
}   // erocksdb::IteratorColumns

}
