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


#include <vector>

#include "rocksdb/db.h"
#include "atoms.h"
#include "erocksdb_db.h"
#include "erl_nif.h"
#include "refobjects.h"
#include "util.h"

namespace erocksdb {

ERL_NIF_TERM
ListColumnFamilies(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    char db_name[4096];

    if(!enif_get_string(env, argv[0], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[1]))
    {
        return enif_make_badarg(env);
    }

    // parse main db options
    rocksdb::DBOptions db_opts;
    fold(env, argv[1], parse_db_option, db_opts);

    std::vector<std::string> column_family_names;

    rocksdb::Status status = rocksdb::DB::ListColumnFamilies(db_opts, db_name, &column_family_names);
    if(!status.ok())
        return error_tuple(env, ATOM_ERROR_DB_OPEN, status);

    ERL_NIF_TERM result = enif_make_list(env, 0);
    try {
        for (size_t i = 0; i < column_family_names.size(); i++) {
            ERL_NIF_TERM cf_name = enif_make_string(env, column_family_names[i].c_str(), ERL_NIF_LATIN1);
            result = enif_make_list_cell(env, cf_name, result);
        }
    } catch (const std::exception& e) {
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_string(env, e.what(), ERL_NIF_LATIN1));
    }

    ERL_NIF_TERM result_out;
    enif_make_reverse_list(env, result, &result_out);

    return enif_make_tuple2(env, ATOM_OK, result_out);
} // erocksdb::ListColumnFamilies

ERL_NIF_TERM
CreateColumnFamily(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;

    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if(NULL==db_ptr.get() || 0!=db_ptr->m_CloseRequested)
      return enif_make_badarg(env);

    char cf_name[4096];
    rocksdb::ColumnFamilyOptions opts;
    if(!enif_get_string(env, argv[1], cf_name, sizeof(cf_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[2]))
    {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM result = fold(env, argv[2], parse_cf_option, opts);
    if (result != erocksdb::ATOM_OK)
    {
        return result;
    }

    rocksdb::ColumnFamilyHandle* handle;
    rocksdb::Status status;
    status = db_ptr->m_Db->CreateColumnFamily(opts, cf_name, &handle);

    if (status.ok())
    {
        ColumnFamilyObject * handle_ptr = ColumnFamilyObject::CreateColumnFamilyObject(db_ptr.get(), handle);
        ERL_NIF_TERM res = enif_make_resource(env, handle_ptr);
        enif_release_resource(handle_ptr);
        return enif_make_tuple2(env, ATOM_OK, res);
    }

    return error_tuple(env, ATOM_ERROR, status);

}   // erocksdb::CreateColumnFamily

ERL_NIF_TERM
DropColumnFamily(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ColumnFamilyObject *cf;
    rocksdb::Status status;
    if (argc > 1) {
        if (!enif_get_db(env, argv[0], &db_ptr) || !enif_get_cf(env, argv[1], &cf_ptr)) 
            return enif_make_badarg(env);
        cf = cf_ptr.get();
        status = db_ptr->m_Db->DropColumnFamily(cf->m_ColumnFamily);
    } else {
        if (!enif_get_cf(env, argv[0], &cf_ptr))
            return enif_make_badarg(env);
        // release snapshot object
        cf = cf_ptr.get();
        status = cf->m_DbPtr->m_Db->DropColumnFamily(cf->m_ColumnFamily);
    }
    if(status.ok())
    {
        // don't close it until someone calls destroy
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, status);
}   // erocksdb::DropColumnFamily


ERL_NIF_TERM
DestroyColumnFamily(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;
    ColumnFamilyObject *cf;
    rocksdb::Status status;
    if (argc > 1)
    {
        if (!enif_get_db(env, argv[0], &db_ptr) || !enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);
        cf = cf_ptr.get();
        status = db_ptr->m_Db->DestroyColumnFamilyHandle(cf->m_ColumnFamily);
    }
    else
    {
        if (!enif_get_cf(env, argv[0], &cf_ptr))
            return enif_make_badarg(env);
        cf = cf_ptr.get();
        status = cf->m_DbPtr->m_Db->DestroyColumnFamilyHandle(cf->m_ColumnFamily);
    }
    cf->m_ColumnFamily = NULL;
    if (status.ok())
    {
        // don't close it until someone calls destroy
        // set closing flag
        ErlRefObject::InitiateCloseRequest(cf);
        return ATOM_OK;
    }
    return error_tuple(env, ATOM_ERROR, status);
} // erocksdb::DropColumnFa

}
