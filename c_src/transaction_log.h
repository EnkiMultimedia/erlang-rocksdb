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

#pragma once
#ifndef INCL_TRANSACTIONS_H
#define INCL_TRANSACTIONS_H

#include <iostream>
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/transaction_log.h"
#include "erl_nif.h"
#include "atoms.h"
#include "util.h"

namespace erocksdb {

class TransactionLogHandler : public rocksdb::WriteBatch::Handler
{
public:
    ERL_NIF_TERM t_List;

    TransactionLogHandler(ErlNifEnv* env);

    virtual rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key,
                             const rocksdb::Slice& value)
    {
        ERL_NIF_TERM row;
        ERL_NIF_TERM key_bin = slice_to_binary(t_Env, key);
        ERL_NIF_TERM value_bin = slice_to_binary(t_Env, value);

        if (column_family_id == 0) {
            row = enif_make_tuple3(t_Env, ATOM_PUT, key_bin, value_bin);
        } else {
            ERL_NIF_TERM cf_id = enif_make_uint(t_Env, column_family_id);
            row = enif_make_tuple4(t_Env, ATOM_PUT, cf_id, key_bin, value_bin);
        }

        // append to list
        t_List = enif_make_list_cell(t_Env, row, t_List);

        return rocksdb::Status::OK();
    }

    virtual rocksdb::Status MergeCF(uint32_t column_family_id, const rocksdb::Slice& key,
                             const rocksdb::Slice& value)
    {
        ERL_NIF_TERM row;
        ERL_NIF_TERM key_bin = slice_to_binary(t_Env, key);
        ERL_NIF_TERM value_bin = slice_to_binary(t_Env, value);

        if (column_family_id == 0) {
            row = enif_make_tuple3(t_Env, ATOM_MERGE, key_bin, value_bin);
        } else {
            ERL_NIF_TERM cf_id = enif_make_uint(t_Env, column_family_id);
            row = enif_make_tuple4(t_Env, ATOM_MERGE, cf_id, key_bin, value_bin);
        }

        // append to list
        t_List = enif_make_list_cell(t_Env, row, t_List);

        return rocksdb::Status::OK();
    }

    virtual rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key)
    {
        ERL_NIF_TERM row;
        ERL_NIF_TERM key_bin = slice_to_binary(t_Env, key);

        if (column_family_id == 0) {
            row = enif_make_tuple2(t_Env, ATOM_DELETE, key_bin);
        } else {
            ERL_NIF_TERM cf_id = enif_make_uint(t_Env, column_family_id);
            row = enif_make_tuple3(t_Env, ATOM_DELETE, cf_id, key_bin);
        }

        // append to list
        t_List = enif_make_list_cell(t_Env, row, t_List);
        return rocksdb::Status::OK();
    }

    virtual rocksdb::Status SingleDeleteCF(uint32_t column_family_id, const rocksdb::Slice& key)
    {
        ERL_NIF_TERM row;
        ERL_NIF_TERM key_bin = slice_to_binary(t_Env, key);

        if (column_family_id == 0) {
            row = enif_make_tuple2(t_Env, ATOM_SINGLE_DELETE, key_bin);
        } else {
            ERL_NIF_TERM cf_id = enif_make_uint(t_Env, column_family_id);
            row = enif_make_tuple3(t_Env, ATOM_SINGLE_DELETE, cf_id, key_bin);
        }

        // append to list
        t_List = enif_make_list_cell(t_Env, row, t_List);

        return rocksdb::Status::OK();
    }

protected:
    ErlNifEnv* t_Env;
}; // class TransactionLogHandler

}

#endif //INCL_TRANSACTIONS_H
