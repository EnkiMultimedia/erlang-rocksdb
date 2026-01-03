// -------------------------------------------------------------------
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
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
#ifndef INCL_EROCKSB_DB_H
#define INCL_EROCKSB_DB_H

#include "erl_nif.h"

// Forward declaration
namespace rocksdb {
    struct DBOptions;
    struct ColumnFamilyOptions;
    struct ReadOptions;
    struct WriteOptions;
    struct TransactionDBOptions;
    struct TransactionOptions;
}

ERL_NIF_TERM parse_db_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::DBOptions& opts);
ERL_NIF_TERM parse_cf_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::ColumnFamilyOptions& opts);
ERL_NIF_TERM parse_read_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::ReadOptions& opts);
ERL_NIF_TERM parse_write_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::WriteOptions& opts);
ERL_NIF_TERM parse_txn_db_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::TransactionDBOptions& opts);

namespace erocksdb {
    ERL_NIF_TERM OpenPessimisticTransactionDB(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
}

#endif
