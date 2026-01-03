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

#pragma once
#ifndef INCL_PESSIMISTIC_TRANSACTION_H
#define INCL_PESSIMISTIC_TRANSACTION_H

#include "erl_nif.h"

namespace erocksdb {

// Transaction creation and lifecycle
ERL_NIF_TERM NewPessimisticTransaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM ReleasePessimisticTransaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// Basic operations
ERL_NIF_TERM PessimisticTransactionPut(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionGet(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionGetForUpdate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionMultiGet(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionMultiGetForUpdate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionDelete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// Transaction control
ERL_NIF_TERM PessimisticTransactionCommit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionRollback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// Iterator
ERL_NIF_TERM PessimisticTransactionIterator(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// Savepoint operations
ERL_NIF_TERM PessimisticTransactionSetSavepoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionRollbackToSavepoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionPopSavepoint(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// Transaction information
ERL_NIF_TERM PessimisticTransactionGetId(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM PessimisticTransactionGetWaitingTxns(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

}  // namespace erocksdb

#endif  // INCL_PESSIMISTIC_TRANSACTION_H
