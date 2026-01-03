
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

#include <string>

#include "atoms.h"
#include "refobjects.h"
#include "rocksdb/db.h"
#include "rocksdb/slice_transform.h"
#include "util.h"

// Erlang helpers:
ERL_NIF_TERM error_einval(ErlNifEnv* env)
{
    return enif_make_tuple2(env, erocksdb::ATOM_ERROR, erocksdb::ATOM_EINVAL);
}

ERL_NIF_TERM error_tuple(ErlNifEnv* env, ERL_NIF_TERM error,
rocksdb::Status& status)
{
    if (status.IsIncomplete())
        return enif_make_tuple2(env, erocksdb::ATOM_ERROR, erocksdb::ATOM_ERROR_INCOMPLETE);

    ERL_NIF_TERM reason = enif_make_string(env, status.ToString().c_str(),
                                           ERL_NIF_LATIN1);
    return enif_make_tuple2(env, erocksdb::ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
}

ERL_NIF_TERM error_tuple(ErlNifEnv* env, ERL_NIF_TERM error, const char* msg)
{
    ERL_NIF_TERM reason = enif_make_string(env, msg, ERL_NIF_LATIN1);
    return enif_make_tuple2(env, erocksdb::ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
}

ERL_NIF_TERM slice_to_binary(ErlNifEnv* env, rocksdb::Slice s)
{
    ERL_NIF_TERM result;
    memcpy(enif_make_new_binary(env, s.size(), &result), s.data(), s.size());
    return result;
}

int
enif_get_std_string(ErlNifEnv* env, ERL_NIF_TERM term, std::string &val)
{
    unsigned len;
    int ret = enif_get_list_length(env, term, &len); // full list iteration
    if(!ret)
        return 0;

    val.resize(len+1); // +1 for terminating null
    ret =  enif_get_string(env, term, &*(val.begin()), val.size(), ERL_NIF_LATIN1); // full list iteration
    if(ret > 0)
    {
        val.resize(ret-1); // trim terminating null
    }
    else if(ret==0)
    {
        val.resize(0);
    }
    else
    {
        // oops string somehow got truncated
        // var is correct size so do nothing
    }
    return 1;
}

int
binary_to_slice(ErlNifEnv* env, ERL_NIF_TERM val, rocksdb::Slice* slice)
{
    ErlNifBinary bin;
    if(!enif_inspect_binary(env, val, &bin))
        return 0;

    *slice = rocksdb::Slice(reinterpret_cast<char*>(bin.data), bin.size);
    return 1;
}

int
enif_get_db(ErlNifEnv* env, ERL_NIF_TERM dbval, erocksdb::ReferencePtr<erocksdb::DbObject>* db_ptr)
{
    db_ptr->assign(erocksdb::DbObject::RetrieveDbObject(env, dbval));

    if(NULL==db_ptr->get())
        return 0;

    if(NULL==db_ptr->get()->m_Db)
        return 0;

    return 1;
}

int
enif_get_cf(ErlNifEnv* env, ERL_NIF_TERM cfval, erocksdb::ReferencePtr<erocksdb::ColumnFamilyObject>* cf_ptr)
{
    cf_ptr->assign(erocksdb::ColumnFamilyObject::RetrieveColumnFamilyObject(env, cfval));

    if(NULL==cf_ptr->get())
        return 0;

    return 1;
}


int
enif_get_backup_engine(
    ErlNifEnv* env, ERL_NIF_TERM val,
    erocksdb::ReferencePtr<erocksdb::BackupEngineObject>* backup_engine_ptr)
{
    backup_engine_ptr->assign(erocksdb::BackupEngineObject::RetrieveBackupEngineObject(env, val));

    if(NULL==backup_engine_ptr->get())
        return 0;

    return 1;
}

int
enif_get_transaction(
    ErlNifEnv* env, ERL_NIF_TERM val,
    erocksdb::ReferencePtr<erocksdb::TransactionObject>* tx_ptr)
{
    tx_ptr->assign(erocksdb::TransactionObject::RetrieveTransactionObject(env, val));

    if(NULL==tx_ptr->get())
        return 0;

    return 1;
}



int parse_int(const std::string& value) {
    size_t endchar;
#ifndef CYGWIN
    int num = std::stoi(value.c_str(), &endchar);
#else
    char* endptr;
    int num = std::strtoul(value.c_str(), &endptr, 0);
    endchar = endptr - value.c_str();
#endif

    if (endchar < value.length()) {
        char c = value[endchar];
        if (c == 'k' || c == 'K')
            num <<= 10;
        else if (c == 'm' || c == 'M')
            num <<= 20;
        else if (c == 'g' || c == 'G')
            num <<= 30;
    }

    return num;
}


uint64_t ParseUint64(const std::string& value) {
  size_t endchar;
#ifndef CYGWIN
  uint64_t num = std::stoull(value.c_str(), &endchar);
#else
  char* endptr;
  uint64_t num = std::strtoul(value.c_str(), &endptr, 0);
  endchar = endptr - value.c_str();
#endif

  if (endchar < value.length()) {
    char c = value[endchar];
    if (c == 'k' || c == 'K')
      num <<= 10LL;
    else if (c == 'm' || c == 'M')
      num <<= 20LL;
    else if (c == 'g' || c == 'G')
      num <<= 30LL;
    else if (c == 't' || c == 'T')
      num <<= 40LL;
  }

  return num;
}
