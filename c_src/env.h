// Copyright (c) 2016-2026 Benoit Chesneau
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

#pragma once
#ifndef INCL_ENV_H
#define INCL_ENV_H

#include <memory>
#include <utility>

#include "erl_nif.h"

#include "rocksdb/env.h"
#include "rocksdb/env_encryption.h"


namespace erocksdb {

  class ManagedEnv {
    protected:
      static ErlNifResourceType* m_Env_RESOURCE;

    public:
      explicit ManagedEnv(rocksdb::Env * Env);

      // Wrapper env that owns its base env (e.g. memenv, encrypted env).
      // owns == true means the destructor will delete env_; the default env
      // singleton must NEVER be deleted, so it is created with owns == false.
      ManagedEnv(rocksdb::Env * Env, bool owns);

      ~ManagedEnv();

      const rocksdb::Env* env();

      // Keep the encryption provider and cipher alive for the lifetime of the
      // env. These are empty for non-encrypted envs.
      void SetEncryption(std::shared_ptr<rocksdb::EncryptionProvider> provider,
                         std::shared_ptr<rocksdb::BlockCipher> cipher);

      static void CreateEnvType(ErlNifEnv * Env);
      static void EnvResourceCleanup(ErlNifEnv *Env, void * Arg);

      static ManagedEnv * CreateEnvResource(rocksdb::Env * env);
      static ManagedEnv * CreateEnvResource(rocksdb::Env * env, bool owns);
      static ManagedEnv * RetrieveEnvResource(ErlNifEnv * Env, const ERL_NIF_TERM & EnvTerm);

    private:
      rocksdb::Env* env_;
      bool owns_env_;
      std::shared_ptr<rocksdb::EncryptionProvider> provider_;
      std::shared_ptr<rocksdb::BlockCipher> cipher_;
  };

  /**
   * Holds a reference on a ManagedEnv resource for as long as something uses
   * its rocksdb::Env (a database, an SST writer or reader). Without it the env
   * is freed when its Erlang term is garbage collected, while still in use.
   */
  class EnvRef {
    public:
      EnvRef() : env_(nullptr) {}
      ~EnvRef() { Reset(); }

      EnvRef(const EnvRef &) = delete;
      EnvRef & operator=(const EnvRef &) = delete;

      // Take a new reference on env.
      void Keep(ManagedEnv * env);

      // Take over the reference the caller holds on a new resource.
      void Adopt(ManagedEnv * env);

      void Swap(EnvRef & other) { std::swap(env_, other.env_); }

      void Reset();

      ManagedEnv * get() const { return env_; }

    private:
      ManagedEnv * env_;
  };

  // Apply the env options of an options list to opts: {env, Env},
  // {env, memenv} and {in_memory, true}; the last one wins. ref keeps the env
  // alive and must outlive every use of opts.env. A memory env is created
  // here and owned by ref.
  void ResolveEnvOption(ErlNifEnv * env, ERL_NIF_TERM options,
                        rocksdb::DBOptions & opts, EnvRef * ref);

}

#endif // INCL_ENV_H
