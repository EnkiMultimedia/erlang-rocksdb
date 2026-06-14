// -------------------------------------------------------------------
// Copyright (c) 2017-2025 Benoit Chesneau. All Rights Reserved.
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

#include "env.h"

#include <array>
#include <cstring>
#include <memory>
#include <string>

#include "aes_block_cipher.h"
#include "atoms.h"
#include "util.h"

namespace erocksdb {

ErlNifResourceType * ManagedEnv::m_Env_RESOURCE(NULL);

void
ManagedEnv::CreateEnvType(
    ErlNifEnv * env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_Env_RESOURCE = enif_open_resource_type(env, NULL, "erocksdb_Env",
                                            &ManagedEnv::EnvResourceCleanup,
                                            flags, NULL);
    return;
}   // ManagedEnv::CreateEnvType


void
ManagedEnv::EnvResourceCleanup(
    ErlNifEnv * /*env*/,
    void * arg)
{
    // Run the C++ destructor so owned wrapper envs (memenv, encrypted) and the
    // held provider/cipher shared_ptrs are released. The resource memory itself
    // is freed by the runtime.
    ManagedEnv * env_ptr = reinterpret_cast<ManagedEnv*>(arg);
    env_ptr->~ManagedEnv();
    return;
}

ManagedEnv *
ManagedEnv::CreateEnvResource(rocksdb::Env * env)
{
    return CreateEnvResource(env, true);
}

ManagedEnv *
ManagedEnv::CreateEnvResource(rocksdb::Env * env, bool owns)
{
    ManagedEnv * ret_ptr;
    void * alloc_ptr;

    alloc_ptr=enif_alloc_resource(m_Env_RESOURCE, sizeof(ManagedEnv));
    ret_ptr=new (alloc_ptr) ManagedEnv(env, owns);
    return(ret_ptr);
}

ManagedEnv *
ManagedEnv::RetrieveEnvResource(ErlNifEnv * Env, const ERL_NIF_TERM & EnvTerm)
{
    ManagedEnv * ret_ptr;
    if (!enif_get_resource(Env, EnvTerm, m_Env_RESOURCE, (void **)&ret_ptr))
        return NULL;
    return ret_ptr;
}

ManagedEnv::ManagedEnv(rocksdb::Env * Env) : env_(Env), owns_env_(true) {}

ManagedEnv::ManagedEnv(rocksdb::Env * Env, bool owns)
    : env_(Env), owns_env_(owns) {}

ManagedEnv::~ManagedEnv()
{
    // Only delete envs we own (memenv, encrypted wrapper). Never delete the
    // rocksdb::Env::Default() singleton: it is process-wide and shared.
    if(owns_env_ && env_)
    {
        delete env_;
    }
    env_ = NULL;

    // Release the provider/cipher last, after the env that may reference them.
    provider_.reset();
    cipher_.reset();

    return;
}

void
ManagedEnv::SetEncryption(
    std::shared_ptr<rocksdb::EncryptionProvider> provider,
    std::shared_ptr<rocksdb::BlockCipher> cipher)
{
    provider_ = std::move(provider);
    cipher_ = std::move(cipher);
}

const rocksdb::Env* ManagedEnv::env() { return env_; }

// Extract a 32-byte AES-256 key term from either {encrypted, Key} or
// #{encrypted => Key}. Returns true and fills key_term on success.
static bool
GetEncryptedKeyTerm(
    ErlNifEnv *env,
    const ERL_NIF_TERM spec,
    ERL_NIF_TERM *key_term)
{
    // Tuple form: {encrypted, Key}
    int arity;
    const ERL_NIF_TERM *tuple;
    if (enif_get_tuple(env, spec, &arity, &tuple))
    {
        if (arity == 2 && tuple[0] == erocksdb::ATOM_ENCRYPTED)
        {
            *key_term = tuple[1];
            return true;
        }
        return false;
    }

    // Map form: #{encrypted => Key}
    if (enif_is_map(env, spec))
    {
        return enif_get_map_value(env, spec, erocksdb::ATOM_ENCRYPTED,
                                  key_term) != 0;
    }

    return false;
}

ERL_NIF_TERM
NewEnv(
    ErlNifEnv *env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    ManagedEnv *env_ptr;
    rocksdb::Env *rdb_env;
    bool owns = true;

    if (argv[0] == erocksdb::ATOM_DEFAULT)
    {
        // Default singleton: never owned, never deleted.
        rdb_env = rocksdb::Env::Default();
        owns = false;
        env_ptr = ManagedEnv::CreateEnvResource(rdb_env, owns);
    }
    else if (argv[0] == erocksdb::ATOM_MEMENV)
    {
        rdb_env = rocksdb::NewMemEnv(rocksdb::Env::Default());
        env_ptr = ManagedEnv::CreateEnvResource(rdb_env, true);
    }
    else
    {
        // Encrypted env: {encrypted, Key} or #{encrypted => Key}, Key a 32-byte
        // binary (AES-256).
        ERL_NIF_TERM key_term;
        if (!GetEncryptedKeyTerm(env, argv[0], &key_term))
            return enif_make_badarg(env);

        ErlNifBinary key_bin;
        if (!enif_inspect_binary(env, key_term, &key_bin))
            return enif_make_badarg(env);

        if (key_bin.size != AesBlockCipher::kKeyBytes)
            return enif_make_badarg(env);

        std::array<unsigned char, AesBlockCipher::kKeyBytes> key;
        std::memcpy(key.data(), key_bin.data, AesBlockCipher::kKeyBytes);

        std::shared_ptr<rocksdb::BlockCipher> cipher =
            std::make_shared<AesBlockCipher>(key);
        std::shared_ptr<rocksdb::EncryptionProvider> provider =
            rocksdb::EncryptionProvider::NewCTRProvider(cipher);
        if (!provider)
            return enif_make_badarg(env);

        // Wrapper env owns nothing of the default singleton; NewEncryptedEnv
        // returns a new Env that we own and must delete.
        rdb_env = rocksdb::NewEncryptedEnv(rocksdb::Env::Default(), provider);
        if (rdb_env == nullptr)
            return enif_make_badarg(env);

        env_ptr = ManagedEnv::CreateEnvResource(rdb_env, true);
        env_ptr->SetEncryption(std::move(provider), std::move(cipher));
    }

    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(env, env_ptr);
    // clear the automatic reference from enif_alloc_resource in EnvObject
    enif_release_resource(env_ptr);
    rdb_env = NULL;
    return enif_make_tuple2(env, ATOM_OK, result);
}


ERL_NIF_TERM
SetEnvBackgroundThreads(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    ManagedEnv* env_ptr = ManagedEnv::RetrieveEnvResource(env, argv[0]);

     if(NULL==env_ptr)
        return enif_make_badarg(env);
    rocksdb::Env* rdb_env = (rocksdb::Env* )env_ptr->env();

    int n;
    if(!enif_get_int(env, argv[1], &n))
        return enif_make_badarg(env);

    if(argc==3)
    {
        if(argv[2] == ATOM_PRIORITY_HIGH)
            rdb_env->SetBackgroundThreads(n, rocksdb::Env::Priority::HIGH);
        else if((argv[2] == ATOM_PRIORITY_LOW))
            rdb_env->SetBackgroundThreads(n, rocksdb::Env::Priority::LOW);
        else
            return enif_make_badarg(env);
    }
    else
    {
        rdb_env->SetBackgroundThreads(n);
    }

    return ATOM_OK;
}   // erocksdb::SetBackgroundThreads


ERL_NIF_TERM
DestroyEnv(
        ErlNifEnv* env,
        int /*argc*/,
        const ERL_NIF_TERM argv[])
{

    ManagedEnv* env_ptr = ManagedEnv::RetrieveEnvResource(env, argv[0]);
    if(nullptr==env_ptr)
        return ATOM_OK;

    env_ptr = nullptr;
    return ATOM_OK;
}   // erocksdb::DestroyEnv



}

