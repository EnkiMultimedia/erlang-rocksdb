// -------------------------------------------------------------------
// Copyright (c) 2017 Benoit Chesneau. All Rights Reserved.
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

#include "erocksdb.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif

#ifndef INCL_UTIL_H
    #include "util.h"
#endif

namespace erocksdb {

ERL_NIF_TERM
DefaultEnv(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    EnvObject* env_ptr;
    rocksdb::Env* default_env = rocksdb::Env::Default();

    env_ptr = EnvObject::CreateEnvObject(default_env);

    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(env, env_ptr);
    // clear the automatic reference from enif_alloc_resource in EnvObject
    enif_release_resource(env_ptr);
    default_env = NULL;

    return enif_make_tuple2(env, ATOM_OK, result);
}   // erocksdb::SnapShot


ERL_NIF_TERM
MemEnv(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    EnvObject* env_ptr;
    rocksdb::Env* mem_env = rocksdb::NewMemEnv(rocksdb::Env::Default());

    env_ptr = EnvObject::CreateEnvObject(mem_env);

    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(env, env_ptr);
    // clear the automatic reference from enif_alloc_resource in EnvObject
    enif_release_resource(env_ptr);
    mem_env = NULL;

    return enif_make_tuple2(env, ATOM_OK, result);
}   // erocksdb::SnapShot


ERL_NIF_TERM
SetBackgroundThreads(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    ReferencePtr<EnvObject> env_ptr;
    int n;

    // retrieve env pointer
    if(!enif_get_db_env(env, argv[0], &env_ptr))
        return enif_make_badarg(env);

    if(!enif_get_int(env, argv[1], &n))
        return enif_make_badarg(env);

    if(argc==3)
    {
        if(argv[2] == ATOM_PRIORITY_HIGH)
             env_ptr->m_Env->SetBackgroundThreads(n, rocksdb::Env::Priority::HIGH);
        else if((argv[2] == ATOM_PRIORITY_LOW))
            env_ptr->m_Env->SetBackgroundThreads(n, rocksdb::Env::Priority::LOW);
        else
            return enif_make_badarg(env);
    }
    else
    {
        env_ptr->m_Env->SetBackgroundThreads(n);
    }

    return ATOM_OK;
}   // erocksdb::SetBackgroundThreads


ERL_NIF_TERM
DestroyEnv(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    ReferencePtr<EnvObject> env_ptr;

    // retrieve env pointer
    env_ptr.assign(EnvObject::RetrieveEnvObject(env, argv[0]));
    if(NULL==env_ptr.get())
        return ATOM_OK;

    ErlRefObject::InitiateCloseRequest(env_ptr.get());
    return ATOM_OK;
}   // erocksdb::DestroyEnv



}

