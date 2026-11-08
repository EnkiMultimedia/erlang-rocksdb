// Copyright (c) 2018 Sergey Yelin
// Copyright (c) 2018-2025 Benoit Chesneau
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

#include "rocksdb/rate_limiter.h"
#include "rate_limiter.h"
#include "atoms.h"


namespace erocksdb {

ErlNifResourceType * RateLimiter::m_RateLimiter_RESOURCE(NULL);

void
RateLimiter::CreateRateLimiterType(ErlNifEnv * env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_RateLimiter_RESOURCE = enif_open_resource_type(env, NULL, "erocksdb_RateLimiter",
                                            &RateLimiter::RateLimiterResourceCleanup,
                                            flags, NULL);
    return;
}   // RateLimiter::CreateRateLimiterType


void
RateLimiter::RateLimiterResourceCleanup(ErlNifEnv * /*env*/, void * arg)
{
    RateLimiter* rate_limiter_ptr = (RateLimiter*)arg;
    rate_limiter_ptr->~RateLimiter();
}   // RateLimiter::RateLimiterResourceCleanup


RateLimiter *
RateLimiter::CreateRateLimiterResource(std::shared_ptr<rocksdb::RateLimiter> rate_limiter)
{
    RateLimiter * ret_ptr;
    void * alloc_ptr;

    alloc_ptr=enif_alloc_resource(m_RateLimiter_RESOURCE, sizeof(RateLimiter));
    ret_ptr=new (alloc_ptr) RateLimiter(rate_limiter);
    return(ret_ptr);
}

RateLimiter *
RateLimiter::RetrieveRateLimiterResource(ErlNifEnv * Env, const ERL_NIF_TERM & RateLimiterTerm)
{
    RateLimiter * ret_ptr;
    if (!enif_get_resource(Env, RateLimiterTerm, m_RateLimiter_RESOURCE, (void **)&ret_ptr))
        return NULL;
    return ret_ptr;
}

RateLimiter::RateLimiter(std::shared_ptr<rocksdb::RateLimiter> RateLimiter) : rate_limiter_(RateLimiter) {}

RateLimiter::~RateLimiter()
{
    if(rate_limiter_)
    {
        rate_limiter_ = NULL;
    }

    return;
}

std::shared_ptr<rocksdb::RateLimiter> RateLimiter::rate_limiter() {
    std::lock_guard<std::mutex> lock(mutex_);
    return rate_limiter_;
}

void RateLimiter::Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    rate_limiter_.reset();
}


ERL_NIF_TERM
NewRateLimiter(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    if(argc!=2)
        return enif_make_badarg(env);

    ErlNifUInt64 rate_bytes_per_sec;
    RateLimiter* rate_limiter_ptr;
    if(!enif_get_uint64(env, argv[0], &rate_bytes_per_sec))
        return enif_make_badarg(env);

    bool auto_tuned = argv[1] == erocksdb::ATOM_TRUE;
    std::shared_ptr<rocksdb::RateLimiter> rate_limiter =
        std::shared_ptr<rocksdb::RateLimiter>(
            rocksdb::NewGenericRateLimiter(
                rate_bytes_per_sec,
                100 * 1000,  // Default value
                10, // Default value
                rocksdb::RateLimiter::Mode::kWritesOnly, // Default value
                auto_tuned));
    rate_limiter_ptr = RateLimiter::CreateRateLimiterResource(rate_limiter);
    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(env, rate_limiter_ptr);
    // clear the automatic reference from enif_alloc_resource in EnvObject
    enif_release_resource(rate_limiter_ptr);
    rate_limiter.reset();
    rate_limiter = NULL;
    return enif_make_tuple2(env, ATOM_OK, result);
}



ERL_NIF_TERM
ReleaseRateLimiter(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    RateLimiter* rate_limiter_ptr = erocksdb::RateLimiter::RetrieveRateLimiterResource(env, argv[0]);
    if(NULL==rate_limiter_ptr)
        return ATOM_OK;
    // the C++ object is destroyed only by the resource cleanup callback
    rate_limiter_ptr->Release();
    return ATOM_OK;
}


}
