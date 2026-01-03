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
//
#include <array>

#include "atoms.h"
#include "rocksdb/cache.h"
#include "cache.h"
#include "util.h"

namespace erocksdb {

ErlNifResourceType * Cache::m_Cache_RESOURCE(NULL);

void
Cache::CreateCacheType(ErlNifEnv * env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_Cache_RESOURCE = enif_open_resource_type(env, NULL, "erocksdb_Cache",
                                            &Cache::CacheResourceCleanup,
                                            flags, NULL);
    return;
}   // Cache::CreateCacheType


void
Cache::CacheResourceCleanup(ErlNifEnv * /*env*/, void * arg)
{
    Cache* cache_ptr = (Cache *)arg;
    cache_ptr->~Cache();
    cache_ptr = nullptr;
    return;
}   // Cache::CacheResourceCleanup


Cache *
Cache::CreateCacheResource(std::shared_ptr<rocksdb::Cache> cache)
{
    Cache * ret_ptr;
    void * alloc_ptr;

    alloc_ptr=enif_alloc_resource(m_Cache_RESOURCE, sizeof(Cache));
    ret_ptr=new (alloc_ptr) Cache(cache);
    return(ret_ptr);
}

Cache *
Cache::RetrieveCacheResource(ErlNifEnv * Env, const ERL_NIF_TERM & CacheTerm)
{
    Cache * ret_ptr;
    if (!enif_get_resource(Env, CacheTerm, m_Cache_RESOURCE, (void **)&ret_ptr))
        return NULL;
    return ret_ptr;
}

Cache::Cache(std::shared_ptr<rocksdb::Cache> Cache) : cache_(Cache) {}

Cache::~Cache()
{
    if(cache_)
    {
        cache_ = nullptr;
    }
    return;
}

std::shared_ptr<rocksdb::Cache> Cache::cache() {
    auto c = cache_;
    return c;
}

ERL_NIF_TERM
NewCache(
    ErlNifEnv *env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    ErlNifUInt64 capacity;
    Cache *cache_ptr;
    std::shared_ptr<rocksdb::Cache> cache;

    if (!enif_is_atom(env, argv[0]) || !enif_get_uint64(env, argv[1], &capacity))
        return enif_make_badarg(env);

    if (argv[0] == erocksdb::ATOM_LRU) {
        cache = rocksdb::NewLRUCache(capacity);
    } else if (argv[0] == erocksdb::ATOM_CLOCK) {
        // RocksDB 10.x: NewClockCache is deprecated, use HyperClockCache
        rocksdb::HyperClockCacheOptions opts(capacity, 0 /* auto entry charge */);
        cache = opts.MakeSharedCache();
    } else {
        return enif_make_badarg(env);
    }

    cache_ptr = Cache::CreateCacheResource(cache);
    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(env, cache_ptr);
    // clear the automatic reference from enif_alloc_resource in EnvObject
    enif_release_resource(cache_ptr);
    cache.reset();
    cache = nullptr;
    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
ReleaseCache(ErlNifEnv *env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    Cache *cache_ptr;
    std::shared_ptr<rocksdb::Cache> cache;

    cache_ptr = erocksdb::Cache::RetrieveCacheResource(env, argv[0]);
    if (nullptr == cache_ptr)
        return ATOM_OK;
    cache = cache_ptr->cache();
    cache.reset();
    cache = nullptr;
    cache_ptr = nullptr;
    return ATOM_OK;
}

ERL_NIF_TERM
cache_info_1(
    ErlNifEnv *env,
    std::shared_ptr<rocksdb::Cache> cache,
    ERL_NIF_TERM item)
{
    if (item == erocksdb::ATOM_USAGE) {
        return enif_make_uint64(env, cache->GetUsage());
    } else if (item == erocksdb::ATOM_PINNED_USAGE) {
        return enif_make_uint64(env, cache->GetPinnedUsage());
    } else if (item == erocksdb::ATOM_CAPACITY) {
        return enif_make_uint64(env, cache->GetCapacity());
    } else if (item == erocksdb::ATOM_STRICT_CAPACITY) {
        if (cache->HasStrictCapacityLimit()) {
            return erocksdb::ATOM_TRUE;
        } else {
            return erocksdb::ATOM_FALSE;
        }
    } else {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM
CacheInfo(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{

    Cache *cache_ptr;
    std::shared_ptr<rocksdb::Cache> cache;

    cache_ptr = erocksdb::Cache::RetrieveCacheResource(env, argv[0]);
    if (nullptr == cache_ptr)
        return enif_make_badarg(env);

    std::lock_guard<std::mutex> guard(cache_ptr->mu);
    cache = cache_ptr->cache();

    if (argc > 1)
    {
        return cache_info_1(env, std::move(cache), argv[1]);
    }

    std::array<ERL_NIF_TERM, 4> items = {
        erocksdb::ATOM_PINNED_USAGE,
        erocksdb::ATOM_USAGE,
        erocksdb::ATOM_STRICT_CAPACITY,
        erocksdb::ATOM_CAPACITY
    };

    ERL_NIF_TERM info = enif_make_list(env, 0);
    for(const auto& item : items) {
        info = enif_make_list_cell(
                env,
                enif_make_tuple2(env, item, cache_info_1(env, cache, item)),
                info);
    }
    return info;
}

ERL_NIF_TERM
SetCapacity(ErlNifEnv * env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    Cache *cache_ptr;
    std::shared_ptr<rocksdb::Cache> cache;
    ErlNifUInt64 capacity;

    cache_ptr = erocksdb::Cache::RetrieveCacheResource(env, argv[0]);
    if (nullptr == cache_ptr)
        return enif_make_badarg(env);

    if (!enif_get_uint64(env, argv[1], &capacity))
        return enif_make_badarg(env);

    std::lock_guard<std::mutex> guard(cache_ptr->mu);
    cache = cache_ptr->cache();
    cache->SetCapacity(capacity);
    return ATOM_OK;
}

ERL_NIF_TERM
SetStrictCapacityLimit(ErlNifEnv * env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    Cache *cache_ptr;
    std::shared_ptr<rocksdb::Cache> cache;
    bool strict_capacity_limit;
    cache_ptr = erocksdb::Cache::RetrieveCacheResource(env, argv[0]);
    if (nullptr == cache_ptr)
        return enif_make_badarg(env);
    strict_capacity_limit = (argv[1] == ATOM_TRUE);
    std::lock_guard<std::mutex> guard(cache_ptr->mu);
    cache = cache_ptr->cache();
    cache->SetStrictCapacityLimit(strict_capacity_limit);
    return ATOM_OK;
}

}
