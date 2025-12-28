// Copyright (c) 2020 Josep-Angel Herrero Bajo
// Copyright (c) 2020-2025 Benoit Chesneau:w
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

#include "rocksdb/statistics.h"

#include "atoms.h"
#include "statistics.h"
#include "util.h"

namespace erocksdb {

ErlNifResourceType* Statistics::m_Statistics_RESOURCE(NULL);

void
Statistics::CreateStatisticsType(ErlNifEnv* env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    m_Statistics_RESOURCE = enif_open_resource_type(env, NULL, "erocksdb_Statistics",
                                                    &Statistics::StatisticsResourceCleanup,
                                                    flags, NULL);
}   // Statistics::CreateStatisticsType


void
Statistics::StatisticsResourceCleanup(ErlNifEnv* /*env*/, void* arg)
{
    Statistics* statistics_ptr = (Statistics*)arg;
    statistics_ptr->~Statistics();
}   // Statistics::StatisticsResourceCleanup


Statistics*
Statistics::CreateStatisticsResource(std::shared_ptr<rocksdb::Statistics> statistics)
{
    Statistics* ret_ptr;
    void* alloc_ptr;

    alloc_ptr = enif_alloc_resource(m_Statistics_RESOURCE, sizeof(Statistics));
    ret_ptr = new (alloc_ptr) Statistics(statistics);
    return ret_ptr;
}

Statistics*
Statistics::RetrieveStatisticsResource(ErlNifEnv* Env, const ERL_NIF_TERM& StatisticsTerm)
{
    Statistics* ret_ptr;
    if (!enif_get_resource(Env, StatisticsTerm, m_Statistics_RESOURCE, (void **)&ret_ptr))
        return nullptr;
    return ret_ptr;
}

Statistics::Statistics(std::shared_ptr<rocksdb::Statistics> statistics_arg) : statistics_(statistics_arg) {}

Statistics::~Statistics()
{
    if(statistics_)
    {
        statistics_ = NULL;
    }
    return;
}

std::shared_ptr<rocksdb::Statistics> Statistics::statistics() {
    return statistics_;
}

ERL_NIF_TERM
NewStatistics(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM[] /*argv[]*/)
{
    Statistics *statistics_ptr = Statistics::CreateStatisticsResource(rocksdb::CreateDBStatistics());
    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(env, statistics_ptr);
    // clear the automatic reference from enif_alloc_resource in EnvObject
    enif_release_resource(statistics_ptr);
    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
ReleaseStatistics(ErlNifEnv* env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    Statistics* statistics_ptr = erocksdb::Statistics::RetrieveStatisticsResource(env, argv[0]);
    if(NULL==statistics_ptr)
        return ATOM_OK;
    statistics_ptr->~Statistics();
    return ATOM_OK;
}


bool StatsLevelAtomToEnum(ERL_NIF_TERM atom, rocksdb::StatsLevel* stats_level)
{
    if (atom == ATOM_STATS_DISABLE_ALL)
    {
        *stats_level = rocksdb::StatsLevel::kDisableAll;
        return true;
    }
    else if (atom == ATOM_STATS_EXCEPT_TICKERS)
    {
        *stats_level = rocksdb::StatsLevel::kExceptTickers;
        return true;
    }
    else if (atom == ATOM_STATS_EXCEPT_HISTOGRAM_OR_TIMERS)
    {
        *stats_level = rocksdb::StatsLevel::kExceptHistogramOrTimers;
        return true;
    }
    else if (atom == ATOM_STATS_EXCEPT_TIMERS)
    {
        *stats_level = rocksdb::StatsLevel::kExceptTimers;
        return true;
    }
    else if (atom == ATOM_STATS_EXCEPT_DETAILED_TIMERS)
    {
        *stats_level = rocksdb::StatsLevel::kExceptDetailedTimers;
        return true;
    }
    else if (atom == ATOM_STATS_EXCEPT_TIME_FOR_MUTEX)
    {
        *stats_level = rocksdb::StatsLevel::kExceptTimeForMutex;
        return true;
    }
    else if (atom == ATOM_STATS_ALL)
    {
        *stats_level = rocksdb::StatsLevel::kAll;
        return true;
    }
    return false;
}

ERL_NIF_TERM StatsLevelEnumToAtom(rocksdb::StatsLevel stats_level)
{
    if (stats_level == rocksdb::StatsLevel::kDisableAll)
    {
        return ATOM_STATS_DISABLE_ALL;
    }
    else if (stats_level == rocksdb::StatsLevel::kExceptTickers)
    {
        return ATOM_STATS_EXCEPT_TICKERS;
    }
    else if (stats_level == rocksdb::StatsLevel::kExceptHistogramOrTimers)
    {
        return ATOM_STATS_EXCEPT_HISTOGRAM_OR_TIMERS;
    }
    else if (stats_level == rocksdb::StatsLevel::kExceptTimers)
    {
        return ATOM_STATS_EXCEPT_TIMERS;
    }
    else if (stats_level == rocksdb::StatsLevel::kExceptDetailedTimers)
    {
        return ATOM_STATS_EXCEPT_DETAILED_TIMERS;
    }
    else if (stats_level == rocksdb::StatsLevel::kExceptTimeForMutex)
    {
        return ATOM_STATS_EXCEPT_TIME_FOR_MUTEX;
    }
    return ATOM_STATS_ALL;
}

ERL_NIF_TERM
SetStatsLevel(
    ErlNifEnv* env,
    int /*argc*/,
    const ERL_NIF_TERM argv[])
{
    Statistics* statistics_ptr = erocksdb::Statistics::RetrieveStatisticsResource(env, argv[0]);
    if (statistics_ptr == nullptr)
        return enif_make_badarg(env);

    std::lock_guard<std::mutex> guard(statistics_ptr->mu);
    std::shared_ptr<rocksdb::Statistics> statistics = statistics_ptr->statistics();

    rocksdb::StatsLevel stats_level;
    if (StatsLevelAtomToEnum(argv[1], &stats_level)) {
        statistics->set_stats_level(stats_level);
    }

    return ATOM_OK;
}

ERL_NIF_TERM
StatisticsInfo(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    Statistics* statistics_ptr = erocksdb::Statistics::RetrieveStatisticsResource(env, argv[0]);
    if (statistics_ptr == nullptr)
        return enif_make_badarg(env);

    std::lock_guard<std::mutex> guard(statistics_ptr->mu);
    std::shared_ptr<rocksdb::Statistics> statistics = statistics_ptr->statistics();

    ERL_NIF_TERM stats_level = StatsLevelEnumToAtom(statistics->get_stats_level());

    ERL_NIF_TERM info = enif_make_list(env, 0);
    return enif_make_list_cell(env,
                               enif_make_tuple2(env, erocksdb::ATOM_STATS_LEVEL, stats_level),
                               info);
}

bool TickerAtomToEnum(ERL_NIF_TERM atom, rocksdb::Tickers* ticker)
{
    // BlobDB Tickers
    if (atom == ATOM_BLOB_DB_NUM_PUT)
    {
        *ticker = rocksdb::BLOB_DB_NUM_PUT;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_WRITE)
    {
        *ticker = rocksdb::BLOB_DB_NUM_WRITE;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_GET)
    {
        *ticker = rocksdb::BLOB_DB_NUM_GET;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_MULTIGET)
    {
        *ticker = rocksdb::BLOB_DB_NUM_MULTIGET;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_SEEK)
    {
        *ticker = rocksdb::BLOB_DB_NUM_SEEK;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_NEXT)
    {
        *ticker = rocksdb::BLOB_DB_NUM_NEXT;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_PREV)
    {
        *ticker = rocksdb::BLOB_DB_NUM_PREV;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_KEYS_WRITTEN)
    {
        *ticker = rocksdb::BLOB_DB_NUM_KEYS_WRITTEN;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_NUM_KEYS_READ)
    {
        *ticker = rocksdb::BLOB_DB_NUM_KEYS_READ;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BYTES_WRITTEN)
    {
        *ticker = rocksdb::BLOB_DB_BYTES_WRITTEN;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BYTES_READ)
    {
        *ticker = rocksdb::BLOB_DB_BYTES_READ;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_WRITE_INLINED)
    {
        *ticker = rocksdb::BLOB_DB_WRITE_INLINED;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_WRITE_INLINED_TTL)
    {
        *ticker = rocksdb::BLOB_DB_WRITE_INLINED_TTL;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_WRITE_BLOB)
    {
        *ticker = rocksdb::BLOB_DB_WRITE_BLOB;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_WRITE_BLOB_TTL)
    {
        *ticker = rocksdb::BLOB_DB_WRITE_BLOB_TTL;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BLOB_FILE_BYTES_WRITTEN)
    {
        *ticker = rocksdb::BLOB_DB_BLOB_FILE_BYTES_WRITTEN;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BLOB_FILE_BYTES_READ)
    {
        *ticker = rocksdb::BLOB_DB_BLOB_FILE_BYTES_READ;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BLOB_FILE_SYNCED)
    {
        *ticker = rocksdb::BLOB_DB_BLOB_FILE_SYNCED;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BLOB_INDEX_EXPIRED_COUNT)
    {
        *ticker = rocksdb::BLOB_DB_BLOB_INDEX_EXPIRED_COUNT;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BLOB_INDEX_EXPIRED_SIZE)
    {
        *ticker = rocksdb::BLOB_DB_BLOB_INDEX_EXPIRED_SIZE;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BLOB_INDEX_EVICTED_COUNT)
    {
        *ticker = rocksdb::BLOB_DB_BLOB_INDEX_EVICTED_COUNT;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_BLOB_INDEX_EVICTED_SIZE)
    {
        *ticker = rocksdb::BLOB_DB_BLOB_INDEX_EVICTED_SIZE;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_GC_NUM_FILES)
    {
        *ticker = rocksdb::BLOB_DB_GC_NUM_FILES;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_GC_NUM_NEW_FILES)
    {
        *ticker = rocksdb::BLOB_DB_GC_NUM_NEW_FILES;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_GC_FAILURES)
    {
        *ticker = rocksdb::BLOB_DB_GC_FAILURES;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_GC_NUM_KEYS_RELOCATED)
    {
        *ticker = rocksdb::BLOB_DB_GC_NUM_KEYS_RELOCATED;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_GC_BYTES_RELOCATED)
    {
        *ticker = rocksdb::BLOB_DB_GC_BYTES_RELOCATED;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_FIFO_NUM_FILES_EVICTED)
    {
        *ticker = rocksdb::BLOB_DB_FIFO_NUM_FILES_EVICTED;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_FIFO_NUM_KEYS_EVICTED)
    {
        *ticker = rocksdb::BLOB_DB_FIFO_NUM_KEYS_EVICTED;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_FIFO_BYTES_EVICTED)
    {
        *ticker = rocksdb::BLOB_DB_FIFO_BYTES_EVICTED;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_CACHE_MISS)
    {
        *ticker = rocksdb::BLOB_DB_CACHE_MISS;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_CACHE_HIT)
    {
        *ticker = rocksdb::BLOB_DB_CACHE_HIT;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_CACHE_ADD)
    {
        *ticker = rocksdb::BLOB_DB_CACHE_ADD;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_CACHE_ADD_FAILURES)
    {
        *ticker = rocksdb::BLOB_DB_CACHE_ADD_FAILURES;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_CACHE_BYTES_READ)
    {
        *ticker = rocksdb::BLOB_DB_CACHE_BYTES_READ;
        return true;
    }
    else if (atom == ATOM_BLOB_DB_CACHE_BYTES_WRITE)
    {
        *ticker = rocksdb::BLOB_DB_CACHE_BYTES_WRITE;
        return true;
    }
    return false;
}

ERL_NIF_TERM
StatisticsTicker(ErlNifEnv *env, int /*argc*/, const ERL_NIF_TERM argv[])
{
    Statistics* statistics_ptr = erocksdb::Statistics::RetrieveStatisticsResource(env, argv[0]);
    if (statistics_ptr == nullptr)
        return enif_make_badarg(env);

    rocksdb::Tickers ticker;
    if (!TickerAtomToEnum(argv[1], &ticker))
        return enif_make_badarg(env);

    std::lock_guard<std::mutex> guard(statistics_ptr->mu);
    std::shared_ptr<rocksdb::Statistics> statistics = statistics_ptr->statistics();

    uint64_t count = statistics->getTickerCount(ticker);
    return enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, count));
}

}
