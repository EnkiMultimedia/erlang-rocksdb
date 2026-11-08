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

#include <mutex>
#include <memory>

#include "erl_nif.h"

// Forward declaration
namespace rocksdb {
    class Statistics;
}

namespace erocksdb {

  class Statistics {
    protected:
      static ErlNifResourceType* m_Statistics_RESOURCE;

    public:
      std::mutex mu;

      explicit Statistics(std::shared_ptr<rocksdb::Statistics> statistics);

      ~Statistics();

      // NULL once released: callers must check the copy they get
      std::shared_ptr<rocksdb::Statistics> statistics();

      // Drop this handle's reference. Databases using the object keep their
      // own. Later calls through the handle see NULL and raise badarg.
      void Release();

      static void CreateStatisticsType(ErlNifEnv* Env);
      static void StatisticsResourceCleanup(ErlNifEnv* Env, void* Arg);

      static Statistics* CreateStatisticsResource(std::shared_ptr<rocksdb::Statistics> statistics);
      static Statistics* RetrieveStatisticsResource(ErlNifEnv* Env, const ERL_NIF_TERM& StatisticsTerm);

    private:
      std::mutex mutex_;  // guards statistics_ against a concurrent release
      std::shared_ptr<rocksdb::Statistics> statistics_;
  };

}
