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
//

#include <memory>
#include <mutex>

#include "erl_nif.h"

namespace rocksdb {
    class WriteBufferManager;
}

namespace erocksdb {

  class WriteBufferManager {
    protected:
      static ErlNifResourceType* m_WriteBufferManager_RESOURCE;

    public:

      explicit WriteBufferManager(std::shared_ptr<rocksdb::WriteBufferManager> mgr);

      ~WriteBufferManager();

      // NULL once released: callers must check the copy they get
      std::shared_ptr<rocksdb::WriteBufferManager> write_buffer_manager();

      // Drop this handle's reference. Databases using the object keep their
      // own. Later calls through the handle see NULL and raise badarg.
      void Release();

      static void CreateWriteBufferManagerType(ErlNifEnv * Env);
      static void WriteBufferManagerResourceCleanup(ErlNifEnv *Env, void * Arg);

      static WriteBufferManager * CreateWriteBufferManagerResource(std::shared_ptr<rocksdb::WriteBufferManager> mgr);
      static WriteBufferManager * RetrieveWriteBufferManagerResource(ErlNifEnv * Env, const ERL_NIF_TERM & term);

    private:
      std::mutex mutex_;  // guards mgr_ against a concurrent release
      std::shared_ptr<rocksdb::WriteBufferManager> mgr_;
  };

}
