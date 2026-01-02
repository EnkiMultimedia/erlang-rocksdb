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

#pragma once

#include <memory>

#include "erl_nif.h"

namespace rocksdb {
    class SstFileWriter;
}

namespace erocksdb {

  class SstFileWriterObject {
    protected:
      static ErlNifResourceType* m_SstFileWriter_RESOURCE;

    public:

      explicit SstFileWriterObject(std::unique_ptr<rocksdb::SstFileWriter> writer);

      ~SstFileWriterObject();

      rocksdb::SstFileWriter* writer();

      static void CreateSstFileWriterType(ErlNifEnv * Env);
      static void SstFileWriterResourceCleanup(ErlNifEnv *Env, void * Arg);

      static SstFileWriterObject * CreateSstFileWriterResource(std::unique_ptr<rocksdb::SstFileWriter> writer);
      static SstFileWriterObject * RetrieveSstFileWriterResource(ErlNifEnv * Env, const ERL_NIF_TERM & term);

    private:
      std::unique_ptr<rocksdb::SstFileWriter> writer_;
  };

  // NIF functions
  ERL_NIF_TERM SstFileWriterOpen(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileWriterPut(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileWriterPutEntity(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileWriterMerge(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileWriterDelete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileWriterDeleteRange(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileWriterFinish(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileWriterFileSize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM ReleaseSstFileWriter(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

}
