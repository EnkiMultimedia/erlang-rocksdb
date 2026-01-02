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
    class SstFileReader;
}

namespace erocksdb {

  class SstFileReaderObject {
    protected:
      static ErlNifResourceType* m_SstFileReader_RESOURCE;

    public:

      explicit SstFileReaderObject(std::unique_ptr<rocksdb::SstFileReader> reader);

      ~SstFileReaderObject();

      rocksdb::SstFileReader* reader();

      static void CreateSstFileReaderType(ErlNifEnv * Env);
      static void SstFileReaderResourceCleanup(ErlNifEnv *Env, void * Arg);

      static SstFileReaderObject * CreateSstFileReaderResource(std::unique_ptr<rocksdb::SstFileReader> reader);
      static SstFileReaderObject * RetrieveSstFileReaderResource(ErlNifEnv * Env, const ERL_NIF_TERM & term);

    private:
      std::unique_ptr<rocksdb::SstFileReader> reader_;
  };

  // NIF functions
  ERL_NIF_TERM SstFileReaderOpen(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileReaderIterator(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileReaderGetTableProperties(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM SstFileReaderVerifyChecksum(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
  ERL_NIF_TERM ReleaseSstFileReader(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

}
