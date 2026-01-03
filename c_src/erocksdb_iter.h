// -------------------------------------------------------------------
// Copyright (c) 2016-2026 Benoit Chesneau. All Rights Reserved.
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

#pragma once
#ifndef INCL_EROCKSDB_ITER_H
#define INCL_EROCKSDB_ITER_H


struct ItrBounds {
    rocksdb::ReadOptions* read_options;
    rocksdb::Slice *upper_bound_slice;
    rocksdb::Slice *lower_bound_slice;

    ItrBounds();
};

int
parse_iterator_options(ErlNifEnv* env,
                       ErlNifEnv* itr_env,
                       ERL_NIF_TERM term,
                       rocksdb::ReadOptions& opts,
                       ItrBounds& bounds);


#endif //INCL_EROCKSDB_ITER_H
