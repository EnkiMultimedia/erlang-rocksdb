# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Build the RocksDB library.
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  ROCKSDB_FOUND               RocksDB library/headers were found
#  ROCKSDB_LIBRARIES           The RocksDB library.
#  ROCKSDB_INCLUDE_DIRS        The location of RocksDB headers.

set(ROCKSDB_ROOT_DIR "${ERLANG_ROCKSDB_DIR}/deps/rocksdb")

include(ExternalProject)

ExternalProject_Add(rocksdb
    SOURCE_DIR "${ROCKSDB_ROOT_DIR}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS -DUSE_RTTI=1
    INSTALL_COMMAND ""
    )

ExternalProject_Get_Property(rocksdb BINARY_DIR)
set(ROCKSDB_LIBRARIES
    ${BINARY_DIR}/librocksdb.a)

set(ROCKSDB_FOUND TRUE)

set(ROCKSDB_INCLUDE_DIRS
    ${ROCKSDB_ROOT_DIR}/include)
message(STATUS "Found RocksDB library: ${ROCKSDB_LIBRARIES}")
message(STATUS "Found RocksDB includes: ${ROCKSDB_INCLUDE_DIRS}")

mark_as_advanced(
    ROCKSDB_ROOT_DIR
    ROCKSDB_LIBRARIES
    ROCKSDB_INCLUDE_DIRS
)