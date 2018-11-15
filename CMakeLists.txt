# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)



# includes
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Compiler Settings
SET (CMAKE_CXX_FLAGS "-O2 -arch x86_64 -Wall")

set(PACKAGE_NAME "rocksdb")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/priv)


set_property(GLOBAL PROPERTY USE_FOLDERS ON)

project(${PACKAGE_NAME} CXX)


# gtest_discover_tests was introduced in cmake 3.10
if(${CMAKE_VERSION} VERSION_EQUAL "3.10" OR ${CMAKE_VERSION} VERSION_GREATER "3.10")
  include(GoogleTest OPTIONAL RESULT_VARIABLE HAVE_CMAKE_GTEST)
endif()

include (erocksdb-functions)

include(build-config)

include(build-rocksdb)
include(rocksdb-deps)

include_directories(${ERLANG_ROCKSDB_DIR}/..)




set(ERLANG_ROCKSDB_EXTERNAL_DEPS
  ${ROCKSDB_LIBRARIES}
  ${LZ4_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${BZIP2_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${SNAPPY_LIBRARY}
  Threads::Threads)

add_subdirectory(c_src)
