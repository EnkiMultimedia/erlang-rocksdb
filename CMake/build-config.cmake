# Copyright (c) 2018-present, Benoît Chesneau
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(ERLANG_ROCKSDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(ERLANG_ROCKSDB_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/c_src")


# Setting Output
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(UNIT_TEST_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/test)