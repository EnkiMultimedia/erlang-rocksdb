# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_path(ZSTD_INCLUDE_DIR zstd.h)

# Prefer the static archive so zstd is linked INTO the NIF (portable binary with
# no libzstd.so runtime dependency). Falls back to shared if no .a is present.
set(_zstd_saved_suffixes "${CMAKE_FIND_LIBRARY_SUFFIXES}")
set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
find_library(ZSTD_LIBRARY NAMES zstd)
if(NOT ZSTD_LIBRARY)
    set(CMAKE_FIND_LIBRARY_SUFFIXES "${_zstd_saved_suffixes}")
    find_library(ZSTD_LIBRARY NAMES zstd)
endif()
set(CMAKE_FIND_LIBRARY_SUFFIXES "${_zstd_saved_suffixes}")

if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
    set(ZSTD_FOUND TRUE)
    message(STATUS "Found ZSTD library: ${ZSTD_LIBRARY}")
    message(STATUS "Found ZSTD include: ${ZSTD_INCLUDE_DIR}")
endif ()
