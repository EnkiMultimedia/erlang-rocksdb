# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_path(ZSTD_INCLUDE_DIR zstd.h)

# WITH_STATIC_ZSTD prefers the static archive so zstd is linked INTO the NIF (no
# libzstd.so runtime dependency), falling back to shared if no .a is present.
# Off by default: distro archives such as Debian/Ubuntu's libzstd.a are built
# without -fPIC and can't be linked into a shared object. For a fully static NIF
# without a PIC system archive, use the bundled zstd instead.
if(WITH_STATIC_ZSTD)
    set(_zstd_saved_suffixes "${CMAKE_FIND_LIBRARY_SUFFIXES}")
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_STATIC_LIBRARY_SUFFIX})
    find_library(ZSTD_LIBRARY NAMES zstd)
    set(CMAKE_FIND_LIBRARY_SUFFIXES "${_zstd_saved_suffixes}")
endif()
find_library(ZSTD_LIBRARY NAMES zstd)

if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
    set(ZSTD_FOUND TRUE)
    message(STATUS "Found ZSTD library: ${ZSTD_LIBRARY}")
    message(STATUS "Found ZSTD include: ${ZSTD_INCLUDE_DIR}")
endif ()
