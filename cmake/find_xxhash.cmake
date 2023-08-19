# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(XXHASH_BUILD_ENABLE_INLINE_API OFF CACHE BOOL "Change XXHASH_BUILD_ENABLE_INLINE_API" FORCE)
set(XXHASH_BUILD_XXHSUM OFF CACHE BOOL "Change XXHASH_BUILD_XXHSUM" FORCE)
if(NOT BUILD_SHARED_LIBS)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static library" FORCE)
endif()
add_subdirectory(
        ${TiFlash_SOURCE_DIR}/contrib/xxHash/cmake_unofficial EXCLUDE_FROM_ALL)
if(ARCH_AMD64)
    add_library(xxhash_dispatch STATIC ${TiFlash_SOURCE_DIR}/contrib/xxHash/xxh_x86dispatch.c)
    target_link_libraries(xxhash_dispatch PUBLIC xxHash::xxhash)
    set(TIFLASH_XXHASH_LIBRARY xxhash_dispatch)
else()
    set(TIFLASH_XXHASH_LIBRARY xxHash::xxhash)
endif()
set(XXHASH_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/xxHash)

