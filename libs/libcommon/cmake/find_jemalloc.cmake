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

option (ENABLE_JEMALLOC "Set to TRUE to use jemalloc" ON)
# 1. The deadlock mentioned in https://github.com/pingcap/tics/issues/3236 is not related to ENABLE_JEMALLOC_PROF.
# 2. It is also expected to be eliminated even if the heap profiling is activated, with a newer version of pprof-rs.
# TODO: Enable continuous heap profiling after we make sure statement 2.
option (ENABLE_JEMALLOC_PROF "Set to ON to enable jemalloc profiling" ON)
option (USE_INTERNAL_JEMALLOC_LIBRARY "Set to FALSE to use system jemalloc library instead of bundled" ${NOT_UNBUNDLED})

if (ENABLE_JEMALLOC AND (CMAKE_BUILD_TYPE_UC STREQUAL "ASAN" OR CMAKE_BUILD_TYPE_UC STREQUAL "UBSAN" OR CMAKE_BUILD_TYPE_UC STREQUAL "TSAN"))
    message (WARNING "ENABLE_JEMALLOC is set to OFF implicitly: jemalloc doesn't work with ${CMAKE_BUILD_TYPE_UC}")
    set (ENABLE_JEMALLOC OFF)
endif ()

if (ENABLE_JEMALLOC)
    if (USE_INTERNAL_JEMALLOC_LIBRARY)
        set (JEMALLOC_LIBRARIES "jemalloc")
    else ()
        find_package (JeMalloc)
    endif ()

    if (JEMALLOC_LIBRARIES)
        set (USE_JEMALLOC 1)
        set (USE_JEMALLOC_PROF ${ENABLE_JEMALLOC_PROF})
    else ()
        message (FATAL_ERROR "ENABLE_JEMALLOC is set to true, but library was not found")
    endif ()

    message (STATUS "Using jemalloc=${USE_JEMALLOC}, enable profile=${ENABLE_JEMALLOC_PROF}: ${JEMALLOC_LIBRARIES}")
endif ()

