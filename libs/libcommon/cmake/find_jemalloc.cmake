# Copyright 2022 PingCAP, Ltd.
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

# Only enable under linux
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(ENABLE_JEMALLOC_DEFAULT 1)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
    set(ENABLE_JEMALLOC_DEFAULT 0)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(ENABLE_JEMALLOC_DEFAULT 0)
endif()

option (ENABLE_JEMALLOC "Set to TRUE to use jemalloc" ${ENABLE_JEMALLOC_DEFAULT})
# TODO: Make ENABLE_JEMALLOC_PROF default value to ${ENABLE_JEMALLOC_DEFAULT} after https://github.com/pingcap/tics/issues/3236 get fixed.
option (ENABLE_JEMALLOC_PROF "Set to ON to enable jemalloc profiling" OFF)
option (USE_INTERNAL_JEMALLOC_LIBRARY "Set to FALSE to use system jemalloc library instead of bundled" ${NOT_UNBUNDLED})

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

