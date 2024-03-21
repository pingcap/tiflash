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

option (ENABLE_MIMALLOC "Set to ON to use mimalloc" OFF)

if (ENABLE_MIMALLOC AND (CMAKE_BUILD_TYPE_UC STREQUAL "ASAN" OR CMAKE_BUILD_TYPE_UC STREQUAL "UBSAN" OR CMAKE_BUILD_TYPE_UC STREQUAL "TSAN"))
    message (WARNING "ENABLE_MIMALLOC is set to OFF implicitly: mimalloc doesn't work with ${CMAKE_BUILD_TYPE_UC}")
    set (ENABLE_MIMALLOC OFF)
endif ()

if (ENABLE_MIMALLOC)
    set (MIMALLOC_LIBRARIES "mimalloc-obj")
    set (USE_MIMALLOC 1)
    message (STATUS "Using mimalloc=${USE_MIMALLOC}: ${MIMALLOC_LIBRARIES}")

    if (ENABLE_JEMALLOC)
        message(FATAL_ERROR "multiple global allocator detected!")
    endif()
endif ()

