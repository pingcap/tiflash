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

if (TIFLASH_ENABLE_AVX_SUPPORT)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} mavx2")
    set (SNAPPY_REQUIRE_AVX2 ON)
endif ()

#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fomit-frame-pointer")
#set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fomit-frame-pointer")

set (SNAPPY_BUILD_TESTS OFF CACHE BOOL "Build Snappy's own tests.")
set (SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "Build Snappy for fuzzing.")

add_subdirectory (snappy)
