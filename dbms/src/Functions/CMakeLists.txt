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

include("${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(tiflash_functions .)
add_headers_and_sources(tiflash_functions ./GatherUtils)

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT
    "${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}"
    CollationStringOptimized.cpp
)

list(REMOVE_ITEM tiflash_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM tiflash_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(tiflash_functions ${tiflash_functions_sources})

target_link_libraries(tiflash_functions PUBLIC dbms PRIVATE ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

target_include_directories (tiflash_functions BEFORE PUBLIC ${TiFlash_SOURCE_DIR}/contrib/libfarmhash)
target_include_directories (tiflash_functions BEFORE PUBLIC ${TiFlash_SOURCE_DIR}/contrib/libmetrohash/src)
target_include_directories (tiflash_functions BEFORE PUBLIC ${DIVIDE_INCLUDE_DIR})

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(tiflash_functions PRIVATE "-g0")
endif ()

if (USE_VECTORCLASS)
    target_include_directories (tiflash_functions BEFORE PUBLIC ${VECTORCLASS_INCLUDE_DIR})
endif ()
