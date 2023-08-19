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

include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_functions .)
add_headers_and_sources(clickhouse_functions ./GatherUtils)
add_headers_and_sources(clickhouse_functions ./Conditional)
add_headers_and_sources(clickhouse_functions ${TiFlash_BINARY_DIR}/dbms/src/Functions)

check_then_add_sources_compile_flag (TIFLASH_ENABLE_AVX_SUPPORT "-mavx2" CollationStringOptimized.cpp)
check_then_add_sources_compile_flag (TIFLASH_COMPILER_MOVBE_SUPPORT "-mmovbe" CollationStringOptimized.cpp)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

target_link_libraries(clickhouse_functions PUBLIC dbms PRIVATE ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

target_include_directories (clickhouse_functions BEFORE PUBLIC ${TiFlash_SOURCE_DIR}/contrib/libfarmhash)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${TiFlash_SOURCE_DIR}/contrib/libmetrohash/src)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${DIVIDE_INCLUDE_DIR})

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif ()

if (USE_ICU)
    #target_link_libraries (clickhouse_functions ${ICU_LIBS})
    target_include_directories (clickhouse_functions PRIVATE ${ICU_INCLUDE_DIR})
endif ()

if (USE_VECTORCLASS)
    target_include_directories (clickhouse_functions BEFORE PUBLIC ${VECTORCLASS_INCLUDE_DIR})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests EXCLUDE_FROM_ALL)
endif ()
