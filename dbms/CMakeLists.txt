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

if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/find_vectorclass.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config.h)
set (CONFIG_BUILD ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_build.cpp)

include (cmake/version.cmake)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config.h.in ${CONFIG_COMMON})
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config_version.h.in ${CONFIG_VERSION})

get_property (BUILD_COMPILE_DEFINITIONS DIRECTORY ${TiFlash_SOURCE_DIR} PROPERTY COMPILE_DEFINITIONS)
get_property (BUILD_INCLUDE_DIRECTORIES DIRECTORY ${TiFlash_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
string (TIMESTAMP BUILD_DATE "%Y-%m-%d" UTC)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config_build.cpp.in ${CONFIG_BUILD})

if (NOT MSVC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif ()

if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

find_package (Threads)

if (ENABLE_FAILPOINTS)
    add_definitions(-DFIU_ENABLE)
endif()

include_directories (src)
add_subdirectory (src)

set(dbms_headers)
set(dbms_sources)

include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_io src/Common)
add_headers_and_sources(clickhouse_common_io src/Common/HashTable)
add_headers_and_sources(clickhouse_common_io src/Common/SyncPoint)
add_headers_and_sources(clickhouse_common_io src/IO)

add_headers_and_sources(dbms src/Analyzers)
add_headers_and_sources(dbms src/Core)
add_headers_and_sources(dbms src/DataStreams)
add_headers_and_sources(dbms src/DataTypes)
add_headers_and_sources(dbms src/Databases)
add_headers_and_sources(dbms src/Debug)
add_headers_and_sources(dbms src/Debug/MockExecutor)
add_headers_and_sources(dbms src/Dictionaries)
add_headers_and_sources(dbms src/Dictionaries/Embedded)
add_headers_and_sources(dbms src/Dictionaries/Embedded/GeodataProviders)
add_headers_and_sources(dbms src/Encryption)
add_headers_and_sources(dbms src/Interpreters)
add_headers_and_sources(dbms src/Interpreters/ClusterProxy)
add_headers_and_sources(dbms src/Columns)
add_headers_and_sources(dbms src/Storages)
add_headers_and_sources(dbms src/Storages/DeltaMerge)
add_headers_and_sources(dbms src/Storages/DeltaMerge/Index)
add_headers_and_sources(dbms src/Storages/DeltaMerge/Filter)
add_headers_and_sources(dbms src/Storages/DeltaMerge/FilterParser)
add_headers_and_sources(dbms src/Storages/DeltaMerge/File)
add_headers_and_sources(dbms src/Storages/DeltaMerge/ColumnFile)
add_headers_and_sources(dbms src/Storages/DeltaMerge/Delta)
add_headers_and_sources(dbms src/Storages/DeltaMerge/ReadThread)
add_headers_and_sources(dbms src/Storages/DeltaMerge/Remote)
add_headers_and_sources(dbms src/Storages/DeltaMerge/Remote/DataStore)
add_headers_and_sources(dbms src/Storages/Distributed)
add_headers_and_sources(dbms src/Storages/Transaction)
add_headers_and_sources(dbms src/Storages/Page/V1)
add_headers_and_sources(dbms src/Storages/Page/V1/mvcc)
add_headers_and_sources(dbms src/Storages/Page/V1/VersionSet)
add_headers_and_sources(dbms src/Storages/Page/V2)
add_headers_and_sources(dbms src/Storages/Page/V2/mvcc)
add_headers_and_sources(dbms src/Storages/Page/V2/VersionSet)
add_headers_and_sources(dbms src/Storages/Page/V2/gc)
add_headers_and_sources(dbms src/WindowFunctions)
add_headers_and_sources(dbms src/TiDB/Schema)
add_headers_and_sources(dbms src/Storages/Page/V3)
add_headers_and_sources(dbms src/Storages/Page/V3/LogFile)
add_headers_and_sources(dbms src/Storages/Page/V3/WAL)
add_headers_and_sources(dbms src/Storages/Page/V3/spacemap)
add_headers_and_sources(dbms src/Storages/Page/V3/PageDirectory)
add_headers_and_sources(dbms src/Storages/Page/V3/Blob)
add_headers_and_sources(dbms src/Storages/Page/universal)
add_headers_and_sources(dbms src/Storages/Page/)
add_headers_and_sources(dbms src/Storages/Page/V3/Remote)
add_headers_and_sources(dbms src/Storages/S3)
add_headers_and_sources(dbms src/TiDB)
add_headers_and_sources(dbms src/Client)
add_headers_only(dbms src/Flash/Coprocessor)
add_headers_only(dbms src/Server)

add_sources_compile_flag_avx2 (
    src/Columns/ColumnString.cpp
    src/Columns/ColumnsCommon.cpp
)

list (APPEND clickhouse_common_io_sources ${CONFIG_BUILD})
list (APPEND clickhouse_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})
list (APPEND clickhouse_common_io_headers ${fiu_include_dirs})

list (APPEND dbms_sources src/Functions/IFunction.cpp src/Functions/FunctionFactory.cpp src/Functions/FunctionHelpers.cpp)
list (APPEND dbms_headers src/Functions/IFunction.h src/Functions/FunctionFactory.h src/Functions/FunctionHelpers.h)

list (APPEND dbms_sources
    src/AggregateFunctions/AggregateFunctionFactory.cpp
    src/AggregateFunctions/AggregateFunctionCombinatorFactory.cpp
    src/AggregateFunctions/AggregateFunctionState.cpp
    src/AggregateFunctions/FactoryHelpers.cpp
    src/AggregateFunctions/parseAggregateFunctionParameters.cpp)

list (APPEND dbms_headers
    src/AggregateFunctions/IAggregateFunction.h
    src/AggregateFunctions/IAggregateFunctionCombinator.h
    src/AggregateFunctions/AggregateFunctionFactory.h
    src/AggregateFunctions/AggregateFunctionCombinatorFactory.h
    src/AggregateFunctions/AggregateFunctionState.h
    src/AggregateFunctions/FactoryHelpers.h
    src/AggregateFunctions/parseAggregateFunctionParameters.h)

list (APPEND dbms_sources src/TableFunctions/ITableFunction.cpp src/TableFunctions/TableFunctionFactory.cpp)
list (APPEND dbms_headers src/TableFunctions/ITableFunction.h src/TableFunctions/TableFunctionFactory.h)

add_library(clickhouse_common_io ${SPLIT_SHARED} ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})
target_include_directories(clickhouse_common_io BEFORE PUBLIC ${XXHASH_INCLUDE_DIR})

if (OS_FREEBSD)
    target_compile_definitions (clickhouse_common_io PUBLIC CLOCK_MONOTONIC_COARSE=CLOCK_MONOTONIC_FAST)
endif ()

add_subdirectory(src/Common/Config)

if (MAKE_STATIC_LIBRARIES)
    add_library(dbms ${dbms_headers} ${dbms_sources})
else ()
    add_library(dbms SHARED ${dbms_headers} ${dbms_sources})
    set_target_properties (dbms PROPERTIES SOVERSION ${VERSION_MAJOR} VERSION ${VERSION_SO} OUTPUT_NAME clickhouse)
    install (TARGETS dbms LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT clickhouse)
endif ()

target_include_directories(dbms PUBLIC "${TIFLASH_PROXY_INCLUDE_DIR}")

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    set_source_files_properties(
        src/Dictionaries/FlatDictionary.cpp
        src/Dictionaries/HashedDictionary.cpp
        src/Dictionaries/CacheDictionary.cpp
        src/Dictionaries/TrieDictionary.cpp
        src/Dictionaries/RangeHashedDictionary.cpp
        src/Dictionaries/ComplexKeyHashedDictionary.cpp
        src/Dictionaries/ComplexKeyCacheDictionary.cpp
        src/Dictionaries/ComplexKeyCacheDictionary_generate1.cpp
        src/Dictionaries/ComplexKeyCacheDictionary_generate2.cpp
        src/Dictionaries/ComplexKeyCacheDictionary_generate3.cpp
        src/Dictionaries/HTTPDictionarySource.cpp
        src/Dictionaries/LibraryDictionarySource.cpp
        src/Dictionaries/ExecutableDictionarySource.cpp
        src/Dictionaries/ClickHouseDictionarySource.cpp
        PROPERTIES COMPILE_FLAGS -g0)
endif ()

if (NOT ARCH_ARM)
    set (LINK_LIBRARIES_ONLY_ON_X86_64 cpuid)
endif()

target_link_libraries (clickhouse_common_io
    PUBLIC
    common
    string_utils
    ${LINK_LIBRARIES_ONLY_ON_X86_64}
    ${LZ4_LIBRARY}
    ${ZSTD_LIBRARY}
    ${DOUBLE_CONVERSION_LIBRARIES}
    ${Poco_Net_LIBRARY}
    ${Poco_Data_LIBRARY}
    ${ZLIB_LIBRARIES}
    ${EXECINFO_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    ${CMAKE_DL_LIBS}
    fiu
    prometheus-cpp::core
    prometheus-cpp::push
    prometheus-cpp::pull
    cpptoml
    magic_enum
    libsymbolization
)
target_include_directories (clickhouse_common_io BEFORE PRIVATE ${kvClient_SOURCE_DIR}/include)
target_compile_definitions(clickhouse_common_io PUBLIC -DTIFLASH_SOURCE_PREFIX=\"${TiFlash_SOURCE_DIR}\")
target_link_libraries (dbms
    clickhouse_parsers
    clickhouse_common_config
    clickhouse_common_io
    flash_service
    kvproto
    kv_client
    tipb
    dtpb
    PSCheckpointProto
    ${RE2_LIBRARY}
    ${RE2_ST_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${BTRIE_LIBRARIES}
    absl::synchronization
    tiflash_contrib::aws_s3
)

# always add GmSSL include dir to the include path for static analysis
target_include_directories(dbms PRIVATE ${TiFlash_SOURCE_DIR}/contrib/GmSSL/include)
if (USE_GM_SSL)
    target_link_libraries(dbms gmssl)
endif ()

if (NOT USE_INTERNAL_RE2_LIBRARY)
    target_include_directories (dbms BEFORE PRIVATE ${RE2_INCLUDE_DIR})
endif ()

target_include_directories (clickhouse_common_io BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

if (Poco_Data_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (clickhouse_common_io PRIVATE ${Poco_Data_INCLUDE_DIRS})
    target_include_directories (dbms PRIVATE ${Poco_Data_INCLUDE_DIRS})
endif()

# NetSSL headers are exposed in clickhouse_common_io's headers.
# We mark it public to make the headers available.
if (Poco_NetSSL_FOUND)
    target_link_libraries (clickhouse_common_io PUBLIC ${Poco_NetSSL_LIBRARY})
endif()

target_link_libraries (dbms ${Poco_Foundation_LIBRARY})

if (USE_ICU)
    target_link_libraries (dbms ${ICU_LIBS})
    target_include_directories (dbms PRIVATE ${ICU_INCLUDE_DIR})
endif ()

target_link_libraries (dbms
    Threads::Threads
)

target_include_directories (dbms BEFORE PRIVATE ${DIVIDE_INCLUDE_DIR})
target_include_directories (dbms BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    target_include_directories (dbms BEFORE PRIVATE ${LZ4_INCLUDE_DIR})
endif ()
if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    target_include_directories (dbms BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()

target_include_directories (dbms PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io PUBLIC ${PCG_RANDOM_INCLUDE_DIR})
target_include_directories (clickhouse_common_io BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

# also for copy_headers.sh:
target_include_directories (clickhouse_common_io BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
# https://cmake.org/pipermail/cmake/2016-May/063400.html
target_link_libraries (clickhouse_common_io PUBLIC ${TIFLASH_XXHASH_LIBRARY})

function(add_target_pch context target)
    if (ENABLE_PCH)
        message(STATUS "Add PCH `${context}` for target `${target}`")
        target_precompile_headers(${target} PRIVATE ${context})
    endif ()
    if(${ARGC} GREATER 2)
        add_target_pch(${context} ${ARGN})
    endif()
endfunction()

if (ENABLE_TESTS)
    include (${TiFlash_SOURCE_DIR}/cmake/find_gtest.cmake)

    if (USE_INTERNAL_GTEST_LIBRARY)
        set(INSTALL_GTEST OFF)
        # Google Test from sources
        add_subdirectory(${TiFlash_SOURCE_DIR}/contrib/googletest/googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)
        # avoid problems with <regexp.h>
        target_compile_definitions (gtest INTERFACE GTEST_HAS_POSIX_RE=0)
        target_include_directories (gtest INTERFACE ${TiFlash_SOURCE_DIR}/contrib/googletest/googletest/include)
    endif ()

    macro(grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    macro(grep_bench_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "bench*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources(${TiFlash_SOURCE_DIR}/dbms dbms_gtest_sources)
    add_executable(gtests_dbms EXCLUDE_FROM_ALL
        ${dbms_gtest_sources}
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/UserConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/RaftConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/AggregateFunctions/AggregateFunctionSum.cpp
    )
    target_include_directories(gtests_dbms BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
    target_compile_definitions(gtests_dbms PUBLIC DBMS_PUBLIC_GTEST)
    target_compile_definitions(dbms PUBLIC MULTIPLE_CONTEXT_GTEST)

    target_link_libraries(gtests_dbms test_util_gtest_main clickhouse_functions tiflash-dttool-lib)

    install (TARGETS gtests_dbms
            COMPONENT tiflash-gtest
            DESTINATION "."
            RUNTIME_DEPENDENCY_SET tiflash-gtest-dependency)
    install (RUNTIME_DEPENDENCY_SET tiflash-gtest-dependency
            COMPONENT tiflash-gtest
            DESTINATION "."
            PRE_EXCLUDE_REGEXES
                "libdl.*"
                "libc-.*"
                "libc\\..*"
                "libgcc_s.*"
                "librt.*"
                "libm.*"
                "ld-linux-x86-64.*"
                "ld-linux-aarch64.*"
                "libpthread.*"# exclude libc dependencies
            DIRECTORIES ${TIFLASH_GTEST_DEPENDENCY_DIRECTORIES})

    if (USE_GM_SSL)
        target_link_libraries(gtests_dbms gmssl)
        target_include_directories(gtests_dbms PRIVATE ${TiFlash_SOURCE_DIR}/contrib/GmSSL/include)
        install (TARGETS gmssl
                COMPONENT tiflash-gtest
                DESTINATION ".")
    endif ()

    target_compile_options(gtests_dbms PRIVATE -Wno-unknown-pragmas -Wno-deprecated-copy)
    add_check(gtests_dbms)

    add_target_pch("pch-dbms.h" gtests_dbms)

    grep_bench_sources(${TiFlash_SOURCE_DIR}/dbms dbms_bench_sources)
    add_executable(bench_dbms EXCLUDE_FROM_ALL
        ${dbms_bench_sources}
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/UserConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/RaftConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/AggregateFunctions/AggregateFunctionSum.cpp
        )
    target_include_directories(bench_dbms BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR} ${benchmark_SOURCE_DIR}/include)
    target_compile_definitions(bench_dbms PUBLIC DBMS_PUBLIC_GTEST)

    target_link_libraries(bench_dbms gtest dbms test_util_bench_main benchmark clickhouse_functions)
    if (ENABLE_TIFLASH_DTWORKLOAD)
        target_link_libraries(bench_dbms dt-workload-lib)
    endif ()
    if (ENABLE_TIFLASH_PAGEWORKLOAD)
        target_link_libraries(bench_dbms page-workload-lib)
    endif ()

    add_check(bench_dbms)
endif ()

# dbms
add_target_pch("pch-dbms.h" flash_service)
add_target_pch("pch-common.h" clickhouse_common_io clickhouse_aggregate_functions)

# disable PCH for `clickhouse_functions`, `dbms` temporarily because some source files need to use specific cxx flags
# add_target_pch("pch-common.h" clickhouse_functions dbms)

add_target_pch("pch-common.h" clickhouse_parsers clickhouse_storages_system dt-workload-lib clickhouse-server-lib)

# common
add_target_pch("pch-kvpb.h" kv_client)

add_target_pch("pch-stl.h" ${Boost_SYSTEM_LIBRARY} cctz ${RE2_LIBRARY} ${RE2_ST_LIBRARY})

# grpc
add_target_pch("$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CURRENT_SOURCE_DIR}/pch-stl.h>" grpc grpc++)

# pb
add_target_pch("pch-stl.h" libprotobuf kvproto tipb libprotoc)

# poco
add_target_pch("pch-stl.h" Net Crypto Util Data NetSSL)
add_target_pch("$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CURRENT_SOURCE_DIR}/pch-stl.h>" XML Foundation JSON)

message (STATUS "Will build ${VERSION_FULL} (TiFlash ${TIFLASH_RELEASE_VERSION})")
