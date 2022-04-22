if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/find_vectorclass.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config.h)
set (CONFIG_BUILD ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_build.cpp)

include (cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL} (TiFlash ${TIFLASH_RELEASE_VERSION})")
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
add_headers_and_sources(clickhouse_common_io src/IO)

add_headers_and_sources(dbms src/Analyzers)
add_headers_and_sources(dbms src/Core)
add_headers_and_sources(dbms src/DataStreams)
add_headers_and_sources(dbms src/DataTypes)
add_headers_and_sources(dbms src/Databases)
add_headers_and_sources(dbms src/Dictionaries)
add_headers_and_sources(dbms src/Dictionaries/Embedded)
add_headers_and_sources(dbms src/Dictionaries/Embedded/GeodataProviders)
add_headers_and_sources(dbms src/Debug)
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
add_headers_and_sources(dbms src/Storages/DeltaMerge/Delta)
add_headers_and_sources(dbms src/Storages/Distributed)
add_headers_and_sources(dbms src/Storages/MergeTree)
add_headers_and_sources(dbms src/Storages/Transaction)
add_headers_and_sources(dbms src/Storages/Page)
add_headers_and_sources(dbms src/Storages/Page/mvcc)
add_headers_and_sources(dbms src/Storages/Page/VersionSet)
add_headers_and_sources(dbms src/Storages/Page/gc)
add_headers_and_sources(dbms src/Storages/Page/stable)
add_headers_and_sources(dbms src/Storages/Page/stable/mvcc)
add_headers_and_sources(dbms src/Storages/Page/stable/VersionSet)
add_headers_and_sources(dbms src/TiDB)
add_headers_and_sources(dbms src/Client)
add_headers_only(dbms src/Flash/Coprocessor)
add_headers_only(dbms src/Server)

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

if (ARCH_FREEBSD)
    target_compile_definitions (clickhouse_common_io PUBLIC CLOCK_MONOTONIC_COARSE=CLOCK_MONOTONIC_FAST)
endif ()

add_subdirectory(src/Common/ZooKeeper)
add_subdirectory(src/Common/Config)

if (MAKE_STATIC_LIBRARIES)
    add_library(dbms ${dbms_headers} ${dbms_sources})
else ()
    add_library(dbms SHARED ${dbms_headers} ${dbms_sources})
    set_target_properties (dbms PROPERTIES SOVERSION ${VERSION_MAJOR} VERSION ${VERSION_SO} OUTPUT_NAME clickhouse)
    install (TARGETS dbms LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT clickhouse)
endif ()

target_include_directories(dbms PUBLIC "${TIFLASH_PROXY_FFI_SRC}")

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
)
target_include_directories (clickhouse_common_io BEFORE PRIVATE ${kvClient_SOURCE_DIR}/include)

target_link_libraries (dbms
    clickhouse_parsers
    clickhouse_common_config
    clickhouse_common_io
    flash_service
    kvproto
    kv_client
    tipb
    dtpb
    ${RE2_LIBRARY}
    ${RE2_ST_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${BTRIE_LIBRARIES}
)

if (NOT USE_INTERNAL_RE2_LIBRARY)
    target_include_directories (dbms BEFORE PRIVATE ${RE2_INCLUDE_DIR})
endif ()

target_include_directories (clickhouse_common_io BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

if (Poco_Data_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (clickhouse_common_io PRIVATE ${Poco_Data_INCLUDE_DIRS})
    target_include_directories (dbms PRIVATE ${Poco_Data_INCLUDE_DIRS})
endif()

if (Poco_NetSSL_FOUND)
    target_link_libraries (clickhouse_common_io ${Poco_NetSSL_LIBRARY})
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

add_subdirectory (tests)

if (ENABLE_TESTS)
    include (${TiFlash_SOURCE_DIR}/cmake/find_gtest.cmake)

    if (USE_INTERNAL_GTEST_LIBRARY)
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

    # attach all dbms gtest sources
    grep_gtest_sources(${TiFlash_SOURCE_DIR}/dbms dbms_gtest_sources)
    add_executable(gtests_dbms EXCLUDE_FROM_ALL
        ${dbms_gtest_sources}
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/UserConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/RaftConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/TestUtils/TiFlashTestBasic.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/AggregateFunctions/AggregateFunctionSum.cpp
    )
    target_include_directories(gtests_dbms BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
    target_link_libraries(gtests_dbms gtest_main dbms clickhouse_functions)
    target_compile_options(gtests_dbms PRIVATE -Wno-unknown-pragmas)
    add_check(gtests_dbms)
endif ()

if (TEST_COVERAGE AND CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(COVERAGE_LCOV_EXCLUDES
            '/Library/Developer/CommandLineTools/usr/include/*'
            '/usr/include/*'
            '/usr/local/*'
            '/usr/lib/*'
            '${CMAKE_SOURCE_DIR}/cmake-build-*/*'
            '${CMAKE_SOURCE_DIR}/contrib/*'
            )
    SETUP_TARGET_FOR_COVERAGE_LCOV(
            NAME tiflash_lcov_coverage
            DEPENDENCIES gtests_dbms
            EXECUTABLE gtests_dbms
    )
    set(COVERAGE_GCOVR_EXCLUDES
            'contrib'
            'cmake-build-debugwithcov'
    )
    SETUP_TARGET_FOR_COVERAGE_GCOVR_HTML(
            NAME tiflash_gcovr_coverage
            DEPENDENCIES gtests_dbms
            EXECUTABLE gtests_dbms
    )
endif()
