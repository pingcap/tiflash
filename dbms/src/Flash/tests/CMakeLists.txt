include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (exchange_perftest
    exchange_perftest.cpp
    ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
    ${TiFlash_SOURCE_DIR}/dbms/src/Functions/FunctionsConversion.cpp)
target_link_libraries (exchange_perftest
    gtest_main
    dbms
    clickhouse_functions
    clickhouse_aggregate_functions
    tiflash-dttool-lib)
