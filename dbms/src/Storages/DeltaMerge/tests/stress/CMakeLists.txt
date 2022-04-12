include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable(dm_stress main.cpp DMStressProxy.cpp ${TiFlash_SOURCE_DIR}/dbms/src/TestUtils/TiFlashTestBasic.cpp)
target_link_libraries(dm_stress dbms gtest clickhouse_functions clickhouse-server-lib)
