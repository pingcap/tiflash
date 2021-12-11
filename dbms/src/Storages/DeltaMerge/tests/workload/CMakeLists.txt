include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable(dt_workload main.cpp DTWorkload.cpp KeyGenerator.cpp TableGenerator.cpp DataGenerator.cpp Limiter.cpp Options.cpp Utils.cpp ${ClickHouse_SOURCE_DIR}/dbms/src/TestUtils/TiFlashTestBasic.cpp)
target_link_libraries(dt_workload dbms gtest clickhouse_functions clickhouse-server-lib)
