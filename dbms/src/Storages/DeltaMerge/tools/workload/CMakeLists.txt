include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(dt-workload-src MainEntry.cpp DTWorkload.cpp KeyGenerator.cpp TableGenerator.cpp DataGenerator.cpp Limiter.cpp Options.cpp Utils.cpp ${TiFlash_SOURCE_DIR}/dbms/src/TestUtils/TiFlashTestEnv.cpp)

add_library(dt-workload-lib ${dt-workload-src})
target_link_libraries(dt-workload-lib dbms clickhouse_functions clickhouse-server-lib)

add_executable(dt-workload Main.cpp ${dt-workload-src})
target_link_libraries(dt-workload dbms gtest clickhouse_functions clickhouse-server-lib)
