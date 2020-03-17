include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable(bank2 main.cpp DeltaStorageProxy.cpp IDGenerator.cpp SimpleLockManager.cpp)
target_link_libraries(bank2 dbms gtest_main clickhouse_functions)
