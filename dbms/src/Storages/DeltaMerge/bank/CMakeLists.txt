include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable(bank main.cpp DeltaStorageProxy.cpp)
target_link_libraries(bank dbms gtest_main clickhouse_functions)
