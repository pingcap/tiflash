include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable(bank main.cpp DeltaMergeStoreProxy.cpp)
target_link_libraries(bank dbms gtest clickhouse_functions)
