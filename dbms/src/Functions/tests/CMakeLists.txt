include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (number_traits number_traits.cpp)
target_link_libraries (number_traits dbms)

add_executable (strings_trim strings_trim.cpp)
target_link_libraries (strings_trim dbms gtest_main clickhouse_functions)

add_executable (strings_pad strings_pad.cpp)
target_link_libraries (strings_pad dbms gtest_main clickhouse_functions)