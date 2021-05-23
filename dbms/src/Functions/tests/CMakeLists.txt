include_directories (${CMAKE_CURRENT_BINARY_DIR})

# Do not add this source file to gtests_dbms since it has its own main function.
add_executable (number_traits number_traits.cpp)
target_link_libraries (number_traits dbms)

add_executable (strings_trim gtest_strings_trim.cpp)
target_link_libraries (strings_trim dbms gtest_main clickhouse_functions)

add_executable (strings_pad gtest_strings_pad.cpp)
target_link_libraries (strings_pad dbms gtest_main clickhouse_functions)

add_executable (datetime_extract gtest_datetime_extract.cpp)
target_link_libraries (datetime_extract dbms gtest_main clickhouse_functions)

add_executable (arithmetic_functions gtest_arithmetic_functions.cpp)
target_link_libraries (arithmetic_functions dbms gtest_main clickhouse_functions)

add_executable (strings_replace gtest_strings_replace.cpp)
target_link_libraries (strings_replace dbms gtest_main clickhouse_functions)
