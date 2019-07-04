include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (number_traits number_traits.cpp)
target_link_libraries (number_traits dbms)

add_executable (strings_trim strings_trim.cpp)
target_link_libraries (strings_trim dbms gtest_main clickhouse_functions)

add_executable (strings_pad strings_pad.cpp)
target_link_libraries (strings_pad dbms gtest_main clickhouse_functions)

add_executable(functions_test_decimal_arith gtest_funtions_decimal_arith.cpp)
target_link_libraries(functions_test_decimal_arith clickhouse_functions gtest_main)
target_include_directories(functions_test_decimal_arith PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/libdivide)
