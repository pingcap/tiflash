include_directories (${CMAKE_CURRENT_BINARY_DIR})

# Do not add this source file to gtests_dbms since it has its own main function.
add_executable (number_traits number_traits.cpp)
target_link_libraries (number_traits dbms)
