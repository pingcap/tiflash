include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (cop_test cop_test.cpp)
target_link_libraries (cop_test dbms)
