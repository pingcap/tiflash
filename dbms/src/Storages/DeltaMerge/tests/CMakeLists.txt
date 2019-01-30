include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (delta_tree delta_tree.cpp)
target_link_libraries (delta_tree dbms)
