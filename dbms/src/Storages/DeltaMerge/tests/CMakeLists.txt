include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (delta_tree delta_tree.cpp)
add_executable (delta_merge_block_stream delta_merge_block_stream.cpp)
add_executable (delta_merge_storage delta_merge_storage.cpp)
target_link_libraries (delta_tree dbms)
target_link_libraries (delta_merge_block_stream dbms)
target_link_libraries (delta_merge_storage dbms)
