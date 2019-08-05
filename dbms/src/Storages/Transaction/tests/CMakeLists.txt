include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (tikv_keyvalue EXCLUDE_FROM_ALL tikv_keyvalue.cpp)
target_link_libraries (tikv_keyvalue dbms)

add_executable (region_persister EXCLUDE_FROM_ALL region_persister.cpp)
target_link_libraries (region_persister dbms)

add_executable (kvstore EXCLUDE_FROM_ALL kvstore.cpp)
target_link_libraries (kvstore dbms)

add_executable (table_info EXCLUDE_FROM_ALL table_info.cpp)
target_link_libraries (table_info dbms)
