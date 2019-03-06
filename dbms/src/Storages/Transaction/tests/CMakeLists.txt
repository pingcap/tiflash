include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (sync_schema sync_schema.cpp)
target_link_libraries (sync_schema dbms)

add_executable (tikv_keyvalue tikv_keyvalue.cpp)
target_link_libraries (tikv_keyvalue dbms)

add_executable (region_persister region_persister.cpp)
target_link_libraries (region_persister dbms)

add_executable (kvstore kvstore.cpp)
target_link_libraries (kvstore dbms)

add_executable (table_info table_info.cpp)
target_link_libraries (table_info dbms)

add_executable (leveldb_test leveldb_test.cpp)
target_link_libraries (leveldb_test dbms)