include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (tikv_keyvalue tikv_keyvalue.cpp)
target_link_libraries (tikv_keyvalue dbms)

add_executable (region_persister region_persister.cpp)
target_link_libraries (region_persister dbms)

add_executable (region_test region.cpp)
target_link_libraries (region_test dbms)
