include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (system_numbers EXCLUDE_FROM_ALL system_numbers.cpp)
target_link_libraries (system_numbers dbms clickhouse_storages_system)

add_executable (storage_log EXCLUDE_FROM_ALL storage_log.cpp)
target_link_libraries (storage_log dbms)

add_executable (hit_log EXCLUDE_FROM_ALL hit_log.cpp)
target_link_libraries (hit_log dbms)

add_executable (seek_speed_test EXCLUDE_FROM_ALL seek_speed_test.cpp)
target_link_libraries (seek_speed_test dbms)

add_executable (part_checker EXCLUDE_FROM_ALL part_checker.cpp)
target_link_libraries (part_checker dbms)

add_executable (part_name EXCLUDE_FROM_ALL part_name.cpp)
target_link_libraries (part_name dbms)

add_executable (remove_symlink_directory EXCLUDE_FROM_ALL remove_symlink_directory.cpp)
target_link_libraries (remove_symlink_directory dbms)

add_executable (merge_selector EXCLUDE_FROM_ALL merge_selector.cpp)
target_link_libraries (merge_selector dbms)

add_executable (merge_selector2 EXCLUDE_FROM_ALL merge_selector2.cpp)
target_link_libraries (merge_selector2 dbms)
