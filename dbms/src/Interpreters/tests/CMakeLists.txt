add_executable (expression EXCLUDE_FROM_ALL expression.cpp)
target_link_libraries (expression dbms)

add_executable (create_query EXCLUDE_FROM_ALL create_query.cpp)
target_link_libraries (create_query dbms)

add_executable (select_query EXCLUDE_FROM_ALL select_query.cpp)
target_link_libraries (select_query clickhouse_storages_system dbms)

add_executable (aggregate EXCLUDE_FROM_ALL aggregate.cpp)
target_link_libraries (aggregate dbms)

add_executable (hash_map EXCLUDE_FROM_ALL hash_map.cpp)
target_include_directories (hash_map BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map dbms)

add_executable (hash_map3 EXCLUDE_FROM_ALL hash_map3.cpp)
target_link_libraries (hash_map3 dbms ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

add_executable (hash_map_string EXCLUDE_FROM_ALL hash_map_string.cpp)
target_include_directories (hash_map_string BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string dbms)

add_executable (hash_map_string_2 EXCLUDE_FROM_ALL hash_map_string_2.cpp)
target_link_libraries (hash_map_string_2 dbms)

add_executable (hash_map_string_3 EXCLUDE_FROM_ALL hash_map_string_3.cpp)
target_link_libraries (hash_map_string_3 dbms ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})
target_include_directories (hash_map_string_3 BEFORE PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/libfarmhash)
target_include_directories (hash_map_string_3 BEFORE PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)

add_executable (hash_map_string_small EXCLUDE_FROM_ALL hash_map_string_small.cpp)
target_include_directories (hash_map_string_small BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string_small dbms)

add_executable (two_level_hash_map EXCLUDE_FROM_ALL two_level_hash_map.cpp)
target_include_directories (two_level_hash_map BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (two_level_hash_map dbms)

add_executable (compiler_test EXCLUDE_FROM_ALL compiler_test.cpp)
target_link_libraries (compiler_test dbms)

add_executable (logical_expressions_optimizer EXCLUDE_FROM_ALL logical_expressions_optimizer.cpp)
target_link_libraries (logical_expressions_optimizer dbms)

add_executable (in_join_subqueries_preprocessor EXCLUDE_FROM_ALL in_join_subqueries_preprocessor.cpp)
target_link_libraries (in_join_subqueries_preprocessor dbms)
add_check(in_join_subqueries_preprocessor)

add_executable (users EXCLUDE_FROM_ALL users.cpp)
target_link_libraries (users dbms ${Boost_FILESYSTEM_LIBRARY} dbms)
