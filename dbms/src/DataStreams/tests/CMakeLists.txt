include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(SRCS )

add_executable (tab_separated_streams EXCLUDE_FROM_ALL tab_separated_streams.cpp ${SRCS})
target_link_libraries (tab_separated_streams dbms clickhouse_functions)

add_executable (block_row_transforms EXCLUDE_FROM_ALL block_row_transforms.cpp ${SRCS})
target_link_libraries (block_row_transforms dbms clickhouse_functions)

add_executable (expression_stream EXCLUDE_FROM_ALL expression_stream.cpp ${SRCS})
target_link_libraries (expression_stream dbms clickhouse_storages_system clickhouse_functions)

add_executable (native_streams EXCLUDE_FROM_ALL native_streams.cpp ${SRCS})
target_link_libraries (native_streams dbms clickhouse_functions)

add_executable (filter_stream EXCLUDE_FROM_ALL filter_stream.cpp ${SRCS})
target_link_libraries (filter_stream dbms clickhouse_storages_system clickhouse_functions)

add_executable (filter_stream_hitlog EXCLUDE_FROM_ALL filter_stream_hitlog.cpp ${SRCS})
target_link_libraries (filter_stream_hitlog dbms clickhouse_functions)

add_executable (sorting_stream EXCLUDE_FROM_ALL sorting_stream.cpp ${SRCS})
target_link_libraries (sorting_stream dbms clickhouse_functions)

add_executable (union_stream2 EXCLUDE_FROM_ALL union_stream2.cpp ${SRCS})
target_link_libraries (union_stream2 dbms clickhouse_functions)

add_executable (collapsing_sorted_stream EXCLUDE_FROM_ALL collapsing_sorted_stream.cpp ${SRCS})
target_link_libraries (collapsing_sorted_stream dbms clickhouse_functions)
