include_directories (${CMAKE_CURRENT_BINARY_DIR})
set(SRCS )

add_executable (read_buffer EXCLUDE_FROM_ALL read_buffer.cpp ${SRCS})
target_link_libraries (read_buffer clickhouse_common_io)

add_executable (read_buffer_perf EXCLUDE_FROM_ALL read_buffer_perf.cpp ${SRCS})
target_link_libraries (read_buffer_perf clickhouse_common_io)

add_executable (read_float_perf EXCLUDE_FROM_ALL read_float_perf.cpp ${SRCS})
target_link_libraries (read_float_perf clickhouse_common_io)

add_executable (write_buffer EXCLUDE_FROM_ALL write_buffer.cpp ${SRCS})
target_link_libraries (write_buffer clickhouse_common_io)

add_executable (write_buffer_perf EXCLUDE_FROM_ALL write_buffer_perf.cpp ${SRCS})
target_link_libraries (write_buffer_perf clickhouse_common_io)

add_executable (valid_utf8_perf EXCLUDE_FROM_ALL valid_utf8_perf.cpp ${SRCS})
target_link_libraries (valid_utf8_perf clickhouse_common_io)

add_executable (valid_utf8 EXCLUDE_FROM_ALL valid_utf8.cpp ${SRCS})
target_link_libraries (valid_utf8 clickhouse_common_io)

add_executable (compressed_buffer EXCLUDE_FROM_ALL compressed_buffer.cpp ${SRCS})
target_link_libraries (compressed_buffer clickhouse_common_io)

add_executable (var_uint EXCLUDE_FROM_ALL var_uint.cpp ${SRCS})
target_link_libraries (var_uint clickhouse_common_io)

add_executable (read_escaped_string EXCLUDE_FROM_ALL read_escaped_string.cpp ${SRCS})
target_link_libraries (read_escaped_string clickhouse_common_io)

add_executable (async_write EXCLUDE_FROM_ALL async_write.cpp ${SRCS})
target_link_libraries (async_write clickhouse_common_io)

add_executable (parse_int_perf EXCLUDE_FROM_ALL parse_int_perf.cpp ${SRCS})
target_link_libraries (parse_int_perf clickhouse_common_io)

add_executable (parse_int_perf2 EXCLUDE_FROM_ALL parse_int_perf2.cpp ${SRCS})
target_link_libraries (parse_int_perf2 clickhouse_common_io)

add_executable (read_write_int EXCLUDE_FROM_ALL read_write_int.cpp ${SRCS})
target_link_libraries (read_write_int clickhouse_common_io)

add_executable (mempbrk EXCLUDE_FROM_ALL mempbrk.cpp ${SRCS})
target_link_libraries (mempbrk clickhouse_common_io)

add_executable (cached_compressed_read_buffer EXCLUDE_FROM_ALL cached_compressed_read_buffer.cpp ${SRCS})
target_link_libraries (cached_compressed_read_buffer clickhouse_common_io)

add_executable (o_direct_and_dirty_pages EXCLUDE_FROM_ALL o_direct_and_dirty_pages.cpp ${SRCS})
target_link_libraries (o_direct_and_dirty_pages clickhouse_common_io)

add_executable (hashing_write_buffer EXCLUDE_FROM_ALL hashing_write_buffer.cpp ${SRCS})
target_link_libraries (hashing_write_buffer clickhouse_common_io)
add_check(hashing_write_buffer)

add_executable (hashing_read_buffer EXCLUDE_FROM_ALL hashing_read_buffer.cpp)
target_link_libraries (hashing_read_buffer clickhouse_common_io)
add_check (hashing_read_buffer)

add_executable (io_operators EXCLUDE_FROM_ALL operators.cpp ${SRCS})
target_link_libraries (io_operators clickhouse_common_io)

if (NOT APPLE AND NOT ARCH_FREEBSD)
    add_executable(write_buffer_aio write_buffer_aio.cpp)
    target_link_libraries (write_buffer_aio clickhouse_common_io ${Boost_FILESYSTEM_LIBRARY})

    add_executable(read_buffer_aio read_buffer_aio.cpp)
    target_link_libraries (read_buffer_aio clickhouse_common_io ${Boost_FILESYSTEM_LIBRARY})
endif ()

add_executable (zlib_buffers EXCLUDE_FROM_ALL zlib_buffers.cpp ${SRCS})
target_link_libraries (zlib_buffers clickhouse_common_io)

add_executable (remote_read_write_buffer EXCLUDE_FROM_ALL remote_read_write_buffer.cpp ${SRCS})
target_link_libraries (remote_read_write_buffer clickhouse_common_io)

add_executable (limit_read_buffer EXCLUDE_FROM_ALL limit_read_buffer.cpp ${SRCS})
target_link_libraries (limit_read_buffer clickhouse_common_io)

add_executable (limit_read_buffer2 EXCLUDE_FROM_ALL limit_read_buffer2.cpp ${SRCS})
target_link_libraries (limit_read_buffer2 clickhouse_common_io)

add_executable (parse_date_time_best_effort EXCLUDE_FROM_ALL parse_date_time_best_effort.cpp ${SRCS})
target_link_libraries (parse_date_time_best_effort clickhouse_common_io)
