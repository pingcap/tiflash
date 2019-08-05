include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(SRCS )

add_executable (lexer EXCLUDE_FROM_ALL lexer.cpp ${SRCS})
target_link_libraries (lexer clickhouse_parsers)

add_executable (select_parser EXCLUDE_FROM_ALL select_parser.cpp ${SRCS})
target_link_libraries (select_parser clickhouse_parsers)

add_executable (create_parser EXCLUDE_FROM_ALL create_parser.cpp ${SRCS})
target_link_libraries (create_parser clickhouse_parsers)

