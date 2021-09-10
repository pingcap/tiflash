include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_functions .)
add_headers_and_sources(clickhouse_functions ./GatherUtils)
add_headers_and_sources(clickhouse_functions ./Conditional)
add_headers_and_sources(clickhouse_functions ${ClickHouse_BINARY_DIR}/dbms/src/Functions)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

target_link_libraries(clickhouse_functions PUBLIC dbms PRIVATE libconsistent-hashing ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

target_include_directories (clickhouse_functions BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libfarmhash)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${DIVIDE_INCLUDE_DIR})

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif ()

if (USE_ICU)
    #target_link_libraries (clickhouse_functions ${ICU_LIBS})
    target_include_directories (clickhouse_functions PRIVATE ${ICU_INCLUDE_DIR})
endif ()

if (USE_VECTORCLASS)
    target_include_directories (clickhouse_functions BEFORE PUBLIC ${VECTORCLASS_INCLUDE_DIR})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests EXCLUDE_FROM_ALL)
endif ()
