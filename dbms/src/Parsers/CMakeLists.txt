include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(tiflash_parsers .)
add_library(tiflash_parsers ${SPLIT_SHARED} ${tiflash_parsers_headers} ${tiflash_parsers_sources})
target_link_libraries (tiflash_parsers tiflash_common_io)
target_include_directories (tiflash_parsers PUBLIC ${DBMS_INCLUDE_DIR})

if (ENABLE_TESTS)
    add_subdirectory (tests EXCLUDE_FROM_ALL)
endif ()
