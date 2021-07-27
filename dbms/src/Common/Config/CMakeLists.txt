
include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(tiflash_common_config .)

add_library(tiflash_common_config ${SPLIT_SHARED} ${tiflash_common_config_headers} ${tiflash_common_config_sources})

target_link_libraries(tiflash_common_config tiflash_common_io string_utils)
target_include_directories(tiflash_common_config PRIVATE ${DBMS_INCLUDE_DIR})
