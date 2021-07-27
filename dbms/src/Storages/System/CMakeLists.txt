
include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(storages_system .)
add_library(tiflash_storages_system ${storages_system_headers} ${storages_system_sources})
target_link_libraries(tiflash_storages_system dbms)
