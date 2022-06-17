include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(flash_service .)
add_headers_and_sources(flash_service ./Coprocessor)
add_headers_and_sources(flash_service ./Mpp)

add_library(flash_service ${flash_service_headers} ${flash_service_sources})
target_link_libraries(flash_service dbms)

if (ENABLE_TESTS)
    add_subdirectory (Coprocessor/tests)
    add_subdirectory (tests)
endif ()
