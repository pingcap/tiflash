set(CRC64_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc64-cxx/include)
set(CRC64_INCREMENTAL_DIR ${CMAKE_BINARY_DIR}/contrib/crc64-cxx/include)

set(CRC64_ENABLE_TESTS OFF CACHE BOOL "Disable CRC64 Tests" FORCE)
set(CRC64_ENABLE_BENCHES OFF CACHE BOOL "Disable CRC64 Benches" FORCE)
set(CRC64_ENABLE_CAPI OFF CACHE BOOL "Disable CRC64 CAPI" FORCE)

if (BUILD_TESTING)
    set(BUILD_TESTING OFF CACHE BOOL "Disable CRC64 TESTS" FORCE)
    add_subdirectory(${ClickHouse_SOURCE_DIR}/contrib/crc64-cxx)
    set(BUILD_TESTING ON CACHE BOOL "Disable CRC64 TESTS" FORCE)
else ()
    add_subdirectory(${ClickHouse_SOURCE_DIR}/contrib/crc64-cxx)
endif ()