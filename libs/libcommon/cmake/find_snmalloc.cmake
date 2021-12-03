option (ENABLE_SNMALLOC "Set to ON to use snmalloc" OFF)

if (ENABLE_SNMALLOC)
    if (NOT CMAKE_VERSION VERSION_GREATER_EQUAL "3.14.0")
        message(FATAL_ERROR "CMake is too old to use snmalloc")
    endif()

    check_cxx_source_compiles(
    "
#include <sys/mman.h>
int main() {
    return MADV_FREE;
}
    "
    TIFLASH_COMPILE_ENV_HAS_MADV_FREE)

    set (USE_SNMALLOC 1)
    message (STATUS "Using snmalloc=${USE_SNMALLOC}: snmallocshim-static")

    if (ENABLE_JEMALLOC OR ENABLE_TCMALLOC OR ENABLE_MIMALLOC)
        message(FATAL_ERROR "multiple global allocator detected!")
    endif()

    set(SNMALLOC_USE_CXX17 ON CACHE BOOL "use c++17 in snmalloc" FORCE)
    set(SNMALLOC_STATIC_LIBRARY_PREFIX "" CACHE STRING "override snmalloc prefix" FORCE)
    add_subdirectory(${ClickHouse_SOURCE_DIR}/contrib/snmalloc)

    if (NOT TIFLASH_COMPILE_ENV_HAS_MADV_FREE)
        message(WARNING "compiling env does not have MADV_FREE, defaulting to 8")
        target_compile_definitions(snmalloc INTERFACE -DMADV_FREE=8)
    endif ()
    include_directories(${ClickHouse_SOURCE_DIR}/contrib/snmalloc/src)
    set(SNMALLOC_LIBRARIES snmallocshim-static)
endif()

