option (ENABLE_MIMALLOC "Set to ON to use mimalloc" OFF)

if (ENABLE_MIMALLOC)
    set (MIMALLOC_LIBRARIES "mimalloc-obj")
    set (USE_MIMALLOC 1)
    message (STATUS "Using mimalloc=${USE_MIMALLOC}: ${MIMALLOC_LIBRARIES}")

    if (ENABLE_JEMALLOC OR ENABLE_TCMALLOC)
        message(FATAL_ERROR "multiple global allocator detected!")
    endif()
endif ()

