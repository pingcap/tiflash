# Only enable under linux
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(ENABLE_JEMALLOC_DEFAULT 1)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
    set(ENABLE_JEMALLOC_DEFAULT 0)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(ENABLE_JEMALLOC_DEFAULT 0)
endif()

option (ENABLE_JEMALLOC "Set to TRUE to use jemalloc" ${ENABLE_JEMALLOC_DEFAULT})
option (USE_INTERNAL_JEMALLOC_LIBRARY "Set to FALSE to use system jemalloc library instead of bundled" ${NOT_UNBUNDLED})

if (ENABLE_JEMALLOC)
    if (USE_INTERNAL_JEMALLOC_LIBRARY)
        set (JEMALLOC_LIBRARIES "jemalloc")
    else ()
        find_package (JeMalloc)
    endif ()

    if (JEMALLOC_LIBRARIES)
        set (USE_JEMALLOC 1)
    else ()
        message (FATAL_ERROR "ENABLE_JEMALLOC is set to true, but library was not found")
    endif ()

    message (STATUS "Using jemalloc=${USE_JEMALLOC}: ${JEMALLOC_LIBRARIES}")
endif ()

