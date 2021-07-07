add_subdirectory (kvproto/cpp)
# -----
set (_save ${ENABLE_TESTS})
set (ENABLE_TESTS 0)
# disable ENABLE_TESTS for client-c
# GTest package conflicts with CH's GTest, so we have to disable test compilation in ch.
add_subdirectory (client-c)
set (ENABLE_TESTS ${_save})
# -------
add_subdirectory (tipb/cpp)

if (NOT MSVC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast")
endif ()

if (USE_INTERNAL_BOOST_LIBRARY)
    add_subdirectory (boost-cmake)
endif ()

if (USE_INTERNAL_LZ4_LIBRARY)
    add_subdirectory (lz4-cmake)
endif ()

if (USE_INTERNAL_ZSTD_LIBRARY)
    add_subdirectory (zstd-cmake)
endif ()

if (USE_INTERNAL_RE2_LIBRARY)
    set(RE2_BUILD_TESTING 0 CACHE INTERNAL "")
    add_subdirectory (re2)
    add_subdirectory (re2_st)
endif ()

if (USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY)
    set (BUILD_TESTING ${ENABLE_TESTS} CACHE INTERNAL "")
    add_subdirectory (double-conversion)
endif ()

if (USE_INTERNAL_CITYHASH_LIBRARY)
    add_subdirectory (libcityhash)
endif ()

if (USE_INTERNAL_FARMHASH_LIBRARY)
    add_subdirectory (libfarmhash)
endif ()

if (USE_INTERNAL_METROHASH_LIBRARY)
    add_subdirectory (libmetrohash)
endif ()

if (USE_INTERNAL_BTRIE_LIBRARY)
    add_subdirectory (libbtrie)
endif ()

if (USE_INTERNAL_UNWIND_LIBRARY)
    add_subdirectory (libunwind-cmake)
endif ()

if (USE_INTERNAL_ZLIB_LIBRARY)
    add_subdirectory (${INTERNAL_ZLIB_NAME})
    # todo: make pull to Dead2/zlib-ng and remove:
    # We should use same defines when including zlib.h as used when zlib compiled
    target_compile_definitions (zlib PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
    target_compile_definitions (zlibstatic PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "AMD64")
       target_compile_definitions (zlib PUBLIC X86_64)
       target_compile_definitions (zlibstatic PUBLIC X86_64)
    endif ()

    set_target_properties(example PROPERTIES EXCLUDE_FROM_ALL 1)
    if (TARGET example64)
        set_target_properties(example64 PROPERTIES EXCLUDE_FROM_ALL 1)
    endif ()

    set_target_properties(minigzip PROPERTIES EXCLUDE_FROM_ALL 1)
    if (TARGET minigzip64)
        set_target_properties(minigzip64 PROPERTIES EXCLUDE_FROM_ALL 1)
    endif ()
endif ()

if (USE_INTERNAL_CCTZ_LIBRARY)
    add_subdirectory (cctz-cmake)
endif ()

# TODO: remove tcmalloc
if (ENABLE_TCMALLOC AND USE_INTERNAL_GPERFTOOLS_LIBRARY)
    add_subdirectory (libtcmalloc)
endif ()

if (ENABLE_JEMALLOC AND USE_INTERNAL_JEMALLOC_LIBRARY)
    add_subdirectory (jemalloc-cmake)
endif ()

if (ENABLE_MIMALLOC)
    # we use object library on default
    set(MI_BUILD_SHARED OFF CACHE BOOL "disable mimalloc shared library on default" FORCE)
    set(MI_BUILD_TESTS OFF CACHE BOOL "disable mimalloc tests on default" FORCE)
    set(MI_BUILD_STATIC OFF CACHE BOOL "disable mimalloc static library on default" FORCE)
    add_subdirectory (mimalloc)
endif()

if (NOT ARCH_ARM)
    add_subdirectory (libcpuid)
endif ()

if (USE_INTERNAL_SSL_LIBRARY)
    if (NOT MAKE_STATIC_LIBRARIES)
        set (BUILD_SHARED 1)
    endif ()
    set (USE_SHARED ${USE_STATIC_LIBRARIES})
    add_subdirectory (ssl)
    target_include_directories(${OPENSSL_CRYPTO_LIBRARY} PUBLIC ${OPENSSL_INCLUDE_DIR})
    target_include_directories(${OPENSSL_SSL_LIBRARY} PUBLIC ${OPENSSL_INCLUDE_DIR})
endif ()

if (USE_INTERNAL_POCO_LIBRARY)
    set (save_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
    set (save_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
    set (_save ${ENABLE_TESTS})
    set (ENABLE_TESTS 0)
    set (CMAKE_DISABLE_FIND_PACKAGE_ZLIB 1)
    if (USE_INTERNAL_SSL_LIBRARY)
        set (DISABLE_INTERNAL_OPENSSL 1 CACHE INTERNAL "")
        set (ENABLE_NETSSL 0 CACHE INTERNAL "") # TODO!
        set (ENABLE_CRYPTO 0 CACHE INTERNAL "") # TODO!
    endif ()
    if (MSVC)
        set (ENABLE_DATA_ODBC 0 CACHE INTERNAL "") # TODO (build fail)
    endif ()
    add_subdirectory (poco)
    unset (CMAKE_DISABLE_FIND_PACKAGE_ZLIB)
    set (ENABLE_TESTS ${_save})
    set (CMAKE_CXX_FLAGS ${save_CMAKE_CXX_FLAGS})
    set (CMAKE_C_FLAGS ${save_CMAKE_C_FLAGS})
    if (OPENSSL_FOUND AND TARGET Crypto)
        # Bug in poco https://github.com/pocoproject/poco/pull/2100 found on macos
        target_include_directories(Crypto PUBLIC ${OPENSSL_INCLUDE_DIR})
    endif ()
endif ()

add_subdirectory (prometheus-cpp)
add_subdirectory (fmtlib-cmake)
