set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)

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

if (USE_QPL AND USE_INTERNAL_QPL_LIBRARY)
    add_subdirectory (qpl-cmake)
endif ()

if (USE_INTERNAL_RE2_LIBRARY)
    add_subdirectory (re2-cmake)
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
    set (CMAKE_POLICY_DEFAULT_CMP0077 NEW)
    add_subdirectory (${INTERNAL_ZLIB_NAME})
    unset (CMAKE_POLICY_DEFAULT_CMP0077)
endif ()

if (USE_INTERNAL_CCTZ_LIBRARY)
    add_subdirectory (cctz-cmake)
endif ()

if (USE_INTERNAL_SSL_LIBRARY)
    add_subdirectory (boringssl-cmake)

    add_library (OpenSSL::Crypto ALIAS crypto)
    add_library (OpenSSL::SSL ALIAS ssl)
endif ()

if (USE_GM_SSL)
    set (save_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
    set (save_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
    set (_save ${ENABLE_TESTS})
    set (CMAKE_CXX_FLAGS "")
    set (CMAKE_C_FLAGS "")
    set (ENABLE_TESTS 0)
    add_subdirectory (GmSSL)
    target_no_warning(gmssl unused-value)
    target_no_warning(gmssl implicit-function-declaration)
    set (ENABLE_TESTS ${_save})
    set (CMAKE_CXX_FLAGS ${save_CMAKE_CXX_FLAGS})
    set (CMAKE_C_FLAGS ${save_CMAKE_C_FLAGS})
endif()

if (USE_INTERNAL_POCO_LIBRARY)
    set (save_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
    set (save_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
    set (_save ${ENABLE_TESTS})
    set (ENABLE_TESTS 0)
    set (CMAKE_DISABLE_FIND_PACKAGE_ZLIB 1)
    if (USE_INTERNAL_SSL_LIBRARY)
        set (DISABLE_INTERNAL_OPENSSL 1 CACHE INTERNAL "")
    endif ()
    if (MSVC)
        set (ENABLE_DATA_ODBC 0 CACHE INTERNAL "") # TODO (build fail)
    endif ()
    add_subdirectory (poco)
    target_compile_options(Net PUBLIC "-Wno-error=deprecated-declarations")
    target_compile_options(Net PUBLIC "-Wno-deprecated-declarations")
    target_no_warning(NetSSL deprecated-declarations)
    target_no_warning(Foundation deprecated-declarations)
    target_no_warning(Net deprecated-enum-enum-conversion)
    unset (CMAKE_DISABLE_FIND_PACKAGE_ZLIB)
    set (ENABLE_TESTS ${_save})
    set (CMAKE_CXX_FLAGS ${save_CMAKE_CXX_FLAGS})
    set (CMAKE_C_FLAGS ${save_CMAKE_C_FLAGS})
    if (OPENSSL_FOUND AND TARGET Crypto)
        # Bug in poco https://github.com/pocoproject/poco/pull/2100 found on macos
        target_include_directories(Crypto PUBLIC ${OPENSSL_INCLUDE_DIR})
    endif ()
endif ()

if (USE_INTERNAL_PROTOBUF_LIBRARY)
    add_subdirectory(protobuf-cmake)
endif ()

if (USE_INTERNAL_GRPC_LIBRARY)
    # Variable RANDOM_FILE will be defined and cached by curl, and break c-ares build if CMake runs the second time.
    unset(RANDOM_FILE CACHE)
    add_subdirectory(abseil-cpp-cmake)
    set (CMAKE_POLICY_DEFAULT_CMP0077 NEW)
    add_subdirectory(grpc-cmake)
    unset (CMAKE_POLICY_DEFAULT_CMP0077)
endif ()

add_subdirectory (kvproto/cpp)
target_no_warning(kvproto deprecated-declarations)
target_no_warning(kvproto non-c-typedef-for-linkage)
target_no_warning(kvproto implicit-const-int-float-conversion)
# -----
set (_save ${ENABLE_TESTS})
set (ENABLE_TESTS 0)
# disable ENABLE_TESTS for client-c
# GTest package conflicts with TiFlash's GTest, so we have to disable test compilation in client-c.
add_subdirectory (client-c)
target_no_warning(kv_client deprecated-declarations)
target_no_warning(kv_client non-c-typedef-for-linkage)
target_no_warning(kv_client implicit-const-int-float-conversion)
set (ENABLE_TESTS ${_save})
# -------
add_subdirectory (tipb/cpp)

add_subdirectory (etcd)

if (USE_INTERNAL_TIFLASH_PROXY)
    add_subdirectory (tiflash-proxy-cmake)
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

if (USE_INTERNAL_CURL_LIBRARY)
    add_subdirectory (curl-cmake)
endif ()

set (CMAKE_POLICY_DEFAULT_CMP0077 NEW)
add_subdirectory (prometheus-cpp-cmake)
unset (CMAKE_POLICY_DEFAULT_CMP0077)

add_subdirectory (fmtlib-cmake)

# Add a target that pop include directory to the linked object
add_library(cpptoml INTERFACE)
target_include_directories(cpptoml INTERFACE
    ${PROJECT_SOURCE_DIR}/contrib/cpptoml)

SET (BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Disable google-benchmark testing" FORCE)
SET (BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "Disable google-benchmark testing" FORCE)
add_subdirectory(benchmark)
target_compile_options(benchmark PRIVATE "-Wno-error=thread-safety-analysis")
target_no_warning(benchmark thread-safety-analysis)

set (BUILD_TESTING OFF CACHE BOOL "Disable cpu-features testing" FORCE)
if (NOT (OS_DARWIN AND ARCH_AARCH64))
    add_subdirectory(cpu_features)
endif()

if (ARCH_AARCH64 AND OS_LINUX)
    add_subdirectory(arm-optimized-routines-cmake)
endif ()

add_subdirectory(magic_enum)

add_subdirectory(aws-cmake)

add_subdirectory(simdjson)

add_subdirectory(fastpforlib)

add_subdirectory(usearch-cmake)
