# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (APPLE)
    if ("${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin" AND NOT "${CMAKE_SYSTEM_VERSION}" VERSION_LESS "16.0.0")
        set (APPLE_SIERRA_OR_NEWER 1)
    else ()
        set (APPLE_SIERRA_OR_NEWER 0)
    endif ()
endif ()

set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/include/common/config_common.h)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/include/common/config_common.h.in ${CONFIG_COMMON})

if (APPLE)
    add_library (apple_rt
        src/apple_rt.cpp
        include/common/apple_rt.h
    )
    target_include_directories (apple_rt PUBLIC ${COMMON_INCLUDE_DIR})
endif ()

add_library (common ${SPLIT_SHARED}
    src/DateLUT.cpp
    src/DateLUTImpl.cpp
    src/preciseExp10.cpp
    src/shift10.cpp
    src/mremap.cpp
    src/JSON.cpp
    src/getMemoryAmount.cpp
    src/ThreadPool.cpp
    src/demangle.cpp
    src/StringRef.cpp
    src/mem_utils_asimd.cpp
    src/mem_utils_avx2.cpp
    src/mem_utils_avx512.cpp
    src/mem_utils_sse2.cpp
    src/crc64_sse2_asimd.cpp
    src/crc64_avx2.cpp
    src/crc64_avx512.cpp
    src/crc64.cpp
    src/simd.cpp
    src/detect_features.cpp
    src/avx2_mem_utils_impl.cpp

    include/common/types.h
    include/common/DateLUT.h
    include/common/DateLUTImpl.h
    include/common/LocalDate.h
    include/common/LocalDateTime.h
    include/common/ErrorHandlers.h
    include/common/preciseExp10.h
    include/common/shift10.h
    include/common/intExp.h
    include/common/mremap.h
    include/common/likely.h
    include/common/logger_useful.h
    include/common/MultiVersion.h
    include/common/strong_typedef.h
    include/common/JSON.h
    include/common/simd.h
    include/common/getMemoryAmount.h
    include/common/ThreadPool.h
    include/common/demangle.h
    include/common/mem_utils.h
    include/common/crc64_arch/crc64_aarch64.h
    include/common/crc64_arch/crc64_x86.h
    include/common/crc64_table.h
    include/common/crc64_fast.h
    include/common/crc64.h
    include/common/detect_features.h

    include/ext/bit_cast.h
    include/ext/collection_cast.h
    include/ext/enumerate.h
    include/ext/function_traits.h
    include/ext/identity.h
    include/ext/map.h
    include/ext/range.h
    include/ext/scope_guard.h
    include/ext/size.h
    include/ext/unlock_guard.h
    include/ext/singleton.h

    ${CONFIG_COMMON}
)

# When testing for memory leaks with Valgrind, dont link tcmalloc or jemalloc.

if (USE_JEMALLOC)
    message (STATUS "Link jemalloc: ${JEMALLOC_LIBRARIES}")
    set (MALLOC_LIBRARIES ${JEMALLOC_LIBRARIES})
elseif(USE_MIMALLOC)
    message (STATUS "Link mimalloc: ${MIMALLOC_LIBRARIES}")
    set (MALLOC_LIBRARIES ${MIMALLOC_LIBRARIES})
else ()
    message (WARNING "Non default allocator is disabled. This is not recommended for production Linux builds.")
endif ()

if (GLIBC_COMPATIBILITY)
    set (GLIBC_COMPATIBILITY_LIBRARIES glibc-compatibility)
endif ()

if (ARCH_AARCH64 AND OS_LINUX)
    target_link_libraries (common PUBLIC aor)
elseif (USE_INTERNAL_MEMCPY)
    set (MEMCPY_LIBRARIES memcpy)
endif ()

find_package (Threads)

target_include_directories (common BEFORE PRIVATE ${CCTZ_INCLUDE_DIR})
target_include_directories (common BEFORE PUBLIC ${CITYHASH_INCLUDE_DIR})
target_include_directories (common PUBLIC ${COMMON_INCLUDE_DIR})

target_include_directories (common BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

if(OS_DARWIN AND ARCH_AARCH64)
    set(CPU_FEATURES_LIBRARY)
else()
    set(CPU_FEATURES_LIBRARY cpu_features)
endif()

# `libcommon` provides a bridge for many other libs.
# We expose the linkage to public because we want headers to be inherited.
target_link_libraries (
    common
    PUBLIC
    pocoext
    ${CITYHASH_LIBRARIES}
    ${CCTZ_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_FILESYSTEM_LIBRARY}
    ${MALLOC_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${GLIBC_COMPATIBILITY_LIBRARIES}
    ${MEMCPY_LIBRARIES}
    fmt
    ${CPU_FEATURES_LIBRARY}
)

if (RT_LIBRARY)
    target_link_libraries (common PUBLIC ${RT_LIBRARY})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (src/tests EXCLUDE_FROM_ALL)
endif ()

# Region for specialized CPU flags tuning

check_then_add_sources_compile_flag (
    TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT
    "-mvpclmulqdq;-Wno-ignored-attributes"
    src/crc64_avx2.cpp
    src/crc64_avx512.cpp
)

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT
    "${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}"
    src/mem_utils_avx2.cpp
    src/crc64_avx2.cpp
    src/avx2_mem_utils_impl.cpp
)

set_property(SOURCE src/avx2_mem_utils_impl.cpp APPEND PROPERTY COMPILE_OPTIONS "-fomit-frame-pointer") # remove frame pointer

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_AVX512_SUPPORT
    "-mavx512bw;-mavx512vl;-mavx512f;-mavx512dq"
    src/mem_utils_avx512.cpp
    src/crc64_avx512.cpp
)

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_ASIMD_SUPPORT
    "-march=armv8-a+simd"
    src/mem_utils_asimd.cpp
)

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_ASIMD_SUPPORT
    "-march=armv8-a+simd+crypto"
    src/crc64_sse2_asimd.cpp
)

if (TIFLASH_ENABLE_SVE_SUPPORT)
    # https://gcc.gnu.org/onlinedocs/gcc/ARM-Options.html
    # set_source_files_properties(<SOURCE_FILE> APPEND COMPILE_FLAGS "-march=armv8.6-a+sve")
endif ()
