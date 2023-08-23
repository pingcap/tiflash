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

include (${TiFlash_SOURCE_DIR}/cmake/add_check.cmake)

add_executable (date_lut_init date_lut_init.cpp)
add_executable (date_lut2 date_lut2.cpp)
add_executable (date_lut3 date_lut3.cpp)
add_executable (date_lut4 date_lut4.cpp)
add_executable (date_lut_default_timezone date_lut_default_timezone.cpp)
add_executable (multi_version multi_version.cpp)

set(PLATFORM_LIBS ${CMAKE_DL_LIBS})

target_link_libraries (date_lut_init common ${PLATFORM_LIBS})
target_link_libraries (date_lut2 common ${PLATFORM_LIBS})
target_link_libraries (date_lut3 common ${PLATFORM_LIBS})
target_link_libraries (date_lut4 common ${PLATFORM_LIBS})
target_link_libraries (date_lut_default_timezone common ${PLATFORM_LIBS})
target_link_libraries (multi_version common)
add_check(multi_version)

add_executable (gtests_libcommon
    gtest_json_test.cpp
    gtest_strong_typedef.cpp
    gtest_mem_utils.cpp
    gtest_mem_utils_opt.cpp
    gtest_crc64.cpp
    gtest_logger.cpp
    gtest_arithmetic_overflow.cpp
)

add_sources_compile_flag_avx2 (gtest_mem_utils_opt.cpp)

target_link_libraries (gtests_libcommon gtest_main common memcpy)
add_check(gtests_libcommon)

set (bench_libcommon_sources bench_mem_utils.cpp)

if (NOT USE_INTERNAL_MEMCPY)
    list (APPEND bench_libcommon_sources bench_memcpy.cpp)
    add_sources_compile_flag_avx2 (bench_memcpy.cpp)
endif ()

add_executable(bench_libcommon
    # TODO: need to fix broken src file if necessary
    # bench_logger.cpp 
    ${bench_libcommon_sources}
)

target_link_libraries(bench_libcommon benchmark::benchmark_main common m memcpy)

if (ARCH_AMD64)
    set_source_files_properties(
            gtest_crc64.cpp
            APPEND COMPILE_FLAGS "-mpclmul")
endif()

if (ARCH_AARCH64)
    set_source_files_properties(
            gtest_crc64.cpp
            APPEND COMPILE_FLAGS "-march=armv8-a+simd+crypto")
endif()
