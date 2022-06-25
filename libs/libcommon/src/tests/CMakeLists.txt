# Copyright 2022 PingCAP, Ltd.
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
    gtest_crc64.cpp
    gtest_logger.cpp
    gtest_arithmetic_overflow.cpp
    )
target_link_libraries (gtests_libcommon gtest_main common)
add_check(gtests_libcommon)

add_executable(bench_libcommon
    bench_logger.cpp
)
target_link_libraries(bench_libcommon benchmark::benchmark_main common)

add_executable (dump_variable dump_variable.cpp)
target_link_libraries (dump_variable clickhouse_common_io)

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
