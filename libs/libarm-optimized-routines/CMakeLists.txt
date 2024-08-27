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

# TODO Enabling this feature causes "is missing exception specification" error on rockylinux8 and arm64
option(USE_ARM_OPTIMIZED_ROUTINES "Use ARM optimized routines" OFF)

set (aor_sources)

if (ARCH_AARCH64 AND OS_LINUX AND USE_ARM_OPTIMIZED_ROUTINES)
    list (APPEND aor_sources aor.cpp aor.h)
else ()
    set (USE_ARM_OPTIMIZED_ROUTINES OFF)
endif ()

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_ASIMD_SUPPORT
    "-march=armv8-a+simd"
    aor_sources
)

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_SVE_SUPPORT
    "-march=armv8-a+sve"
    aor_sources
)

if (USE_ARM_OPTIMIZED_ROUTINES)
    add_library (aor STATIC ${aor_sources})
    target_link_libraries(aor PUBLIC tiflash-aarch64-string tiflash-aarch64-math)
    target_include_directories(aor PUBLIC ${TiFlash_SOURCE_DIR}/contrib/arm-optimized-routines/string/include)

    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fomit-frame-pointer")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fomit-frame-pointer")
endif ()
