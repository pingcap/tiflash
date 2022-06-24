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

ENABLE_LANGUAGE(C)
ENABLE_LANGUAGE(ASM)
set(TIFLASH_AOR_DIR ../arm-optimized-routines)

# GCC/Clang enables ASIMD for Armv8-a by default, we let us use
# ASIMD variant anyway

file(GLOB TIFLASH_AARCH64_STRING_FILES ${TIFLASH_AOR_DIR}/string/aarch64/*.S)
add_library(tiflash-aarch64-string STATIC ${TIFLASH_AARCH64_STRING_FILES} src/aor.cpp)
target_compile_options(tiflash-aarch64-string PRIVATE -march=armv8.8-a+sve)
target_include_directories(tiflash-aarch64-string PRIVATE ${TIFLASH_AOR_DIR}/string/include)
target_include_directories(tiflash-aarch64-string PUBLIC include)

file(GLOB TIFLASH_AARCH64_MATH_FILES ${TIFLASH_AOR_DIR}/math/*.c)
add_library(tiflash-aarch64-math STATIC ${TIFLASH_AARCH64_MATH_FILES})
target_include_directories(tiflash-aarch64-math PRIVATE ${TIFLASH_AOR_DIR}/math/include)