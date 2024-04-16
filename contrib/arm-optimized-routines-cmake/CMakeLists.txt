# Copyright 2022 PingCAP, Inc.
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

# This library is to override performance-critical routines for aarch64 targets.
# The implementations are imported from official ARM repo.
# To reduce dispatching cost, indirect function technique is utilized. Therefore,
# this library should only be enabled with ELF targets.

# Considerations:
# - By Jun, 2022, most enterprise OSs (CentOS 7, CentOS Stream 8 and RHEL 8) still
#   use relatively old glibc on ARM64, where ASIMD, MTE, DC ZVA and SVE are not
#   fully utilized. However, it is becoming increasingly common to use ARM64 instances
#   in cloud-native situations.
# - `optimized-routines` repo is actively maintained by ARM officials. Therefore,
#   the qualities can be ensured while using it also enables us to keep sync with latest
#   acceleration techniques.

ENABLE_LANGUAGE(C)
ENABLE_LANGUAGE(ASM)
set(TIFLASH_AOR_DIR ${TiFlash_SOURCE_DIR}/contrib/arm-optimized-routines)

file(GLOB TIFLASH_AARCH64_STRING_FILES ${TIFLASH_AOR_DIR}/string/aarch64/*.S)
add_library(tiflash-aarch64-string STATIC ${TIFLASH_AARCH64_STRING_FILES})
target_include_directories(tiflash-aarch64-string PRIVATE ${TIFLASH_AOR_DIR}/string/include)

file(GLOB TIFLASH_AARCH64_MATH_FILES ${TIFLASH_AOR_DIR}/math/*.c)
add_library(tiflash-aarch64-math STATIC ${TIFLASH_AARCH64_MATH_FILES})
target_include_directories(tiflash-aarch64-math PRIVATE ${TIFLASH_AOR_DIR}/math/include)

# it is reasonable to keep these libraries optimized
target_compile_options(tiflash-aarch64-string PRIVATE -O3 -g3 -fno-omit-frame-pointer -ffunction-sections -fdata-sections)
target_compile_options(tiflash-aarch64-math PRIVATE -O3 -g3 -fno-omit-frame-pointer -ffunction-sections -fdata-sections)
