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

find_program (OBJCOPY_PATH NAMES "objcopy")
find_program (LLVM_OBJCOPY_PATH NAMES "llvm-objcopy${COMPILER_POSTFIX}" "llvm-objcopy")

if (LLVM_OBJCOPY_PATH)
    set(CMAKE_OBJCOPY ${LLVM_OBJCOPY_PATH})
else ()
    set(CMAKE_OBJCOPY ${OBJCOPY_PATH})
endif ()

message(STATUS "executing: ${CMAKE_OBJCOPY} --compress-debug-sections=zlib-gnu ${CMAKE_INSTALL_PREFIX}/tiflash")
execute_process(COMMAND ${CMAKE_OBJCOPY} --compress-debug-sections=zlib-gnu ${CMAKE_INSTALL_PREFIX}/tiflash)