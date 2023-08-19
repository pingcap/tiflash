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

find_program(OBJCOPY_EXECUTABLE "objcopy")

if(OBJCOPY_EXECUTABLE)
    message(STATUS "Compressing debug sections for ${CMAKE_INSTALL_PREFIX}/tiflash...")
    execute_process(COMMAND ${OBJCOPY_EXECUTABLE} --compress-debug-sections=zlib-gnu ${CMAKE_INSTALL_PREFIX}/tiflash)
else()
    message(WARNING "objcopy not found in PATH. Skipped debug section compression.")
endif()
