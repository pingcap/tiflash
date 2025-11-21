# Copyright 2025 PingCAP, Inc.
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

find_program (OBJCOPY_EXECUTABLE "objcopy")
if (OBJCOPY_EXECUTABLE)
    message (STATUS "Compressing debug sections for libclara...")
    # Note: CMAKE_SHARED_LIBRARY_PREFIX is not available in the cmake script mode. So we manually check the library name.
    if (EXISTS ${CMAKE_INSTALL_PREFIX}/libclara_shared.so)
        execute_process (COMMAND ${OBJCOPY_EXECUTABLE} --compress-debug-sections=zlib-gnu ${CMAKE_INSTALL_PREFIX}/libclara_shared.so)
    elseif (EXISTS ${CMAKE_INSTALL_PREFIX}/libclara_sharedd.so)
        execute_process (COMMAND ${OBJCOPY_EXECUTABLE} --compress-debug-sections=zlib-gnu ${CMAKE_INSTALL_PREFIX}/libclara_sharedd.so)
    else ()
        message (STATUS "libclara_shared.so or libclara_sharedd.so not found. Skipped debug section compression for libclara.")
    endif ()
else ()
    message(WARNING "objcopy not found in PATH. Skipped debug section compression for libclara.")
endif ()
