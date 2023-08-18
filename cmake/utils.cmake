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

function (print_flags)
    set (FULL_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UC}}")
    set (FULL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE_UC}}")
    set (FULL_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_EXE_LINKER_FLAGS_${CMAKE_BUILD_TYPE_UC}}")
    message (STATUS "Compiler C   = ${CMAKE_C_COMPILER} ${FULL_C_FLAGS}")
    message (STATUS "Compiler CXX = ${CMAKE_CXX_COMPILER} ${FULL_CXX_FLAGS}")
    message (STATUS "Linker Flags = ${FULL_EXE_LINKER_FLAGS}")
endfunction ()

function (check_then_add_sources_compile_flag check_var flag)
    # flag split by `;`
    if (${check_var})
        message(STATUS "[*] add compile options `${flag}` to sources `${ARGN}`")
        set_property(SOURCE ${ARGN} APPEND PROPERTY COMPILE_OPTIONS ${flag})
    endif ()
endfunction ()

function (add_sources_compile_flag_avx2)
    check_then_add_sources_compile_flag (TIFLASH_ENABLE_AVX_SUPPORT "-mavx2" ${ARGN})
endfunction ()