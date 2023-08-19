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

include (CMakePushCheckState)
cmake_push_check_state ()

if (NOT USE_LLVM_LIBUNWIND)
    if (CMAKE_SYSTEM MATCHES "Linux")
        option (USE_INTERNAL_UNWIND_LIBRARY "Set to FALSE to use system unwind library instead of bundled" ${NOT_UNBUNDLED})
    else ()
        option (USE_INTERNAL_UNWIND_LIBRARY "Set to FALSE to use system unwind library instead of bundled" OFF)
    endif ()

    if (ENABLE_UNWIND)

        option (USE_INTERNAL_UNWIND_LIBRARY "Set to FALSE to use system unwind library instead of bundled" ${NOT_UNBUNDLED})

        if (NOT USE_INTERNAL_UNWIND_LIBRARY)
            find_library (UNWIND_LIBRARY unwind)
            find_path (UNWIND_INCLUDE_DIR NAMES unwind.h PATHS ${UNWIND_INCLUDE_PATHS})

            include (CheckCXXSourceCompiles)
            set(CMAKE_REQUIRED_INCLUDES ${UNWIND_INCLUDE_DIR})
            set(CMAKE_REQUIRED_LIBRARIES ${UNWIND_LIBRARY})
            check_cxx_source_compiles("
            #include <ucontext.h>
            #define UNW_LOCAL_ONLY
            #include <libunwind.h>
            int main () {
            unw_context_t context;
            unw_cursor_t cursor;
            unw_init_local2(&cursor, &context, UNW_INIT_SIGNAL_FRAME);
            return 0;
            }
            " HAVE_UNWIND_INIT_LOCAL_SIGNAL)
            if (NOT HAVE_UNWIND_INIT_LOCAL_SIGNAL)
            set(UNWIND_LIBRARY "")
            set(UNWIND_INCLUDE_DIR "")
            set(UNWIND_INCREMENTAL_DIR "")
            endif ()

        endif ()

        if (UNWIND_LIBRARY AND UNWIND_INCLUDE_DIR)
            set (USE_UNWIND 1)
        elseif (CMAKE_SYSTEM MATCHES "Linux")
            set (USE_INTERNAL_UNWIND_LIBRARY 1)
            set (UNWIND_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/libunwind/include")
            set (UNWIND_INCREMENTAL_DIR "${PROJECT_BINARY_DIR}/contrib/libunwind-cmake/include")
            set (UNWIND_LIBRARY unwind)
            set (USE_UNWIND 1)
        endif ()

    endif ()
else()
    # TODO:
    # LLVM exposes libunwind.h recently, but it is not yet avaiable in 13.0.0
    # https://lists.llvm.org/pipermail/llvm-dev/2021-December/154418.html
    find_library (UNWIND_LIBRARY unwind)
    set(UNWIND_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/libunwind-llvm")
    set (USE_UNWIND 1)
endif ()

message (STATUS "Using unwind=${USE_UNWIND}: ${UNWIND_INCLUDE_DIR} : ${UNWIND_LIBRARY}")

cmake_pop_check_state ()
