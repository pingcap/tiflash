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

enable_language (ASM)

if (ARCH_AARCH64)
    list (APPEND glibc_compatibility_sources musl/aarch64/syscall.s musl/aarch64/longjmp.s)
elseif (ARCH_AMD64)
    list (APPEND glibc_compatibility_sources musl/x86_64/syscall.s musl/x86_64/longjmp.s)
else ()
    message (FATAL_ERROR "glibc_compatibility can only be used on x86_64 or aarch64.")
endif ()

add_library (glibc-compatibility glibc-compatibility.c
musl/pipe2.c
musl/fallocate.c
musl/vasprintf.c
musl/lgamma.c
musl/posix_spawn.c
musl/futimens.c
musl/syscall_ret.c
musl/sched_cpucount.c
${glibc_compatibility_sources})

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (ARCH_AARCH64)
        set_source_files_properties(musl/aarch64/longjmp.s musl/aarch64/syscall.s
                PROPERTIES COMPILE_FLAGS -Wno-unused-command-line-argument)
    elseif (ARCH_AMD64)
        set_source_files_properties(musl/x86_64/longjmp.s musl/x86_64/syscall.s
                PROPERTIES COMPILE_FLAGS -Wno-unused-command-line-argument)
    else ()
        message(FATAL_ERROR "glibc_compatibility can only be used on x86_64 or aarch64.")
    endif ()
endif()
add_subdirectory (tests EXCLUDE_FROM_ALL)
