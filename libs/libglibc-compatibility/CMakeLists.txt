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

add_library (glibc-compatibility glibc-compatibility.c
musl/pipe2.c
musl/fallocate.c
musl/longjmp.s
musl/vasprintf.c
musl/lgamma.c
musl/posix_spawn.c
musl/futimens.c
musl/syscall.s
musl/syscall_ret.c
musl/sched_cpucount.c)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set_source_files_properties(musl/longjmp.s musl/syscall.s
            PROPERTIES COMPILE_FLAGS -Wno-unused-command-line-argument)
endif()
add_subdirectory (tests EXCLUDE_FROM_ALL)
