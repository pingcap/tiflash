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

# Compiler

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    set (COMPILER_APPLE_CLANG 1)
    set (COMPILER_CLANG 1) # Safe to treat AppleClang as a regular Clang, in general.
    set (MOST_DEBUGGABLE_LEVEL -O0)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
    set (MOST_DEBUGGABLE_LEVEL -O0)
else ()
    message (FATAL_ERROR "Compiler ${CMAKE_CXX_COMPILER_ID} is not supported")
endif ()

# Print details to output
execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version OUTPUT_VARIABLE COMPILER_SELF_IDENTIFICATION OUTPUT_STRIP_TRAILING_WHITESPACE)
message (STATUS "Using compiler:\n${COMPILER_SELF_IDENTIFICATION}")

# Require minimum compiler versions
set (CLANG_MINIMUM_VERSION 12)

if (COMPILER_GCC)
    message (FATAL_ERROR "GCC is not officially supported.")
elseif (COMPILER_CLANG)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Compilation with Clang version ${CMAKE_CXX_COMPILER_VERSION} is unsupported, the minimum required version is ${CLANG_MINIMUM_VERSION}.")
    endif ()
endif ()

# Linker

string (REGEX MATCHALL "[0-9]+" COMPILER_VERSION_LIST ${CMAKE_CXX_COMPILER_VERSION})
list (GET COMPILER_VERSION_LIST 0 COMPILER_VERSION_MAJOR)

# Example values: `lld-10`, `gold`.
option (LINKER_NAME "Linker name or full path")

find_program (LLD_PATH NAMES "lld${COMPILER_POSTFIX}" "lld")
find_program (GOLD_PATH NAMES "ld.gold" "gold")
if (OS_DARWIN)
    find_program (LD_PATH NAMES "ld")
endif ()

if (NOT LINKER_NAME)
    if (OS_DARWIN AND COMPILER_APPLE_CLANG)
        set (LINKER_NAME "ld")
    elseif (LLD_PATH)
        set (LINKER_NAME "lld")
    elseif (GOLD_PATH)
        message (WARNING "Linking with gold is not recommended. Please use lld.")
        set (LINKER_NAME "gold")
    endif ()
endif ()

if (LINKER_NAME)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    message(STATUS "Using linker: ${LINKER_NAME}")
else()
    message(STATUS "Using linker: <default>")
endif()

# Archiver

find_program (LLVM_AR_PATH NAMES "llvm-ar-${COMPILER_VERSION_MAJOR}" "llvm-ar")

if (LLVM_AR_PATH)
    set (CMAKE_AR "${LLVM_AR_PATH}")
    message(STATUS "Using archiver: ${CMAKE_AR}")
elseif (OS_DARWIN AND COMPILER_APPLE_CLANG)
    message (STATUS "Using archiver: <default>")
else ()
    message (FATAL_ERROR "Cannot find ar.")
endif ()


# Ranlib

find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib-${COMPILER_VERSION_MAJOR}" "llvm-ranlib")

if (LLVM_RANLIB_PATH)
    set (CMAKE_RANLIB "${LLVM_RANLIB_PATH}")
    message(STATUS "Using ranlib: ${CMAKE_RANLIB}")
elseif (OS_DARWIN AND COMPILER_APPLE_CLANG)
    message (STATUS "Using ranlib: <default>")
else ()
    message (FATAL_ERROR "Cannot find ranlib.")
endif ()


# Objcopy

find_program (OBJCOPY_PATH NAMES "llvm-objcopy-${COMPILER_VERSION_MAJOR}" "llvm-objcopy" "objcopy")

if (OBJCOPY_PATH)
    set (CMAKE_OBJCOPY "${OBJCOPY_PATH}")
    message (STATUS "Using objcopy: ${OBJCOPY_PATH}")
elseif (OS_DARWIN AND COMPILER_APPLE_CLANG)
    message (STATUS "Using objcopy: <default>")
else ()
    message (FATAL_ERROR "Cannot find objcopy.")
endif ()
