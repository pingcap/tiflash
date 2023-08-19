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

set (SAN_FLAGS "${SAN_FLAGS} -g3 -fno-omit-frame-pointer -DSANITIZER")
if (SAN_DEBUG)
    set (SAN_FLAGS "${SAN_FLAGS} ${MOST_DEBUGGABLE_LEVEL}")
else ()
    set (SAN_FLAGS "${SAN_FLAGS} -O3")
endif ()

set (CMAKE_CXX_FLAGS_ASAN                "${CMAKE_CXX_FLAGS_ASAN}  ${SAN_FLAGS} -fsanitize=address")
set (CMAKE_C_FLAGS_ASAN                  "${CMAKE_C_FLAGS_ASAN}    ${SAN_FLAGS} -fsanitize=address")
set (CMAKE_EXE_LINKER_FLAGS_ASAN         "${CMAKE_EXE_LINKER_FLAGS_ASAN}        -fsanitize=address")
set (CMAKE_CXX_FLAGS_UBSAN               "${CMAKE_CXX_FLAGS_UBSAN} ${SAN_FLAGS} -fsanitize=undefined")
set (CMAKE_C_FLAGS_UBSAN                 "${CMAKE_C_FLAGS_UBSAN}   ${SAN_FLAGS} -fsanitize=undefined")
set (CMAKE_EXE_LINKER_FLAGS_UBSAN        "${CMAKE_EXE_LINKER_FLAGS_UBSAN}       -fsanitize=undefined")
set (CMAKE_CXX_FLAGS_MSAN                "${CMAKE_CXX_FLAGS_MSAN}  ${SAN_FLAGS} -fsanitize=memory")
set (CMAKE_C_FLAGS_MSAN                  "${CMAKE_C_FLAGS_MSAN}    ${SAN_FLAGS} -fsanitize=memory")
set (CMAKE_EXE_LINKER_FLAGS_MSAN         "${CMAKE_EXE_LINKER_FLAGS_MSAN}        -fsanitize=memory")
set (CMAKE_CXX_FLAGS_TSAN                "${CMAKE_CXX_FLAGS_TSAN}  ${SAN_FLAGS} -fsanitize=thread")
set (CMAKE_C_FLAGS_TSAN                  "${CMAKE_C_FLAGS_TSAN}    ${SAN_FLAGS} -fsanitize=thread")
set (CMAKE_EXE_LINKER_FLAGS_TSAN         "${CMAKE_EXE_LINKER_FLAGS_TSAN}        -fsanitize=thread")

# clang use static linking by default
if (MAKE_STATIC_LIBRARIES AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (CMAKE_EXE_LINKER_FLAGS_ASAN     "${CMAKE_EXE_LINKER_FLAGS_ASAN} -static-libasan")
    set (CMAKE_EXE_LINKER_FLAGS_UBSAN    "${CMAKE_EXE_LINKER_FLAGS_UBSAN} -static-libubsan")
    set (CMAKE_EXE_LINKER_FLAGS_MSAN     "${CMAKE_EXE_LINKER_FLAGS_MSAN} -static-libmsan")
    set (CMAKE_EXE_LINKER_FLAGS_TSAN     "${CMAKE_EXE_LINKER_FLAGS_TSAN} -static-libtsan")
endif ()
