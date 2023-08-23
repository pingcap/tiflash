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

option (USE_INTERNAL_GTEST_LIBRARY "Set to FALSE to use system Google Test instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/googletest/googletest/CMakeLists.txt")
   if (USE_INTERNAL_GTEST_LIBRARY)
       message (WARNING "submodule contrib/googletest is missing. to fix try run: \n git submodule update --init --recursive")
       set (USE_INTERNAL_GTEST_LIBRARY 0)
   endif ()
   set (MISSING_INTERNAL_GTEST_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_GTEST_LIBRARY)
    find_path (GTEST_INCLUDE_DIR NAMES /gtest/gtest.h PATHS ${GTEST_INCLUDE_PATHS})
    find_path (GTEST_ROOT NAMES src/gtest-all.cc PATHS /usr/src/googletest/googletest /usr/src/gtest)
endif ()

if (GTEST_INCLUDE_DIR AND GTEST_ROOT)
    # googletest package have no lib
    add_library(gtest ${GTEST_ROOT}/src/gtest-all.cc)
    add_library(gtest_main ${GTEST_ROOT}/src/gtest_main.cc)
    target_include_directories(gtest PRIVATE ${GTEST_ROOT})
    target_link_libraries(gtest_main gtest)
    set (GTEST_LIBRARY gtest_main)
elseif (NOT MISSING_INTERNAL_GTEST_LIBRARY)
    set (USE_INTERNAL_GTEST_LIBRARY 1)
    set (GTEST_LIBRARY gtest_main)
endif ()

message (STATUS "Using gtest: ${GTEST_INCLUDE_DIR} : ${GTEST_LIBRARY}")
