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

include (CheckCXXSourceCompiles)
include (CMakePushCheckState)

cmake_push_check_state ()

# clang4 : -no-pie cause error
# clang6 : -no-pie cause warning

if (MAKE_STATIC_LIBRARIES)
    set (TEST_FLAG "-Wl,-Bstatic -stdlib=libc++ -lc++ -lc++abi -Wl,-Bdynamic")
else ()
    set (TEST_FLAG "-stdlib=libc++ -lc++ -lc++abi")
endif ()

set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG}")

check_cxx_source_compiles("
    #include <iostream>
    int main() {
        std::cerr << std::endl;
        return 0;
    }
    " HAVE_LIBCXX)

cmake_pop_check_state ()
