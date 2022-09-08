# Copyright 2022 PingCAP, Ltd.
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

option(TIFLASH_FOLLY_MEMCPY_IS_MEMCPY "use folly memcpy as default `memcpy` and `memmove`" ON)

set_property(SOURCE folly/memcpy.S PROPERTY LANGUAGE CXX)

if (TIFLASH_ENABLE_AVX_SUPPORT)
    set_property(SOURCE folly/memcpy.S APPEND PROPERTY COMPILE_OPTIONS "-x" "assembler-with-cpp")
    set_property(SOURCE folly/memcpy.S APPEND PROPERTY COMPILE_FLAGS "-mavx -mavx2")
    set_property(SOURCE folly/FollyMemcpy.cpp APPEND PROPERTY COMPILE_FLAGS "-mavx -mavx2")
endif ()

set (memcpy_sources)

list (APPEND memcpy_sources folly/memcpy.S folly/FollyMemcpy.cpp)

if (USE_INTERNAL_MEMCPY)
    if (TIFLASH_FOLLY_MEMCPY_IS_MEMCPY)
        message (STATUS "Use folly memcpy as default `memcpy` and `memmove`")
        add_definitions(-DFOLLY_MEMCPY_IS_MEMCPY=1)
    else ()
        list (APPEND memcpy_sources memcpy.cpp)
    endif ()
endif()

add_library (memcpy STATIC ${memcpy_sources})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fomit-frame-pointer")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fomit-frame-pointer")
