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


# TODO? Maybe recursive collect on all deps

get_property (dirs1 TARGET dbms PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

get_property (dirs1 TARGET tiflash_common_io PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

get_property (dirs1 TARGET common PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

if (USE_INTERNAL_BOOST_LIBRARY)
    get_property (dirs1 TARGET ${Boost_PROGRAM_OPTIONS_LIBRARY} PROPERTY INCLUDE_DIRECTORIES)
    list(APPEND dirs ${dirs1})
endif ()

if (USE_INTERNAL_POCO_LIBRARY)
    list(APPEND dirs "./contrib/poco/Foundation/include")
endif ()

list(REMOVE_DUPLICATES dirs)
file (WRITE ${CMAKE_CURRENT_BINARY_DIR}/include_directories.txt "")
foreach (dir ${dirs})
    string (REPLACE "${TiFlash_SOURCE_DIR}" "." dir "${dir}")
    file (APPEND ${CMAKE_CURRENT_BINARY_DIR}/include_directories.txt "-I ${dir} ")
endforeach ()
