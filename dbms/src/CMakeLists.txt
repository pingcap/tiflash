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

add_subdirectory (Columns)
add_subdirectory (Common)
add_subdirectory (Core)
add_subdirectory (DataStreams)
add_subdirectory (Operators)
add_subdirectory (DataTypes)
add_subdirectory (Dictionaries)
add_subdirectory (Storages)
add_subdirectory (Parsers)
add_subdirectory (IO)
add_subdirectory (Functions)
add_subdirectory (AggregateFunctions)
add_subdirectory (Server)
add_subdirectory (Client)
add_subdirectory (TableFunctions)
add_subdirectory (Flash)
if (ENABLE_TESTS)
  add_subdirectory (TestUtils EXCLUDE_FROM_ALL)
endif ()

check_then_add_sources_compile_flag (
  TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT
  "${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}"
  DeltaMerge/BitmapFilter/BitmapFilter.cpp
  DeltaMerge/DMVersionFilterBlockInputStream.cpp
)
