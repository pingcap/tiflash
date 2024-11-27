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

check_then_add_sources_compile_flag (
    TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT
    "${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}"
    column_vector_perftest.cpp
)

add_executable (column_vector_perftest column_vector_perftest.cpp)
target_link_libraries (column_vector_perftest dbms delta_merge)
