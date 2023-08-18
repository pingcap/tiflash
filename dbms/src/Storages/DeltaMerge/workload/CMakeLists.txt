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

include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(dt-workload-src MainEntry.cpp DTWorkload.cpp KeyGenerator.cpp TableGenerator.cpp DataGenerator.cpp Limiter.cpp Options.cpp Utils.cpp ${TiFlash_SOURCE_DIR}/dbms/src/TestUtils/TiFlashTestEnv.cpp)

add_library(dt-workload-lib ${dt-workload-src})
target_link_libraries(dt-workload-lib PRIVATE dbms delta_merge)
