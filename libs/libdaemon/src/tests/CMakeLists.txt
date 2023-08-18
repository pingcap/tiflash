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

include (${TiFlash_SOURCE_DIR}/cmake/add_check.cmake)

add_executable(gtests_libdaemon EXCLUDE_FROM_ALL gtest_daemon_config.cpp)
target_link_libraries(gtests_libdaemon gtest_main daemon tipb kvproto)
#add for libcctz used by BaseDaemon
if (APPLE)
    set_target_properties(gtests_libdaemon PROPERTIES LINK_FLAGS "-framework CoreFoundation")
endif ()
add_check(gtests_libdaemon)

target_include_directories (gtests_libdaemon PRIVATE ${TiFlash_SOURCE_DIR}/libs/libpocoext/include)