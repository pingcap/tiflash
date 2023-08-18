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


# glob all unit tests of PageStorage into gtests_page_storage
macro(grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()

# attach all dm gtest sources
grep_gtest_sources(${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/V3/tests ps_v3_gtest_sources)

add_executable(gtests_page_storage_v3 ${ps_v3_gtest_sources} ${TiFlash_SOURCE_DIR}/dbms/src/TestUtils/gtests_dbms_main.cpp)
target_link_libraries(gtests_page_storage_v3 page_storage_v3 gtest_main)
target_compile_options(gtests_page_storage_v3 PRIVATE -Wno-unknown-pragmas)
target_compile_definitions(gtests_page_storage_v3 PRIVATE DBMS_PUBLIC_GTEST)
add_check(gtests_page_storage_v3)