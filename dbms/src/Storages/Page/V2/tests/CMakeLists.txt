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


# FIXME: mock disk full by FailPoint, and move this test into a unittest case.
add_executable(test_page_storage_write_disk_full test_page_storage_write_disk_full.cpp)
target_link_libraries(test_page_storage_write_disk_full dbms)
target_compile_options(test_page_storage_write_disk_full PRIVATE -Wno-format)

add_executable(mem_usage_test mem_usage_test.cpp)
