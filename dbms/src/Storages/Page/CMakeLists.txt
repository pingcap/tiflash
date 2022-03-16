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

add_subdirectory(V2)

# PageStorage Stress test
if (ENABLE_V3_PAGESTORAGE)
    add_headers_and_sources(page_stress_testing stress)
    add_headers_and_sources(page_stress_testing stress/workload)
    add_executable(page_stress_testing EXCLUDE_FROM_ALL ${page_stress_testing_sources})
    target_link_libraries(page_stress_testing dbms page_storage_v3)
    target_include_directories(page_stress_testing PRIVATE stress)
    target_compile_options(page_stress_testing PRIVATE -Wno-format -lc++) # turn off printf format check
endif()