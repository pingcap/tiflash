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

include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(flash_service .)
add_headers_and_sources(flash_service ./Coprocessor)
add_headers_and_sources(flash_service ./Mpp)
add_headers_and_sources(flash_service ./Executor)
add_headers_and_sources(flash_service ./Pipeline)
add_headers_and_sources(flash_service ./Pipeline/Exec)
add_headers_and_sources(flash_service ./Pipeline/Schedule)
add_headers_and_sources(flash_service ./Pipeline/Schedule/Events)
add_headers_and_sources(flash_service ./Pipeline/Schedule/Events/Impls)
add_headers_and_sources(flash_service ./Pipeline/Schedule/Reactor)
add_headers_and_sources(flash_service ./Pipeline/Schedule/Tasks)
add_headers_and_sources(flash_service ./Pipeline/Schedule/Tasks/Impls)
add_headers_and_sources(flash_service ./Pipeline/Schedule/TaskQueues)
add_headers_and_sources(flash_service ./Pipeline/Schedule/ThreadPool)
add_headers_and_sources(flash_service ./Planner)
add_headers_and_sources(flash_service ./Planner/Plans)
add_headers_and_sources(flash_service ./Statistics)
add_headers_and_sources(flash_service ./Management)
add_headers_and_sources(flash_service ./Disaggregated)
add_headers_and_sources(flash_service ./ResourceControl)

add_library(flash_service ${flash_service_headers} ${flash_service_sources})
target_link_libraries(flash_service dbms)
