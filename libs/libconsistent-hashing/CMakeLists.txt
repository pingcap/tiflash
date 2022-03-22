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

cmake_minimum_required(VERSION 2.8)
project(libconsistent-hashing CXX)

add_library(libconsistent-hashing yandex/consistent_hashing.cpp yandex/popcount.cpp mailru/sumbur.cpp)
target_include_directories(libconsistent-hashing PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})