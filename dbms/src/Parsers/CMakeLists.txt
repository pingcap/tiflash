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
add_headers_and_sources(tiflash_parsers .)
add_library(tiflash_parsers ${SPLIT_SHARED} ${tiflash_parsers_headers} ${tiflash_parsers_sources})
target_link_libraries (tiflash_parsers tiflash_common_io)
target_include_directories (tiflash_parsers PUBLIC ${DBMS_INCLUDE_DIR})
