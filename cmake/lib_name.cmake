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

set(DIVIDE_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/libdivide)
set(CITYHASH_CONTRIB_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/libcityhash/include)
set(COMMON_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/libs/libcommon/include ${TiFlash_BINARY_DIR}/libs/libcommon/include)
set(DBMS_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/dbms/src ${TiFlash_BINARY_DIR}/dbms/src)
set(DOUBLE_CONVERSION_CONTRIB_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/double-conversion)
set(PCG_RANDOM_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/libpcg-random/include)
