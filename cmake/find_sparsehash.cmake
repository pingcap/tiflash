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

option (USE_INTERNAL_SPARCEHASH_LIBRARY "Set to FALSE to use system sparsehash library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_SPARCEHASH_LIBRARY)
    find_path (SPARCEHASH_INCLUDE_DIR NAMES sparsehash/sparse_hash_map PATHS ${SPARCEHASH_INCLUDE_PATHS})
endif ()

if (SPARCEHASH_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_SPARCEHASH_LIBRARY 1)
    set (SPARCEHASH_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/libsparsehash")
endif ()

message (STATUS "Using sparsehash: ${SPARCEHASH_INCLUDE_DIR}")
