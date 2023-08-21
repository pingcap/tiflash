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

# Currently kvproto should always use bundled library.

if (NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/kvproto/proto/errorpb.proto")
	message (FATAL_ERROR "kvproto submodule in contrib/kvproto is missing. Try run 'git submodule update --init --recursive'")
endif ()

message(STATUS "Using kvproto: ${TiFlash_SOURCE_DIR}/contrib/kvproto/cpp")
set (KVPROTO_FOUND TRUE)
