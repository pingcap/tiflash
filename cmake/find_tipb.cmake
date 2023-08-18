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


if (NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/tipb/proto/select.proto")
	message (FATAL_ERROR "tipb submodule in contrib/tipb is missing. Try run 'git submodule update --init --recursive'")
endif ()

message(STATUS "Using tipb: ${TiFlash_SOURCE_DIR}/contrib/tipb/cpp")
