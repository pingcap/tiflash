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

include (${TiFlash_SOURCE_DIR}/cmake/add_check.cmake)

foreach (T longjmp siglongjmp)
    add_executable (${T} ${T}.c)
    target_link_libraries (${T} glibc-compatibility)
    set_target_properties (${T} PROPERTIES LINKER_LANGUAGE CXX)
    add_check (${T})
endforeach ()
