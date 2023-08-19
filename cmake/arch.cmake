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

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*|ARM64.*)")
    set (ARCH_AARCH64 1)
endif ()
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(amd64.*|AMD64.*|x86_64.*|X86_64.*)")
    set (ARCH_AMD64 1)
endif ()
if (ARCH_AARCH64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
    set (ARCH_ARM 1)
endif ()
if (CMAKE_LIBRARY_ARCHITECTURE MATCHES "i386")
    set (ARCH_I386 1)
endif ()
if ( ( ARCH_ARM AND NOT ARCH_AARCH64 ) OR ARCH_I386)
    set (ARCH_32 1)
    message (FATAL_ERROR "32bit platforms are not supported")
endif ()
