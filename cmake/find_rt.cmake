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

if (OS_DARWIN)
    # lib from libs/libcommon
    set (RT_LIBRARY "apple_rt")
else ()
    find_library (RT_LIBRARY rt)
endif ()

message(STATUS "Using rt: ${RT_LIBRARY}")

function (target_link_rt_by_force TARGET)
    if (NOT OS_DARWIN)
        set (FLAGS "-Wl,-no-as-needed -lrt -Wl,-as-needed")
        set_property (TARGET ${TARGET} APPEND PROPERTY LINK_FLAGS "${FLAGS}")
    endif ()
endfunction ()
