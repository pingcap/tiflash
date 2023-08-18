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

# Adding test output on failure
enable_testing ()

if (NOT TARGET check)
    if (CMAKE_CONFIGURATION_TYPES)
        add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND}
            --force-new-ctest-process --output-on-failure --build-config "$<CONFIGURATION>"
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
    else ()
        add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND}
            --force-new-ctest-process --output-on-failure
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
    endif ()
endif ()

macro (add_check target)
    add_test (NAME test_${target} COMMAND ${target} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    add_dependencies (check ${target})
endmacro (add_check)
