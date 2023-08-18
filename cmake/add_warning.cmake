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

include (CheckCXXCompilerFlag)
include (CheckCCompilerFlag)

# Try to add -Wflag if compiler supports it
macro (add_warning flag)
    string (REPLACE "-" "_" underscored_flag ${flag})
    string (REPLACE "+" "x" underscored_flag ${underscored_flag})

    check_cxx_compiler_flag("-W${flag}" SUPPORTS_CXXFLAG_${underscored_flag})
    check_c_compiler_flag("-W${flag}" SUPPORTS_CFLAG_${underscored_flag})

    if (SUPPORTS_CXXFLAG_${underscored_flag})
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W${flag}")
    else ()
        message (STATUS "Flag -W${flag} is unsupported")
    endif ()

    if (SUPPORTS_CFLAG_${underscored_flag})
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W${flag}")
    else ()
        message (STATUS "Flag -W${flag} is unsupported")
    endif ()

endmacro ()

# Try to add -Wno flag if compiler supports it
macro (no_warning flag)
    add_warning(no-${flag})
endmacro ()


# The same but only for specified target.
macro (target_add_warning target flag)
    string (REPLACE "-" "_" underscored_flag ${flag})
    string (REPLACE "+" "x" underscored_flag ${underscored_flag})

    check_cxx_compiler_flag("-W${flag}" SUPPORTS_CXXFLAG_${underscored_flag})

    if (SUPPORTS_CXXFLAG_${underscored_flag})
        target_compile_options (${target} PRIVATE "-W${flag}")
    else ()
        message (STATUS "Flag -W${flag} is unsupported")
    endif ()
endmacro ()

macro (target_no_warning target flag)
    target_add_warning(${target} no-${flag})
endmacro ()
