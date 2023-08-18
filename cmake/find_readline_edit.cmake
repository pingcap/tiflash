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

option (ENABLE_READLINE "Enable READLINE" OFF)

include (CMakePushCheckState)
cmake_push_check_state ()

if (ENABLE_READLINE)
    set (READLINE_PATHS "/usr/local/opt/readline/lib")
    # First try find custom lib for macos users (default lib without history support)
    find_library (READLINE_LIB NAMES readline PATHS ${READLINE_PATHS} NO_DEFAULT_PATH)
    if (NOT READLINE_LIB)
        find_library (READLINE_LIB NAMES readline PATHS ${READLINE_PATHS})
    endif ()

    list(APPEND CMAKE_FIND_LIBRARY_SUFFIXES .so.2)

    find_library (TERMCAP_LIB NAMES termcap)
    find_library (EDIT_LIB NAMES edit)

    set(READLINE_INCLUDE_PATHS "/usr/local/opt/readline/include")
    if (READLINE_LIB AND TERMCAP_LIB)
        find_path (READLINE_INCLUDE_DIR NAMES readline/readline.h PATHS ${READLINE_INCLUDE_PATHS} NO_DEFAULT_PATH)
        if (NOT READLINE_INCLUDE_DIR)
            find_path (READLINE_INCLUDE_DIR NAMES readline/readline.h PATHS ${READLINE_INCLUDE_PATHS})
        endif ()
        if (READLINE_INCLUDE_DIR AND READLINE_LIB)
            set (USE_READLINE 1)
            set (LINE_EDITING_LIBS ${READLINE_LIB} ${TERMCAP_LIB})
            message (STATUS "Using line editing libraries (readline): ${READLINE_INCLUDE_DIR} : ${LINE_EDITING_LIBS}")
        endif ()
    elseif (EDIT_LIB AND TERMCAP_LIB)
        find_library (CURSES_LIB NAMES curses)
        find_path (READLINE_INCLUDE_DIR NAMES editline/readline.h PATHS ${READLINE_INCLUDE_PATHS})
        if (CURSES_LIB AND READLINE_INCLUDE_DIR)
            set (USE_LIBEDIT 1)
            set (LINE_EDITING_LIBS ${EDIT_LIB} ${CURSES_LIB} ${TERMCAP_LIB})
            message (STATUS "Using line editing libraries (edit): ${READLINE_INCLUDE_DIR} : ${LINE_EDITING_LIBS}")
        endif ()
    endif ()
endif ()

if (LINE_EDITING_LIBS AND READLINE_INCLUDE_DIR)
    include (CheckCXXSourceRuns)

    set (CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} ${LINE_EDITING_LIBS})
    set (CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES} ${READLINE_INCLUDE_DIR})
    check_cxx_source_runs ("
        #include <stdio.h>
        #include <readline/readline.h>
        #include <readline/history.h>
        int main() {
            add_history(NULL);
            append_history(1,NULL);
            return 0;
        }
    " HAVE_READLINE_HISTORY)
else ()
    message (STATUS "Not using any library for line editing.")
endif ()

cmake_pop_check_state ()
