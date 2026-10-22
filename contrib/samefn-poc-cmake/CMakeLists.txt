# Copyright 2026 PingCAP, Inc.
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

# samefn-poc (the submodule contrib/samefn-poc, pinned to a commit): one correctly rounded,
# versioned sin() shared by TiDB, TiKV and TiFlash, so SIN gives the same bits in every engine
# and on every CPU. TiFlash links the archive that samefn-poc prebuilt for this platform
# (go/samefn/lib/<os>_<arch>/libsamefn.a) through the C API in include/samefn.h; building
# TiFlash needs no Rust toolchain for it.
#
# The archive carries its own copy of Rust's std, as process_metrics and symbolization do, and
# their unmangled std symbols would clash at the final link. So the archive is prelinked into
# one object that keeps only the samefn_* symbols global.

set (SAMEFN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../samefn-poc")
if (APPLE)
    set (_SAMEFN_OS "darwin")
else ()
    set (_SAMEFN_OS "linux")
endif ()
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
    set (_SAMEFN_ARCH "amd64")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64|ARM64)$")
    set (_SAMEFN_ARCH "arm64")
else ()
    message (FATAL_ERROR "samefn-poc: no prebuilt archive for ${CMAKE_SYSTEM_PROCESSOR}")
endif ()
set (_SAMEFN_ARCHIVE "${SAMEFN_DIR}/go/samefn/lib/${_SAMEFN_OS}_${_SAMEFN_ARCH}/libsamefn.a")
if (NOT EXISTS "${_SAMEFN_ARCHIVE}")
    message (FATAL_ERROR "samefn-poc: ${_SAMEFN_ARCHIVE} not found; run: git submodule update --init contrib/samefn-poc")
endif ()
message (STATUS "Using samefn-poc archive: ${_SAMEFN_ARCHIVE}")

set (_SAMEFN_OBJECT "${CMAKE_CURRENT_BINARY_DIR}/samefn_prelinked.o")
set (_SAMEFN_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}samefn_prelinked${CMAKE_STATIC_LIBRARY_SUFFIX}")
if (APPLE)
    set (_SAMEFN_EXPORTS "${CMAKE_CURRENT_BINARY_DIR}/samefn.exports")
    file (WRITE ${_SAMEFN_EXPORTS} "_samefn_*\n")
    # Apple's ld: with -r, the symbols not exported become static.
    set (_SAMEFN_PRELINK
        COMMAND ${CMAKE_LINKER} -r -arch ${CMAKE_SYSTEM_PROCESSOR} -platform_version macos 12.0 12.0
            -o ${_SAMEFN_OBJECT} -force_load ${_SAMEFN_ARCHIVE} -exported_symbols_list ${_SAMEFN_EXPORTS})
else ()
    set (_SAMEFN_PRELINK
        COMMAND ${CMAKE_LINKER} -r -o ${_SAMEFN_OBJECT} --whole-archive ${_SAMEFN_ARCHIVE} --no-whole-archive
        COMMAND ${CMAKE_OBJCOPY} --wildcard --keep-global-symbol=samefn_* ${_SAMEFN_OBJECT})
endif ()

add_custom_command (OUTPUT ${_SAMEFN_LIBRARY}
    COMMENT "Prelinking the samefn-poc archive"
    ${_SAMEFN_PRELINK}
    COMMAND ${CMAKE_COMMAND} -E rm -f ${_SAMEFN_LIBRARY}
    COMMAND ${CMAKE_AR} rcs ${_SAMEFN_LIBRARY} ${_SAMEFN_OBJECT}
    VERBATIM
    DEPENDS ${_SAMEFN_ARCHIVE})

add_custom_target (_samefn ALL DEPENDS ${_SAMEFN_LIBRARY})
add_library (samefn STATIC IMPORTED GLOBAL)
set_target_properties (samefn PROPERTIES IMPORTED_LOCATION ${_SAMEFN_LIBRARY})
add_dependencies (samefn _samefn)
target_include_directories (samefn INTERFACE ${SAMEFN_DIR}/include)
# CORE-MATH's sin raises floating-point flags with feraiseexcept, from libm on Linux (macOS has
# it in libSystem).
if (NOT APPLE)
    set_property (TARGET samefn APPEND PROPERTY INTERFACE_LINK_LIBRARIES m)
endif ()
