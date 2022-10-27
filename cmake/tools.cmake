# Compiler

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    set (COMPILER_APPLE_CLANG 1)
    set (COMPILER_CLANG 1) # Safe to treat AppleClang as a regular Clang, in general.
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
else ()
    message (FATAL_ERROR "Compiler ${CMAKE_CXX_COMPILER_ID} is not supported")
endif ()

# Print details to output
execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version OUTPUT_VARIABLE COMPILER_SELF_IDENTIFICATION OUTPUT_STRIP_TRAILING_WHITESPACE)
message (STATUS "Using compiler:\n${COMPILER_SELF_IDENTIFICATION}")

# Require minimum compiler versions
set (CLANG_MINIMUM_VERSION 5)
set (GCC_MINIMUM_VERSION 7)

if (COMPILER_GCC)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Compilation with GCC version ${CMAKE_CXX_COMPILER_VERSION} is not supported, minimum required version is ${CLANG_MINIMUM_VERSION}")
    endif ()
elseif (COMPILER_CLANG)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Compilation with Clang version ${CMAKE_CXX_COMPILER_VERSION} is unsupported, the minimum required version is ${CLANG_MINIMUM_VERSION}.")
    endif ()
endif ()

# Linker

string (REGEX MATCHALL "[0-9]+" COMPILER_VERSION_LIST ${CMAKE_CXX_COMPILER_VERSION})
list (GET COMPILER_VERSION_LIST 0 COMPILER_VERSION_MAJOR)

# Example values: `lld-10`, `gold`.
option (LINKER_NAME "Linker name or full path")

find_program (LLD_PATH NAMES "lld${COMPILER_POSTFIX}" "lld")
find_program (GOLD_PATH NAMES "ld.gold" "gold")
if (ARCH_DARWIN)
    find_program (LD_PATH NAMES "ld")
endif ()

if (NOT LINKER_NAME)
    if (ARCH_DARWIN AND COMPILER_APPLE_CLANG)
        set (LINKER_NAME "ld")
    elseif (COMPILER_CLANG AND LLD_PATH)
        set (LINKER_NAME "lld")
    elseif (GOLD_PATH)
        message (WARNING "Linking with gold is not recommended. Please use lld.")
        set (LINKER_NAME "gold")
    endif ()
endif ()

if (LINKER_NAME)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    message(STATUS "Using linker: ${LINKER_NAME}")
else()
    message(STATUS "Using linker: <default>")
endif()

# Archiver

if (COMPILER_GCC)
    find_program (LLVM_AR_PATH NAMES "llvm-ar" "llvm-ar-15" "llvm-ar-14" "llvm-ar-13" "llvm-ar-12")
else ()
    find_program (LLVM_AR_PATH NAMES "llvm-ar-${COMPILER_VERSION_MAJOR}" "llvm-ar")
endif ()

if (LLVM_AR_PATH)
    set (CMAKE_AR "${LLVM_AR_PATH}")
endif ()

message(STATUS "Using archiver: ${CMAKE_AR}")

# Ranlib

if (COMPILER_GCC)
    find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib" "llvm-ranlib-15" "llvm-ranlib-14" "llvm-ranlib-13" "llvm-ranlib-12")
else ()
    find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib-${COMPILER_VERSION_MAJOR}" "llvm-ranlib")
endif ()

if (LLVM_RANLIB_PATH)
    set (CMAKE_RANLIB "${LLVM_RANLIB_PATH}")
endif ()

message(STATUS "Using ranlib: ${CMAKE_RANLIB}")

# Install Name Tool

if (COMPILER_GCC)
    find_program (LLVM_INSTALL_NAME_TOOL_PATH NAMES "llvm-install-name-tool" "llvm-install-name-tool-15" "llvm-install-name-tool-14" "llvm-install-name-tool-13" "llvm-install-name-tool-12")
else ()
    find_program (LLVM_INSTALL_NAME_TOOL_PATH NAMES "llvm-install-name-tool-${COMPILER_VERSION_MAJOR}" "llvm-install-name-tool")
endif ()

if (LLVM_INSTALL_NAME_TOOL_PATH)
    set (CMAKE_INSTALL_NAME_TOOL "${LLVM_INSTALL_NAME_TOOL_PATH}")
endif ()

message(STATUS "Using install-name-tool: ${CMAKE_INSTALL_NAME_TOOL}")

# Objcopy

if (COMPILER_GCC)
    find_program (OBJCOPY_PATH NAMES "llvm-objcopy" "llvm-objcopy-15" "llvm-objcopy-14" "llvm-objcopy-13" "llvm-objcopy-12" "objcopy")
else ()
    find_program (OBJCOPY_PATH NAMES "llvm-objcopy-${COMPILER_VERSION_MAJOR}" "llvm-objcopy" "objcopy")
endif ()

if (OBJCOPY_PATH)
    message (STATUS "Using objcopy: ${OBJCOPY_PATH}")
else ()
    message (FATAL_ERROR "Cannot find objcopy.")
endif ()

# Strip

if (COMPILER_GCC)
    find_program (STRIP_PATH NAMES "llvm-strip" "llvm-strip-15" "llvm-strip-14" "llvm-strip-13" "llvm-strip-12" "strip")
else ()
    find_program (STRIP_PATH NAMES "llvm-strip-${COMPILER_VERSION_MAJOR}" "llvm-strip" "strip")
endif ()

if (STRIP_PATH)
    message (STATUS "Using strip: ${STRIP_PATH}")
else ()
    message (FATAL_ERROR "Cannot find strip.")
endif ()
