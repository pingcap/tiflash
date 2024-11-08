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

project (TiFlash)
cmake_minimum_required (VERSION 3.21)

set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${TiFlash_SOURCE_DIR}/cmake/Modules/")
set(CMAKE_MACOSX_RPATH 1)

option(TIFLASH_ENABLE_LLVM_DEVELOPMENT "enable facilities for development with LLVM" OFF)

if(CMAKE_PREFIX_PATH)
    # append paths for cmake to check libs
    set(ENV{LD_LIBRARY_PATH}
            "${CMAKE_PREFIX_PATH}/lib:${CMAKE_PREFIX_PATH}/lib/x86_64-unknown-linux-gnu/:${CMAKE_PREFIX_PATH}/lib/aarch64-unknown-linux-gnu/")
endif()

message (STATUS "Using CXX=${CMAKE_CXX_COMPILER}, ver=${CMAKE_CXX_COMPILER_VERSION};CC=${CMAKE_C_COMPILER}, ver=${CMAKE_C_COMPILER_VERSION}")
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # Require at least gcc 7
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7 AND NOT CMAKE_VERSION VERSION_LESS 2.8.9)
        message (FATAL_ERROR "GCC version must be at least 7! For example, if GCC 7 is available under gcc-7, g++-7 names, do the following: export CC=gcc-7 CXX=g++-7; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
    endif ()
    set(MOST_DEBUGGABLE_LEVEL -Og)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Require at least clang 5
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5)
        message (FATAL_ERROR "Clang version must be at least 5!")
    endif ()
    set(MOST_DEBUGGABLE_LEVEL -O0)
else ()
    message (WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang 5+ and GCC 7+.")
endif ()

# Write compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

option (USE_CCACHE "Set to OFF to disable ccache" ON)
include (cmake/find_ccache.cmake)

if (NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
    set (CMAKE_BUILD_TYPE "RelWithDebInfo")
    message (STATUS "CMAKE_BUILD_TYPE is not set, set to default = ${CMAKE_BUILD_TYPE}")
endif ()
string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UC)
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

# ASan - build type with address sanitizer
# UBSan - build type with undefined behaviour sanitizer
# TSan is not supported due to false positive errors in libstdc++ and necessity to rebuild libstdc++ with TSan
set (CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel;ASan;UBSan" CACHE STRING "" FORCE)

include (cmake/arch.cmake)

if (ARCH_AMD64)
    option(TIFLASH_ENABLE_AVX_SUPPORT "enable AVX and AVX2 support" ON)
    option(TIFLASH_ENABLE_AVX512_SUPPORT "enable AVX512 support" ON)
endif ()

if (ARCH_AARCH64)
    option(TIFLASH_ENABLE_ASIMD_SUPPORT "enable Advanced SIMD support" ON)
    option(TIFLASH_ENABLE_SVE_SUPPORT "enable Scalable Vector Extension support" OFF)
endif ()

if (TIFLASH_ENABLE_AVX_SUPPORT)
    add_definitions(-DTIFLASH_ENABLE_AVX_SUPPORT=1)
endif ()

if (TIFLASH_ENABLE_AVX512_SUPPORT)
    add_definitions(-DTIFLASH_ENABLE_AVX512_SUPPORT=1)
endif ()

if (TIFLASH_ENABLE_ASIMD_SUPPORT)
    add_definitions(-DTIFLASH_ENABLE_ASIMD_SUPPORT=1)
endif ()

if (TIFLASH_ENABLE_SVE_SUPPORT)
    add_definitions(-DTIFLASH_ENABLE_SVE_SUPPORT=1)
endif ()

if (NOT MSVC)
    set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wall")    # -Werror is also added inside directories with our own code.
endif ()

if (COMPILER_GCC OR COMPILER_CLANG)
    set (CXX_WARNING_FLAGS "${CXX_WARNING_FLAGS} -Wnon-virtual-dtor")
endif ()

if (COMPILER_CLANG)
    # Clang doesn't have int128 predefined macros, workaround by manually defining them
    # Reference: https://stackoverflow.com/questions/41198673/uint128-t-not-working-with-clang-and-libstdc
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__GLIBCXX_BITSIZE_INT_N_0=128 -D__GLIBCXX_TYPE_INT_N_0=__int128")

    option(ENABLE_TIME_TRACES "Enable clang feature time traces" OFF)
    if (ENABLE_TIME_TRACES)
        set (CLANG_TIME_TRACES_FLAGS "-ftime-trace")
        message (STATUS "Using clang time traces flag `${CLANG_TIME_TRACES_FLAGS}`. Generates JSON file based on output filename. Results can be analyzed with chrome://tracing or https://www.speedscope.app for flamegraph visualization.")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CLANG_TIME_TRACES_FLAGS}")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CLANG_TIME_TRACES_FLAGS}")
    endif ()

    # https://clang.llvm.org/docs/ThinLTO.html
    # Applies to clang only.
    option(ENABLE_THINLTO "Clang-specific link time optimization" OFF)

    if (ENABLE_THINLTO AND NOT ENABLE_TESTS)
        # Link time optimization
        set (THINLTO_JOBS "0" CACHE STRING "ThinLTO compilation parallelism")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -flto=thin -fvisibility=hidden -fvisibility-inlines-hidden -fsplit-lto-unit")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto=thin -fvisibility=hidden -fvisibility-inlines-hidden -fwhole-program-vtables -fsplit-lto-unit")
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto=thin -flto-jobs=${THINLTO_JOBS} -fvisibility=hidden -fvisibility-inlines-hidden -fwhole-program-vtables -fsplit-lto-unit")
    elseif (ENABLE_THINLTO)
        message (WARNING "Cannot enable ThinLTO")
    endif ()
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # clang: warning: argument unused during compilation: '-stdlib=libc++'
    # clang: warning: argument unused during compilation: '-specs=/usr/share/dpkg/no-pie-compile.specs' [-Wunused-command-line-argument]
    set (COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wno-unused-command-line-argument")
endif ()

if (ARCH_LINUX)
    set (CXX11_ABI "ENABLE" CACHE STRING "Use C++11 ABI: DEFAULT, ENABLE, DISABLE")
endif ()

option (USE_STATIC_LIBRARIES "Set to FALSE to use shared libraries" ON)
option (MAKE_STATIC_LIBRARIES "Set to FALSE to make shared libraries" ${USE_STATIC_LIBRARIES})
if (NOT MAKE_STATIC_LIBRARIES)
    option (SPLIT_SHARED_LIBRARIES "DEV ONLY. Keep all internal libs as separate .so for faster linking" OFF)
endif ()

if (SPLIT_SHARED_LIBRARIES)
    set (SPLIT_SHARED SHARED)
endif ()

if (USE_STATIC_LIBRARIES)
    list(REVERSE CMAKE_FIND_LIBRARY_SUFFIXES)
endif ()

if (ARCH_AMD64)
    option (USE_INTERNAL_MEMCPY "Use internal implementation of 'memcpy' function instead of provided by libc. Only for x86_64." ON)
endif ()

if ((ARCH_AMD64 OR ARCH_AARCH64) AND OS_LINUX)
    option (GLIBC_COMPATIBILITY "Set to TRUE to enable compatibility with older glibc libraries. Only for x86_64, Linux. Implies USE_INTERNAL_MEMCPY." ON)
endif ()

if (CXX11_ABI STREQUAL ENABLE)
    set (CXX11_ABI_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=1")
elseif (CXX11_ABI STREQUAL DISABLE)
    set (CXX11_ABI_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")
else ()
    set (CXX11_ABI_FLAGS "")
endif ()

set (COMPILER_FLAGS                      "${COMPILER_FLAGS} ${CXX11_ABI_FLAGS}")

string(REGEX MATCH "-?[0-9]+(.[0-9]+)?$" COMPILER_POSTFIX ${CMAKE_CXX_COMPILER})

find_program (LLVM_OBJCOPY_PATH NAMES "llvm-objcopy${COMPILER_POSTFIX}" "llvm-objcopy")
find_program (LLVM_AR_PATH NAMES "llvm-ar${COMPILER_POSTFIX}" "llvm-ar")
find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib${COMPILER_POSTFIX}" "llvm-ranlib")

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND LLVM_OBJCOPY_PATH)
    set(CMAKE_OBJCOPY ${LLVM_OBJCOPY_PATH})
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND LLVM_AR_PATH)
    set(CMAKE_AR ${LLVM_AR_PATH})
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND LLVM_RANLIB_PATH)
    set(CMAKE_RANLIB ${LLVM_RANLIB_PATH})
endif()

find_program (LLD_PATH NAMES "lld${COMPILER_POSTFIX}" "lld")
find_program (GOLD_PATH NAMES "gold")

if (NOT LINKER_NAME)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND LLD_PATH)
        set (LINKER_NAME "lld")
    elseif (GOLD_PATH)
        set (LINKER_NAME "gold")
    endif ()
endif ()

if (LINKER_NAME)
    message(STATUS "Using linker: ${LINKER_NAME} (selected from: LLD_PATH=${LLD_PATH}; GOLD_PATH=${GOLD_PATH}; COMPILER_POSTFIX=${COMPILER_POSTFIX})")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
endif ()

option (PIPE "-pipe compiler option [less /tmp usage, more ram usage]" ON)
if (PIPE)
    set (COMPILER_FLAGS                  "${COMPILER_FLAGS} -pipe")
endif ()

include (cmake/test_cpu.cmake)

option (ARCHNATIVE "Enable -march=native compiler flag" OFF)
if (ARCHNATIVE)
    set (COMPILER_FLAGS                  "${COMPILER_FLAGS} -march=native")
endif ()

# Special options for better optimized code with clang
#if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
#    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO  "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wno-unused-command-line-argument -mllvm -inline-threshold=10000")
#endif ()

if (CMAKE_VERSION VERSION_LESS "3.8.0")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
else ()
    set (CMAKE_CXX_STANDARD 17)
    set (CMAKE_CXX_EXTENSIONS 1) # https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html#prop_tgt:CXX_EXTENSIONS
    set (CMAKE_CXX_STANDARD_REQUIRED ON)
endif ()

if (NOT ARCH_ARM)
    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3")
    set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} -O3")
endif ()

option (DEBUG_WITHOUT_DEBUG_INFO "Set to ON to build dev target without debug info (remove flag `-g` in order to accelerate compiling speed and reduce target binary size)" OFF)
if (DEBUG_WITHOUT_DEBUG_INFO)
    string(REPLACE "-g" "" CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")
    string(REPLACE "-g" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
    message (STATUS "Build debug target without debug info")
else ()
    set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g3 -ggdb3")
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3 -ggdb3")
endif ()

set (CMAKE_BUILD_COLOR_MAKEFILE          ON)
set (CMAKE_CXX_FLAGS                     "${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS} ${PLATFORM_EXTRA_CXX_FLAG} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${CXX_WARNING_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS}")
set (CMAKE_CXX_FLAGS_RELWITHDEBINFO      "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${CMAKE_CXX_FLAGS_ADD}")
set (CMAKE_CXX_FLAGS_DEBUG               "${CMAKE_CXX_FLAGS_DEBUG} ${MOST_DEBUGGABLE_LEVEL} -fverbose-asm -fno-inline ${CMAKE_CXX_FLAGS_ADD}")

set (CMAKE_C_FLAGS                       "${CMAKE_C_FLAGS} ${COMPILER_FLAGS} -fno-omit-frame-pointer ${COMMON_WARNING_FLAGS} ${GLIBC_COMPATIBILITY_COMPILE_FLAGS} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_RELWITHDEBINFO        "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${CMAKE_C_FLAGS_ADD}")
set (CMAKE_C_FLAGS_DEBUG                 "${CMAKE_C_FLAGS_DEBUG} ${MOST_DEBUGGABLE_LEVEL} -fverbose-asm -fno-inline ${CMAKE_C_FLAGS_ADD}")

if (MAKE_STATIC_LIBRARIES AND NOT APPLE AND NOT (CMAKE_CXX_COMPILER_ID STREQUAL "Clang"))
    set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif ()

set(THREADS_PREFER_PTHREAD_FLAG ON)

include (cmake/test_compiler.cmake)

if (ARCH_LINUX AND CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (TIFLASH_LLVM_TOOLCHAIN 1)
    set (CMAKE_EXE_LINKER_FLAGS          "${CMAKE_EXE_LINKER_FLAGS} ${GLIBC_COMPATIBILITY_LINK_FLAGS} ${CXX11_ABI_FLAGS}")

    option (USE_LIBCXX "Use libc++ and libc++abi instead of libstdc++ (only make sense on Linux with Clang)" ${HAVE_LIBCXX})
    option (USE_LLVM_LIBUNWIND "Use libunwind from LLVM" OFF)
    option (USE_LLVM_COMPILER_RT "Use compiler-rt from LLVM" OFF)


    set (LIBCXX_PATH "" CACHE STRING "Use custom path for libc++. It should be used for MSan.")
    set (TIFLASH_LLVM_LINKAGE_FLAGS "")
    if (USE_LIBCXX)
        set (CMAKE_CXX_FLAGS             "${CMAKE_CXX_FLAGS} -stdlib=libc++") # Ok for clang6, for older can cause 'not used option' warning
        set (CMAKE_CXX_FLAGS_DEBUG       "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG=0") # More checks in debug build.

        list (APPEND TIFLASH_LLVM_LINKAGE_FLAGS -stdlib=libc++ -lc++ -lc++abi)
        if (LIBCXX_PATH)
          link_directories ("${LIBCXX_PATH}/lib")
        endif()
    endif ()

    if (USE_LLVM_LIBUNWIND)
      set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -unwindlib=libunwind")
      list (APPEND TIFLASH_LLVM_LINKAGE_FLAGS -unwindlib=libunwind -lunwind)
    endif ()

    if (USE_LLVM_COMPILER_RT)
      set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rtlib=compiler-rt")
      list (APPEND TIFLASH_LLVM_LINKAGE_FLAGS -rtlib=compiler-rt)
    endif ()

    link_libraries (-Wl,-Bdynamic ${TIFLASH_LLVM_LINKAGE_FLAGS})
endif ()

if (USE_STATIC_LIBRARIES AND HAVE_NO_PIE)
    set (CMAKE_CXX_FLAGS                 "${CMAKE_CXX_FLAGS} ${FLAG_NO_PIE}")
    set (CMAKE_C_FLAGS                   "${CMAKE_C_FLAGS} ${FLAG_NO_PIE}")
endif ()

if (NOT MAKE_STATIC_LIBRARIES)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

include (cmake/sanitize.cmake)

# Using "include-what-you-use" tool.
option (USE_INCLUDE_WHAT_YOU_USE "Use 'include-what-you-use' tool" OFF)
if (USE_INCLUDE_WHAT_YOU_USE)
    find_program(IWYU_PATH NAMES include-what-you-use iwyu)
    if (NOT IWYU_PATH)
        message(FATAL_ERROR "Could not find the program include-what-you-use")
    endif()
    if (${CMAKE_VERSION} VERSION_LESS "3.3.0")
        message(FATAL_ERROR "include-what-you-use requires CMake version at least 3.3.")
    endif()
endif ()

option (UNBUNDLED "Try find all libraries in system (if fail - use bundled from contrib/)" OFF)
if (UNBUNDLED)
    set(NOT_UNBUNDLED 0)
else ()
    set(NOT_UNBUNDLED 1)
endif ()
# Using system libs can cause lot of warnings in includes.
if (UNBUNDLED OR NOT (ARCH_LINUX OR APPLE) OR ARCH_32)
    option (NO_WERROR "Disable -Werror compiler option" ON)
endif ()

if (PREBUILT_LIBS_ROOT)
    list(PREPEND CMAKE_SYSTEM_PREFIX_PATH ${PREBUILT_LIBS_ROOT})
    message(STATUS "Add ${PREBUILT_LIBS_ROOT} to cmake search path for pre-built libraries")
endif()

message (STATUS "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ; USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES} MAKE_STATIC_LIBRARIES=${MAKE_STATIC_LIBRARIES} UNBUNDLED=${UNBUNDLED}")

include(GNUInstallDirs)

# openssl, zlib before poco
include (cmake/find_ssl.cmake)
if (NOT OPENSSL_FOUND)
    message (FATAL_ERROR "Need openssl for build. debian tip: sudo apt install libssl-dev")
endif ()

include (cmake/lib_name.cmake)
include (cmake/find_icu4c.cmake)
include (cmake/find_boost.cmake)

option (ENABLE_CPPUNIT "Enable CppUnit" OFF)

include (cmake/find_zlib.cmake)
include (cmake/find_zstd.cmake)
if (ENABLE_ODBC)
    include (cmake/find_ltdl.cmake) # for odbc
    if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/contrib/poco/cmake/FindODBC.cmake)
        include (${CMAKE_CURRENT_SOURCE_DIR}/contrib/poco/cmake/FindODBC.cmake) # for poco
    else ()
        include (cmake/find_odbc.cmake)
    endif ()
    message (STATUS "Using odbc: ${ODBC_INCLUDE_DIRECTORIES} : ${ODBC_LIBRARIES}")
endif()
include (cmake/find_poco.cmake)
include (cmake/find_lz4.cmake)
include (cmake/find_sparsehash.cmake)
include (cmake/find_rt.cmake)
include (cmake/find_execinfo.cmake)
include (cmake/find_readline_edit.cmake)
include (cmake/find_re2.cmake)
include (cmake/find_protobuf.cmake)
include (cmake/find_grpc.cmake)
include (cmake/find_kvproto.cmake)
include (cmake/find_tipb.cmake)
include (cmake/find_curl.cmake)
include (cmake/find_prometheus.cmake)
include (cmake/find_tiflash_proxy.cmake)
include (cmake/find_xxhash.cmake)

include (cmake/find_contrib_lib.cmake)
find_contrib_lib(cityhash)
find_contrib_lib(farmhash)
find_contrib_lib(metrohash)
find_contrib_lib(btrie)
find_contrib_lib(double-conversion)

# Find libunwind, must be before jemalloc
option (ENABLE_UNWIND "Enable libunwind (better stacktraces)" ON)
include (libs/libdaemon/cmake/find_unwind.cmake)

# Need to process before "contrib" dir:
include (libs/libcommon/cmake/find_gperftools.cmake)
include (libs/libcommon/cmake/find_jemalloc.cmake)
include (libs/libcommon/cmake/find_mimalloc.cmake)
include (libs/libcommon/cmake/find_cctz.cmake)

include (cmake/print_flags.cmake)

# Enable tests by default when build type is debug
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set (ENABLE_TESTS_DEFAULT ON)
else ()
    set (ENABLE_TESTS_DEFAULT OFF)
endif ()
option (ENABLE_TESTS "Enables unit tests" ${ENABLE_TESTS_DEFAULT})

add_subdirectory (contrib) # Should be done after option ENABLE_TESTS, cause we will disable tests under contrib/

# Enable failpoints injection by default when ENABLE_TESTS is turn ON.
if (ENABLE_TESTS)
    enable_testing()
    set (ENABLE_FAILPOINTS_DEFAULT ON)
    message (STATUS "Tests are enabled")
else ()
    set (ENABLE_FAILPOINTS_DEFAULT OFF)
    message (STATUS "Tests are disabled")
endif()
option (ENABLE_FAILPOINTS "Enables failpoints injection" ${ENABLE_FAILPOINTS_DEFAULT})
if (ENABLE_FAILPOINTS)
    message (STATUS "Failpoints are enabled")
else (ENABLE_FAILPOINTS)
    message (STATUS "Failpoints are disabled")
endif (ENABLE_FAILPOINTS)

# Enable PageStorage V3 test.
option (ENABLE_V3_PAGESTORAGE "Enables V3 PageStorage" ON)

# Flags for test coverage
option (TEST_COVERAGE "Enables flags for test coverage" OFF)
option (TEST_COVERAGE_XML "Output XML report for test coverage" OFF)
if (TEST_COVERAGE AND CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    include(CodeCoverage)
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage")
endif ()

option (TEST_LLVM_COVERAGE "Enables flags for test coverage" OFF)
if (TEST_LLVM_COVERAGE AND CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fprofile-instr-generate -fcoverage-mapping -DTIFLASH_LLVM_COVERAGE=1")
endif ()

if (ARCH_AMD64)
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("-mvpclmulqdq -Werror -Wall -Wextra" TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT)
    if(TIFLASH_COMPILER_VPCLMULQDQ_SUPPORT)
        add_definitions(-DTIFLASH_COMPILER_VPCLMULQDQ_SUPPORT=1)
    else()
        add_definitions(-DTIFLASH_COMPILER_VPCLMULQDQ_SUPPORT=0)
    endif()
else()
    add_definitions(-DTIFLASH_COMPILER_VPCLMULQDQ_SUPPORT=0)
endif()

add_subdirectory (libs)
add_subdirectory (dbms)

include (cmake/print_include_directories.cmake)

# Building a rust project may require setting up toolchains. Rustup cannot handle parallel requests, hence it may fail to
# finish the setup if multiple projects want to install a same version. To mitigate the situation, we force cargo targets
# to be invoked sequentially by adding linear dependency relations between them.
# Another way to do so is to use JOB_POOL with single concurrency. However, it is Ninja specific and it seems to have bugs
# with a large number of threads.
include (cmake/sequential.cmake)
build_sequentially (symbolization tiflash_proxy)
