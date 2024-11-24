# We want to use C++23, but GRPC is not ready
set (CMAKE_CXX_STANDARD 20)

set(_gRPC_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/grpc")
set(_gRPC_BINARY_DIR "${TiFlash_BINARY_DIR}/contrib/grpc")

# Use re2 from ClickHouse contrib, not from gRPC third_party.
if(NOT RE2_INCLUDE_DIR)
  message(FATAL_ERROR " grpc: The location of the \"re2\" library is unknown")
endif()
set(gRPC_RE2_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_RE2_INCLUDE_DIR "${RE2_INCLUDE_DIR}")
set(_gRPC_RE2_LIBRARIES "${RE2_LIBRARY}")

# Use zlib from ClickHouse contrib, not from gRPC third_party.
if(NOT ZLIB_INCLUDE_DIRS)
  message(FATAL_ERROR " grpc: The location of the \"zlib\" library is unknown")
endif()
set(gRPC_ZLIB_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_ZLIB_INCLUDE_DIR "${ZLIB_INCLUDE_DIRS}")
set(_gRPC_ZLIB_LIBRARIES "${ZLIB_LIBRARIES}")

# Use protobuf from ClickHouse contrib, not from gRPC third_party.
if(NOT Protobuf_INCLUDE_DIR OR NOT Protobuf_LIBRARY)
  message(FATAL_ERROR " grpc: The location of the \"protobuf\" library is unknown")
elseif (NOT Protobuf_PROTOC_EXECUTABLE)
  message(FATAL_ERROR " grpc: The location of the protobuf compiler is unknown")
elseif (NOT Protobuf_PROTOC_LIBRARY)
  message(FATAL_ERROR " grpc: The location of the protobuf compiler's library is unknown")
endif()
set(gRPC_PROTOBUF_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_PROTOBUF_WELLKNOWN_INCLUDE_DIR "${Protobuf_INCLUDE_DIR}")
set(_gRPC_PROTOBUF_LIBRARIES "${Protobuf_LIBRARY}")
set(_gRPC_PROTOBUF_PROTOC "protoc")
set(_gRPC_PROTOBUF_PROTOC_EXECUTABLE "${Protobuf_PROTOC_EXECUTABLE}")
set(_gRPC_PROTOBUF_PROTOC_LIBRARIES "${Protobuf_PROTOC_LIBRARY}")

# Use OpenSSL from ClickHouse contrib, not from gRPC third_party.
set(gRPC_SSL_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_SSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR})
set(_gRPC_SSL_LIBRARIES ${OPENSSL_LIBRARIES})

# Use abseil-cpp from ClickHouse contrib, not from gRPC third_party.
set(gRPC_ABSL_PROVIDER "clickhouse" CACHE STRING "" FORCE)

# Choose to build static or shared library for c-ares.
if (MAKE_STATIC_LIBRARIES)
  set(CARES_STATIC ON CACHE BOOL "" FORCE)
  set(CARES_SHARED OFF CACHE BOOL "" FORCE)
else ()
  set(CARES_STATIC OFF CACHE BOOL "" FORCE)
  set(CARES_SHARED ON CACHE BOOL "" FORCE)
endif ()

# Disable looking for libnsl on a platforms that has gethostbyname in glibc
#
# c-ares searching for gethostbyname in the libnsl library, however in the
# version that shipped with gRPC it doing it wrong [1], since it uses
# CHECK_LIBRARY_EXISTS(), which will return TRUE even if the function exists in
# another dependent library. The upstream already contains correct macro [2],
# but it is not included in gRPC (even upstream gRPC, not the one that is
# shipped with clickhousee).
#
#   [1]: https://github.com/c-ares/c-ares/blob/e982924acee7f7313b4baa4ee5ec000c5e373c30/CMakeLists.txt#L125
#   [2]: https://github.com/c-ares/c-ares/blob/44fbc813685a1fa8aa3f27fcd7544faf612d376a/CMakeLists.txt#L146
#
# And because if you by some reason have libnsl [3] installed, clickhouse will
# reject to start w/o it. While this is completelly different library.
#
#   [3]: https://packages.debian.org/bullseye/libnsl2

if (NOT CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  set(HAVE_LIBNSL OFF CACHE BOOL "" FORCE)
endif()

# We don't want to build C# extensions.
set(gRPC_BUILD_CSHARP_EXT OFF)

no_warning(deprecated-builtins)
add_subdirectory("${_gRPC_SOURCE_DIR}" "${_gRPC_BINARY_DIR}")

# clean up some clang warnings
target_no_warning(grpc deprecated-declarations)
target_no_warning(grpc non-c-typedef-for-linkage)
target_no_warning(grpc implicit-const-int-float-conversion)
target_no_warning(grpc unused-but-set-variable)
target_no_warning(grpc unused-function)
target_no_warning(grpc++ deprecated-declarations)
target_no_warning(grpc++ non-c-typedef-for-linkage)
target_no_warning(grpc++ implicit-const-int-float-conversion)
target_no_warning(grpc++ unused-but-set-variable)

execute_process(
  COMMAND grep "//gpr_log(GPR_DEBUG, \"cannot set inq fd=%d errno=%d\", tcp->fd, errno);" "${_gRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_posix.cc"
  RESULT_VARIABLE HAVE_APPLY_PATCH)
# grep - Normally, the exit status is 0 if selected lines are found and 1 otherwise. But the exit status is 2 if an error occurred.
if (HAVE_APPLY_PATCH EQUAL 1)
  message(STATUS "grpc patch not apply: ${HAVE_APPLY_PATCH}, patching...")
  # update the patch using `git format-patch` if you upgrade grpc
  set (GRPC_PATCH_FILE "${TiFlash_SOURCE_DIR}/contrib/grpc-cmake/0001-disable-tcp-set-inq-debug-message.patch")
  # apply the patch
  execute_process(
    COMMAND git apply -v "${GRPC_PATCH_FILE}"
    WORKING_DIRECTORY "${_gRPC_SOURCE_DIR}"
    COMMAND_ECHO STDOUT
    RESULT_VARIABLE PATCH_SUCC)
  if (NOT PATCH_SUCC EQUAL 0)
    message(FATAL_ERROR "Can not apply grpc patch ${GRPC_PATCH_FILE}")
  else ()
    message(STATUS "grpc patch done")
  endif ()
elseif (HAVE_APPLY_PATCH EQUAL 0)
  message(STATUS "grpc patch have been applied: ${HAVE_APPLY_PATCH}")
else ()
  message(FATAL_ERROR "Can not check the grpc patch status")
endif ()


# The contrib/grpc/CMakeLists.txt redefined the PROTOBUF_GENERATE_GRPC_CPP() function for its own purposes,
# so we need to redefine it back.
include("${TiFlash_SOURCE_DIR}/contrib/grpc-cmake/protobuf_generate_grpc.cmake")
