# TODO We should make a bundled gPRC and protoobuf repository, instance of rely on system library.

# gRPC and relateds
if (GRPC_ROOT_DIR)
    list(PREPEND CMAKE_SYSTEM_PREFIX_PATH ${GRPC_ROOT_DIR})
    message(STATUS "Add ${GRPC_ROOT_DIR} to search path for protobuf && grpc")
endif()

find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf: ${Protobuf_VERSION} : ${Protobuf_INCLUDE_DIRS}, ${Protobuf_LIBRARIES}")

include_directories(${PROTOBUF_INCLUDE_DIRS})

find_package(c-ares REQUIRED)
message(STATUS "Using c-ares: ${c-ares_INCLUDE_DIR}, ${c-ares_LIBRARY}")

find_package(ZLIB REQUIRED)
message(STATUS "Using ZLIB: ${ZLIB_INCLUDE_DIRS}, ${ZLIB_LIBRARIES}")

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC: ${gRPC_VERSION}")

set(gRPC_FOUND TRUE)
