cmake_minimum_required(VERSION 2.8)
project(kvClient)

if (NOT gRPC_FOUND)
    include (cmake/find_grpc.cmake)
endif ()

if (NOT Poco_Foundation_LIBRARY)
    include (cmake/find_poco.cmake)
endif()

if (NOT KVPROTO_FOUND)
    include (cmake/find_kvproto.cmake)
    add_subdirectory (third_party/kvproto/cpp)
endif ()

add_subdirectory (src)
