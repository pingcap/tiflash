# Normally we use the internal gRPC framework.
# You can set USE_INTERNAL_GRPC_LIBRARY to OFF to force using the external gRPC framework, which should be installed in the system in this case.
# The external gRPC framework can be installed in the system by running
# sudo apt-get install libgrpc++-dev protobuf-compiler-grpc
option(USE_INTERNAL_GRPC_LIBRARY "Set to FALSE to use system gRPC library instead of bundled. (Experimental. Set to OFF on your own risk)" ${NOT_UNBUNDLED})

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/grpc/CMakeLists.txt")
    if(USE_INTERNAL_GRPC_LIBRARY)
        message(WARNING "submodule contrib/grpc is missing. to fix try run: \n git submodule update --init")
        message(WARNING "Can't use internal grpc")
        set(USE_INTERNAL_GRPC_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_GRPC_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_GRPC_LIBRARY)
    find_package(gRPC)
    if(NOT gRPC_INCLUDE_DIRS OR NOT gRPC_LIBRARIES)
        message(WARNING "Can't find system gRPC library")
        set(EXTERNAL_GRPC_LIBRARY_FOUND 0)
    elseif(NOT gRPC_CPP_PLUGIN)
        message(WARNING "Can't find system grpc_cpp_plugin")
        set(EXTERNAL_GRPC_LIBRARY_FOUND 0)
    else()
        set(EXTERNAL_GRPC_LIBRARY_FOUND 1)
    endif()
endif()

if(NOT EXTERNAL_GRPC_LIBRARY_FOUND AND NOT MISSING_INTERNAL_GRPC_LIBRARY)
    set(gRPC_INCLUDE_DIRS "${TiFlash_SOURCE_DIR}/contrib/grpc/include")
    set(gRPC_LIBRARIES grpc grpc++)
    set(gRPC_CPP_PLUGIN $<TARGET_FILE:grpc_cpp_plugin>)

    include("${TiFlash_SOURCE_DIR}/contrib/grpc-cmake/protobuf_generate_grpc.cmake")

    set(USE_INTERNAL_GRPC_LIBRARY 1)
endif()

set(gRPC_FOUND TRUE)

message(STATUS "Using gRPC: ${gRPC_VERSION} : ${gRPC_INCLUDE_DIRS}, ${gRPC_LIBRARIES}, ${gRPC_CPP_PLUGIN}")
