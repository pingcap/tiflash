project (etcd)

cmake_minimum_required (VERSION 3.5)

set (CMAKE_CXX_STANDARD 17)

if (CMAKE_VERSION VERSION_LESS "3.8.0")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
else ()
    set (CMAKE_CXX_STANDARD 17)
    set (CMAKE_CXX_EXTENSIONS 1) # https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html#prop_tgt:CXX_EXTENSIONS
    set (CMAKE_CXX_STANDARD_REQUIRED ON)
endif ()

if (NOT Protobuf_INCLUDE_DIR OR NOT gRPC_FOUND OR NOT ABSL_ROOT_DIR)
    message(FATAL_ERROR "protobuf/grpc/absl not found! protobuf: ${Protobuf_INCLUDE_DIR} grpc: ${gRPC_FOUND} absl: ${ABSL_ROOT_DIR}")
endif ()

# .proto files under "proto"
set (PROTO_DEF_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
file (GLOB PROTO_DEFS "${PROTO_DEF_DIR}/*.proto")
# # .proto files under "include"
set (PROTO_INC_DEF_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
get_filename_component (PROTO_INC_DEF_DIR ${PROTO_INC_DEF_DIR} ABSOLUTE)
file (GLOB PROTO_INC_DEFS "${PROTO_INC_DEF_DIR}/*.proto")
list (APPEND PROTO_INC_DEFS
    "${PROTO_INC_DEF_DIR}/google/api/http.proto"
    "${PROTO_INC_DEF_DIR}/google/api/annotations.proto"
)
# The output dir for .pb.h, .pb.cc. Generated under build directory.
set (PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/etcd")
file (MAKE_DIRECTORY "${PROTO_OUTPUT_DIR}")
message(STATUS ".pb.h, .pb.cc of etcd are generated under ${PROTO_OUTPUT_DIR}")
# A temporary directory for proto file without gogoproto marks
set (PROTO_DEF_TMP_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file (MAKE_DIRECTORY "${PROTO_DEF_TMP_DIR}")

set (CLEAN_PROTO_DEFS "")
foreach (F ${PROTO_DEFS})
    get_filename_component (ABS_FIL ${F} ABSOLUTE)
    get_filename_component (FIL_WE ${F} NAME_WE)
    list (APPEND CLEAN_PROTO_DEFS "${PROTO_DEF_TMP_DIR}/${FIL_WE}.proto")
    # Generate a output file by copying and cleaning up gogoproto marks.
    # Will be re-generated if the original .proto file is changed.
    add_custom_command(
        OUTPUT "${PROTO_DEF_TMP_DIR}/${FIL_WE}.proto"
        WORKING_DIRECTORY ${PROTO_DEF_DIR}
        COMMAND cp ${ABS_FIL} ${PROTO_DEF_TMP_DIR}/${FIL_WE}.proto
        COMMAND bash -c "source ${CMAKE_CURRENT_SOURCE_DIR}/common.sh && clean_gogo_proto ${PROTO_DEF_TMP_DIR}/${FIL_WE}.proto"
        DEPENDS ${ABS_FIL}
        COMMENT "Generating clean proto file on ${ABS_FIL}"
        VERBATIM)
endforeach()
# Grab all clean .proto files as a target
add_custom_target (__clean_etcd_defs
    DEPENDS ${CLEAN_PROTO_DEFS}
    VERBATIM)

set (PROTO_SRCS "")
set (PROTO_HDRS "")
foreach (F ${PROTO_INC_DEFS}) # .proto under "include"
    get_filename_component (ABS_FIL ${F} ABSOLUTE)
    # We need relative path for .proto under subdirectory
    string(REPLACE "${PROTO_INC_DEF_DIR}/" "" RELA_FIL ${ABS_FIL})
    string(REPLACE ".proto" ".pb.cc" PROTO_SRC ${RELA_FIL})
    string(REPLACE ".proto" ".pb.h" PROTO_HDR ${RELA_FIL})

    # message(STATUS "${F} ${ABS_FIL} ${RELA_FIL}")
    list (APPEND PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_SRC}")
    list (APPEND PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_HDR}")
    # Create the generated cpp files for .proto file.
    # Add dependency on the original .proto file
    # so that it can re-generate source files if .proto changed.
    add_custom_command(
        OUTPUT "${PROTO_OUTPUT_DIR}/${PROTO_SRC}"
               "${PROTO_OUTPUT_DIR}/${PROTO_HDR}"
        WORKING_DIRECTORY ${PROTO_INC_DEF_DIR}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --cpp_out=${PROTO_OUTPUT_DIR} -I=${PROTO_DEF_TMP_DIR}:${PROTO_INC_DEF_DIR} ${RELA_FIL}
        DEPENDS ${ABS_FIL} ${Protobuf_PROTOC_EXECUTABLE} __clean_etcd_defs
        COMMENT "Running C++ protocol buffer compiler on ${ABS_FIL}"
        VERBATIM)
endforeach() # end for .proto under "include"
foreach (F ${PROTO_DEFS}) # .proto under "proto"
    get_filename_component (ABS_FIL ${F} ABSOLUTE)
    get_filename_component (FIL_WE ${F} NAME_WE)

    # message(STATUS "${F} ${ABS_FIL}")
    list (APPEND PROTO_SRCS "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.cc")
    list (APPEND PROTO_HDRS "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.h")
    # Need to appen grpc services files
    list (APPEND PROTO_SRCS "${PROTO_OUTPUT_DIR}/${FIL_WE}.grpc.pb.cc")
    list (APPEND PROTO_HDRS "${PROTO_OUTPUT_DIR}/${FIL_WE}.grpc.pb.h")
    # Create the generated cpp files for .proto file.
    # Add dependency on the original .proto file and `__clean_etcd_defs`
    # so that it can re-generate source files if .proto changed.
    add_custom_command(
        OUTPUT "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.cc"
               "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.h"
        WORKING_DIRECTORY ${PROTO_DEF_TMP_DIR}
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --cpp_out=${PROTO_OUTPUT_DIR} -I=${PROTO_DEF_TMP_DIR}:${PROTO_INC_DEF_DIR} ${PROTO_DEF_TMP_DIR}/${FIL_WE}.proto 
        # And also grpc service
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --grpc_out=${PROTO_OUTPUT_DIR} -I=${PROTO_DEF_TMP_DIR}:${PROTO_INC_DEF_DIR}  --plugin=protoc-gen-grpc=${gRPC_CPP_PLUGIN} ${PROTO_DEF_TMP_DIR}/${FIL_WE}.proto
        DEPENDS ${ABS_FIL} ${Protobuf_PROTOC_EXECUTABLE} ${gRPC_CPP_PLUGIN} __clean_etcd_defs
        COMMENT "Running C++ protocol buffer compiler on ${ABS_FIL}"
        VERBATIM)
endforeach() # end for .proto under "proto"

# Mark those clean .proto, .pb.h, .pb.cc files are generated
# so that they can be re-build if changed and clean by `make clean`
set_source_files_properties(
    ${CLEAN_PROTO_DEFS} ${PROTO_SRCS} ${PROTO_HDRS}
    PROPERTIES GENERATED TRUE
)

add_library(etcdpb
    ${PROTO_SRCS} ${PROTO_HDRS}
)
target_link_libraries(etcdpb
    absl::synchronization
)
target_include_directories(etcdpb PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/etcd
    ${Protobuf_INCLUDE_DIR}
    ${gRPC_INCLUDE_DIRS}
)
target_no_warning(etcdpb deprecated-declarations)
