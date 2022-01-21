cmake_minimum_required (VERSION 2.8)

set (CMAKE_CXX_STANDARD 17)

if (CMAKE_VERSION VERSION_LESS "3.8.0")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
else ()
    set (CMAKE_CXX_STANDARD 17)
    set (CMAKE_CXX_EXTENSIONS 0) # https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html#prop_tgt:CXX_EXTENSIONS
    set (CMAKE_CXX_STANDARD_REQUIRED ON)
    set (CXX_FLAGS_INTERNAL_COMPILER "-std=c++1z")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
endif ()

if (NOT EXISTS "${TiFlash_SOURCE_DIR}/dbms/src/Storages/DeltaMerge/File/dtpb/cpp/dtpb/dmfile.pb.h")
    message (FATAL_ERROR "dmfile cpp files in DeltaMerge/File/dtpb/cpp/dtpb is missing. Try go to DeltaMerge/File/dtpb, and run ./generate_cpp.sh")
endif ()

file(GLOB __tmp RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} dtpb/*.cc dtpb/*.h)
list(APPEND generated_files ${__tmp})

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-parameter")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")

add_library(dtpb ${generated_files})
target_include_directories(dtpb PUBLIC ./)
