add_library (memcpy STATIC memcpy.cpp)
target_include_directories(memcpy PUBLIC ${TiFlash_SOURCE_DIR}/libs/libcommon/include)
