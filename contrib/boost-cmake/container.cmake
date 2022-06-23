set (BOOST_CONTAINER_LIBRARY_DIR ${TiFlash_SOURCE_DIR}/contrib/boost/libs/container)
# list files explicitly because there are conflicting sources
add_library(boost_container STATIC
        ${BOOST_CONTAINER_LIBRARY_DIR}/src/alloc_lib.c
        ${BOOST_CONTAINER_LIBRARY_DIR}/src/dlmalloc.cpp
        ${BOOST_CONTAINER_LIBRARY_DIR}/src/global_resource.cpp
        ${BOOST_CONTAINER_LIBRARY_DIR}/src/monotonic_buffer_resource.cpp
        ${BOOST_CONTAINER_LIBRARY_DIR}/src/pool_resource.cpp
        ${BOOST_CONTAINER_LIBRARY_DIR}/src/synchronized_pool_resource.cpp
        ${BOOST_CONTAINER_LIBRARY_DIR}/src/unsynchronized_pool_resource.cpp)
# override system include if it is needed
target_include_directories(boost_container BEFORE PRIVATE ${TiFlash_SOURCE_DIR}/contrib/boost)
set_source_files_properties(${BOOST_CONTAINER_LIBRARY_DIR}/src/alloc_lib.c PROPERTIES COMPILE_FLAGS -Wno-unused-but-set-variable)