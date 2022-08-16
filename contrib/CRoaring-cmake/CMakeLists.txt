set(CRoaring_SRC_DIR ${TiFlash_SOURCE_DIR}/contrib/CRoaring)
set(SRCS
    ${CRoaring_SRC_DIR}/src/array_util.c
    ${CRoaring_SRC_DIR}/src/bitset_util.c
    ${CRoaring_SRC_DIR}/src/license-comment.h
    ${CRoaring_SRC_DIR}/src/memory.c
    ${CRoaring_SRC_DIR}/src/roaring_array.c
    ${CRoaring_SRC_DIR}/src/roaring.c
    ${CRoaring_SRC_DIR}/src/roaring_priority_queue.c
    ${CRoaring_SRC_DIR}/src/containers/array.c
    ${CRoaring_SRC_DIR}/src/containers/bitset.c
    ${CRoaring_SRC_DIR}/src/containers/containers.c
    ${CRoaring_SRC_DIR}/src/containers/convert.c
    ${CRoaring_SRC_DIR}/src/containers/mixed_andnot.c
    ${CRoaring_SRC_DIR}/src/containers/mixed_equal.c
    ${CRoaring_SRC_DIR}/src/containers/mixed_intersection.c
    ${CRoaring_SRC_DIR}/src/containers/mixed_negation.c
    ${CRoaring_SRC_DIR}/src/containers/mixed_subset.c
    ${CRoaring_SRC_DIR}/src/containers/mixed_union.c
    ${CRoaring_SRC_DIR}/src/containers/mixed_xor.c
    ${CRoaring_SRC_DIR}/src/containers/run.c
)

add_library(croaring ${SRCS})
target_include_directories(croaring SYSTEM PUBLIC 
    ${CRoaring_SRC_DIR}/include
    ${CRoaring_SRC_DIR}/cpp)