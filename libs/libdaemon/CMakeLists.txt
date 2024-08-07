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

add_library (daemon ${SPLIT_SHARED}
    src/BaseDaemon.cpp
    src/GraphiteWriter.cpp
    src/OwnPatternFormatter.cpp

    include/daemon/BaseDaemon.h
    include/daemon/GraphiteWriter.h
    include/daemon/OwnPatternFormatter.h
)

if (USE_UNWIND)
    target_compile_definitions (daemon PRIVATE USE_UNWIND=1)
    if (USE_LLVM_LIBUNWIND)
        target_compile_definitions (daemon PRIVATE USE_LLVM_LIBUNWIND=1)
    endif()
    target_include_directories (daemon BEFORE PRIVATE ${UNWIND_INCLUDE_DIR})
    target_include_directories (daemon BEFORE PRIVATE ${UNWIND_INCREMENTAL_DIR})
    target_link_libraries (daemon ${UNWIND_LIBRARY})
endif ()

target_include_directories (daemon PUBLIC include)
target_include_directories (daemon PRIVATE ${TiFlash_SOURCE_DIR}/libs/libpocoext/include)

target_link_libraries (daemon tiflash_vector_features tiflash_common_io tiflash_common_config grpc grpc++ ${EXECINFO_LIBRARY})
if (ENABLE_TESTS)
    add_subdirectory (src/tests EXCLUDE_FROM_ALL)
endif ()
