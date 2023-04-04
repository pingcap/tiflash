# Copyright 2022 PingCAP, Ltd.
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

add_library (pocoext ${SPLIT_SHARED}
    src/LevelFilterChannel.cpp
    src/ThreadNumber.cpp
    src/SourceFilterChannel.cpp
    src/ReloadableSplitterChannel.cpp
    src/TiFlashLogFileChannel.cpp

    include/Poco/Ext/LevelFilterChannel.h
    include/Poco/Ext/ThreadNumber.h
    include/Poco/Ext/SourceFilterChannel.h
    include/Poco/Ext/ReloadableSplitterChannel.h
    include/Poco/Ext/TiFlashLogFileChannel.h
    include/Poco/Ext/TiFlashArchiveByTimestampsStrategy.h)

target_include_directories (pocoext PUBLIC include PRIVATE ${COMMON_INCLUDE_DIR})

if (NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (pocoext BEFORE PUBLIC ${Poco_INCLUDE_DIRS})
endif ()

target_link_libraries(pocoext
    ${LZ4_LIBRARY}
    ${ZSTD_LIBRARY}
    Poco::Foundation
    Poco::Net
    Poco::Util
    Poco::XML
)
