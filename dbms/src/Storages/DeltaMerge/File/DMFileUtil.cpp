// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "DMFileUtil.h"

namespace DB::DM
{

String getPathByStatus(const String & parent_path, UInt64 file_id, DMFileStatus status)
{
    String s = parent_path + "/";
    switch (status)
    {
    case DMFileStatus::READABLE:
        s += details::FOLDER_PREFIX_READABLE;
        break;
    case DMFileStatus::WRITABLE:
    case DMFileStatus::WRITING:
        s += details::FOLDER_PREFIX_WRITABLE;
        break;
    case DMFileStatus::DROPPED:
        s += details::FOLDER_PREFIX_DROPPED;
        break;
    }
    s += DB::toString(file_id);
    return s;
}

String getNGCPath(const String & parent_path, UInt64 file_id, DMFileStatus status)
{
    return details::getNGCPath(getPathByStatus(parent_path, file_id, status));
}

String colDataFileName(const FileNameBase & file_name_base)
{
    return file_name_base + details::DATA_FILE_SUFFIX;
}
String colIndexFileName(const FileNameBase & file_name_base)
{
    return file_name_base + details::INDEX_FILE_SUFFIX;
}
String colMarkFileName(const FileNameBase & file_name_base)
{
    return file_name_base + details::MARK_FILE_SUFFIX;
}

} // namespace DB::DM
