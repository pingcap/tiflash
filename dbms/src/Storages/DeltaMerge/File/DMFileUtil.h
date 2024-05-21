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

#pragma once

#include <IO/WriteHelpers.h>
#include <common/types.h>

namespace DB::DM
{

namespace details
{
inline constexpr static const char * NGC_FILE_NAME = "NGC";
inline constexpr static const char * FOLDER_PREFIX_WRITABLE = ".tmp.dmf_";
inline constexpr static const char * FOLDER_PREFIX_READABLE = "dmf_";
inline constexpr static const char * FOLDER_PREFIX_DROPPED = ".del.dmf_";
inline constexpr static const char * DATA_FILE_SUFFIX = ".dat";
inline constexpr static const char * INDEX_FILE_SUFFIX = ".idx";
inline constexpr static const char * MARK_FILE_SUFFIX = ".mrk";

inline String getNGCPath(const String & prefix)
{
    return prefix + "/" + NGC_FILE_NAME;
}
} // namespace details

enum class DMFileStatus : int
{
    WRITABLE,
    WRITING,
    READABLE,
    DROPPED,
};

String getPathByStatus(const String & parent_path, UInt64 file_id, DMFileStatus status);
String getNGCPath(const String & parent_path, UInt64 file_id, DMFileStatus status);
using FileNameBase = String;
String colDataFileName(const FileNameBase & file_name_base);
String colIndexFileName(const FileNameBase & file_name_base);
String colMarkFileName(const FileNameBase & file_name_base);

} // namespace DB::DM
