// Copyright 2026 PingCAP, Inc.
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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile_fwd.h>
#include <common/logger_useful.h>

#include <memory>
#include <vector>

namespace DB
{
class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
} // namespace DB

namespace DB::DM
{

struct LocalReadObject
{
    String s3_key;
    UInt64 file_size = 0;
};

/// Collect S3 objects to stage locally before reading a MetaV2 DMFile.
///
/// Collects physical `.merged` blobs referenced by `read_columns`, as well as
/// standalone column subfiles (e.g. large `.dat` files not merged into `.merged`).
///
/// Returns empty for non-MetaV2 DMFiles or when paths are not valid remote S3 keys.
std::vector<LocalReadObject> collectMetaV2MergedFilesForLocalRead(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const LoggerPtr & log,
    const String & tracing_id);

/// Download collected MetaV2 objects into FileCache and return pins for reader lifetime.
/// Returns empty when staging is disabled, FileCache is unavailable, or nothing to stage.
/// Per-object download failures are logged and counted; successful pins are still returned.
std::vector<FileSegmentPtr> tryDownloadMetaV2MergedFilesForLocalRead(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    bool enable_write_filecache_local_read,
    const LoggerPtr & log,
    const String & tracing_id);

} // namespace DB::DM
