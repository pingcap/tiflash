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

namespace DB::DM
{

struct LocalReadObject
{
    String s3_key;
    UInt64 file_size = 0;
};

/// Collect S3 objects to stage locally before reading a MetaV2 DMFile.
///
/// Currently only collects physical `.merged` blobs referenced by `read_columns`.
/// Logical subfiles not merged into `.merged` (e.g. large standalone column `.dat`
/// files) are skipped; the read path falls back to direct S3 read for them.
///
/// TODO: Also collect standalone column subfiles (via `colDataPath` / `colMarkPath`)
/// when they are not present in `merged_sub_file_infos`, so FileCache can
/// pre-download them as independent S3 objects.
///
/// Returns empty for non-MetaV2 DMFiles or when paths are not valid remote S3 keys.
std::vector<LocalReadObject> collectMetaV2MergedFilesForLocalRead(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const LoggerPtr & log,
    const String & tracing_id);

} // namespace DB::DM
