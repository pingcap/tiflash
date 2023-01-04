// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <common/types.h>
#include <fmt/format.h>

#include <string_view>

namespace DB::S3
{
enum class S3FilenameType
{
    Invalid,
    CheckpointManifest,
    StableFile,
    CheckpointDataFile,
    LockFileToStableFile,
    LockFileToCheckpointData,
    DelMarkToStableFile,
    DelMarkToCheckpointData,
    StorePrefix,
};

struct S3FilenameView
{
    S3FilenameType type{S3FilenameType::Invalid};
    UInt64 store_id{0};
    std::string_view path;
    std::string_view lock_suffix;

    String toFullKey() const;

    /// CheckpointDataFile/StableFile utils ///

    bool isDataFile() const { return type == S3FilenameType::StableFile || type == S3FilenameType::CheckpointDataFile; }
    String getLockKey(UInt64 lock_store_id, UInt64 lock_seq) const;
    String getDelMarkKey() const;

    /// CheckpointManifest/CheckpointDataFile utils ///

    // Get the upload sequence from Manifest or CheckpointDataFile S3 key.
    // Exception will be throw if the key is invalid or the type is not
    // one of CheckpointManifest/CheckpointDataFile
    UInt64 getUploadSequence() const;

    /// LockFile utils ///

    bool isLockFile() const { return type == S3FilenameType::LockFileToCheckpointData || type == S3FilenameType::LockFileToStableFile; }
    struct LockInfo
    {
        UInt64 store_id{0};
        UInt64 sequence{0};
    };
    LockInfo getLockInfo() const;
    S3FilenameView asDataFile() const;

    bool isDelMark() const { return type == S3FilenameType::DelMarkToStableFile || type == S3FilenameType::DelMarkToCheckpointData; }

public:
    // The result return a view from the `fullpath`.
    // If parsing from a raw char ptr, do NOT create a temporary String object.
    static S3FilenameView fromKey(std::string_view fullpath);

    static S3FilenameView fromStoreKeyPrefix(std::string_view prefix);
};

struct S3Filename
{
    S3FilenameType type{S3FilenameType::Invalid};
    UInt64 store_id{0};
    String path;

    static S3Filename fromStoreId(UInt64 store_id);
    static S3Filename fromDMFileOID(const DM::Remote::DMFileOID & oid);
    static S3Filename newCheckpointData(UInt64 store_id, UInt64 upload_seq, UInt64 file_idx);
    static S3Filename newCheckpointManifest(UInt64 store_id, UInt64 upload_seq);

    String toFullKey() const;

    String toManifestPrefix() const;

    String toDataPrefix() const;

    static String getLockPrefix();

    S3FilenameView toView() const
    {
        return S3FilenameView{
            .type = type,
            .store_id = store_id,
            .path = path,
        };
    }
};

} // namespace DB::S3
