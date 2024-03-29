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

#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/KVStore/Types.h>
#include <common/defines.h>
#include <common/types.h>
#include <fmt/format.h>

#include <string_view>

namespace DB::S3
{

using DMFileOID = ::DB::DM::Remote::DMFileOID;

enum class S3FilenameType
{
    Invalid,
    CheckpointManifest,
    DataFile, // StableDTFile or CheckpointDataFile,
    LockFile, // Its key point to a DataFile that is hold by a tiflash node
    DelMark, // Its key point to a DataFile that is marked as deleted
    StorePrefix, // The key prefix for a store
};

/// S3Filename and S3FilenameView are utils class for generating the S3 object key
/// from a local file or parsing the info from S3 object key. They define how objects
/// are organized on S3.
///
/// Specifically, there are 4 kinds of objects stored on S3
/// - CheckpointManifest
/// - DataFile, including CheckpointDataFile and DTFile in the Stable layer
/// - LockFile, its key point to a DataFile that is held by a tiflash node
/// - DelMark, its key point to a DataFile that is marked as deleted
///
/// CheckpointManifest are stored with a store_id prefix: "s${store_id}/manifest/mf_${upload_seq}".
///
/// DataFile are also stored with a store_id prefix: "s${store_id}/data/${data_subpath}"
/// - for CheckpointDataFile, data_subpath is "dat_${upload_seq}_{dat_index}"
/// - for StableDTFile, data_subpath is "t_${table_id}/dmf_${dmf_id}"
/// - for StableDTFile under keyspace, data_subpath is "ks_${ks_id}_t_${table_id}/dmf_${dmf_id}"
/// However, thus DataFile is generated and upload to S3 by the ${store_id}, it could be shared
/// by multiple TiFlash instances. We need LockFile to specify the DataFile is being used.
///
/// The pattern of LockFile is "lock/s${store_id}/${data_subpath}.lock_s${lock_store}_${lock_seq}"
/// All LockFiles are store with the same prefix "lock/s${store_id}/". So that we can use
/// less S3 LIST request for the S3 data GC.
///
/// Delmark are stored along with its DataFile: "s${store_id}/data/${data_subpath}.del".
/// It is a mark that will be upload during S3 data GC, it means the file is not held by any
/// TiFlash instance.


// Usually use for parsing key from S3 Object. And it can also generate related keys to the object.
// Note that it is only a "view". Caller should ensure the key is valid during this view's lifetime.
struct S3FilenameView
{
    S3FilenameType type{S3FilenameType::Invalid};
    StoreID store_id{0};
    std::string_view data_subpath;
    std::string_view lock_suffix;

    String toFullKey() const;

    /// CheckpointDataFile/StableFile utils ///

    ALWAYS_INLINE bool isDataFile() const { return type == S3FilenameType::DataFile; }
    bool isDMFile() const;
    DMFileOID getDMFileOID() const;
    // Return the lock key prefix for finding any locks on this data file through `S3::LIST`
    String getLockPrefix() const;
    // Return the lock key for writing lock file on S3
    String getLockKey(StoreID lock_store_id, UInt64 lock_seq) const;
    // Return the delmark key for writing delmark file on S3
    String getDelMarkKey() const;

    /// CheckpointManifest/CheckpointDataFile utils ///

    // Get the upload sequence from Manifest S3 key.
    // Exception will be throw if the key is invalid or the type is not
    // one of CheckpointManifest
    UInt64 getUploadSequence() const;

    /// LockFile utils ///

    ALWAYS_INLINE inline bool isLockFile() const { return type == S3FilenameType::LockFile; }
    struct LockInfo
    {
        StoreID store_id{0};
        UInt64 sequence{0};
    };
    LockInfo getLockInfo() const;
    S3FilenameView asDataFile() const;

    /// DelMark

    ALWAYS_INLINE bool isDelMark() const { return type == S3FilenameType::DelMark; }

    ALWAYS_INLINE bool isValid() const { return type != S3FilenameType::Invalid; }

public:
    // The result return a view from the `fullpath`.
    // If parsing from a raw char ptr, do NOT create a temporary String object.
    static S3FilenameView fromKey(std::string_view fullpath);

    static S3FilenameView fromStoreKeyPrefix(std::string_view prefix);

    // Return a view from the `fullpath` with a `s3://` prefix.
    // Note: bucket and root should not be included in the `fullpath`.
    static S3FilenameView fromKeyWithPrefix(std::string_view fullpath);
};

// Use for generating the S3 object key
struct S3Filename
{
    S3FilenameType type{S3FilenameType::Invalid};
    StoreID store_id{0};
    String data_subpath;

    static String allStorePrefix();
    static S3Filename fromStoreId(StoreID store_id);
    static S3Filename fromDMFileOID(const DMFileOID & oid);
    static S3Filename fromTableID(StoreID store_id, KeyspaceID keyspace_id, TableID table_id);
    static S3Filename newCheckpointData(StoreID store_id, UInt64 upload_seq, UInt64 file_idx);
    static S3Filename newCheckpointManifest(StoreID store_id, UInt64 upload_seq);

    // Generate an id template for checkpoint data id
    // Available placeholders: {seq}, {index}.
    static String newCheckpointDataNameTemplate(StoreID store_id, UInt64 lock_seq);
    // Generate an id template for checkpoint manifest id
    // Available placeholders: {seq}.
    static String newCheckpointManifestNameTemplate(StoreID store_id);

    String toFullKey() const;

    // `toFullKeyWithPrefix` will as a `s3:://` prefix in full key.
    // You can pass a full key with prefix to `FileProvider` as file path,
    // if you want to read/write S3 object as file.
    // Note: bucket and root are not included in the result.
    String toFullKeyWithPrefix() const;

    String toManifestPrefix() const;

    String toDataPrefix() const;

    static String getLockPrefix();

    S3FilenameView toView() const
    {
        return S3FilenameView{
            .type = type,
            .store_id = store_id,
            .data_subpath = data_subpath,
        };
    }
};

} // namespace DB::S3
