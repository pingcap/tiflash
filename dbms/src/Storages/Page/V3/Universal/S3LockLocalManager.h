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

#include <Common/nocopyable.h>
#include <Flash/Disaggregated/S3LockClient.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <aws/s3/S3Client.h>


namespace DB::S3
{
class IS3LockClient;
using S3LockClientPtr = std::shared_ptr<IS3LockClient>;
} // namespace DB::S3

namespace DB::PS::V3
{
class S3LockLocalManager;
using S3LockLocalManagerPtr = std::unique_ptr<S3LockLocalManager>;

class S3LockLocalManager
{
public:
    S3LockLocalManager();

    // Try to restore from S3 latest manifest.
    // When this function is called by the first time, it will setup the
    // `last_upload_sequence`, copy the checkpoint info from the latest
    // manifest to `directory` and return a `ManifestFilePrefix`.
    // Otherwise it returns std::nullopt.
    std::optional<CheckpointProto::ManifestFilePrefix> initStoreInfo(
        StoreID actual_store_id,
        DB::S3::S3LockClientPtr s3lock_client_,
        const universal::PageDirectoryPtr & directory);

    void waitUntilInited();

    struct ExtraLockInfo
    {
        UInt64 upload_sequence;
        std::unordered_set<String> pre_lock_keys;
    };
    ExtraLockInfo allocateNewUploadLocksInfo();

    void createS3LockForWriteBatch(UniversalWriteBatch & write_batch);

    // after write batch applied, we can clean the applied locks from `pre_locks_files`
    void cleanAppliedS3ExternalFiles(std::unordered_set<String> && applied_s3files);

    DISALLOW_COPY_AND_MOVE(S3LockLocalManager);


private:
    // return the s3 lock_key
    String createS3Lock(const String & datafile_key, const S3::S3FilenameView & s3_file, UInt64 lock_store_id);

private:
    std::mutex mtx_store_init;
    std::condition_variable cv_init;
    StoreID store_id;
    S3::S3LockClientPtr s3lock_client;
    std::atomic<bool> inited_from_s3 = false;

    std::shared_mutex mtx_sequence;
    UInt64 last_upload_sequence = 0;

    std::mutex mtx_lock_keys;
    std::unordered_set<String> pre_lock_keys;

    LoggerPtr log;
};
} // namespace DB::PS::V3
