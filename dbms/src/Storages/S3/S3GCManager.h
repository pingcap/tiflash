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

#include <Core/Types.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <common/types.h>

#include <memory>
#include <unordered_set>

namespace Aws
{
namespace S3
{
class S3Client;
} // namespace S3
namespace Utils
{
class DateTime;
} // namespace Utils
} // namespace Aws

namespace DB
{
class Context;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB

namespace DB::S3
{
struct S3FilenameView;
class IS3LockClient;
using S3LockClientPtr = std::shared_ptr<IS3LockClient>;

struct S3GCConfig
{
    Int64 manifest_expired_hour = 1;
    Int64 delmark_expired_hour = 1;

    Int64 mark_delete_timeout_seconds = 10;

    // The temporary path for storing
    // downloaded manifest
    String temp_path;
};

class S3GCManager
{
public:
    explicit S3GCManager(std::shared_ptr<TiFlashS3Client> client_, S3LockClientPtr lock_client_, S3GCConfig config_);

    bool runOnAllStores();

// private:
    void runForStore(UInt64 gc_store_id);

    void cleanUnusedLocks(
        UInt64 gc_store_id,
        String scan_prefix,
        UInt64 safe_sequence,
        const std::unordered_set<String> & valid_lock_files,
        const Aws::Utils::DateTime &);

    void cleanOneLock(const String & lock_key, const S3FilenameView & lock_filename_view, const Aws::Utils::DateTime &);

    void tryCleanExpiredDataFiles(UInt64 gc_store_id, const Aws::Utils::DateTime &);

    void removeDataFileIfDelmarkExpired(
        const String & datafile_key,
        const String & delmark_key,
        const Aws::Utils::DateTime & timepoint,
        const Aws::Utils::DateTime & delmark_mtime);

    std::vector<UInt64> getAllStoreIds() const;

    std::unordered_set<String> getValidLocksFromManifest(const String & manifest_key);

    void removeOutdatedManifest(const CheckpointManifestS3Set & manifests, const Aws::Utils::DateTime &);

    String getTemporaryDownloadFile(String s3_key);

private:
    const std::shared_ptr<TiFlashS3Client> client;

    const S3LockClientPtr lock_client;

    S3GCConfig config;

    LoggerPtr log;
};

class S3GCManagerService
{
public:
    explicit S3GCManagerService(Context & context, Int64 interval_seconds);
    ~S3GCManagerService();

private:
    Context & global_ctx;
    std::unique_ptr<S3GCManager> manager;
    BackgroundProcessingPool::TaskHandle timer;
};
using S3GCManagerServicePtr = std::unique_ptr<S3GCManagerService>;

} // namespace DB::S3
