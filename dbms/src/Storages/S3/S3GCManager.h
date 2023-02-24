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

class S3GCManager
{
public:
    explicit S3GCManager(const String & temp_path_);

    bool runOnAllStores();

private:
    void runForStore(UInt64 gc_store_id);

    void cleanUnusedLocksOnPrefix(
        UInt64 gc_store_id,
        String scan_prefix,
        UInt64 safe_sequence,
        const std::unordered_set<String> & valid_lock_files);

    void tryCleanLock(const String & lock_key, const S3FilenameView & lock_filename_view);

    void tryCleanExpiredDataFiles(UInt64 gc_store_id);

    void removeDataFileIfDelmarkExpired(
        const String & datafile_key,
        const String & delmark_key,
        const Aws::Utils::DateTime & delmark_mtime);

    std::vector<UInt64> getAllStoreIds() const;

    std::unordered_set<String> getValidLocksFromManifest(const String & manifest_key);

    void removeOutdatedManifest(const CheckpointManifestS3Set & manifests);

    String getTemporaryDownloadFile(String s3_key);

private:
    std::shared_ptr<TiFlashS3Client> client;

    // The temporary path for storing
    // downloaded manifest
    String temp_path;

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
