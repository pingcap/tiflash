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

#include <Core/Types.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore_fwd.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <common/types.h>

#include <memory>
#include <unordered_set>

namespace pingcap::pd
{
class IClient;
using ClientPtr = std::shared_ptr<IClient>;
} // namespace pingcap::pd
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
class OwnerManager;
using OwnerManagerPtr = std::shared_ptr<OwnerManager>;
} // namespace DB

namespace DB::S3
{
struct S3FilenameView;
class IS3LockClient;
using S3LockClientPtr = std::shared_ptr<IS3LockClient>;

// fwd
class S3GCManagerService;
using S3GCManagerServicePtr = std::unique_ptr<S3GCManagerService>;

struct S3GCConfig
{
    // The interval of the S3 GC routine runs
    Int64 interval_seconds = 600;

    // The maximum number of manifest files preserve
    // for each store
    size_t manifest_preserve_count = 10;
    // Only preserve the manifest that is created
    // recently.
    Int64 manifest_expired_hour = 1;

    S3GCMethod method = S3GCMethod::Lifecycle;

    bool verify_locks = false;

    // Only has meaning when method == ScanThenDelete
    Int64 delmark_expired_hour = 1;

    // The RPC timeout for sending mark delete to S3LockService
    Int64 mark_delete_timeout_seconds = 10;
};

struct GcStats
{
    double duration_read_locks = 0.0;
    double duration_clean_locks = 0.0;
    double duration_clean_manifests = 0.0;
    double duration_verify_locks = 0.0;
    double duration_scan_then_clean_data_files = 0.0;
};

class S3GCManager
{
public:
    explicit S3GCManager(
        pingcap::pd::ClientPtr pd_client_,
        OwnerManagerPtr gc_owner_manager_,
        S3LockClientPtr lock_client_,
        DM::Remote::IDataStorePtr remote_data_store_,
        S3GCConfig config_);

    ~S3GCManager() = default;

    bool runOnAllStores();

    void shutdown() { shutdown_called = true; }

    // private:
    void runForStore(UInt64 gc_store_id);

    void runForTombstoneStore(UInt64 gc_store_id);

    void cleanUnusedLocks(
        UInt64 gc_store_id,
        const String & scan_prefix,
        UInt64 safe_sequence,
        const std::unordered_set<String> & valid_lock_files,
        const Aws::Utils::DateTime &);

    void cleanOneLock(const String & lock_key, const S3FilenameView & lock_filename_view, const Aws::Utils::DateTime &);

    void tryCleanExpiredDataFiles(UInt64 gc_store_id, const Aws::Utils::DateTime &);

    void removeDataFileIfDelmarkExpired(
        const String & datafile_key,
        const String & delmark_key,
        const Aws::Utils::DateTime & timepoint,
        const Aws::Utils::DateTime & delmark_mtime,
        const LoggerPtr & sub_logger) const;

    void lifecycleMarkDataFileDeleted(const String & datafile_key, const LoggerPtr & sub_logger);
    void physicalRemoveDataFile(const String & datafile_key, const LoggerPtr & sub_logger) const;

    void verifyLocks(const std::unordered_set<String> & valid_lock_files);

    static std::vector<UInt64> getAllStoreIds();

    std::unordered_set<String> getValidLocksFromManifest(const Strings & manifest_keys);

    void removeOutdatedManifest(
        const CheckpointManifestS3Set & manifests,
        const Aws::Utils::DateTime * const timepoint); // NOLINT(readability-avoid-const-params-in-decls)

private:
    const pingcap::pd::ClientPtr pd_client;

    const OwnerManagerPtr gc_owner_manager;
    const S3LockClientPtr lock_client;

    DM::Remote::IDataStorePtr remote_data_store;

    std::atomic<bool> shutdown_called;

    bool lifecycle_has_been_set = false;
    S3GCConfig config;

    LoggerPtr log;
};

class S3GCManagerService
{
public:
    explicit S3GCManagerService(
        Context & context,
        pingcap::pd::ClientPtr pd_client,
        OwnerManagerPtr gc_owner_manager_,
        S3LockClientPtr lock_client,
        const S3GCConfig & config);

    ~S3GCManagerService();

    void shutdown();

    void wake() const;

private:
    Context & global_ctx;
    std::unique_ptr<S3GCManager> manager;
    BackgroundProcessingPool::TaskHandle timer;
};

} // namespace DB::S3

template <>
struct fmt::formatter<DB::S3::GcStats>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::S3::GcStats & stat, FormatContext & ctx) const
    {
        return fmt::format_to(
            ctx.out(),
            "{{read_locks={:.2f}s clean_locks={:.2f}s clean_manifests={:.2f}s verify_locks={:.2f}s "
            "scan_then_clean_data_files={:.2f}s}}",
            stat.duration_read_locks,
            stat.duration_clean_locks,
            stat.duration_clean_manifests,
            stat.duration_verify_locks,
            stat.duration_scan_then_clean_data_files);
    }
};
