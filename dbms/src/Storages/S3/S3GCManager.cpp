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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CommonPrefix.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <common/logger_useful.h>

#include <chrono>
#include <thread>

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::S3
{

S3GCManager::S3GCManager(const String & temp_path_)
    : client(nullptr)
    , temp_path(temp_path_)
    , log(Logger::get())
{
    client = S3::ClientFactory::instance().createWithBucket();
}

bool S3GCManager::runOnAllStores()
{
    const std::vector<UInt64> all_store_ids = getAllStoreIds();
    LOG_TRACE(log, "all_store_ids: {}", all_store_ids);
    for (const auto gc_store_id : all_store_ids)
    {
        runForStore(gc_store_id);
    }
    // always return false, run in fixed rate
    return false;
}

void S3GCManager::runForStore(UInt64 gc_store_id)
{
    LOG_DEBUG(log, "run gc, gc_store_id={}", gc_store_id);
    // Get the latest manifest
    auto manifests = CheckpointManifestS3Set::getFromS3(*client, gc_store_id);
    {
        // clean the outdated manifest files
        removeOutdatedManifest(manifests);
    }

    LOG_INFO(log, "latest manifest, gc_store_id={} upload_seq={} key={}", gc_store_id, manifests.latestUploadSequence(), manifests.latestManifestKey());
    // Parse from the latest manifest and collect valid lock files
    // TODO: collect valid lock files in multiple manifest?
    const std::unordered_set<String> valid_lock_files = getValidLocksFromManifest(manifests.latestManifestKey());
    LOG_INFO(log, "latest manifest, key={} n_locks={}", manifests.latestManifestKey(), valid_lock_files.size());

    // Scan and remove the expired locks
    {
        // All locks share the same prefix
        const auto lock_prefix = S3Filename::getLockPrefix();
        cleanUnusedLocksOnPrefix(gc_store_id, lock_prefix, manifests.latestUploadSequence(), valid_lock_files);
    }

    // After removing the expired lock, we need to scan the data files
    // with expired delmark
    tryCleanExpiredDataFiles(gc_store_id);
}

void S3GCManager::cleanUnusedLocksOnPrefix(
    UInt64 gc_store_id,
    String scan_prefix,
    UInt64 safe_sequence,
    const std::unordered_set<String> & valid_lock_files)
{
    // List the lock files under this prefix
    listPrefix(*client, client->bucket(), scan_prefix, [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        for (const auto & object : objects)
        {
            const auto & lock_key = object.GetKey();
            LOG_TRACE(log, "lock_key={}", lock_key);
            const auto lock_filename_view = S3FilenameView::fromKey(lock_key);
            RUNTIME_CHECK(lock_filename_view.isLockFile(), lock_key);
            const auto lock_info = lock_filename_view.getLockInfo();
            // The lock file is not managed by `gc_store_id`, skip
            if (lock_info.store_id != gc_store_id)
                continue;
            // The lock is not managed by the latest manifest yet, wait for
            // next GC round
            if (lock_info.sequence > safe_sequence)
                continue;
            // The lock is still valid
            if (valid_lock_files.count(lock_key) > 0)
                continue;

            // The data file is not used by `gc_store_id` anymore, remove the lock file
            tryCleanLock(lock_key, lock_filename_view);
        }
        return PageResult{.num_keys = objects.size(), .more = true};
    });
}

void S3GCManager::tryCleanLock(const String & lock_key, const S3FilenameView & lock_filename_view)
{
    const auto unlocked_datafilename_view = lock_filename_view.asDataFile();
    RUNTIME_CHECK(unlocked_datafilename_view.isDataFile());
    const auto unlocked_datafile_key = unlocked_datafilename_view.toFullKey();
    const auto unlocked_datafile_delmark_key = unlocked_datafilename_view.getDelMarkKey();

    // delete S3 lock file
    deleteObject(*client, client->bucket(), lock_key);

    bool delmark_exists = false;
    Aws::Utils::DateTime mtime;
    std::tie(delmark_exists, mtime) = tryGetObjectModifiedTime(*client, client->bucket(), unlocked_datafile_delmark_key);
    if (!delmark_exists)
    {
        // TODO: try create delmark through S3LockService
        uploadEmptyFile(*client, client->bucket(), unlocked_datafile_delmark_key);
        LOG_INFO(log, "creating delmark, key={}", unlocked_datafile_key);
        return;
    }

    assert(delmark_exists); // function should return in previous if-branch
    removeDataFileIfDelmarkExpired(unlocked_datafile_key, unlocked_datafile_delmark_key, mtime);
}

void S3GCManager::removeDataFileIfDelmarkExpired(
    const String & datafile_key,
    const String & delmark_key,
    const Aws::Utils::DateTime & delmark_mtime)
{
    // delmark exist
    bool expired = false;
    {
        // Get the time diff by `now`-`mtime`
        Aws::Utils::DateTime now = Aws::Utils::DateTime::Now();
        auto diff_seconds = Aws::Utils::DateTime::Diff(now, delmark_mtime).count() / 1000.0;
        static constexpr Int64 DELMARK_EXPIRED_HOURS = 1;
        if (diff_seconds > DELMARK_EXPIRED_HOURS * 3600) // TODO: make it configurable
        {
            expired = true;
        }
        LOG_INFO(
            log,
            "delmark exist, datafile={} mark_time={} now={} diff_sec={:.3f} expired={}",
            datafile_key,
            delmark_mtime.ToGmtString(Aws::Utils::DateFormat::ISO_8601),
            now.ToGmtString(Aws::Utils::DateFormat::ISO_8601),
            diff_seconds,
            expired);
    }
    // The delmark is not expired, wait for next GC round
    if (!expired)
        return;
    // The data file is marked as delete and delmark expired, safe to be
    // physical delete.
    deleteObject(*client, client->bucket(), datafile_key); // TODO: it is safe to ignore if not exist
    LOG_INFO(log, "datafile deleted, key={}", datafile_key);
    // TODO: mock crash before deleting delmark on S3
    deleteObject(*client, client->bucket(), delmark_key);
    LOG_INFO(log, "datafile delmark deleted, key={}", delmark_key);
}

void S3GCManager::tryCleanExpiredDataFiles(UInt64 gc_store_id)
{
    // StableFiles and CheckpointDataFile are stored with the same prefix, scan
    // the keys by prefix, and if there is an expired delmark, then try to remove
    // its correspond StableFile or CheckpointDataFile.
    const auto prefix = S3Filename::fromStoreId(gc_store_id).toDataPrefix();
    listPrefix(*client, client->bucket(), prefix, [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        for (const auto & object : objects)
        {
            const auto & delmark_key = object.GetKey();
            LOG_TRACE(log, "key={}", object.GetKey());
            const auto filename_view = S3FilenameView::fromKey(delmark_key);
            // Only remove the data file with expired delmark
            if (!filename_view.isDelMark())
                continue;
            auto datafile_key = filename_view.asDataFile().toFullKey();
            removeDataFileIfDelmarkExpired(datafile_key, delmark_key, object.GetLastModified());
        }
        return PageResult{.num_keys = objects.size(), .more = true};
    });
}

std::vector<UInt64> S3GCManager::getAllStoreIds() const
{
    std::vector<UInt64> all_store_ids;
    // The store key are "s${store_id}/", we need setting delimiter "/" to get the
    // common prefixes result.
    // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
    listPrefix(
        *client,
        client->bucket(),
        /*prefix*/ S3Filename::allStorePrefix(),
        /*delimiter*/ "/",
        [&all_store_ids](const Aws::S3::Model::ListObjectsV2Result & result) {
            const Aws::Vector<Aws::S3::Model::CommonPrefix> & prefixes = result.GetCommonPrefixes();
            for (const auto & prefix : prefixes)
            {
                const auto filename_view = S3FilenameView::fromStoreKeyPrefix(prefix.GetPrefix());
                RUNTIME_CHECK(filename_view.type == S3FilenameType::StorePrefix, prefix.GetPrefix());
                all_store_ids.emplace_back(filename_view.store_id);
            }
            return PageResult{.num_keys = prefixes.size(), .more = true};
        });

    return all_store_ids;
}

std::unordered_set<String> S3GCManager::getValidLocksFromManifest(const String & manifest_key)
{
    // download the latest manifest from S3 to local file
    const String local_manifest_path = getTemporaryDownloadFile(manifest_key);
    downloadFile(*client, client->bucket(), local_manifest_path, manifest_key);
    LOG_INFO(log, "Download manifest, from={} to={}", manifest_key, local_manifest_path);

    // TODO: parse lock from manifest
    // using ManifestReader = PS::V3::CheckpointManifestFileReader<PS::V3::universal::PageDirectoryTrait>;
    // auto reader = ManifestReader::create(ManifestReader::Options{.file_path = local_manifest_path});
    // return reader->readLocks();
    return {};
}

void S3GCManager::removeOutdatedManifest(const CheckpointManifestS3Set & manifests)
{
    // clean the outdated manifest files
    std::chrono::milliseconds expired_ms(1 * 3600 * 1000); // 1 hour
    for (const auto & mf : manifests.objects())
    {
        Aws::Utils::DateTime now = Aws::Utils::DateTime::Now();
        if (auto diff_ms = Aws::Utils::DateTime::Diff(now, mf.second.last_modification);
            diff_ms <= expired_ms)
        {
            continue;
        }
        // expired manifest
        deleteObject(*client, client->bucket(), mf.second.key);
    }
}

String S3GCManager::getTemporaryDownloadFile(String s3_key)
{
    // FIXME: Is there any other logic that download manifest?
    std::replace(s3_key.begin(), s3_key.end(), '/', '_');
    return fmt::format("{}/{}_{}", temp_path, s3_key, std::hash<std::thread::id>()(std::this_thread::get_id()));
}

S3GCManagerService::S3GCManagerService(Context & context, Int64 interval_seconds)
    : global_ctx(context.getGlobalContext())
{
    manager = std::make_unique<S3GCManager>(global_ctx.getTemporaryPath());

    timer = global_ctx.getBackgroundPool().addTask(
        [this]() {
            return manager->runOnAllStores();
        },
        false,
        /*interval_ms*/ interval_seconds * 1000);
}

S3GCManagerService::~S3GCManagerService()
{
    if (timer)
    {
        global_ctx.getBackgroundPool().removeTask(timer);
        timer = nullptr;
    }
}

} // namespace DB::S3
