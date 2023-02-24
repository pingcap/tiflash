// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Disaggregated/S3LockClient.h>
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
extern const int TIMEOUT_EXCEEDED;
} // namespace DB::ErrorCodes

namespace DB::S3
{

S3GCManager::S3GCManager(std::shared_ptr<TiFlashS3Client> client_, S3LockClientPtr lock_client_, S3GCConfig config_)
    : client(std::move(client_))
    , lock_client(std::move(lock_client_))
    , config(config_)
    , log(Logger::get())
{
}

bool S3GCManager::runOnAllStores()
{
    const std::vector<UInt64> all_store_ids = getAllStoreIds();
    LOG_TRACE(log, "all_store_ids: {}", all_store_ids);
    // TODO: Get all store status from pd after getting the store ids from S3.
    for (const auto gc_store_id : all_store_ids)
    {
        runForStore(gc_store_id);
    }
    // always return false, run in fixed rate
    return false;
}

void S3GCManager::runForStore(UInt64 gc_store_id)
{
    // get a timepoint at the begin, only remove objects that expired compare
    // to this timepoint
    const Aws::Utils::DateTime gc_timepoint = Aws::Utils::DateTime::Now();
    LOG_DEBUG(log, "run gc, gc_store_id={} timepoint={}", gc_store_id, gc_timepoint.ToGmtString(Aws::Utils::DateFormat::ISO_8601));

    // TODO: Get store ids from pd. If the store id is tombstoned,
    //       then run gc on the store as if no locks.

    // Get the latest manifest
    const auto manifests = CheckpointManifestS3Set::getFromS3(*client, gc_store_id);
    // clean the outdated manifest files
    removeOutdatedManifest(manifests, gc_timepoint);

    LOG_INFO(log, "latest manifest, gc_store_id={} upload_seq={} key={}", gc_store_id, manifests.latestUploadSequence(), manifests.latestManifestKey());
    // Parse from the latest manifest and collect valid lock files
    // TODO: collect valid lock files in multiple manifest?
    const std::unordered_set<String> valid_lock_files = getValidLocksFromManifest(manifests.latestManifestKey());
    LOG_INFO(log, "latest manifest, key={} n_locks={}", manifests.latestManifestKey(), valid_lock_files.size());

    // Scan and remove the expired locks
    const auto lock_prefix = S3Filename::getLockPrefix();
    cleanUnusedLocks(gc_store_id, lock_prefix, manifests.latestUploadSequence(), valid_lock_files, gc_timepoint);

    // After removing the expired lock, we need to scan the data files
    // with expired delmark
    tryCleanExpiredDataFiles(gc_store_id, gc_timepoint);
}

void S3GCManager::cleanUnusedLocks(
    UInt64 gc_store_id,
    String scan_prefix,
    UInt64 safe_sequence,
    const std::unordered_set<String> & valid_lock_files,
    const Aws::Utils::DateTime & timepoint)
{
    // All locks (even for different stores) share the same prefix, list the lock files under this prefix
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
            cleanOneLock(lock_key, lock_filename_view, timepoint);
        }
        return PageResult{.num_keys = objects.size(), .more = true};
    });
}

void S3GCManager::cleanOneLock(const String & lock_key, const S3FilenameView & lock_filename_view, const Aws::Utils::DateTime & timepoint)
{
    const auto unlocked_datafilename_view = lock_filename_view.asDataFile();
    RUNTIME_CHECK(unlocked_datafilename_view.isDataFile());
    const auto unlocked_datafile_key = unlocked_datafilename_view.toFullKey();
    const auto unlocked_datafile_delmark_key = unlocked_datafilename_view.getDelMarkKey();

    // delete S3 lock file
    deleteObject(*client, client->bucket(), lock_key);

    // TODO: If `lock_key` is the only lock to datafile and GCManager crashs
    //       after the lock deleted but before delmark uploaded, then the
    //       datafile is not able to be cleaned.
    //       Need another logic to cover this corner case.

    bool delmark_exists = false;
    Aws::Utils::DateTime mtime;
    std::tie(delmark_exists, mtime) = tryGetObjectModifiedTime(*client, client->bucket(), unlocked_datafile_delmark_key);
    if (!delmark_exists)
    {
        bool ok;
        String err_msg;
        try
        {
            // delmark not exist, lets try create a delmark through S3LockService
            std::tie(ok, err_msg) = lock_client->sendTryMarkDeleteRequest(unlocked_datafile_key, config.mark_delete_timeout_seconds);
        }
        catch (DB::Exception & e)
        {
            if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
            {
                ok = false;
                err_msg = e.message();
            }
            else
            {
                e.rethrow();
            }
        }
        if (ok)
        {
            LOG_INFO(log, "delmark created, key={}", unlocked_datafile_key);
        }
        else
        {
            LOG_INFO(log, "delmark create failed, key={} reason={}", unlocked_datafile_key, err_msg);
        }
        // no matter delmark create success or not, leave it to later GC round.
        return;
    }

    assert(delmark_exists); // function should return in previous if-branch
    removeDataFileIfDelmarkExpired(unlocked_datafile_key, unlocked_datafile_delmark_key, timepoint, mtime);
}

void S3GCManager::removeDataFileIfDelmarkExpired(
    const String & datafile_key,
    const String & delmark_key,
    const Aws::Utils::DateTime & timepoint,
    const Aws::Utils::DateTime & delmark_mtime)
{
    // delmark exist
    bool expired = false;
    {
        // Get the time diff by `timepoint`-`mtime`
        auto diff_seconds = Aws::Utils::DateTime::Diff(timepoint, delmark_mtime).count() / 1000.0;
        if (diff_seconds > config.delmark_expired_hour * 3600)
        {
            expired = true;
        }
        LOG_INFO(
            log,
            "delmark exist, datafile={} mark_mtime={} now={} diff_sec={:.3f} expired={}",
            datafile_key,
            delmark_mtime.ToGmtString(Aws::Utils::DateFormat::ISO_8601),
            timepoint.ToGmtString(Aws::Utils::DateFormat::ISO_8601),
            diff_seconds,
            expired);
    }
    // The delmark is not expired, wait for next GC round
    if (!expired)
        return;

    // The data file is marked as delete and delmark expired, safe to be
    // physical delete.
    // It is safe to ignore if datafile_key not exist and S3 won't report
    // error when the key is not exist
    deleteObject(*client, client->bucket(), datafile_key);
    LOG_INFO(log, "datafile deleted, key={}", datafile_key);

    deleteObject(*client, client->bucket(), delmark_key);
    LOG_INFO(log, "datafile delmark deleted, key={}", delmark_key);
}

void S3GCManager::tryCleanExpiredDataFiles(UInt64 gc_store_id, const Aws::Utils::DateTime & timepoint)
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
            removeDataFileIfDelmarkExpired(datafile_key, delmark_key, timepoint, object.GetLastModified());
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

void S3GCManager::removeOutdatedManifest(const CheckpointManifestS3Set & manifests, const Aws::Utils::DateTime & timepoint)
{
    // clean the outdated manifest files
    auto expired_bound_sec = config.manifest_expired_hour * 3600;
    for (const auto & mf : manifests.objects())
    {
        auto diff_sec = Aws::Utils::DateTime::Diff(timepoint, mf.second.last_modification).count() / 1000.0;
        if (diff_sec <= expired_bound_sec)
        {
            continue;
        }
        // expired manifest, remove
        deleteObject(*client, client->bucket(), mf.second.key);
        LOG_INFO(
            log,
            "remove outdated manifest, key={} mtime={} diff_sec={:.3f}",
            mf.second.key,
            mf.second.last_modification.ToGmtString(Aws::Utils::DateFormat::ISO_8601),
            diff_sec);
    }
}

String S3GCManager::getTemporaryDownloadFile(String s3_key)
{
    // FIXME: Is there any other logic that download manifest?
    std::replace(s3_key.begin(), s3_key.end(), '/', '_');
    return fmt::format("{}/{}_{}", config.temp_path, s3_key, std::hash<std::thread::id>()(std::this_thread::get_id()));
}

S3GCManagerService::S3GCManagerService(
    Context & context,
    S3LockClientPtr lock_client,
    const S3GCConfig & config)
    : global_ctx(context.getGlobalContext())
{
    auto s3_client = S3::ClientFactory::instance().createWithBucket();
    manager = std::make_unique<S3GCManager>(std::move(s3_client), std::move(lock_client), config);

    timer = global_ctx.getBackgroundPool().addTask(
        [this]() {
            return manager->runOnAllStores();
        },
        false,
        /*interval_ms*/ config.interval_seconds * 1000);
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
