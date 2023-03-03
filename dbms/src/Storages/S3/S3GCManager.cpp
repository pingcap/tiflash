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
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <Storages/Transaction/Types.h>
#include <TiDB/OwnerManager.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CommonPrefix.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <common/logger_useful.h>
#include <kvproto/metapb.pb.h>
#include <pingcap/pd/IClient.h>

#include <chrono>
#include <limits>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
extern const int TIMEOUT_EXCEEDED;
} // namespace DB::ErrorCodes

namespace DB::S3
{

S3GCManager::S3GCManager(
    pingcap::pd::ClientPtr pd_client_,
    std::shared_ptr<TiFlashS3Client> client_,
    OwnerManagerPtr gc_owner_manager_,
    S3LockClientPtr lock_client_,
    S3GCConfig config_)
    : pd_client(std::move(pd_client_))
    , client(std::move(client_))
    , gc_owner_manager(std::move(gc_owner_manager_))
    , lock_client(std::move(lock_client_))
    , shutdown_called(false)
    , config(config_)
    , log(Logger::get())
{
}

std::unordered_map<StoreID, metapb::Store>
getStoresFromPD(const pingcap::pd::ClientPtr & pd_client)
{
    const auto stores_from_pd = pd_client->getAllStores(false);
    std::unordered_map<StoreID, metapb::Store> stores;
    for (const auto & s : stores_from_pd)
    {
        auto [iter, inserted] = stores.emplace(s.id(), s);
        RUNTIME_CHECK_MSG(inserted, "duplicated store id from pd response, duplicated_store_id={}", iter->first);
    }
    return stores;
}

bool S3GCManager::runOnAllStores()
{
    // Only the GC Manager node run the GC logic
    if (bool is_gc_owner = gc_owner_manager->isOwner(); !is_gc_owner)
    {
        return false;
    }

    const std::vector<UInt64> all_store_ids = getAllStoreIds();
    LOG_TRACE(log, "all_store_ids: {}", all_store_ids);
    // Get all store status from pd after getting the store ids from S3.
    const auto stores_from_pd = getStoresFromPD(pd_client);
    for (const auto gc_store_id : all_store_ids)
    {
        if (shutdown_called)
        {
            LOG_INFO(log, "shutting down, break");
            break;
        }

        std::optional<metapb::StoreState> s = std::nullopt;
        if (auto iter = stores_from_pd.find(gc_store_id); iter != stores_from_pd.end())
        {
            s = iter->second.state();
        }
        if (!s || *s == metapb::StoreState::Tombstone)
        {
            if (!s)
            {
                LOG_INFO(log, "store not found from pd, maybe already removed. gc_store_id={}", gc_store_id);
            }
            runForTombstonedStore(gc_store_id);
        }
        else
        {
            runForStore(gc_store_id);
        }
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

    // Get the latest manifest
    const auto manifests = CheckpointManifestS3Set::getFromS3(*client, gc_store_id);
    if (manifests.empty())
    {
        LOG_INFO(log, "no manifest on this store, skip gc_store_id={}", gc_store_id);
        return;
    }

    LOG_INFO(log, "latest manifest, gc_store_id={} upload_seq={} key={}", gc_store_id, manifests.latestUploadSequence(), manifests.latestManifestKey());
    // Parse from the latest manifest and collect valid lock files
    // TODO: collect valid lock files in multiple manifest?
    std::unordered_set<String> valid_lock_files = getValidLocksFromManifest(manifests.latestManifestKey());
    LOG_INFO(log, "latest manifest, key={} n_locks={}", manifests.latestManifestKey(), valid_lock_files.size());

    // Scan and remove the expired locks
    const auto lock_prefix = S3Filename::getLockPrefix();
    cleanUnusedLocks(gc_store_id, lock_prefix, manifests.latestUploadSequence(), valid_lock_files, gc_timepoint);

    // clean the outdated manifest objects
    removeOutdatedManifest(manifests, &gc_timepoint);

    // After removing the expired lock, we need to scan the data files
    // with expired delmark
    tryCleanExpiredDataFiles(gc_store_id, gc_timepoint);
}

void S3GCManager::runForTombstonedStore(UInt64 gc_store_id)
{
    // get a timepoint at the begin, only remove objects that expired compare
    // to this timepoint
    const Aws::Utils::DateTime gc_timepoint = Aws::Utils::DateTime::Now();
    LOG_DEBUG(log, "run gc, gc_store_id={} timepoint={}", gc_store_id, gc_timepoint.ToGmtString(Aws::Utils::DateFormat::ISO_8601));

    // If the store id is tombstoned, then run gc on the store as if no locks.
    // Scan and remove all expired locks
    LOG_INFO(log, "store is tombstone, clean all locks");
    std::unordered_set<String> valid_lock_files;
    const auto lock_prefix = S3Filename::getLockPrefix();
    cleanUnusedLocks(gc_store_id, lock_prefix, std::numeric_limits<UInt64>::max(), valid_lock_files, gc_timepoint);

    // clean all manifest objects
    const auto manifests = CheckpointManifestS3Set::getFromS3(*client, gc_store_id);
    removeOutdatedManifest(manifests, nullptr);

    // TODO: write a mark file and skip `cleanUnusedLocks` and `removeOutdatedManifest`
    //       in the next round.

    // After all the locks removed, the data files may still being locked by another
    // store id, we need to scan the data files with expired delmark
    tryCleanExpiredDataFiles(gc_store_id, gc_timepoint);
}

void S3GCManager::cleanUnusedLocks(
    UInt64 gc_store_id,
    const String & scan_prefix,
    UInt64 safe_sequence,
    const std::unordered_set<String> & valid_lock_files,
    const Aws::Utils::DateTime & timepoint)
{
    // All locks (even for different stores) share the same prefix, list the lock files under this prefix
    listPrefix(*client, client->bucket(), scan_prefix, [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        if (shutdown_called)
        {
            LOG_INFO(log, "shutting down, break");
            // .more=false to break the list
            return PageResult{.num_keys = objects.size(), .more = false};
        }

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
    physicalRemoveDataFile(datafile_key);

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
        if (shutdown_called)
        {
            LOG_INFO(log, "shutting down, break");
            // .more=false to break the list
            return PageResult{.num_keys = objects.size(), .more = false};
        }

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

void S3GCManager::physicalRemoveDataFile(const String & datafile_key)
{
    auto view = S3FilenameView::fromKey(datafile_key);
    if (!view.isDMFile())
    {
        // CheckpointDataFile is a single object, remove it.
        deleteObject(*client, client->bucket(), datafile_key);
    }
    else
    {
        // DMFile is composed by multiple objects, need extra work to remove all of them.
        // TODO: remove all objects belong to this DMFile
        LOG_WARNING(log, "remove dmfile, key={}", datafile_key);
    }
    LOG_INFO(log, "datafile deleted, key={}", datafile_key);
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

    // parse lock from manifest
    PS::V3::CheckpointProto::StringsInternMap strings_cache;
    using ManifestReader = DB::PS::V3::CPManifestFileReader;
    auto reader = ManifestReader::create(ManifestReader::Options{.file_path = local_manifest_path});
    auto mf_prefix = reader->readPrefix();

    while (true)
    {
        // TODO: calculate the valid size of each CheckpointDataFile in the manifest
        auto part_edit = reader->readEdits(strings_cache);
        if (!part_edit)
            break;
    }

    std::unordered_set<String> locks;
    while (true)
    {
        auto part_locks = reader->readLocks();
        if (!part_locks)
            break;
        locks.merge(part_locks.value());
    }

    return locks;
}

void S3GCManager::removeOutdatedManifest(const CheckpointManifestS3Set & manifests, const Aws::Utils::DateTime * const timepoint)
{
    if (timepoint == nullptr)
    {
        for (const auto & mf : manifests.objects())
        {
            // store tombstoned, remove all manifests
            deleteObject(*client, client->bucket(), mf.second.key);
            LOG_INFO(
                log,
                "remove outdated manifest because of store tombstone, key={} mtime={}",
                mf.second.key,
                mf.second.last_modification.ToGmtString(Aws::Utils::DateFormat::ISO_8601));
        }
        return;
    }

    assert(timepoint != nullptr);
    // clean the outdated manifest files
    const auto outdated_mfs = manifests.outdatedObjects(config.manifest_preserve_count, config.manifest_expired_hour, *timepoint);
    for (const auto & mf : outdated_mfs)
    {
        // expired manifest, remove
        deleteObject(*client, client->bucket(), mf.key);
        LOG_INFO(
            log,
            "remove outdated manifest, key={} mtime={}",
            mf.key,
            mf.last_modification.ToGmtString(Aws::Utils::DateFormat::ISO_8601));
    }
}

String S3GCManager::getTemporaryDownloadFile(String s3_key)
{
    // TODO: Use DataStoreS3 for downloading manifest files
    std::replace(s3_key.begin(), s3_key.end(), '/', '_');
    return fmt::format("{}/{}_{}", config.temp_path, s3_key, std::hash<std::thread::id>()(std::this_thread::get_id()));
}

/// Service ///

S3GCManagerService::S3GCManagerService(
    Context & context,
    pingcap::pd::ClientPtr pd_client,
    OwnerManagerPtr gc_owner_manager_,
    S3LockClientPtr lock_client,
    const S3GCConfig & config)
    : global_ctx(context.getGlobalContext())
{
    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    manager = std::make_unique<S3GCManager>(std::move(pd_client), std::move(s3_client), std::move(gc_owner_manager_), std::move(lock_client), config);

    timer = global_ctx.getBackgroundPool().addTask(
        [this]() {
            return manager->runOnAllStores();
        },
        false,
        /*interval_ms*/ config.interval_seconds * 1000);
}

S3GCManagerService::~S3GCManagerService()
{
    shutdown();
}

void S3GCManagerService::shutdown()
{
    manager->shutdown();
    if (timer)
    {
        global_ctx.getBackgroundPool().removeTask(timer);
        timer = nullptr;
    }
}

} // namespace DB::S3
