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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Core/Types.h>
#include <Flash/Disaggregated/S3LockClient.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <TiDB/OwnerManager.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CommonPrefix.h>
#include <common/logger_useful.h>
#include <kvproto/metapb.pb.h>
#include <pingcap/pd/IClient.h>

#include <chrono>
#include <ext/scope_guard.h>
#include <limits>
#include <magic_enum.hpp>
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
    OwnerManagerPtr gc_owner_manager_,
    S3LockClientPtr lock_client_,
    DM::Remote::IDataStorePtr remote_data_store_,
    S3GCConfig config_)
    : pd_client(std::move(pd_client_))
    , gc_owner_manager(std::move(gc_owner_manager_))
    , lock_client(std::move(lock_client_))
    , remote_data_store(std::move(remote_data_store_))
    , shutdown_called(false)
    , config(config_)
    , log(Logger::get())
{}

std::unordered_map<StoreID, metapb::Store> getStoresFromPD(const pingcap::pd::ClientPtr & pd_client)
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

    GET_METRIC(tiflash_storage_s3_gc_status, type_running).Set(1.0);
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_s3_gc_status, type_running).Set(0.0); });

    if (config.method == S3GCMethod::Lifecycle && !lifecycle_has_been_set)
    {
        auto client = S3::ClientFactory::instance().sharedTiFlashClient();
        lifecycle_has_been_set = ensureLifecycleRuleExist(*client, /*expire_days*/ 1);
        if (lifecycle_has_been_set)
        {
            GET_METRIC(tiflash_storage_s3_gc_status, type_lifecycle_added).Set(1.0);
            GET_METRIC(tiflash_storage_s3_gc_status, type_lifecycle_failed).Set(0.0);
        }
        else
        {
            GET_METRIC(tiflash_storage_s3_gc_status, type_lifecycle_added).Set(0.0);
            GET_METRIC(tiflash_storage_s3_gc_status, type_lifecycle_failed).Set(1.0);
        }
    }

    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_s3_gc_seconds, type_total).Observe(watch.elapsedSeconds()); });
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
        if (bool is_gc_owner = gc_owner_manager->isOwner(); !is_gc_owner)
        {
            LOG_INFO(log, "GC owner changed, break");
            break;
        }

        Stopwatch store_gc_watch;
        SCOPE_EXIT(
            { GET_METRIC(tiflash_storage_s3_gc_seconds, type_one_store).Observe(store_gc_watch.elapsedSeconds()); });
        std::optional<metapb::StoreState> s = std::nullopt;
        if (auto iter = stores_from_pd.find(gc_store_id); iter != stores_from_pd.end())
        {
            s = iter->second.state();
        }
        try
        {
            if (!s || *s == metapb::StoreState::Tombstone)
            {
                if (!s)
                {
                    LOG_INFO(log, "store not found from pd, maybe already removed. gc_store_id={}", gc_store_id);
                }
                runForTombstoneStore(gc_store_id);
            }
            else
            {
                runForStore(gc_store_id);
            }
        }
        catch (...)
        {
            // log error and continue on next store_id
            tryLogCurrentException(log, fmt::format("gc_store_id={}", gc_store_id));
        }
    }
    // always return false, run in fixed rate
    return false;
}

void S3GCManager::runForStore(UInt64 gc_store_id)
{
    // get a timepoint at the begin, only remove objects that expired compare
    // to this timepoint
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    const Aws::Utils::DateTime gc_timepoint = Aws::Utils::DateTime::Now();
    LOG_DEBUG(
        log,
        "run gc, gc_store_id={} timepoint={}",
        gc_store_id,
        gc_timepoint.ToGmtString(Aws::Utils::DateFormat::ISO_8601));

    Stopwatch watch;
    // Get the latest manifest
    const auto manifests = CheckpointManifestS3Set::getFromS3(*client, gc_store_id);
    if (manifests.empty())
    {
        LOG_INFO(log, "no manifest on this store, skip gc_store_id={}", gc_store_id);
        return;
    }

    LOG_INFO(
        log,
        "latest manifest, gc_store_id={} upload_seq={} key={}",
        gc_store_id,
        manifests.latestUploadSequence(),
        manifests.latestManifestKey());
    // Parse from the latest manifest and collect valid lock files
    // collect valid lock files from preserved manifests
    std::unordered_set<String> valid_lock_files = getValidLocksFromManifest(
        manifests.preservedManifests(config.manifest_preserve_count, config.manifest_expired_hour, gc_timepoint));
    LOG_INFO(log, "latest manifest, key={} n_locks={}", manifests.latestManifestKey(), valid_lock_files.size());
    GET_METRIC(tiflash_storage_s3_gc_seconds, type_read_locks)
        .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);

    // Scan and remove the expired locks
    const auto lock_prefix = S3Filename::getLockPrefix();
    cleanUnusedLocks(gc_store_id, lock_prefix, manifests.latestUploadSequence(), valid_lock_files, gc_timepoint);
    GET_METRIC(tiflash_storage_s3_gc_seconds, type_clean_locks)
        .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);

    // clean the outdated manifest objects
    removeOutdatedManifest(manifests, &gc_timepoint);
    GET_METRIC(tiflash_storage_s3_gc_seconds, type_clean_manifests)
        .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);

    if (config.verify_locks)
    {
        verifyLocks(valid_lock_files);
    }

    switch (config.method)
    {
    case S3GCMethod::Lifecycle:
    {
        // nothing need to be done, the expired files will be deleted by S3-like
        // system's lifecycle management
        break;
    }
    case S3GCMethod::ScanThenDelete:
    {
        // After removing the expired lock, we need to scan the data files
        // with expired delmark
        tryCleanExpiredDataFiles(gc_store_id, gc_timepoint);
        GET_METRIC(tiflash_storage_s3_gc_seconds, type_scan_then_clean_data_files)
            .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);
        break;
    }
    }
    LOG_INFO(log, "gc on store done, gc_store_id={}", gc_store_id);
}

void S3GCManager::runForTombstoneStore(UInt64 gc_store_id)
{
    // get a timepoint at the begin, only remove objects that expired compare
    // to this timepoint
    const Aws::Utils::DateTime gc_timepoint = Aws::Utils::DateTime::Now();
    LOG_DEBUG(
        log,
        "run gc, gc_store_id={} timepoint={} tombstone=true",
        gc_store_id,
        gc_timepoint.ToGmtString(Aws::Utils::DateFormat::ISO_8601));

    Stopwatch watch;
    // If the store id is tombstone, then run gc on the store as if no locks.
    // Scan and remove all expired locks
    LOG_INFO(log, "store is tombstone, clean all locks");
    const auto lock_prefix = S3Filename::getLockPrefix();
    // clean all by setting `safe_sequence` to MaxUInt64 and empty `valid_lock_files`
    std::unordered_set<String> valid_lock_files;
    cleanUnusedLocks(gc_store_id, lock_prefix, std::numeric_limits<UInt64>::max(), valid_lock_files, gc_timepoint);
    GET_METRIC(tiflash_storage_s3_gc_seconds, type_clean_locks)
        .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);

    // clean all manifest objects
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    const auto manifests = CheckpointManifestS3Set::getFromS3(*client, gc_store_id);
    removeOutdatedManifest(manifests, nullptr);
    GET_METRIC(tiflash_storage_s3_gc_seconds, type_clean_manifests)
        .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);

    switch (config.method)
    {
    case S3GCMethod::Lifecycle:
    {
        // nothing need to be done, the expired files will be deleted by S3-like
        // system's lifecycle management
        break;
    }
    case S3GCMethod::ScanThenDelete:
    {
        // TODO: write a mark file and skip `cleanUnusedLocks` and `removeOutdatedManifest`
        //       in the next round to reduce S3 LIST calls.

        // After all the locks removed, the data files may still being locked by another
        // store id, we need to scan the data files with expired delmark
        tryCleanExpiredDataFiles(gc_store_id, gc_timepoint);
        GET_METRIC(tiflash_storage_s3_gc_seconds, type_scan_then_clean_data_files)
            .Observe(watch.elapsedMillisecondsFromLastTime() / 1000.0);
        break;
    }
    }
    LOG_INFO(log, "gc on store done, gc_store_id={} tombstone=true", gc_store_id);
}

void S3GCManager::cleanUnusedLocks(
    UInt64 gc_store_id,
    const String & scan_prefix,
    UInt64 safe_sequence,
    const std::unordered_set<String> & valid_lock_files,
    const Aws::Utils::DateTime & timepoint)
{
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    // All locks (even for different stores) share the same prefix, list the lock files under this prefix
    S3::listPrefix(*client, scan_prefix, [&](const Aws::S3::Model::Object & object) {
        if (shutdown_called)
        {
            LOG_INFO(log, "shutting down, break");
            // .more=false to break the list
            return PageResult{.num_keys = 1, .more = false};
        }

        do
        {
            const auto & lock_key = object.GetKey();
            LOG_TRACE(log, "lock_key={}", lock_key);
            const auto lock_filename_view = S3FilenameView::fromKey(lock_key);
            RUNTIME_CHECK(lock_filename_view.isLockFile(), lock_key);
            const auto lock_info = lock_filename_view.getLockInfo();
            // The lock file is not managed by `gc_store_id`, skip
            if (lock_info.store_id != gc_store_id)
                break;
            // The lock is not managed by the latest manifest yet, wait for
            // next GC round
            if (lock_info.sequence > safe_sequence)
                break;
            // The lock is still valid
            if (valid_lock_files.count(lock_key) > 0)
                break;

            // The data file is not used by `gc_store_id` anymore, remove the lock file
            cleanOneLock(lock_key, lock_filename_view, timepoint);
        } while (false);
        return PageResult{.num_keys = 1, .more = true};
    });
}

void S3GCManager::cleanOneLock(
    const String & lock_key,
    const S3FilenameView & lock_filename_view,
    const Aws::Utils::DateTime & timepoint)
{
    Stopwatch watch;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_s3_gc_seconds, type_clean_one_lock).Observe(watch.elapsedSeconds()); });
    const auto unlocked_datafilename_view = lock_filename_view.asDataFile();
    RUNTIME_CHECK(unlocked_datafilename_view.isDataFile());
    const auto unlocked_datafile_key = unlocked_datafilename_view.toFullKey();
    const auto unlocked_datafile_delmark_key = unlocked_datafilename_view.getDelMarkKey();
    auto sub_logger = log->getChild(fmt::format("remove_key={}", unlocked_datafile_key));

    // delete S3 lock file
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    deleteObject(*client, lock_key);
    const auto elapsed_remove_lock = watch.elapsedMillisecondsFromLastTime() / 1000.0;

    // TODO: If `lock_key` is the only lock to datafile and GCManager crashes
    //       after the lock deleted but before delmark uploaded, then the
    //       datafile is not able to be cleaned.
    //       Need another logic to cover this corner case.

    const auto delmark_object_info = S3::tryGetObjectInfo(*client, unlocked_datafile_delmark_key);
    const auto elapsed_try_get_delmark = watch.elapsedMillisecondsFromLastTime() / 1000.0;
    double elapsed_try_mark_delete = 0.0;
    double elapsed_lifecycle_mark_delete = 0.0;
    if (!delmark_object_info.exist)
    {
        bool ok;
        String err_msg;
        try
        {
            // delmark not exist, lets try create a delmark through S3LockService
            std::tie(ok, err_msg)
                = lock_client->sendTryMarkDeleteRequest(unlocked_datafile_key, config.mark_delete_timeout_seconds);
            elapsed_try_mark_delete = watch.elapsedMillisecondsFromLastTime() / 1000.0;
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
            LOG_INFO(sub_logger, "delmark created, key={}", unlocked_datafile_key);
            switch (config.method)
            {
            case S3GCMethod::Lifecycle:
            {
                // Note that the min time of lifecycle check is 1 day. There is some
                // time gap between delmark's lifecycle and its datafile lifecycle.
                // Or S3GCManage could crash between delmark created and rewriting
                // the datafile.
                // However, After the lock key is not seen in the manifest file after
                // 1 day, we consider it is long enough for no other write node try
                // access to the data file.
                lifecycleMarkDataFileDeleted(unlocked_datafile_key, sub_logger);
                elapsed_lifecycle_mark_delete = watch.elapsedMillisecondsFromLastTime() / 1000.0;
                LOG_INFO(
                    sub_logger,
                    "cleanOneLock done, method={} key={} remove_lock={:.3f} get_delmark={:.3f} mark_delete={:.3f} "
                    "lifecycle_mark_delete={:.3f}",
                    magic_enum::enum_name(config.method),
                    unlocked_datafile_key,
                    elapsed_remove_lock,
                    elapsed_try_get_delmark,
                    elapsed_try_mark_delete,
                    elapsed_lifecycle_mark_delete);
                return;
            }
            case S3GCMethod::ScanThenDelete:
                break;
            }
        }
        else
        {
            LOG_INFO(
                sub_logger,
                "delmark create failed, method={} key={} reason={} remove_lock={:.3f} get_delmark={:.3f} "
                "mark_delete={:.3f} lifecycle_mark_delete={:.3f}",
                magic_enum::enum_name(config.method),
                unlocked_datafile_key,
                err_msg,
                elapsed_remove_lock,
                elapsed_try_get_delmark,
                elapsed_try_mark_delete,
                elapsed_lifecycle_mark_delete);
        }
        // no matter delmark create success or not, leave it to later GC round.
        return;
    }

    assert(delmark_object_info.exist); // function should return in previous if-branch
    switch (config.method)
    {
    case S3GCMethod::Lifecycle:
    {
        return;
    }
    case S3GCMethod::ScanThenDelete:
    {
        const auto elapsed_scan_try_remove_datafile = watch.elapsedMillisecondsFromLastTime() / 1000.0;
        // delmark exist, check whether we need to physical remove the datafile
        removeDataFileIfDelmarkExpired(
            unlocked_datafile_key,
            unlocked_datafile_delmark_key,
            timepoint,
            delmark_object_info.last_modification_time,
            sub_logger);
        LOG_INFO(
            sub_logger,
            "cleanOneLock done, method={} key={} remove_lock={:.3f} get_delmark={:.3f} mark_delete={:.3f} "
            "scan_try_physical_remove={:.3f}",
            magic_enum::enum_name(config.method),
            unlocked_datafile_key,
            elapsed_remove_lock,
            elapsed_try_get_delmark,
            elapsed_try_mark_delete,
            elapsed_scan_try_remove_datafile);
        return;
    }
    }
}

void S3GCManager::removeDataFileIfDelmarkExpired(
    const String & datafile_key,
    const String & delmark_key,
    const Aws::Utils::DateTime & timepoint,
    const Aws::Utils::DateTime & delmark_mtime,
    const LoggerPtr & sub_logger) const
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
            sub_logger,
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
    physicalRemoveDataFile(datafile_key, sub_logger);

    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    deleteObject(*client, delmark_key);
    LOG_INFO(sub_logger, "datafile delmark deleted, key={}", delmark_key);
}

void S3GCManager::tryCleanExpiredDataFiles(UInt64 gc_store_id, const Aws::Utils::DateTime & timepoint)
{
    // StableFiles and CheckpointDataFile are stored with the same prefix, scan
    // the keys by prefix, and if there is an expired delmark, then try to remove
    // its correspond StableFile or CheckpointDataFile.
    const auto prefix = S3Filename::fromStoreId(gc_store_id).toDataPrefix();
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    S3::listPrefix(*client, prefix, [&](const Aws::S3::Model::Object & object) {
        if (shutdown_called)
        {
            LOG_INFO(log, "shutting down, break");
            // .more=false to break the list
            return PageResult{.num_keys = 1, .more = false};
        }

        do
        {
            const auto & delmark_key = object.GetKey();
            LOG_TRACE(log, "key={}", object.GetKey());
            const auto filename_view = S3FilenameView::fromKey(delmark_key);
            // Only remove the data file with expired delmark
            if (!filename_view.isDelMark())
                break;
            const auto datafile_key = filename_view.asDataFile().toFullKey();
            auto sub_logger = log->getChild(fmt::format("remove_key={}", datafile_key));
            removeDataFileIfDelmarkExpired(datafile_key, delmark_key, timepoint, object.GetLastModified(), sub_logger);
        } while (false);
        return PageResult{.num_keys = 1, .more = true};
    });
}

void S3GCManager::lifecycleMarkDataFileDeleted(const String & datafile_key, const LoggerPtr & sub_logger)
{
    assert(config.method == S3GCMethod::Lifecycle);

    auto view = S3FilenameView::fromKey(datafile_key);
    RUNTIME_CHECK(view.isDataFile(), magic_enum::enum_name(view.type), datafile_key);
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    if (!view.isDMFile())
    {
        // CheckpointDataFile is a single object, add tagging for it and update its mtime
        rewriteObjectWithTagging(*client, datafile_key, String(TaggingObjectIsDeleted));
        LOG_INFO(sub_logger, "datafile deleted by lifecycle tagging");
    }
    else
    {
        // DMFile is composed by multiple objects, need extra work to remove all of them.
        // Rewrite all objects with tagging belong to this DMFile. Note "/" is need for
        // scanning only the sub objects of given key of this DMFile
        // TODO: If GCManager unexpectedly exit in the middle, it will leave some broken
        //       sub file for DMFile, try clean them later.
        std::vector<String> sub_keys;
        S3::listPrefix(*client, datafile_key + "/", [&sub_keys](const Aws::S3::Model::Object & object) {
            sub_keys.emplace_back(object.GetKey());
            return PageResult{.num_keys = 1, .more = true};
        });
        // set tagging for all subkeys in parallel
        remote_data_store->setTaggingsForKeys(sub_keys, TaggingObjectIsDeleted);
        for (const auto & sub_key : sub_keys)
        {
            LOG_INFO(sub_logger, "datafile deleted by lifecycle tagging, sub_key={}", sub_key);
        }
        LOG_INFO(
            sub_logger,
            "datafile deleted by lifecycle tagging, all sub keys are deleted, n_sub_keys={}",
            sub_keys.size());
    }
}

void S3GCManager::physicalRemoveDataFile(const String & datafile_key, const LoggerPtr & sub_logger) const
{
    assert(config.method == S3GCMethod::ScanThenDelete);

    auto view = S3FilenameView::fromKey(datafile_key);
    RUNTIME_CHECK(view.isDataFile(), magic_enum::enum_name(view.type), datafile_key);
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    if (!view.isDMFile())
    {
        // CheckpointDataFile is a single object, remove it.
        deleteObject(*client, datafile_key);
        LOG_INFO(sub_logger, "datafile deleted");
    }
    else
    {
        // DMFile is composed by multiple objects, need extra work to remove all of them.
        // Remove all objects belong to this DMFile. Note suffix "/" is need for scanning
        // only the sub objects of given key of this DMFile.
        // TODO: If GCManager unexpectedly exit in the middle, it will leave some broken
        //       sub file for DMFile, try clean them later.
        std::vector<String> sub_keys;
        S3::listPrefix(*client, datafile_key + "/", [&client, &sub_logger](const Aws::S3::Model::Object & object) {
            const auto & sub_key = object.GetKey();
            deleteObject(*client, sub_key);
            LOG_INFO(sub_logger, "datafile deleted, sub_key={}", sub_key);
            return PageResult{.num_keys = 1, .more = true};
        });
        LOG_INFO(sub_logger, "datafile deleted, all sub keys are deleted");
    }
}

void S3GCManager::verifyLocks(const std::unordered_set<String> & valid_lock_files)
{
    for (const auto & lock_key : valid_lock_files)
    {
        const auto lock_view = S3FilenameView::fromKey(lock_key);
        RUNTIME_CHECK(lock_view.isLockFile(), lock_key, magic_enum::enum_name(lock_view.type));
        const auto data_file_view = lock_view.asDataFile();
        auto client = S3::ClientFactory::instance().sharedTiFlashClient();
        String data_file_check_key;
        if (!data_file_view.isDMFile())
        {
            data_file_check_key = data_file_view.toFullKey();
        }
        else
        {
            data_file_check_key = data_file_view.toFullKey() + "/meta";
        }
        LOG_INFO(log, "Checking consistency, lock_key={} data_file_key={}", lock_key, data_file_check_key);
        auto object_info = S3::tryGetObjectInfo(*client, data_file_check_key);
        RUNTIME_ASSERT(
            object_info.exist,
            log,
            "S3 file has already been removed! lock_key={} data_file={}",
            lock_key,
            data_file_check_key);
    }
    LOG_INFO(log, "All valid lock consistency check passed, num_locks={}", valid_lock_files.size());
}

std::vector<UInt64> S3GCManager::getAllStoreIds()
{
    std::vector<UInt64> all_store_ids;
    // The store key are "s${store_id}/", we need setting delimiter "/" to get the
    // common prefixes result.
    // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    S3::listPrefixWithDelimiter(
        *client,
        /*prefix*/ S3Filename::allStorePrefix(),
        /*delimiter*/ "/",
        [&all_store_ids](const Aws::S3::Model::CommonPrefix & prefix) {
            const auto filename_view = S3FilenameView::fromStoreKeyPrefix(prefix.GetPrefix());
            RUNTIME_CHECK(filename_view.type == S3FilenameType::StorePrefix, prefix.GetPrefix());
            all_store_ids.emplace_back(filename_view.store_id);
            return PageResult{.num_keys = 1, .more = true};
        });

    return all_store_ids;
}

std::unordered_set<String> S3GCManager::getValidLocksFromManifest(const Strings & manifest_keys)
{
    // parse lock from manifest
    std::unordered_set<String> locks;
    PS::V3::CheckpointProto::StringsInternMap strings_cache;
    using ManifestReader = DB::PS::V3::CPManifestFileReader;
    for (const auto & manifest_key : manifest_keys)
    {
        LOG_INFO(log, "Reading manifest, key={}", manifest_key);
        auto manifest_file = S3RandomAccessFile::create(manifest_key);
        auto reader = ManifestReader::create(ManifestReader::Options{.plain_file = manifest_file});
        auto mf_prefix = reader->readPrefix();

        while (true)
        {
            // TODO: calculate the valid size of each CheckpointDataFile in the manifest
            auto part_edit = reader->readEdits(strings_cache);
            if (!part_edit)
                break;
        }

        while (true)
        {
            auto part_locks = reader->readLocks();
            if (!part_locks)
                break;
            locks.merge(part_locks.value());
        }
    }

    return locks;
}

void S3GCManager::removeOutdatedManifest(
    const CheckpointManifestS3Set & manifests,
    const Aws::Utils::DateTime * const timepoint)
{
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    if (timepoint == nullptr)
    {
        for (const auto & mf : manifests.objects())
        {
            // store is tombstone, remove all manifests
            deleteObject(*client, mf.second.key);
            LOG_INFO(
                log,
                "remove outdated manifest because store is tombstone, key={} mtime={}",
                mf.second.key,
                mf.second.last_modification.ToGmtString(Aws::Utils::DateFormat::ISO_8601));
        }
        return;
    }

    assert(timepoint != nullptr);
    // clean the outdated manifest files
    const auto outdated_mfs
        = manifests.outdatedObjects(config.manifest_preserve_count, config.manifest_expired_hour, *timepoint);
    for (const auto & mf : outdated_mfs)
    {
        // expired manifest, remove
        deleteObject(*client, mf.key);
        LOG_INFO(
            log,
            "remove outdated manifest, key={} mtime={}",
            mf.key,
            mf.last_modification.ToGmtString(Aws::Utils::DateFormat::ISO_8601));
    }
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
    manager = std::make_unique<S3GCManager>(
        std::move(pd_client),
        std::move(gc_owner_manager_),
        std::move(lock_client),
        context.getSharedContextDisagg()->remote_data_store,
        config);

    timer = global_ctx.getBackgroundPool().addTask(
        [this]() { return manager->runOnAllStores(); },
        false,
        /*interval_ms*/ config.interval_seconds * 1000);
}

S3GCManagerService::~S3GCManagerService()
{
    shutdown();
}

void S3GCManagerService::shutdown()
{
    if (manager)
    {
        // set the shutdown flag
        manager->shutdown();
    }

    if (timer)
    {
        // Remove the task handler. It will block until the task break
        global_ctx.getBackgroundPool().removeTask(timer);
        timer = nullptr;
        // then we can reset the manager
        manager = nullptr;
    }
}

void S3GCManagerService::wake() const
{
    if (timer)
    {
        timer->wake();
    }
}

} // namespace DB::S3
