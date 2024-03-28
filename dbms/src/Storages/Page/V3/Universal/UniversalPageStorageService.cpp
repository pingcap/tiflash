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
#include <Common/FailPoint.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <aws/s3/S3Client.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char force_stop_background_checkpoint_upload[];
}

UniversalPageStorageService::UniversalPageStorageService(Context & global_context_)
    : global_context(global_context_)
    , uni_page_storage(nullptr)
    , log(Logger::get("UniPSService"))
{}

UniversalPageStorageServicePtr UniversalPageStorageService::create(
    Context & context,
    const String & name,
    PSDiskDelegatorPtr delegator,
    const PageStorageConfig & config)
{
    auto service = UniversalPageStorageServicePtr(new UniversalPageStorageService(context));
    service->uni_page_storage = UniversalPageStorage::create(name, delegator, config, context.getFileProvider());
    service->uni_page_storage->restore();

    // Starts the checkpoint upload timer
    // for disagg tiflash write node
    if (S3::ClientFactory::instance().isEnabled() && !context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        service->removeAllLocalCheckpointFiles();
        // TODO: make this interval reloadable
        auto interval_s = context.getSettingsRef().remote_checkpoint_interval_seconds;
        // Only upload checkpoint when S3 is enabled
        service->checkpoint_pool = std::make_unique<BackgroundProcessingPool>(1, "ps-checkpoint");
        service->remote_checkpoint_handle = service->checkpoint_pool->addTask(
            [service] { return service->uploadCheckpoint(); },
            /*multi*/ false,
            /*interval_ms*/ interval_s * 1000);
    }

    auto & bkg_pool = context.getBackgroundPool();
    service->gc_handle = bkg_pool.addTask(
        [service] { return service->gc(); },
        false,
        /*interval_ms*/ 60 * 1000);
    return service;
}

UniversalPageStorageServicePtr UniversalPageStorageService::createForTest(
    Context & context,
    const String & name,
    PSDiskDelegatorPtr delegator,
    const PageStorageConfig & config)
{
    auto service = UniversalPageStorageServicePtr(new UniversalPageStorageService(context));
    service->uni_page_storage = UniversalPageStorage::create(name, delegator, config, context.getFileProvider());
    service->uni_page_storage->restore();
    // not register background task under test
    return service;
}

bool CheckpointUploadFunctor::operator()(const PS::V3::LocalCheckpointFiles & checkpoint) const
{
    // Persist checkpoint to remote_source
    return remote_store->putCheckpointFiles(checkpoint, store_id, sequence);
}

void UniversalPageStorageService::setUploadAllData()
{
    upload_all_at_next_upload = true;
    gc_handle->wake();
    LOG_INFO(log, "sync_all flag is set, next checkpoint will upload all existing data");
}

bool UniversalPageStorageService::uploadCheckpoint()
{
    // If another thread is running, just skip
    bool v = false;
    if (!is_checkpoint_uploading.compare_exchange_strong(v, true))
        return false;

    fiu_do_on(FailPoints::force_stop_background_checkpoint_upload, {
        // Disable background upload checkpoint process in unit tests
        LOG_WARNING(log, "!!!force disable UniversalPageStorageService::uploadCheckpoint!!!");
        return false;
    });

    SCOPE_EXIT({
        bool is_running = true;
        is_checkpoint_uploading.compare_exchange_strong(is_running, false);
    });

    if (!global_context.isTMTContextInited())
    {
        LOG_INFO(log, "Skip checkpoint because context is not initialized");
        return false;
    }

    auto & tmt = global_context.getTMTContext();

    auto store_info = tmt.getKVStore()->clonedStoreMeta();
    if (store_info.id() == InvalidStoreID)
    {
        LOG_INFO(log, "Skip checkpoint because store meta is not initialized");
        return false;
    }
    auto remote_store = global_context.getSharedContextDisagg()->remote_data_store;
    if (remote_store == nullptr)
    {
        LOG_INFO(log, "Skip checkpoint because remote data store is not initialized");
        return false;
    }
    auto s3lock_client = tmt.getS3LockClient();
    const bool force_upload = upload_all_at_next_upload.load();
    bool upload_done = uploadCheckpointImpl(store_info, s3lock_client, remote_store, force_upload);
    if (force_upload && upload_done)
        upload_all_at_next_upload = false;
    // always return false to run at fixed rate
    return false;
}

struct FileIdsToCompactGetter
{
    UniversalPageStoragePtr uni_page_storage;
    DM::Remote::RemoteGCThreshold gc_threshold;
    const DM::Remote::IDataStorePtr & remote_store;
    LoggerPtr log;

    std::unordered_set<String> operator()() const
    {
        // In order to not block local GC updating the cache, get a copy of the cache
        auto stats = uni_page_storage->getRemoteDataFilesStatCache();
        auto file_ids_to_compact = PS::V3::getRemoteFileIdsNeedCompact(stats, gc_threshold, remote_store, log);
        // update cache by the S3 result
        uni_page_storage->updateRemoteFilesStatCache(stats);
        return file_ids_to_compact;
    }
};

bool UniversalPageStorageService::uploadCheckpointImpl(
    const metapb::Store & store_info,
    const S3::S3LockClientPtr & s3lock_client,
    const DM::Remote::IDataStorePtr & remote_store,
    bool force_sync_data)
{
    // `initLocksLocalManager` enable writes to remote store.
    // if it is the first time after restart, it will load the last_upload_sequence
    // and last_checkpoint_sequence from remote store.
    uni_page_storage->initLocksLocalManager(store_info.id(), s3lock_client);

    // do a pre-check to avoid allocating upload_sequence but checkpoint is
    // actually skip
    // TODO: we can do it in a better way by splitting `dumpIncrementalCheckpoint`
    //       into smaller parts to avoid this.
    if (!force_sync_data && uni_page_storage->canSkipCheckpoint())
    {
        return false;
    }

    auto wi = PS::V3::CheckpointProto::WriterInfo();
    {
        wi.set_store_id(store_info.id());
        wi.set_version(store_info.version());
        wi.set_version_git(store_info.git_hash());
        wi.set_start_at_ms(store_info.start_timestamp() * 1000); // TODO: Check whether * 1000 is correct..
        auto * ri = wi.mutable_remote_info();
        ri->set_type_name("S3");
        // ri->set_name(); this field is not used currently
        auto client = S3::ClientFactory::instance().sharedTiFlashClient();
        ri->set_root(client->root());
    }

    // TODO: directly write into remote store. But take care of the order
    //       of CheckpointData files, lock files, and CheckpointManifest.
    const auto upload_info = uni_page_storage->allocateNewUploadLocksInfo();
    auto local_dir = getCheckpointLocalDir(upload_info.upload_sequence);
    Poco::File(local_dir).createDirectories();
    auto local_dir_str = local_dir.toString() + "/";
    SCOPE_EXIT({
        // No matter the local checkpoint files are uploaded successfully or fails, delete them directly.
        // Since the directory has been created before, it should exists.
        Poco::File(local_dir).remove(true);
    });

    /*
     * If using `snapshot->sequence` as a part of manifest name, we can NOT
     * guarantee **all** locks generated with sequence less than the
     * checkpoint's snapshot->sequence are fully uploaded to S3.
     * In order to make the locks within same `upload_sequence` are public
     * to S3GCManager atomically, we must use a standalone `upload_sequence`
     * (which is managed by `S3LockLocalManager`) to override the
     * CheckpointDataFile and CheckpointManifest key.
     *
     * Example:
     * timeline:
     *    │--------------- A lockkey is uploading -----------------│
     *    ^ snapshot->sequence=10
     *             
     *             │-------- Checkpoint dumped and uploaded --│
     *             ^ snapshot->sequence=12
     * The checkpoint with snapshot->sequence=12 could finished before the lockkey with
     * sequence=10 uploaded.
     */
    const auto & settings = global_context.getSettingsRef(); // TODO: make it dynamic reloadable
    auto gc_threshold = DM::Remote::RemoteGCThreshold{
        .min_age_seconds = settings.remote_gc_min_age_seconds,
        .valid_rate = settings.remote_gc_ratio,
        .min_file_threshold = static_cast<size_t>(settings.remote_gc_small_size),
    };

    if (force_sync_data)
    {
        LOG_INFO(log, "Upload checkpoint with all existing data");
    }
    UniversalPageStorage::DumpCheckpointOptions opts{
        .data_file_id_pattern = S3::S3Filename::newCheckpointDataNameTemplate(store_info.id(), upload_info.upload_sequence),
        .data_file_path_pattern = local_dir_str + "dat_{seq}_{index}",
        .manifest_file_id_pattern = S3::S3Filename::newCheckpointManifestNameTemplate(store_info.id()),
        .manifest_file_path_pattern = local_dir_str + "mf_{seq}",
        .writer_info = wi,
        .must_locked_files = upload_info.pre_lock_keys,
        .persist_checkpoint = CheckpointUploadFunctor{
            .store_id = store_info.id(),
            // Note that we use `upload_sequence` but not `snapshot.sequence` for
            // the S3 key.
            .sequence = upload_info.upload_sequence,
            .remote_store = remote_store,
        },
        .override_sequence = upload_info.upload_sequence, // override by upload_sequence
        .full_compact = force_sync_data,
        .compact_getter = FileIdsToCompactGetter{
            .uni_page_storage = uni_page_storage,
            .gc_threshold = gc_threshold,
            .remote_store = remote_store,
            .log = log,
        },
        .only_upload_manifest = settings.remote_checkpoint_only_upload_manifest,
    };

    const auto write_stats = uni_page_storage->dumpIncrementalCheckpoint(opts);
    GET_METRIC(tiflash_storage_checkpoint_flow, type_incremental).Increment(write_stats.incremental_data_bytes);
    GET_METRIC(tiflash_storage_checkpoint_flow, type_compaction).Increment(write_stats.compact_data_bytes);

    LOG_INFO(
        log,
        "Upload checkpoint success,{} upload_sequence={} incremental_bytes={} compact_bytes={}",
        force_sync_data ? " sync_all=true" : "",
        upload_info.upload_sequence,
        write_stats.incremental_data_bytes,
        write_stats.compact_data_bytes);

    return true;
}

bool UniversalPageStorageService::gc() const
{
    // TODO: reload config
    return this->uni_page_storage->gc();
}

UniversalPageStorageService::~UniversalPageStorageService()
{
    shutdown();
}

void UniversalPageStorageService::shutdown()
{
    if (remote_checkpoint_handle)
    {
        checkpoint_pool->removeTask(remote_checkpoint_handle);
        remote_checkpoint_handle = nullptr;
    }

    if (gc_handle)
    {
        auto & bkg_pool = global_context.getBackgroundPool();
        bkg_pool.removeTask(gc_handle);
        gc_handle = nullptr;
    }
}

void UniversalPageStorageService::removeAllLocalCheckpointFiles() const
{
    Poco::File temp_dir(global_context.getTemporaryPath());
    if (temp_dir.exists() && temp_dir.isDirectory())
    {
        std::vector<String> short_names;
        temp_dir.list(short_names);
        for (const auto & name : short_names)
        {
            if (startsWith(name, checkpoint_dirname_prefix))
            {
                auto checkpoint_dirname = global_context.getTemporaryPath() + "/" + name;
                Poco::File(checkpoint_dirname).remove(true);
            }
        }
    }
}

Poco::Path UniversalPageStorageService::getCheckpointLocalDir(UInt64 seq) const
{
    return Poco::Path(global_context.getTemporaryPath() + fmt::format("/{}{}", checkpoint_dirname_prefix, seq))
        .absolute();
}

} // namespace DB
