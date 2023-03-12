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
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>
#include <aws/s3/S3Client.h>

#include <ext/scope_guard.h>

namespace DB
{

UniversalPageStorageService::UniversalPageStorageService(Context & global_context_)
    : global_context(global_context_)
    , uni_page_storage(nullptr)
    , log(Logger::get())
{
}

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
        // TODO: make this interval reloadable
        auto interval_s = context.getSettingsRef().remote_checkpoint_interval_seconds;
        // Only upload checkpoint when S3 is enabled
        service->checkpoint_pool = std::make_unique<BackgroundProcessingPool>(1, "ps-checkpoint");
        service->remote_checkpoint_handle = service->checkpoint_pool->addTask(
            [service] {
                return service->uploadCheckpoint();
            },
            /*multi*/ false,
            /*interval_ms*/ interval_s * 1000);
    }

    auto & bkg_pool = context.getBackgroundPool();
    service->gc_handle = bkg_pool.addTask(
        [service] {
            return service->gc();
        },
        false,
        /*interval_ms*/ 60 * 1000);
    return service;
}

UniversalPageStorageServicePtr
UniversalPageStorageService::createForTest(
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

bool UniversalPageStorageService::uploadCheckpoint()
{
    // If another thread is running, just skip
    bool v = false;
    if (!is_checkpoint_uploading.compare_exchange_strong(v, true))
        return false;

    SCOPE_EXIT({
        bool is_running = true;
        is_checkpoint_uploading.compare_exchange_strong(is_running, false);
    });

    auto & tmt = global_context.getTMTContext();

    auto store_info = tmt.getKVStore()->getStoreMeta();
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
    return uploadCheckpointImpl(store_info, s3lock_client, remote_store);
}

bool UniversalPageStorageService::uploadCheckpointImpl(
    const metapb::Store & store_info,
    const S3::S3LockClientPtr & s3lock_client,
    const DM::Remote::IDataStorePtr & remote_store)
{
    uni_page_storage->initLocksLocalManager(store_info.id(), s3lock_client);
    const auto upload_info = uni_page_storage->allocateNewUploadLocksInfo();

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


    auto local_dir = Poco::Path(global_context.getTemporaryPath() + fmt::format("/checkpoint_upload_{}", upload_info.upload_sequence)).absolute();
    Poco::File(local_dir).createDirectories();
    auto local_dir_str = local_dir.toString() + "/";

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
    };
    uni_page_storage->dumpIncrementalCheckpoint(opts);

    LOG_DEBUG(log, "Upload checkpoint with upload sequence {} success", upload_info.upload_sequence);

    // the checkpoint is uploaded to remote data store, remove local temp files
    Poco::File(local_dir).remove(true);

    // always return false to run at fixed rate
    return false;
}

bool UniversalPageStorageService::gc()
{
    Timepoint now = Clock::now();
    const std::chrono::seconds try_gc_period(60);
    if (now < (last_try_gc_time.load() + try_gc_period))
        return false;

    last_try_gc_time = now;
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
} // namespace DB
