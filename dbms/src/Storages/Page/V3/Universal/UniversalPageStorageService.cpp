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
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>

#include "ext/scope_guard.h"

namespace DB
{
UniversalPageStorageServicePtr UniversalPageStorageService::create(
    Context & context,
    const String & name,
    PSDiskDelegatorPtr delegator,
    const PageStorageConfig & config)
{
    auto service = UniversalPageStorageServicePtr(new UniversalPageStorageService(context));
    service->uni_page_storage = UniversalPageStorage::create(name, delegator, config, context.getFileProvider());
    service->uni_page_storage->restore();
    auto & bkg_pool = context.getBackgroundPool();

    if (S3::ClientFactory::instance().isEnabled())
    {
        // Only upload checkpoint when S3 is enabled
        service->remote_checkpoint_handle = bkg_pool.addTask(
            [service] {
                return service->uploadCheckpoint();
            },
            /*multi*/ false,
            /*interval_ms*/ 5 * 60 * 1000);
    }

    service->gc_handle = bkg_pool.addTask(
        [service] {
            return service->gc();
        },
        false,
        /*interval_ms*/ 60 * 1000);
    return service;
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
        LOG_INFO(Logger::get(), "Skip checkpoint because store meta is not initialized");
        return false;
    }

    auto s3lock_client = tmt.getS3LockClient();
    uni_page_storage->initLocksLocalManager(store_info.id(), s3lock_client);
    const auto upload_info = uni_page_storage->getUploadLocksInfo();

    auto wi = PS::V3::CheckpointProto::WriterInfo();
    {
        wi.set_store_id(store_info.id());
        wi.set_version(store_info.version());
        wi.set_version_git(store_info.git_hash());
        wi.set_start_at_ms(store_info.start_timestamp() * 1000); // TODO: Check whether * 1000 is correct..
        auto * ri = wi.mutable_remote_info();
        ri->set_type_name("S3");
        // ri->set_name(); FIXME: what does this field used for?
    }

    DM::Remote::IDataStorePtr remote_store; // FIXME: get the remote store from Context?

    UniversalPageStorage::DumpCheckpointOptions opts{
        .data_file_id_pattern = "",
        .data_file_path_pattern = "",
        .manifest_file_id_pattern = "",
        .manifest_file_path_pattern = "",
        .writer_info = wi,
        .must_locked_files = upload_info.pre_lock_keys,
    };
    const auto res = uni_page_storage->dumpIncrementalCheckpoint(opts);
    if (res.new_manifest_files.empty())
    {
        // No new checkpoint generated
        return false;
    }

    RUNTIME_CHECK_MSG(res.new_manifest_files.size() == 1, "Only support 1 manifest per checkpoint, actual n_manifest={}", res.new_manifest_files.size());

    DM::Remote::IDataStore::LocalCheckpointFiles checkpoint_files;
    for (const auto & df : res.new_data_files)
    {
        checkpoint_files.data_files.emplace_back(df.path);
    }
    checkpoint_files.manifest_file = res.new_manifest_files[0].path;

    remote_store->putCheckpointFiles(checkpoint_files, store_info.id(), upload_info.upload_sequence);

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
    auto & bkg_pool = global_context.getBackgroundPool();
    if (gc_handle)
    {
        bkg_pool.removeTask(gc_handle);
        gc_handle = nullptr;
    }
    if (remote_checkpoint_handle)
    {
        bkg_pool.removeTask(remote_checkpoint_handle);
        remote_checkpoint_handle = nullptr;
    }
}
} // namespace DB
