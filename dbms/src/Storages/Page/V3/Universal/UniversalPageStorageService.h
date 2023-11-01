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

#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService_fwd.h>

namespace DB::DM::Remote
{
class IDataStore;
using IDataStorePtr = std::shared_ptr<IDataStore>;
} // namespace DB::DM::Remote

namespace DB
{
struct CheckpointUploadFunctor
{
    const StoreID store_id;
    const UInt64 sequence;
    const DM::Remote::IDataStorePtr remote_store;

    bool operator()(const PS::V3::LocalCheckpointFiles & checkpoint) const;
};

// This is wrapper class for UniversalPageStorage.
// It mainly manages background tasks like gc for UniversalPageStorage.
// It is like StoragePool for Page V2, and GlobalStoragePool for Page V3.
class UniversalPageStorageService final
{
public:
    static UniversalPageStorageServicePtr create(
        Context & context,
        const String & name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config);

    bool gc() const;

    bool uploadCheckpoint();

    // Set a flag for sync all data to remote store at next checkpoint
    void setUploadAllData();

    UniversalPageStoragePtr getUniversalPageStorage() const { return uni_page_storage; }
    ~UniversalPageStorageService();
    void shutdown();

    bool uploadCheckpointImpl(
        const metapb::Store & store_info,
        const S3::S3LockClientPtr & s3lock_client,
        const DM::Remote::IDataStorePtr & remote_store,
        bool force_sync_data);

    static UniversalPageStorageServicePtr createForTest(
        Context & context,
        const String & name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config);

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    explicit UniversalPageStorageService(Context & global_context_);
    // If the TiFlash process restart unexpectedly, some local checkpoint files can be left,
    // remove these files when the process restarting.
    void removeAllLocalCheckpointFiles() const;
    Poco::Path getCheckpointLocalDir(UInt64 seq) const;

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    std::atomic_bool is_checkpoint_uploading{false};

    // Once this flag is set, all data will be synced to remote store at next time
    // `uploadCheckpoint` is called.
    std::atomic_bool upload_all_at_next_upload{false};

    Context & global_context;
    UniversalPageStoragePtr uni_page_storage;
    BackgroundProcessingPool::TaskHandle gc_handle;

    LoggerPtr log;

    // A standalone thread pool to avoid checkpoint uploading being affected by
    // other background tasks unexpectly.
    std::unique_ptr<BackgroundProcessingPool> checkpoint_pool;
    BackgroundProcessingPool::TaskHandle remote_checkpoint_handle;

    inline static const String checkpoint_dirname_prefix = "checkpoint_upload_";
};
} // namespace DB
