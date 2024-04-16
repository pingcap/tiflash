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

#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreS3.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerContext.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/PathPool.h>

namespace DB
{

void SharedContextDisagg::initReadNodePageCache(
    const PathPool & path_pool,
    const String & cache_dir,
    size_t cache_capacity)
{
    RUNTIME_CHECK(rn_page_cache_storage == nullptr && rn_page_cache == nullptr);

    try
    {
        PSDiskDelegatorPtr delegator;
        if (!cache_dir.empty())
        {
            delegator = path_pool.getPSDiskDelegatorFixedDirectory(cache_dir);
            LOG_INFO(
                Logger::get(),
                "Initialize Read Node page cache in cache directory. path={} capacity={}",
                cache_dir,
                cache_capacity);
        }
        else
        {
            delegator = path_pool.getPSDiskDelegatorGlobalMulti(PathPool::read_node_cache_path_prefix);
            LOG_INFO(Logger::get(), "Initialize Read Node page cache in data directory. capacity={}", cache_capacity);
        }

        PageStorageConfig config;
        rn_page_cache_storage = UniversalPageStorageService::create( //
            global_context,
            "read_cache",
            delegator,
            config);
        rn_page_cache = DM::Remote::RNLocalPageCache::create({
            .underlying_storage = rn_page_cache_storage->getUniversalPageStorage(),
            .max_size_bytes = cache_capacity,
        });
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

void SharedContextDisagg::initReadNodeDeltaIndexCache(size_t max_size)
{
    RUNTIME_CHECK(rn_delta_index_cache == nullptr);

    if (max_size > 0)
    {
        LOG_INFO(Logger::get(), "Initialize Read Node delta index cache, max_size={}", max_size);
        rn_delta_index_cache = std::make_shared<DM::Remote::RNDeltaIndexCache>(max_size);
    }
    else
    {
        LOG_INFO(Logger::get(), "Skipped initialize Read Node delta index cache");
    }
}

void SharedContextDisagg::initWriteNodeSnapManager()
{
    RUNTIME_CHECK(wn_snapshot_manager == nullptr);

    wn_snapshot_manager = std::make_shared<DM::Remote::WNDisaggSnapshotManager>(global_context.getBackgroundPool());
}

void SharedContextDisagg::initRemoteDataStore(const FileProviderPtr & file_provider, bool s3_enabled)
{
    if (!s3_enabled)
        return;

    // Now only S3 data store is supported
    remote_data_store = std::make_shared<DM::Remote::DataStoreS3>(file_provider);
}

void SharedContextDisagg::initFastAddPeerContext(UInt64 fap_concur)
{
    LOG_INFO(Logger::get(), "Init FAP Context, concurrency={}", fap_concur);
    fap_context = std::make_shared<FastAddPeerContext>();
    fap_context->initFAPAsyncTasks(fap_concur);
}

} // namespace DB
