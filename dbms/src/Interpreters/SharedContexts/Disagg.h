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

#include <Core/TiFlashDisaggregatedMode.h>
#include <Encryption/FileProvider_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SharedContexts/Disagg_fwd.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore_fwd.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache_fwd.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache_fwd.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager_fwd.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService_fwd.h>
#include <Storages/PathPool_fwd.h>

#include <boost/core/noncopyable.hpp>

namespace DB
{
class FastAddPeerContext;
using FastAddPeerContextPtr = std::shared_ptr<FastAddPeerContext>;

/**
 * A shared context containing disaggregated mode related things.
 *
 * Usually you don't need synchronization when reading from this struct, as initializations are done
 * before this struct is available to the public.
 *
 * This struct is intentionally not placed inside `Context` to avoid touching too many compile units
 * when changing the interface or member.
 */
struct SharedContextDisagg : private boost::noncopyable
{
    Context & global_context;

    DisaggregatedMode disaggregated_mode = DisaggregatedMode::None;

    // Only meaningful in DisaggregatedComputeMode.
    bool use_autoscaler = false;

    /// For both read node (downloading) and write node (uploading).
    DM::Remote::IDataStorePtr remote_data_store;

    FastAddPeerContextPtr fap_context;

    /// Only for write node.
    DM::Remote::WNDisaggSnapshotManagerPtr wn_snapshot_manager;

    /// The PS instance available on Read Node.
    UniversalPageStorageServicePtr rn_page_cache_storage;

    /// Only for read node.
    /// It is a cache for the pages data. It uses ps_rn_page_cache as storage to cache page data to local disk based on the LRU mechanism.
    DB::DM::Remote::RNLocalPageCachePtr rn_page_cache;

    /// Only for read node.
    /// It is a cache for the delta index, stores in the memory.
    DB::DM::Remote::RNDeltaIndexCachePtr rn_delta_index_cache;

    static SharedContextDisaggPtr create(Context & global_context_)
    {
        return std::make_shared<SharedContextDisagg>(global_context_);
    }

    /// Use `SharedContextDisagg::create` instead.
    explicit SharedContextDisagg(Context & global_context_)
        : global_context(global_context_)
    {}

    void initReadNodePageCache(const PathPool & path_pool, const String & cache_dir, size_t cache_capacity);

    /// Note that the unit of max_size is quantity, not byte size. It controls how
    /// **many** of delta index will be maintained.
    void initReadNodeDeltaIndexCache(size_t max_size);

    void initWriteNodeSnapManager();

    void initRemoteDataStore(const FileProviderPtr & file_provider, bool s3_enabled);

    void initFastAddPeerContext(UInt64 fap_concur);

    bool isDisaggregatedComputeMode() const { return disaggregated_mode == DisaggregatedMode::Compute; }

    bool isDisaggregatedStorageMode() const { return disaggregated_mode == DisaggregatedMode::Storage; }

    bool notDisaggregatedMode() const { return disaggregated_mode == DisaggregatedMode::None; }
};

} // namespace DB
