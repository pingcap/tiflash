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

#include <Common/Stopwatch.h>
#include <Poco/Message.h>
#include <Storages/Page/ExternalPageCallbacks.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>

namespace DB::PS::V3
{
enum class GCStageType
{
    Unknown,
    OnlyInMem,
    FullGCNothingMoved,
    FullGC,
};
struct GCTimeStatistics
{
    GCStageType stage = GCStageType::Unknown;
    bool executeNextImmediately() const { return stage == GCStageType::FullGC; };

    bool compact_wal_happen = false;

    Poco::Message::Priority getLoggingLevel() const;

    UInt64 total_cost_ms = 0;

    UInt64 compact_wal_ms = 0;
    UInt64 compact_directory_ms = 0;
    UInt64 compact_spacemap_ms = 0;
    // Full GC
    UInt64 full_gc_prepare_ms = 0;
    UInt64 full_gc_get_entries_ms = 0;
    UInt64 full_gc_blobstore_copy_ms = 0;
    UInt64 full_gc_apply_ms = 0;

    // GC external page
    UInt64 num_external_callbacks = 0;
    // Breakdown the duration for cleaning external pages
    // ms is usually too big for these operation, store by ns (10^-9)
    UInt64 external_page_scan_ns = 0;
    UInt64 external_page_get_alive_ns = 0;
    UInt64 external_page_remove_ns = 0;

private:
    // Total time of cleaning external pages
    UInt64 clean_external_page_ms = 0;

public:
    void finishCleanExternalPage(UInt64 clean_cost_ms);

    String toLogging() const;
};

template <typename Trait>
class ExternalPageCallbacksManager
{
public:
    using PageId = typename Trait::PageId;
    using Prefix = typename Trait::Prefix;
    using PageEntriesEdit = DB::PS::V3::PageEntriesEdit<PageId>;
    using ExternalPageCallbacks = typename Trait::ExternalPageCallbacks;
    using BlobStore = typename Trait::BlobStore;
    using PageDirectory = typename Trait::PageDirectory;

public:
    void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks);

    void unregisterExternalPagesCallbacks(const Prefix & prefix);

    bool gc(
        typename Trait::BlobStore & blob_store,
        typename Trait::PageDirectory & page_directory,
        const WriteLimiterPtr & write_limiter,
        const ReadLimiterPtr & read_limiter,
        RemoteFileValidSizes * remote_valid_sizes,
        LoggerPtr log);

private:
    void cleanExternalPage(PageDirectory & page_directory, Stopwatch & gc_watch, GCTimeStatistics & statistics);

    GCTimeStatistics doGC(
        typename Trait::BlobStore & blob_store,
        typename Trait::PageDirectory & page_directory,
        const WriteLimiterPtr & write_limiter,
        const ReadLimiterPtr & read_limiter,
        RemoteFileValidSizes * remote_valid_sizes);

private:
    std::atomic<bool> gc_is_running = false;

    std::mutex callbacks_mutex;
    // Only std::map not std::unordered_map. We need insert/erase do not invalid other iterators.
    using ExternalPageCallbacksContainer = std::map<Prefix, std::shared_ptr<ExternalPageCallbacks>>;
    ExternalPageCallbacksContainer callbacks_container;
};

namespace u128
{
struct ExternalPageCallbacksManagerTrait
{
    using PageId = PageIdV3Internal;
    using Prefix = NamespaceID;
    using ExternalPageCallbacks = DB::ExternalPageCallbacks;
    using PageDirectory = PageDirectoryType;
    using BlobStore = BlobStoreType;
};
using ExternalPageCallbacksManager = ExternalPageCallbacksManager<ExternalPageCallbacksManagerTrait>;
} // namespace u128
namespace universal
{
struct ExternalPageCallbacksManagerTrait
{
    using PageId = UniversalPageId;
    using Prefix = String;
    using ExternalPageCallbacks = UniversalExternalPageCallbacks;
    using PageDirectory = PageDirectoryType;
    using BlobStore = BlobStoreType;
};
using ExternalPageCallbacksManager = ExternalPageCallbacksManager<ExternalPageCallbacksManagerTrait>;
} // namespace universal
} // namespace DB::PS::V3
