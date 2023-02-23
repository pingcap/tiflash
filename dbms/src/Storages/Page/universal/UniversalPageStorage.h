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

#pragma once

#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/ExternalPageCallbacks.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectory/ExternalIdTrait.h>
#include <common/defines.h>


namespace DB
{
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
class Context;
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

class KVStoreReader;

using UniversalPages = std::vector<UniversalPageId>;
using UniversalPageMap = std::map<UniversalPageId, Page>;

struct UniversalExternalPageCallbacks
{
    // `scanner` for scanning available external page ids on disks.
    // `remover` will be called with living normal page ids after gc run a round, user should remove those
    //           external pages(files) in `pending_external_pages` but not in `valid_normal_pages`
    using PathAndIdsVec = std::vector<std::pair<String, std::set<PageId>>>;
    using ExternalPagesScanner = std::function<PathAndIdsVec()>;
    using ExternalPagesRemover
        = std::function<void(const PathAndIdsVec & pending_external_pages, const std::set<PageId> & valid_normal_pages)>;
    ExternalPagesScanner scanner = nullptr;
    ExternalPagesRemover remover = nullptr;
    String prefix;
};

class UniversalPageStorage final
{
public:
    using SnapshotPtr = PageStorageSnapshotPtr;

public:
    static UniversalPageStoragePtr
    create(
        String name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config,
        const String & remote_dir,
        const FileProviderPtr & file_provider);

    UniversalPageStorage(
        String name,
        PSDiskDelegatorPtr delegator_,
        const PageStorageConfig & config_,
        const FileProviderPtr & file_provider_)
        : storage_name(std::move(name))
        , delegator(std::move(delegator_))
        , config(config_)
        , file_provider(file_provider_)
        , log(Logger::get("UniversalPageStorage", name))
    {
    }

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

        UInt64 total_cost_ms = 0;

        UInt64 dump_snapshots_ms = 0;
        UInt64 gc_in_mem_entries_ms = 0;
        UInt64 blobstore_remove_entries_ms = 0;
        UInt64 blobstore_get_gc_stats_ms = 0;
        // Full GC
        UInt64 full_gc_get_entries_ms = 0;
        UInt64 full_gc_blobstore_copy_ms = 0;
        UInt64 full_gc_apply_ms = 0;

        // GC external page
        UInt64 clean_external_page_ms = 0;
        UInt64 num_external_callbacks = 0;
        // ms is usually too big for these operation, store by ns (10^-9)
        UInt64 external_page_scan_ns = 0;
        UInt64 external_page_get_alive_ns = 0;
        UInt64 external_page_remove_ns = 0;

        String toLogging() const;
    };

    ~UniversalPageStorage() = default;

    void restore();

    SnapshotPtr getSnapshot(const String & tracing_id) const
    {
        return page_directory->createSnapshot(tracing_id);
    }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const
    {
        return page_directory->getSnapshotsStat();
    }

    FileUsageStatistics getFileUsageStatistics() const
    {
        auto u = blob_store->getFileUsageStatistics();
        u.merge(page_directory->getFileUsageStatistics());
        return u;
    }

    void write(UniversalWriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr) const;

    Page read(const UniversalPageId & page_id, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true);

    UniversalPageMap read(const UniversalPageIds & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true);

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<UniversalPageId, FieldIndices>;

    UniversalPageMap read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true);

    Page read(const PageReadFields & page_field, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true);

    void traverseEntries(const std::function<void(UniversalPageId page_id, DB::PageEntry entry)> & acceptor, SnapshotPtr snapshot = {});

    UniversalPageId getNormalPageId(const UniversalPageId & page_id, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true);

    DB::PageEntry getEntry(const UniversalPageId & page_id, SnapshotPtr snapshot);

    DB::PS::V3::PageEntryV3 getEntryV3(const UniversalPageId & page_id, SnapshotPtr snapshot);

    PageId getMaxId() const;

    // We may skip the GC to reduce useless reading by default.
    bool gc(bool not_skip = false, const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr);

    GCTimeStatistics doGC(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter);
    void cleanExternalPage(Stopwatch & gc_watch, GCTimeStatistics & statistics);

    void doCheckpoint(std::shared_ptr<const PS::V3::Remote::WriterInfo> writer_info)
    {
        if (config.ps_remote_directory.get().empty())
            return;
        return checkpointImpl(writer_info, config.ps_remote_directory);
    }

    void checkpointImpl(std::shared_ptr<const PS::V3::Remote::WriterInfo> writer_info, const std::string & remote_directory);

    bool isEmpty() const
    {
        return page_directory->numPages() == 0;
    }

    // Register and unregister external pages GC callbacks
    // Note that user must ensure that it is safe to call `scanner` and `remover` even after unregister.
    void registerUniversalExternalPagesCallbacks(const UniversalExternalPageCallbacks & callbacks);
    void unregisterUniversalExternalPagesCallbacks(const String & prefix);

    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    PageStorageConfig config;
    FileProviderPtr file_provider;

    PS::V3::universal::PageDirectoryPtr page_directory;
    PS::V3::universal::BlobStorePtr blob_store;

    std::atomic<bool> gc_is_running = false;

    std::mutex callbacks_mutex;
    // Only std::map not std::unordered_map. We need insert/erase do not invalid other iterators.
    using UniversalExternalPageCallbacksContainer = std::map<String, std::shared_ptr<UniversalExternalPageCallbacks>>;
    UniversalExternalPageCallbacksContainer callbacks_container;

    LoggerPtr log;
};

} // namespace DB
