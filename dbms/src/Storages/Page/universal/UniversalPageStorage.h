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

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/ExternalPageCallbacks.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/UniversalPage.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
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

class UniversalPageReader;
using UniversalPageReaderPtr = std::shared_ptr<UniversalPageReader>;

class UniversalPageStorage final : public std::enable_shared_from_this<UniversalPageStorage>
{
public:
    using SnapshotPtr = PageStorageSnapshotPtr;

public:
    static UniversalPageStoragePtr
    create(
        String name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config,
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

    UniversalPageReaderPtr getReader(SnapshotPtr snapshot);

    UniversalPageMap read(const UniversalPageId & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        UNUSED(page_ids, read_limiter, snapshot);
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<UniversalPageId, FieldIndices>;

    UniversalPageMap read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        if (!snapshot)
            snapshot = this->getSnapshot("");

        // get the entries from directory, keep track
        // for not found page_ids
        UniversalPageIds page_ids_not_found;
        PS::V3::universal::BlobStoreTrait::FieldReadInfos read_infos;
        for (const auto & [page_id, field_indices] : page_fields)
        {
            const auto & [id, entry] = page_directory->getByIDOrNull(page_id, snapshot);

            if (entry.isValid())
            {
                auto info = PS::V3::universal::BlobStoreTrait::FieldReadInfo(page_id, entry, field_indices);
                read_infos.emplace_back(info);
            }
            else
            {
                page_ids_not_found.emplace_back(id);
            }
        }

        // read page data from blob_store
        UniversalPageMap page_map = blob_store->read(read_infos, read_limiter);
        for (const auto & page_id_not_found : page_ids_not_found)
        {
            UniversalPage page_not_found("");
            page_map[page_id_not_found] = page_not_found;
        }
        return page_map;
    }

    UniversalPage read(const PageReadFields & page_field, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        UNUSED(page_field, read_limiter, snapshot);
        throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
    }

    PageEntry getPageEntry(UniversalPageId page_id, SnapshotPtr snapshot = {}) const
    {
        if (!snapshot)
            snapshot = this->getSnapshot("");

        try
        {
            const auto & [id, entry] = page_directory->getByIDOrNull(page_id, snapshot);
            UNUSED(id);
            PageEntry entry_ret;
            entry_ret.file_id = entry.file_id;
            entry_ret.offset = entry.offset;
            entry_ret.tag = entry.tag;
            entry_ret.size = entry.size;
            entry_ret.field_offsets = entry.field_offsets;
            entry_ret.checksum = entry.checksum;

            return entry_ret;
        }
        catch (DB::Exception & e)
        {
            LOG_WARNING(log, "{}", e.message());
            return {.file_id = INVALID_BLOBFILE_ID}; // return invalid PageEntry
        }
    }

    // We may skip the GC to reduce useless reading by default.
    bool gc(bool not_skip = false, const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr)
    {
        std::ignore = not_skip;
        // If another thread is running gc, just return;
        bool v = false;
        if (!gc_is_running.compare_exchange_strong(v, true))
            return false;

        const GCTimeStatistics statistics = doGC(write_limiter, read_limiter);
        assert(statistics.stage != GCStageType::Unknown); // `doGC` must set the stage
        LOG_DEBUG(log, statistics.toLogging());

        return statistics.executeNextImmediately();
    }

    GCTimeStatistics doGC(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter);

    bool isEmpty() const
    {
        return page_directory->numPages() == 0;
    }

    // Register and unregister external pages GC callbacks
    // Note that user must ensure that it is safe to call `scanner` and `remover` even after unregister.
    void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) { UNUSED(callbacks); }
    void unregisterExternalPagesCallbacks(NamespaceId /*ns_id*/) {}

    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    PageStorageConfig config;
    FileProviderPtr file_provider;

    PS::V3::universal::PageDirectoryPtr page_directory;
    PS::V3::universal::BlobStorePtr blob_store;

    std::atomic<bool> gc_is_running = false;

    LoggerPtr log;
};

} // namespace DB
