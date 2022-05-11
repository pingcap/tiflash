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

#include <Common/Logger.h>
#include <Core/Types.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <condition_variable>
#include <functional>
#include <optional>
#include <queue>
#include <set>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>


namespace DB
{
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
class Context;
class PageStorage;
using PageStoragePtr = std::shared_ptr<PageStorage>;
class RegionPersister;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


enum class PageStorageRunMode : UInt8
{
    ONLY_V2 = 1,
    ONLY_V3 = 2,
    MIX_MODE = 3,
};

struct ExternalPageCallbacks
{
    // `scanner` for scanning avaliable external page ids on disks.
    // `remover` will be called with living normal page ids after gc run a round, user should remove those
    //           external pages(files) in `pending_external_pages` but not in `valid_normal_pages`
    using PathAndIdsVec = std::vector<std::pair<String, std::set<PageId>>>;
    using ExternalPagesScanner = std::function<PathAndIdsVec()>;
    using ExternalPagesRemover
        = std::function<void(const PathAndIdsVec & pengding_external_pages, const std::set<PageId> & valid_normal_pages)>;
    ExternalPagesScanner scanner = nullptr;
    ExternalPagesRemover remover = nullptr;
    NamespaceId ns_id = MAX_NAMESPACE_ID;
};

/**
 * A storage system stored pages. Pages are serialized objects referenced by PageID. Store Page with the same PageID
 * will cover the old ones.
 * Users should call #gc() constantly to release disk space.
 *
 * This class is multi-threads safe. Support multi threads write, and multi threads read.
 */
class PageStorage : private boost::noncopyable
{
public:
    using SnapshotPtr = PageStorageSnapshotPtr;

    struct Config
    {
        //==========================================================================================
        // V2 config
        //==========================================================================================
        SettingBool sync_on_write = true;

        SettingUInt64 file_roll_size = PAGE_FILE_ROLL_SIZE;
        SettingUInt64 file_max_size = PAGE_FILE_MAX_SIZE;
        SettingUInt64 file_small_size = PAGE_FILE_SMALL_SIZE;

        SettingUInt64 file_meta_roll_size = PAGE_META_ROLL_SIZE;

        // When the value of gc_force_hardlink_rate is less than or equal to 1,
        // It means that candidates whose valid rate is greater than this value will be forced to hardlink(This will reduce the gc duration).
        // Otherwise, if gc_force_hardlink_rate is greater than 1, hardlink won't happen
        SettingDouble gc_force_hardlink_rate = 2;

        SettingDouble gc_max_valid_rate = 0.35;
        SettingUInt64 gc_min_bytes = PAGE_FILE_ROLL_SIZE;
        SettingUInt64 gc_min_files = 10;
        // Minimum number of legacy files to be selected for compaction
        SettingUInt64 gc_min_legacy_num = 3;

        SettingUInt64 gc_max_expect_legacy_files = 100;
        SettingDouble gc_max_valid_rate_bound = 1.0;

        // Maximum write concurrency. Must not be changed once the PageStorage object is created.
        SettingUInt64 num_write_slots = 1;

        // Maximum seconds of reader / writer idle time.
        // 0 for never reclaim idle file descriptor.
        SettingUInt64 open_file_max_idle_time = 15;

        // Probability to do gc when write is low.
        // The probability is `prob_do_gc_when_write_is_low` out of 1000.
        SettingUInt64 prob_do_gc_when_write_is_low = 10;

        MVCC::VersionSetConfig version_set_config;

        //==========================================================================================
        // V3 config
        //==========================================================================================
        SettingUInt64 blob_file_limit_size = BLOBFILE_LIMIT_SIZE;
        SettingUInt64 blob_spacemap_type = 2;
        SettingUInt64 blob_cached_fd_size = BLOBSTORE_CACHED_FD_SIZE;
        SettingDouble blob_heavy_gc_valid_rate = 0.2;
        SettingUInt64 blob_block_alignment_bytes = 0;

        SettingUInt64 wal_roll_size = PAGE_META_ROLL_SIZE;
        SettingUInt64 wal_recover_mode = 0;
        SettingUInt64 wal_max_persisted_log_files = MAX_PERSISTED_LOG_FILES;

        void reload(const Config & rhs)
        {
            // Reload is not atomic, but should be good enough

            // Reload gc threshold
            gc_force_hardlink_rate = rhs.gc_force_hardlink_rate;
            gc_max_valid_rate = rhs.gc_max_valid_rate;
            gc_min_bytes = rhs.gc_min_bytes;
            gc_min_files = rhs.gc_min_files;
            gc_min_legacy_num = rhs.gc_min_legacy_num;
            prob_do_gc_when_write_is_low = rhs.prob_do_gc_when_write_is_low;
            // Reload fd idle time
            open_file_max_idle_time = rhs.open_file_max_idle_time;

            // Reload V3 setting
            blob_file_limit_size = rhs.blob_file_limit_size;
            blob_spacemap_type = rhs.blob_spacemap_type;
            blob_cached_fd_size = rhs.blob_cached_fd_size;
            blob_heavy_gc_valid_rate = rhs.blob_heavy_gc_valid_rate;
            blob_block_alignment_bytes = rhs.blob_block_alignment_bytes;

            wal_roll_size = rhs.wal_roll_size;
            wal_recover_mode = rhs.wal_recover_mode;
            wal_max_persisted_log_files = rhs.wal_max_persisted_log_files;
        }

        String toDebugStringV2() const
        {
            return fmt::format(
                "PageStorage::Config {{gc_min_files: {}, gc_min_bytes:{}, gc_force_hardlink_rate: {:.3f}, gc_max_valid_rate: {:.3f}, "
                "gc_min_legacy_num: {}, gc_max_expect_legacy: {}, gc_max_valid_rate_bound: {:.3f}, prob_do_gc_when_write_is_low: {}, "
                "open_file_max_idle_time: {}}}",
                gc_min_files,
                gc_min_bytes,
                gc_force_hardlink_rate.get(),
                gc_max_valid_rate.get(),
                gc_min_legacy_num,
                gc_max_expect_legacy_files.get(),
                gc_max_valid_rate_bound.get(),
                prob_do_gc_when_write_is_low,
                open_file_max_idle_time);
        }

        String toDebugStringV3() const
        {
            return fmt::format(
                "PageStorage::Config V3 {{"
                "blob_file_limit_size: {}, blob_spacemap_type: {}, "
                "blob_cached_fd_size: {}, blob_heavy_gc_valid_rate: {:.3f}, blob_block_alignment_bytes: {}, "
                "wal_roll_size: {}, wal_recover_mode: {}, wal_max_persisted_log_files: {}}}",
                blob_file_limit_size.get(),
                blob_spacemap_type.get(),
                blob_cached_fd_size.get(),
                blob_heavy_gc_valid_rate.get(),
                blob_block_alignment_bytes.get(),
                wal_roll_size.get(),
                wal_recover_mode.get(),
                wal_max_persisted_log_files.get());
        }
    };
    void reloadSettings(const Config & new_config) { config.reload(new_config); };
    Config getSettings() const { return config; }

public:
    static PageStoragePtr
    create(
        String name,
        PSDiskDelegatorPtr delegator,
        const PageStorage::Config & config,
        const FileProviderPtr & file_provider,
        bool use_v3 = false);

    PageStorage(
        String name,
        PSDiskDelegatorPtr delegator_,
        const Config & config_,
        const FileProviderPtr & file_provider_)
        : storage_name(std::move(name))
        , delegator(std::move(delegator_))
        , config(config_)
        , file_provider(file_provider_)
    {
    }

    virtual ~PageStorage() = default;

    // Return the map[ns_id, max_page_id]
    // The caller should ensure that it only allocate new id that is larger than `max_page_id`. Reusing the
    // same ID for different kind of write (put/ref/put_external) would make PageStorage run into unexpected error.
    //
    // Note that for V2, we always return a map with only one element: <ns_id=0, max_id> cause V2 have no
    // idea about ns_id.
    virtual std::map<NamespaceId, PageId> restore() = 0;

    virtual void drop() = 0;

    virtual SnapshotPtr getSnapshot(const String & tracing_id) = 0;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    virtual SnapshotsStatistics getSnapshotsStat() const = 0;

    virtual size_t getNumberOfPages() = 0;

    virtual std::set<PageId> getAliveExternalPageIds(NamespaceId ns_id) = 0;

    void write(WriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr)
    {
        writeImpl(std::move(write_batch), write_limiter);
    }

    // If we can't get the entry.
    // Then the null entry will be return
    PageEntry getEntry(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot = {})
    {
        return getEntryImpl(ns_id, page_id, snapshot);
    }

    Page read(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_id, read_limiter, snapshot, throw_on_not_exist);
    }

    PageMap read(NamespaceId ns_id, const PageIds & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_ids, read_limiter, snapshot, throw_on_not_exist);
    }

    /**
     * If throw_on_not_exist is false, Also we do have some of page_id not found.
     * Then the return value will record the all of page_id which not found.
     */
    PageIds read(NamespaceId ns_id, const PageIds & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_ids, handler, read_limiter, snapshot, throw_on_not_exist);
    }

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<PageId, FieldIndices>;

    PageMap read(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_fields, read_limiter, snapshot, throw_on_not_exist);
    }

    Page read(NamespaceId ns_id, const PageReadFields & page_field, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_field, read_limiter, snapshot, throw_on_not_exist);
    }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot = {})
    {
        traverseImpl(acceptor, snapshot);
    }

    PageId getNormalPageId(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true)
    {
        return getNormalPageIdImpl(ns_id, page_id, snapshot, throw_on_not_exist);
    }

    // We may skip the GC to reduce useless reading by default.
    bool gc(bool not_skip = false, const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr)
    {
        return gcImpl(not_skip, write_limiter, read_limiter);
    }

    // Register and unregister external pages GC callbacks
    virtual void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) = 0;
    virtual void unregisterExternalPagesCallbacks(NamespaceId /*ns_id*/){};

#ifndef DBMS_PUBLIC_GTEST
protected:
#endif
    virtual void writeImpl(WriteBatch && write_batch, const WriteLimiterPtr & write_limiter) = 0;

    virtual PageEntry getEntryImpl(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot) = 0;

    virtual Page readImpl(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) = 0;

    virtual PageMap readImpl(NamespaceId ns_id, const PageIds & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) = 0;

    virtual PageIds readImpl(NamespaceId ns_id, const PageIds & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) = 0;

    virtual PageMap readImpl(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) = 0;

    virtual Page readImpl(NamespaceId ns_id, const PageReadFields & page_field, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot, bool throw_on_not_exist) = 0;

    virtual void traverseImpl(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) = 0;

    virtual PageId getNormalPageIdImpl(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot, bool throw_on_not_exist) = 0;

    virtual bool gcImpl(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) = 0;

    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    Config config;
    FileProviderPtr file_provider;
};

class PageReader : private boost::noncopyable
{
public:
    /// Not snapshot read.
    explicit PageReader(const PageStorageRunMode & run_mode_, NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, ReadLimiterPtr read_limiter_)
        : run_mode(run_mode_)
        , ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , read_limiter(read_limiter_)
    {
    }

    /// Snapshot read.
    PageReader(const PageStorageRunMode & run_mode_, NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, const PageStorage::SnapshotPtr & snap_, ReadLimiterPtr read_limiter_)
        : run_mode(run_mode_)
        , ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , snap(snap_)
        , read_limiter(read_limiter_)
    {
    }

    PageReader(const PageStorageRunMode & run_mode_, NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, PageStorage::SnapshotPtr && snap_, ReadLimiterPtr read_limiter_)
        : run_mode(run_mode_)
        , ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , snap(std::move(snap_))
        , read_limiter(read_limiter_)
    {
    }

    DB::Page read(PageId page_id) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->read(ns_id, page_id, read_limiter, snap);
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->read(ns_id, page_id, read_limiter, snap);
        }
        case PageStorageRunMode::MIX_MODE:
        {
            const auto & page_from_v3 = storage_v3->read(ns_id, page_id, read_limiter, toConcreteV3Snapshot(), false);
            if (page_from_v3.isValid())
            {
                return page_from_v3;
            }
            return storage_v2->read(ns_id, page_id, read_limiter, toConcreteV2Snapshot());
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    PageMap read(const PageIds & page_ids) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->read(ns_id, page_ids, read_limiter, snap);
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->read(ns_id, page_ids, read_limiter, snap);
        }
        case PageStorageRunMode::MIX_MODE:
        {
            auto page_maps = storage_v3->read(ns_id, page_ids, read_limiter, toConcreteV3Snapshot(), false);
            PageIds invalid_page_ids;
            for (const auto & [query_page_id, page] : page_maps)
            {
                if (!page.isValid())
                {
                    invalid_page_ids.emplace_back(query_page_id);
                }
            }

            if (!invalid_page_ids.empty())
            {
                const auto & page_maps_from_v2 = storage_v2->read(ns_id, invalid_page_ids, read_limiter, toConcreteV2Snapshot());
                for (const auto & [page_id_, page_] : page_maps_from_v2)
                {
                    page_maps[page_id_] = page_;
                }
            }

            return page_maps;
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    void read(const PageIds & page_ids, PageHandler & handler) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            storage_v2->read(ns_id, page_ids, handler, read_limiter, snap);
            break;
        }
        case PageStorageRunMode::ONLY_V3:
        {
            storage_v3->read(ns_id, page_ids, handler, read_limiter, snap);
            break;
        }
        case PageStorageRunMode::MIX_MODE:
        {
            const auto & page_ids_not_found = storage_v3->read(ns_id, page_ids, handler, read_limiter, toConcreteV3Snapshot(), false);
            storage_v2->read(ns_id, page_ids_not_found, handler, read_limiter, toConcreteV2Snapshot());
            break;
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMap read(const std::vector<PageReadFields> & page_fields) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->read(ns_id, page_fields, read_limiter, snap);
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->read(ns_id, page_fields, read_limiter, snap);
        }
        case PageStorageRunMode::MIX_MODE:
        {
            auto page_maps = storage_v3->read(ns_id, page_fields, read_limiter, toConcreteV3Snapshot(), false);

            std::vector<PageReadFields> invalid_page_fields;

            for (const auto & page_field : page_fields)
            {
                if (!page_maps[page_field.first].isValid())
                {
                    invalid_page_fields.emplace_back(page_field);
                }
            }

            if (!invalid_page_fields.empty())
            {
                auto page_maps_from_v2 = storage_v2->read(ns_id, invalid_page_fields, read_limiter, toConcreteV2Snapshot());
                for (const auto & page_field_ : invalid_page_fields)
                {
                    page_maps[page_field_.first] = page_maps_from_v2[page_field_.first];
                }
            }

            return page_maps;
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    PageId getNormalPageId(PageId page_id) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->getNormalPageId(ns_id, page_id, snap);
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->getNormalPageId(ns_id, page_id, snap);
        }
        case PageStorageRunMode::MIX_MODE:
        {
            PageId resolved_page_id = storage_v3->getNormalPageId(ns_id, page_id, toConcreteV3Snapshot(), false);
            if (resolved_page_id != INVALID_PAGE_ID)
            {
                return resolved_page_id;
            }
            return storage_v2->getNormalPageId(ns_id, page_id, toConcreteV2Snapshot());
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    UInt64 getPageChecksum(PageId page_id) const
    {
        return getPageEntry(page_id).checksum;
    }

    PageEntry getPageEntry(PageId page_id) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->getEntry(ns_id, page_id, snap);
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->getEntry(ns_id, page_id, snap);
        }
        case PageStorageRunMode::MIX_MODE:
        {
            PageEntry page_entry = storage_v3->getEntry(ns_id, page_id, toConcreteV3Snapshot());
            if (page_entry.file_id != INVALID_BLOBFILE_ID)
            {
                return page_entry;
            }
            return storage_v2->getEntry(ns_id, page_id, toConcreteV2Snapshot());
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->getSnapshot(tracing_id);
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->getSnapshot(tracing_id);
        }
        case PageStorageRunMode::MIX_MODE:
        {
            return std::make_shared<PageStorageSnapshotMixed>(storage_v2->getSnapshot(fmt::format("{}-v2", tracing_id)), //
                                                              storage_v3->getSnapshot(fmt::format("{}-v3", tracing_id)));
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->getSnapshotsStat();
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->getSnapshotsStat();
        }
        case PageStorageRunMode::MIX_MODE:
        {
            SnapshotsStatistics statistics_total;
            const auto & statistics_from_v2 = storage_v2->getSnapshotsStat();
            const auto & statistics_from_v3 = storage_v3->getSnapshotsStat();

            statistics_total.num_snapshots = statistics_from_v2.num_snapshots + statistics_from_v3.num_snapshots;
            if (statistics_from_v2.longest_living_seconds > statistics_from_v3.longest_living_seconds)
            {
                statistics_total.longest_living_seconds = statistics_from_v2.longest_living_seconds;
                statistics_total.longest_living_from_thread_id = statistics_from_v2.longest_living_from_thread_id;
                statistics_total.longest_living_from_tracing_id = statistics_from_v2.longest_living_from_tracing_id;
            }
            else
            {
                statistics_total.longest_living_seconds = statistics_from_v3.longest_living_seconds;
                statistics_total.longest_living_from_thread_id = statistics_from_v3.longest_living_from_thread_id;
                statistics_total.longest_living_from_tracing_id = statistics_from_v3.longest_living_from_tracing_id;
            }

            return statistics_total;
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            storage_v2->traverse(acceptor, nullptr);
            break;
        }
        case PageStorageRunMode::ONLY_V3:
        {
            storage_v3->traverse(acceptor, nullptr);
            break;
        }
        case PageStorageRunMode::MIX_MODE:
        {
            // Used by RegionPersister::restore
            // Must traverse storage_v3 before storage_v2
            storage_v3->traverse(acceptor, toConcreteV3Snapshot());
            storage_v2->traverse(acceptor, toConcreteV2Snapshot());
            break;
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

private:
    PageStorage::SnapshotPtr toConcreteV3Snapshot() const
    {
        return snap ? toConcreteMixedSnapshot(snap)->getV3Snasphot() : snap;
    }

    PageStorage::SnapshotPtr toConcreteV2Snapshot() const
    {
        return snap ? toConcreteMixedSnapshot(snap)->getV2Snasphot() : snap;
    }

private:
    const PageStorageRunMode run_mode;
    NamespaceId ns_id;
    PageStoragePtr storage_v2;
    PageStoragePtr storage_v3;
    PageStorage::SnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};
using PageReaderPtr = std::shared_ptr<PageReader>;

class PageWriter : private boost::noncopyable
{
public:
    PageWriter(PageStorageRunMode run_mode_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_)
        : run_mode(run_mode_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
    {
    }

    void write(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            storage_v2->write(std::move(write_batch), write_limiter);
            break;
        }
        case PageStorageRunMode::ONLY_V3:
        {
            storage_v3->write(std::move(write_batch), write_limiter);
            break;
        }
        case PageStorageRunMode::MIX_MODE:
        {
            const auto & ns_id = write_batch.getNamespaceId();
            WriteBatch wb_for_v2{ns_id};
            WriteBatch wb_for_put_v3{ns_id};

            for (const auto & write : write_batch.getWrites())
            {
                switch (write.type)
                {
                // PUT/PUT_EXTERNAL only for V3
                case WriteBatch::WriteType::PUT:
                case WriteBatch::WriteType::PUT_EXTERNAL:
                {
                    break;
                }
                // Both need del in v2 and v3
                case WriteBatch::WriteType::DEL:
                {
                    wb_for_v2.copyWrite(write);
                    break;
                }
                case WriteBatch::WriteType::REF:
                {
                    PageId resolved_page_id = storage_v3->getNormalPageId(ns_id,
                                                                          write.ori_page_id,
                                                                          /*snapshot*/ nullptr,
                                                                          false);
                    // if normal id is not ok, read from v2 and create a new put + ref
                    if (resolved_page_id == INVALID_PAGE_ID)
                    {
                        const auto & entry_for_put = storage_v2->getEntry(ns_id, write.ori_page_id, /*snapshot*/ {});
                        if (entry_for_put.file_id != 0)
                        {
                            auto page_for_put = storage_v2->read(ns_id, write.ori_page_id);
                            assert(entry_for_put.size == page_for_put.data.size());

                            // Page with fields
                            if (!entry_for_put.field_offsets.empty())
                            {
                                wb_for_put_v3.putPage(write.ori_page_id, //
                                                      0,
                                                      std::make_shared<ReadBufferFromMemory>(page_for_put.data.begin(), page_for_put.data.size()),
                                                      page_for_put.data.size(),
                                                      Page::fieldOffsetsToSizes(entry_for_put.field_offsets, entry_for_put.size));
                            }
                            else
                            { // Normal page with fields
                                wb_for_put_v3.putPage(write.ori_page_id, //
                                                      0,
                                                      std::make_shared<ReadBufferFromMemory>(page_for_put.data.begin(),
                                                                                             page_for_put.data.size()),
                                                      page_for_put.data.size());
                            }

                            LOG_FMT_INFO(Logger::get("PageWriter"), "Can't find [origin_id={}] in v3. Created a new page with [field_offsets={}] into V3", write.ori_page_id, entry_for_put.field_offsets.size());
                        }
                        else
                        {
                            throw Exception(fmt::format("Can't find origin entry in V2 and V3, [ns_id={}, ori_page_id={}]",
                                                        ns_id,
                                                        write.ori_page_id),
                                            ErrorCodes::LOGICAL_ERROR);
                        }
                    }
                    // else V3 found the origin one.
                    // Then do nothing.
                    break;
                }
                default:
                {
                    throw Exception(fmt::format("Unknown write type: {}", write.type));
                }
                }
            }

            if (!wb_for_put_v3.empty())
            {
                // The `writes` in wb_for_put_v3 must come before the `writes` in write_batch
                wb_for_put_v3.copyWrites(write_batch.getWrites());
                storage_v3->write(std::move(wb_for_put_v3), write_limiter);
            }
            else
            {
                storage_v3->write(std::move(write_batch), write_limiter);
            }


            if (!wb_for_v2.empty())
            {
                storage_v2->write(std::move(wb_for_v2), write_limiter);
            }
            break;
        }
        }
    }

    friend class RegionPersister;

private:
    PageStorage::Config getSettings() const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->getSettings();
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->getSettings();
        }
        case PageStorageRunMode::MIX_MODE:
        {
            throw Exception("Not support.", ErrorCodes::NOT_IMPLEMENTED);
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    void reloadSettings(const PageStorage::Config & new_config) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            storage_v2->reloadSettings(new_config);
            break;
        }
        case PageStorageRunMode::ONLY_V3:
        {
            storage_v3->reloadSettings(new_config);
            break;
        }
        case PageStorageRunMode::MIX_MODE:
        {
            storage_v2->reloadSettings(new_config);
            storage_v3->reloadSettings(new_config);
            break;
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    };

    bool gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::ONLY_V2:
        {
            return storage_v2->gc(not_skip, write_limiter, read_limiter);
        }
        case PageStorageRunMode::ONLY_V3:
        {
            return storage_v3->gc(not_skip, write_limiter, read_limiter);
        }
        case PageStorageRunMode::MIX_MODE:
        {
            bool ok = storage_v2->gc(not_skip, write_limiter, read_limiter);
            ok |= storage_v3->gc(not_skip, write_limiter, read_limiter);
            return ok;
        }
        default:
            throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
        }
    }

private:
    PageStorageRunMode run_mode;
    PageStoragePtr storage_v2;
    PageStoragePtr storage_v3;
};
using PageWriterPtr = std::shared_ptr<PageWriter>;


} // namespace DB
