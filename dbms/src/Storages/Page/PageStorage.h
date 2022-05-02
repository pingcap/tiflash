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

#include <Core/Types.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/WriteBatch.h>
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
                "blob_cached_fd_size: {}, blob_heavy_gc_valid_rate: {:.3f}, "
                "wal_roll_size: {}, wal_recover_mode: {}, wal_max_persisted_log_files: {}}}",
                blob_file_limit_size.get(),
                blob_spacemap_type.get(),
                blob_cached_fd_size.get(),
                blob_heavy_gc_valid_rate.get(),
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

    virtual void restore() = 0;

    virtual void drop() = 0;

    virtual PageId getMaxId(NamespaceId ns_id) = 0;

    virtual SnapshotPtr getSnapshot(const String & tracing_id) = 0;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    virtual SnapshotsStatistics getSnapshotsStat() const = 0;

    void write(WriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr)
    {
        writeImpl(std::move(write_batch), write_limiter);
    }

    PageEntry getEntry(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot = {})
    {
        return getEntryImpl(ns_id, page_id, snapshot);
    }

    Page read(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        return readImpl(ns_id, page_id, read_limiter, snapshot);
    }

    PageMap read(NamespaceId ns_id, const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        return readImpl(ns_id, page_ids, read_limiter, snapshot);
    }

    void read(NamespaceId ns_id, const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        readImpl(ns_id, page_ids, handler, read_limiter, snapshot);
    }

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<PageId, FieldIndices>;

    PageMap read(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {})
    {
        return readImpl(ns_id, page_fields, read_limiter, snapshot);
    }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot = {})
    {
        traverseImpl(acceptor, snapshot);
    }

    PageId getNormalPageId(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot = {})
    {
        return getNormalPageIdImpl(ns_id, page_id, snapshot);
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

    virtual Page readImpl(NamespaceId ns_id, PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) = 0;

    virtual PageMap readImpl(NamespaceId ns_id, const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) = 0;

    virtual void readImpl(NamespaceId ns_id, const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) = 0;

    virtual PageMap readImpl(NamespaceId ns_id, const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) = 0;

    virtual void traverseImpl(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) = 0;

    virtual PageId getNormalPageIdImpl(NamespaceId ns_id, PageId page_id, SnapshotPtr snapshot) = 0;

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
    explicit PageReader(NamespaceId ns_id_, PageStoragePtr storage_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage(storage_)
        , read_limiter(read_limiter_)
    {}
    /// Snapshot read.
    PageReader(NamespaceId ns_id_, PageStoragePtr storage_, const PageStorage::SnapshotPtr & snap_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage(storage_)
        , snap(snap_)
        , read_limiter(read_limiter_)
    {}
    PageReader(NamespaceId ns_id_, PageStoragePtr storage_, PageStorage::SnapshotPtr && snap_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage(storage_)
        , snap(std::move(snap_))
        , read_limiter(read_limiter_)
    {}

    DB::Page read(PageId page_id) const
    {
        return storage->read(ns_id, page_id, read_limiter, snap);
    }

    PageMap read(const std::vector<PageId> & page_ids) const
    {
        return storage->read(ns_id, page_ids, read_limiter, snap);
    }

    void read(const std::vector<PageId> & page_ids, PageHandler & handler) const
    {
        storage->read(ns_id, page_ids, handler, read_limiter, snap);
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMap read(const std::vector<PageReadFields> & page_fields) const
    {
        return storage->read(ns_id, page_fields, read_limiter, snap);
    }

    PageId getMaxId() const
    {
        return storage->getMaxId(ns_id);
    }

    PageId getNormalPageId(PageId page_id) const
    {
        return storage->getNormalPageId(ns_id, page_id, snap);
    }

    UInt64 getPageChecksum(PageId page_id) const
    {
        return storage->getEntry(ns_id, page_id, snap).checksum;
    }

    PageEntry getPageEntry(PageId page_id) const
    {
        return storage->getEntry(ns_id, page_id, snap);
    }

private:
    NamespaceId ns_id;
    PageStoragePtr storage;
    PageStorage::SnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};


} // namespace DB
