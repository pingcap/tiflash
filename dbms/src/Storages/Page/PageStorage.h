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

#include <Common/Logger.h>
#include <Core/Types.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/WALRecoveryMode.h>
#include <Storages/Page/WriteBatch.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <condition_variable>
#include <functional>
#include <memory>
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
        SettingUInt64 wal_recover_mode = static_cast<UInt64>(WALRecoveryMode::TolerateCorruptedTailRecords);
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
    void reloadSettings(const Config & new_config)
    {
        config.reload(new_config);
        reloadConfig();
    }
    Config getSettings() const { return config; }

public:
    static PageStoragePtr
    create(
        String name,
        PSDiskDelegatorPtr delegator,
        const PageStorage::Config & config,
        const FileProviderPtr & file_provider,
        Context & global_ctx,
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

    // Get the max id from PageStorage.
    //
    // For V2, every table have its own three PageStorage (meta/data/log).
    // So this function return the Page id starts from 0 and is continuously incremented to
    // new pages.
    // For V3, PageStorage is global(distinguish by ns_id for different table).
    // In order to avoid Page id from being reused (and cause troubles while restoring WAL from disk),
    // this function returns the global max id regardless of ns_id. This causes the ids in a table
    // to not be continuously incremented.
    // Note that Page id 1 in each ns_id is special.
    virtual PageId getMaxId() = 0;

    virtual SnapshotPtr getSnapshot(const String & tracing_id) = 0;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    virtual SnapshotsStatistics getSnapshotsStat() const = 0;

    virtual FileUsageStatistics getFileUsageStatistics() const
    {
        // return all zeros by default
        return FileUsageStatistics{};
    }

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

    virtual void shutdown() {}

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

    virtual void reloadConfig() {}

    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    Config config;
    FileProviderPtr file_provider;
};

// An impl class to hide the details for PageReaderImplMixed
class PageReaderImpl;
// A class to wrap read with a specify snapshot
class PageReader : private boost::noncopyable
{
public:
    /// Not snapshot read.
    explicit PageReader(const PageStorageRunMode & run_mode_, NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, ReadLimiterPtr read_limiter_);

    /// Snapshot read.
    PageReader(const PageStorageRunMode & run_mode_, NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, PageStorage::SnapshotPtr snap_, ReadLimiterPtr read_limiter_);

    ~PageReader();

    DB::Page read(PageId page_id) const;

    PageMap read(const PageIds & page_ids) const;

    void read(const PageIds & page_ids, PageHandler & handler) const;

    using PageReadFields = PageStorage::PageReadFields;
    PageMap read(const std::vector<PageReadFields> & page_fields) const;

    PageId getNormalPageId(PageId page_id) const;

    PageEntry getPageEntry(PageId page_id) const;

    PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const;

    FileUsageStatistics getFileUsageStatistics() const;

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool only_v2 = false, bool only_v3 = false) const;

private:
    std::unique_ptr<PageReaderImpl> impl;
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

    void write(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

    friend class RegionPersister;

    // Only used for META and KVStore write del.
    void writeIntoV2(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

    // Only used for DATA transform data
    void writeIntoV3(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

private:
    void writeIntoMixMode(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

    // A wrap of getSettings only used for `RegionPersister::gc`
    PageStorage::Config getSettings() const;

    // A wrap of reloadSettings only used for `RegionPersister::gc`
    void reloadSettings(const PageStorage::Config & new_config) const;

    // A wrap of gc only used for `RegionPersister::gc`
    bool gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) const;

private:
    PageStorageRunMode run_mode;
    PageStoragePtr storage_v2;
    PageStoragePtr storage_v3;
};
using PageWriterPtr = std::shared_ptr<PageWriter>;


} // namespace DB
