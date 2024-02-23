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
#include <IO/FileProvider/FileProvider_fwd.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/ExternalPageCallbacks.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/PageStorage_fwd.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/WALRecoveryMode.h>
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
class WriteBatch;
class WriteBatchWrapper;
class UniversalWriteBatch;

class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
class Context;
class RegionPersister;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

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

    void reloadSettings(const PageStorageConfig & new_config)
    {
        config.reload(new_config);
        reloadConfig();
    }
    PageStorageConfig getSettings() const { return config; }

public:
    static PageStoragePtr create(
        String name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config,
        const FileProviderPtr & file_provider,
        Context & global_ctx,
        bool use_v3 = false,
        bool no_more_insert_to_v2 = false);

    PageStorage(
        String name,
        PSDiskDelegatorPtr delegator_,
        const PageStorageConfig & config_,
        const FileProviderPtr & file_provider_)
        : storage_name(std::move(name))
        , delegator(std::move(delegator_))
        , config(config_)
        , file_provider(file_provider_)
    {}

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
    virtual PageIdU64 getMaxId() = 0;

    virtual SnapshotPtr getSnapshot(const String & tracing_id) = 0;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    virtual SnapshotsStatistics getSnapshotsStat() const = 0;

    virtual FileUsageStatistics getFileUsageStatistics() const
    {
        // return all zeros by default
        return FileUsageStatistics{};
    }

    virtual size_t getNumberOfPages() = 0;

    virtual std::set<PageIdU64> getAliveExternalPageIds(NamespaceID ns_id) = 0;

    void write(WriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr)
    {
        writeImpl(std::move(write_batch), write_limiter);
    }

    // If we can't get the entry.
    // Then the null entry will be return
    PageEntry getEntry(NamespaceID ns_id, PageIdU64 page_id, SnapshotPtr snapshot = {})
    {
        return getEntryImpl(ns_id, page_id, snapshot);
    }

    Page read(
        NamespaceID ns_id,
        PageIdU64 page_id,
        const ReadLimiterPtr & read_limiter = nullptr,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_id, read_limiter, snapshot, throw_on_not_exist);
    }

    PageMapU64 read(
        NamespaceID ns_id,
        const PageIdU64s & page_ids,
        const ReadLimiterPtr & read_limiter = nullptr,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_ids, read_limiter, snapshot, throw_on_not_exist);
    }

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<PageIdU64, FieldIndices>;

    PageMapU64 read(
        NamespaceID ns_id,
        const std::vector<PageReadFields> & page_fields,
        const ReadLimiterPtr & read_limiter = nullptr,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_fields, read_limiter, snapshot, throw_on_not_exist);
    }

    Page read(
        NamespaceID ns_id,
        const PageReadFields & page_field,
        const ReadLimiterPtr & read_limiter = nullptr,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true)
    {
        return readImpl(ns_id, page_field, read_limiter, snapshot, throw_on_not_exist);
    }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot = {})
    {
        traverseImpl(acceptor, snapshot);
    }

    PageIdU64 getNormalPageId(
        NamespaceID ns_id,
        PageIdU64 page_id,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true)
    {
        return getNormalPageIdImpl(ns_id, page_id, snapshot, throw_on_not_exist);
    }

    // We may skip the GC to reduce useless reading by default.
    bool gc(
        bool not_skip = false,
        const WriteLimiterPtr & write_limiter = nullptr,
        const ReadLimiterPtr & read_limiter = nullptr)
    {
        return gcImpl(not_skip, write_limiter, read_limiter);
    }

    virtual void shutdown() {}

    // Register and unregister external pages GC callbacks
    // Note that user must ensure that it is safe to call `scanner` and `remover` even after unregister.
    virtual void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) = 0;
    virtual void unregisterExternalPagesCallbacks(NamespaceID /*ns_id*/){};

#ifndef DBMS_PUBLIC_GTEST
protected:
#endif
    virtual void writeImpl(WriteBatch && write_batch, const WriteLimiterPtr & write_limiter) = 0;

    virtual PageEntry getEntryImpl(NamespaceID ns_id, PageIdU64 page_id, SnapshotPtr snapshot) = 0;

    virtual Page readImpl(
        NamespaceID ns_id,
        PageIdU64 page_id,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist)
        = 0;

    virtual PageMapU64 readImpl(
        NamespaceID ns_id,
        const PageIdU64s & page_ids,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist)
        = 0;

    virtual PageMapU64 readImpl(
        NamespaceID ns_id,
        const std::vector<PageReadFields> & page_fields,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist)
        = 0;

    virtual Page readImpl(
        NamespaceID ns_id,
        const PageReadFields & page_field,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist)
        = 0;

    virtual void traverseImpl(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) = 0;

    virtual PageIdU64 getNormalPageIdImpl(
        NamespaceID ns_id,
        PageIdU64 page_id,
        SnapshotPtr snapshot,
        bool throw_on_not_exist)
        = 0;

    virtual bool gcImpl(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) = 0;

    virtual void reloadConfig() {}

    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    PageStorageConfig config;
    FileProviderPtr file_provider;
};

// An impl class to hide the details for PageReaderImplMixed
class PageReaderImpl;
// A class to wrap read with a specify snapshot
class PageReader : private boost::noncopyable
{
public:
    /// Not snapshot read.
    explicit PageReader(
        const PageStorageRunMode & run_mode_,
        KeyspaceID keyspace_id_,
        StorageType tag_,
        NamespaceID ns_id_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        UniversalPageStoragePtr uni_ps_,
        ReadLimiterPtr read_limiter_);

    /// Snapshot read.
    PageReader(
        const PageStorageRunMode & run_mode_,
        KeyspaceID keyspace_id_,
        StorageType tag_,
        NamespaceID ns_id_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        UniversalPageStoragePtr uni_ps_,
        PageStorage::SnapshotPtr snap_,
        ReadLimiterPtr read_limiter_);

    ~PageReader();

    DB::Page read(PageIdU64 page_id) const;

    PageMapU64 read(const PageIdU64s & page_ids) const;

    using PageReadFields = PageStorage::PageReadFields;
    PageMapU64 read(const std::vector<PageReadFields> & page_fields) const;

    PageIdU64 getNormalPageId(PageIdU64 page_id) const;

    PageEntry getPageEntry(PageIdU64 page_id) const;

    PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const;

    FileUsageStatistics getFileUsageStatistics() const;

    void traverse(
        const std::function<void(const DB::Page & page)> & acceptor,
        bool only_v2 = false,
        bool only_v3 = false) const;

private:
    std::unique_ptr<PageReaderImpl> impl;
};

class PageWriter : private boost::noncopyable
{
public:
    PageWriter(
        PageStorageRunMode run_mode_,
        StorageType tag_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        UniversalPageStoragePtr uni_ps_)
        : run_mode(run_mode_)
        , tag(tag_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , uni_ps(uni_ps_)
    {}

    void write(WriteBatchWrapper && write_batch, WriteLimiterPtr write_limiter) const;

    friend class RegionPersister;

    // Only used for META and KVStore write del.
    void writeIntoV2(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

    // Only used for DATA transform data
    void writeIntoV3(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    void writeIntoMixMode(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

    void writeIntoUni(UniversalWriteBatch && write_batch, WriteLimiterPtr write_limiter) const;

    // A wrap of getSettings only used for `RegionPersister::gc`
    PageStorageConfig getSettings() const;

    // A wrap of reloadSettings only used for `RegionPersister::gc`
    void reloadSettings(const PageStorageConfig & new_config) const;

    // A wrap of gc only used for `RegionPersister::gc`
    bool gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) const;

private:
    PageStorageRunMode run_mode;
    StorageType tag;
    PageStoragePtr storage_v2;
    PageStoragePtr storage_v3;
    UniversalPageStoragePtr uni_ps;
};

} // namespace DB
