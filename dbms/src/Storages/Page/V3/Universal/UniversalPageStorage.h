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
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/FileProvider/FileProvider_fwd.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/ExternalPageCallbacks.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>
#include <Storages/Page/V3/CheckpointFile/CPDumpStat.h>
#include <Storages/Page/V3/CheckpointFile/CheckpointFiles.h>
#include <Storages/Page/V3/GCDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageType.h>
#include <Storages/Page/V3/Universal/S3LockLocalManager.h>
#include <Storages/Page/V3/Universal/S3PageReader.h>
#include <common/defines.h>

namespace DB
{
class UniversalWriteBatch;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

namespace S3
{
class IS3LockClient;
using S3LockClientPtr = std::shared_ptr<IS3LockClient>;
} // namespace S3

namespace PS::V3
{
class S3LockLocalManager;
using S3LockLocalManagerPtr = std::unique_ptr<S3LockLocalManager>;
} // namespace PS::V3
using PS::V3::PageType;
using PS::V3::PageTypeAndConfig;
using PS::V3::PageTypeConfig;

class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

using UniversalPageMap = std::map<UniversalPageId, Page>;
using UniversalPageIdAndEntry = std::pair<UniversalPageId, PS::V3::PageEntryV3>;
using UniversalPageIdAndEntries = std::vector<UniversalPageIdAndEntry>;

class UniversalPageStorage final
{
public:
    using SnapshotPtr = PageStorageSnapshotPtr;

public:
    static UniversalPageStoragePtr create(
        const String & name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config,
        const FileProviderPtr & file_provider);

    UniversalPageStorage(
        String name,
        PSDiskDelegatorPtr delegator_,
        const PageStorageConfig & config_,
        const FileProviderPtr & file_provider_);

    ~UniversalPageStorage();

    void restore();

    SnapshotPtr getSnapshot(const String & tracing_id) const { return page_directory->createSnapshot(tracing_id); }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const { return page_directory->getSnapshotsStat(); }

    FileUsageStatistics getFileUsageStatistics() const
    {
        auto u = blob_store->getFileUsageStatistics();
        u.merge(page_directory->getFileUsageStatistics());
        return u;
    }

    size_t getNumberOfPages(const String & prefix) const;

    void write(
        UniversalWriteBatch && write_batch,
        PageType page_type = PageType::Normal,
        const WriteLimiterPtr & write_limiter = nullptr) const;

    Page read(
        const UniversalPageId & page_id,
        const ReadLimiterPtr & read_limiter = nullptr,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true) const;

    UniversalPageMap read(
        const UniversalPageIds & page_ids,
        const ReadLimiterPtr & read_limiter = nullptr,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true) const;

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<UniversalPageId, FieldIndices>;

    UniversalPageMap read(
        const std::vector<PageReadFields> & page_fields,
        const ReadLimiterPtr & read_limiter = nullptr,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true) const;

    void traverse(
        const String & prefix,
        const std::function<void(const UniversalPageId & page_id, const DB::Page & page)> & acceptor,
        SnapshotPtr snapshot = {}) const;

    void traverseEntries(
        const String & prefix,
        const std::function<void(UniversalPageId page_id, DB::PageEntry entry)> & acceptor,
        SnapshotPtr snapshot = {}) const;

    UniversalPageId getNormalPageId(
        const UniversalPageId & page_id,
        SnapshotPtr snapshot = {},
        bool throw_on_not_exist = true) const;

    DB::PageEntry getEntry(const UniversalPageId & page_id, SnapshotPtr snapshot = {}) const;

    std::optional<DB::PS::V3::CheckpointLocation> getCheckpointLocation(
        const UniversalPageId & page_id,
        SnapshotPtr snapshot = {}) const;

    void waitUntilInitedFromRemoteStore() const;

    void initLocksLocalManager(StoreID store_id, S3::S3LockClientPtr lock_client);

    bool canSkipCheckpoint() const;

    PS::V3::S3LockLocalManager::ExtraLockInfo allocateNewUploadLocksInfo() const;

    struct DumpCheckpointOptions
    {
        /**
         * The data file id and path. Available placeholders: {seq}, {index}.
         * We accept "/" in the file name.
         *
         * File path is where the data file is put in the local FS. It should be a valid FS path.
         * File ID is how that file is referenced by other Files, which can be anything you want.
         */
        const std::string & data_file_id_pattern;
        const std::string & data_file_path_pattern;

        /**
         * The manifest file id and path. Available placeholders: {seq}.
         * We accept "/" in the file name.
         *
         * File path is where the manifest file is put in the local FS. It should be a valid FS path.
         * File ID is how that file is referenced by other Files, which can be anything you want.
         */
        const std::string & manifest_file_id_pattern;
        const std::string & manifest_file_path_pattern;

        /**
         * The writer info field in the dumped files.
         */
        const PS::V3::CheckpointProto::WriterInfo & writer_info;

        /**
         * The list of lock files that will be always appended to the checkpoint file.
         *
         * Note: In addition to the specified lock files, the checkpoint file will also contain
         * lock files from `writeEditsAndApplyCheckpointInfo`.
         */
        const std::unordered_set<String> & must_locked_files = {};

        /**
         * A callback to persist the checkpoint to remote data store.
         *
         * If the checkpoint persist failed, it must throw an exception or return false
         * to prevent the incremental data lost between checkpoints.
         */
        const std::function<bool(const PS::V3::LocalCheckpointFiles &)> persist_checkpoint;

        /**
         * Override the value of `seq` placeholder in the data files and manifest file.
         * By default it is std::nullopt, use the snapshot->sequence as `seq` value.
         */
        std::optional<UInt64> override_sequence = std::nullopt;

        /**
         * Trigger a full compaction aka re-upload all local page data to S3.
         */
        bool full_compact = false;
        /**
         * When full_compact is false, try use this callback to get the S3 data
         * file list for compaction.
         */
        const std::function<std::unordered_set<String>()> compact_getter = nullptr;

        /**
         * Only upload the manifest file.
         */
        bool only_upload_manifest = false;

        UInt64 max_data_file_size = 256 * 1024 * 1024; // 256MB
        UInt64 max_edit_records_per_part = 100000;
    };

    PS::V3::CPDataDumpStats dumpIncrementalCheckpoint(const DumpCheckpointOptions & options);

    PS::V3::CPDataFilesStatCache::CacheMap getRemoteDataFilesStatCache() const
    {
        return remote_data_files_stat_cache.getCopy();
    }

    void updateRemoteFilesStatCache(const PS::V3::CPDataFilesStatCache::CacheMap & updated_stat)
    {
        remote_data_files_stat_cache.updateCache(updated_stat);
    }

    PageIdU64 getMaxIdAfterRestart() const;

    // We may skip the GC to reduce useless reading by default.
    bool gc(
        bool not_skip = false,
        const WriteLimiterPtr & write_limiter = nullptr,
        const ReadLimiterPtr & read_limiter = nullptr);

    bool isEmpty() const { return page_directory->numPages() == 0; }

    // Register and unregister external pages GC callbacks
    // Note that user must ensure that it is safe to call `scanner` and `remover` even after unregister.
    void registerUniversalExternalPagesCallbacks(const UniversalExternalPageCallbacks & callbacks);
    void unregisterUniversalExternalPagesCallbacks(const String & prefix);

private:
    void tryUpdateLocalCacheForRemotePages(UniversalWriteBatch & wb, SnapshotPtr snapshot) const;

public:
    friend class PageReaderImplUniversal;

    // private: // TODO: make these private
    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    PageStorageConfig config;
    FileProviderPtr file_provider;

    PS::V3::universal::PageDirectoryPtr page_directory;

    using BlobStorePtr = std::unique_ptr<PS::V3::universal::BlobStoreType>;
    BlobStorePtr blob_store;

    PS::V3::CPDataFilesStatCache remote_data_files_stat_cache;

    PS::V3::S3PageReaderPtr remote_reader;
    PS::V3::S3LockLocalManagerPtr remote_locks_local_mgr;

    PS::V3::universal::ExternalPageCallbacksManager manager;

    LoggerPtr log;

    mutable std::mutex checkpoint_mu;
    // We should restore this from remote store after restart
    UInt64 last_checkpoint_sequence = 0;
};

} // namespace DB
