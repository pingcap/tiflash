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
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/GCDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Universal/S3PageReader.h>
#include <common/defines.h>

namespace DB
{
class UniversalWriteBatch;
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

using UniversalPageMap = std::map<UniversalPageId, Page>;
using UniversalPageIdAndEntry = std::pair<UniversalPageId, PS::V3::PageEntryV3>;
using UniversalPageIdAndEntries = std::vector<UniversalPageIdAndEntry>;

class UniversalPageStorage final
{
public:
    using SnapshotPtr = PageStorageSnapshotPtr;

public:
    static UniversalPageStoragePtr
    create(
        const String & name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config,
        const FileProviderPtr & file_provider,
        std::shared_ptr<Aws::S3::S3Client> s3_client = nullptr,
        const String & bucket = "");

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

    size_t getNumberOfPages(const String & prefix) const;

    void write(UniversalWriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr) const;

    Page read(const UniversalPageId & page_id, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true) const;

    UniversalPageMap read(const UniversalPageIds & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true) const;

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<UniversalPageId, FieldIndices>;

    UniversalPageMap read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true) const;

    void traverse(const String & prefix, const std::function<void(const UniversalPageId & page_id, const DB::Page & page)> & acceptor, SnapshotPtr snapshot = {}) const;

    void traverseEntries(const String & prefix, const std::function<void(UniversalPageId page_id, DB::PageEntry entry)> & acceptor, SnapshotPtr snapshot = {}) const;

    UniversalPageId getNormalPageId(const UniversalPageId & page_id, SnapshotPtr snapshot = {}, bool throw_on_not_exist = true) const;

    DB::PageEntry getEntry(const UniversalPageId & page_id, SnapshotPtr snapshot = {}) const;

    struct DumpCheckpointOptions
    {
        /**
         * The data file id and path. Available placeholders: {sequence}, {sub_file_index}.
         * We accept "/" in the file name.
         *
         * File path is where the data file is put in the local FS. It should be a valid FS path.
         * File ID is how that file is referenced by other Files, which can be anything you want.
         */
        const std::string & data_file_id_pattern;
        const std::string & data_file_path_pattern;

        /**
         * The manifest file id and path. Available placeholders: {sequence}.
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
    };

    struct DumpCheckpointResult
    {
        struct FileInfo
        {
            const String id;
            const String path;
        };
        std::vector<FileInfo> new_data_files;
        std::vector<FileInfo> new_manifest_files;
    };

    DumpCheckpointResult dumpIncrementalCheckpoint(const DumpCheckpointOptions & options);

    PageIdU64 getMaxIdAfterRestart() const;

    // We may skip the GC to reduce useless reading by default.
    bool gc(bool not_skip = false, const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr);

    bool isEmpty() const
    {
        return page_directory->numPages() == 0;
    }

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

    PS::V3::S3PageReaderPtr remote_reader;

    PS::V3::universal::ExternalPageCallbacksManager manager;

    LoggerPtr log;

    std::mutex checkpoint_mu;
    // TODO: We should restore this from WAL. Otherwise the "last_sequence" in the files is not reliable
    UInt64 last_checkpoint_sequence = 0;
};

} // namespace DB
