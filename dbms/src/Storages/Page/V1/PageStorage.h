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

#include <Encryption/FileProvider.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V1/Page.h>
#include <Storages/Page/V1/PageFile.h>
#include <Storages/Page/V1/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/V1/VersionSet/PageEntriesVersionSetWithDelta.h>
#include <Storages/Page/V1/WriteBatch.h>

#include <functional>
#include <optional>
#include <set>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>

namespace DB::PS::V1
{
#define DELTA_VERSION_SET

/**
 * A storage system stored pages. Pages are serialized objects referenced by PageId. Store Page with the same PageId
 * will cover the old ones. The file used to persist the Pages called PageFile. The meta data of a Page, like the
 * latest PageFile the Page is stored, the offset in file, and checksum, are cached in memory. Users should call
 * #gc() constantly to clean up the sparse PageFiles and release disk space.
 *
 * This class is multi-threads safe. Support single thread write, and multi threads read.
 */
class PageStorage
{
public:
    struct Config
    {
        Config() = default;

        bool sync_on_write = true;

        size_t file_roll_size = PAGE_FILE_ROLL_SIZE;
        size_t file_max_size = PAGE_FILE_MAX_SIZE;
        size_t file_small_size = PAGE_FILE_SMALL_SIZE;

        Float64 merge_hint_low_used_rate = 0.35;
        size_t merge_hint_low_used_file_total_size = PAGE_FILE_ROLL_SIZE;
        size_t merge_hint_low_used_file_num = 10;

        // Minimum number of legacy files to be selected for compaction
        size_t gc_compact_legacy_min_num = 3;

        MVCC::VersionSetConfig version_set_config;
    };

    struct ListPageFilesOption
    {
        bool remove_tmp_files;
        bool ignore_legacy;
        bool ignore_checkpoint;

        ListPageFilesOption()
            : remove_tmp_files(false)
            , ignore_legacy(false)
            , ignore_checkpoint(false)
        {}
    };

#ifdef DELTA_VERSION_SET
    using VersionedPageEntries = PageEntriesVersionSetWithDelta;
#else
    using VersionedPageEntries = PageEntriesVersionSet;
#endif

    using SnapshotPtr = VersionedPageEntries::SnapshotPtr;
    using WriterPtr = std::unique_ptr<PageFile::Writer>;
    using ReaderPtr = std::shared_ptr<PageFile::Reader>;
    using OpenReadFiles = std::map<PageFileIdAndLevel, ReaderPtr>;

    using PathAndIdsVec = std::vector<std::pair<String, std::set<PageId>>>;
    using ExternalPagesScanner = std::function<PathAndIdsVec()>;
    using ExternalPagesRemover
        = std::function<void(const PathAndIdsVec & pengding_external_pages, const std::set<PageId> & valid_normal_pages)>;

public:
    PageStorage(String name, const String & storage_path, const Config & config_, const FileProviderPtr & file_provider_);

    PageId getMaxId();

    void write(const WriteBatch & wb);

    SnapshotPtr getSnapshot();
    size_t getNumSnapshots() const;

    PageEntry getEntry(PageId page_id, SnapshotPtr snapshot);
    Page read(PageId page_id, SnapshotPtr snapshot);
    PageMap read(const PageIds & page_ids, SnapshotPtr snapshot);
    void read(const PageIds & page_ids, const PageHandler & handler, SnapshotPtr snapshot);
    void traverse(const std::function<void(const Page & page)> & acceptor, SnapshotPtr snapshot);
    bool gc();

    PageId getNormalPageId(PageId page_id, SnapshotPtr snapshot);

    // Register two callback:
    // `scanner` for scanning avaliable external page ids.
    // `remover` will be called with living normal page ids after gc run a round.
    void registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover);

    static std::set<PageFile, PageFile::Comparator>
    listAllPageFiles(const String & storage_path, const FileProviderPtr & file_provider, Poco::Logger * page_file_log, ListPageFilesOption option = ListPageFilesOption());

    static std::optional<PageFile> tryGetCheckpoint(const String & storage_path, const FileProviderPtr & file_provider, Poco::Logger * page_file_log, bool remove_old = false);

private:
    PageFile::Writer & getWriter();
    ReaderPtr getReader(const PageFileIdAndLevel & file_id_level);
    // gc helper functions
    using GcCandidates = std::set<PageFileIdAndLevel>;
    using GcLivesPages = std::map<PageFileIdAndLevel, std::pair<size_t, PageIds>>;
    GcCandidates gcSelectCandidateFiles(const std::set<PageFile, PageFile::Comparator> & page_files,
                                        const GcLivesPages & file_valid_pages,
                                        const PageFileIdAndLevel & writing_file_id_level,
                                        UInt64 & candidate_total_size,
                                        size_t & migrate_page_count) const;

    std::set<PageFile, PageFile::Comparator> gcCompactLegacy(std::set<PageFile, PageFile::Comparator> && page_files);

    static void prepareSnapshotWriteBatch(SnapshotPtr snapshot, WriteBatch & wb);

    static constexpr const char * ARCHIVE_SUBDIR = "archive";

    void archievePageFiles(const std::set<PageFile, PageFile::Comparator> & page_files_to_archieve);

    PageEntriesEdit gcMigratePages(const SnapshotPtr & snapshot,
                                   const GcLivesPages & file_valid_pages,
                                   const GcCandidates & merge_files,
                                   size_t migrate_page_count) const;

    static void gcRemoveObsoleteData(std::set<PageFile, PageFile::Comparator> & page_files,
                                     const PageFileIdAndLevel & writing_file_id_level,
                                     const std::set<PageFileIdAndLevel> & live_files);

private:
    String storage_name; // Identify between different Storage
    String storage_path;
    Config config;

    FileProviderPtr file_provider;

    PageFile write_file;
    WriterPtr write_file_writer;

    OpenReadFiles open_read_files;
    std::mutex open_read_files_mutex; // A mutex only used to protect open_read_files.

    Poco::Logger * page_file_log;
    Poco::Logger * log;

    VersionedPageEntries versioned_page_entries;

    std::mutex write_mutex;

    std::atomic<bool> gc_is_running = false;

    ExternalPagesScanner external_pages_scanner = nullptr;
    ExternalPagesRemover external_pages_remover = nullptr;

    size_t deletes = 0;
    size_t puts = 0;
    size_t refs = 0;
    size_t upserts = 0;
};

class PageReader
{
public:
    /// Not snapshot read.
    explicit PageReader(PageStorage & storage_)
        : storage(storage_)
        , snap(nullptr)
    {}
    /// Snapshot read.
    PageReader(PageStorage & storage_, const PageStorage::SnapshotPtr & snap_)
        : storage(storage_)
        , snap(snap_)
    {}

    Page read(PageId page_id) const { return storage.read(page_id, snap); }
    PageMap read(const PageIds & page_ids) const { return storage.read(page_ids, snap); }
    void read(const PageIds & page_ids, PageHandler & handler) const { storage.read(page_ids, handler, snap); };

    PageId getNormalPageId(PageId page_id) const { return storage.getNormalPageId(page_id, snap); }
    UInt64 getPageChecksum(PageId page_id) const { return storage.getEntry(page_id, snap).checksum; }
    PageEntry getPageEntry(PageId page_id) const { return storage.getEntry(page_id, snap); }

private:
    PageStorage & storage;
    PageStorage::SnapshotPtr snap;
};

} // namespace DB::PS::V1
