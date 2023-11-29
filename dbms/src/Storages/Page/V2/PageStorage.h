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

#include <Interpreters/SettingsCommon.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageDefines.h>
#include <Storages/Page/V2/PageFile.h>
#include <Storages/Page/V2/VersionSet/PageEntriesVersionSetWithDelta.h>
#include <Storages/Page/WriteBatchImpl.h>

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
namespace PS::V2
{
/**
 * A storage system stored pages. Pages are serialized objects referenced by PageID. Store Page with the same PageID
 * will cover the old ones. The file used to persist the Pages called PageFile. The meta data of a Page, like the
 * latest PageFile the Page is stored, the offset in file, and checksum, are cached in memory. Users should call
 * #gc() constantly to clean up the sparse PageFiles and release disk space.
 *
 * This class is multi-threads safe. Support multi threads write, and multi threads read.
 */
class PageStorage : public DB::PageStorage
{
public:
    struct ListPageFilesOption
    {
        ListPageFilesOption()
            : remove_tmp_files(false)
            , ignore_legacy(false)
            , ignore_checkpoint(false)
            , remove_invalid_files(false)
        {}

        bool remove_tmp_files;
        bool ignore_legacy;
        bool ignore_checkpoint;
        bool remove_invalid_files;
    };

    using VersionedPageEntries = PageEntriesVersionSetWithDelta;
    using WriterPtr = std::unique_ptr<PageFile::Writer>;
    using ReaderPtr = std::shared_ptr<PageFile::Reader>;
    using OpenReadFiles = std::map<PageFileIdAndLevel, ReaderPtr>;

    using MetaMergingQueue = std::priority_queue<
        PageFile::MetaMergingReaderPtr,
        std::vector<PageFile::MetaMergingReaderPtr>,
        PageFile::MergingPtrComparator>;


    // Statistics for write
    struct StatisticsInfo
    {
        size_t puts = 0;
        size_t refs = 0;
        size_t deletes = 0;
        size_t upserts = 0;
        bool empty() const { return puts == 0 && refs == 0 && deletes == 0 && upserts == 0; }
        String toString() const;
        void mergeEdits(const PageEntriesEdit & edit);

        bool equals(const StatisticsInfo & rhs) const;
    };

public:
    PageStorage(
        String name,
        PSDiskDelegatorPtr delegator, //
        const PageStorageConfig & config_,
        const FileProviderPtr & file_provider_,
        BackgroundProcessingPool & ver_compact_pool_,
        bool no_more_insert_ = false);
    ~PageStorage() override { shutdown(); } // NOLINT(clang-analyzer-optin.cplusplus.VirtualCall)

    void restore() override;

    void drop() override;

    PageId getMaxId() override;

    PageId getNormalPageIdImpl(NamespaceID ns_id, PageId page_id, SnapshotPtr snapshot, bool throw_on_not_exist)
        override;

    DB::PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) override;

    using ConcreteSnapshotRawPtr = VersionedPageEntries::Snapshot *;
    using ConcreteSnapshotPtr = VersionedPageEntries::SnapshotPtr;
    ConcreteSnapshotPtr getConcreteSnapshot();

    SnapshotsStatistics getSnapshotsStat() const override;

    size_t getNumberOfPages() override;

    std::set<PageId> getAliveExternalPageIds(NamespaceID ns_id) override;

    void writeImpl(DB::WriteBatch && wb, const WriteLimiterPtr & write_limiter) override;

    DB::PageEntry getEntryImpl(NamespaceID ns_id, PageId page_id, SnapshotPtr snapshot) override;

    DB::Page readImpl(
        NamespaceID ns_id,
        PageId page_id,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    PageMap readImpl(
        NamespaceID ns_id,
        const PageIds & page_ids,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    PageMap readImpl(
        NamespaceID ns_id,
        const std::vector<PageReadFields> & page_fields,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    DB::Page readImpl(
        NamespaceID ns_id,
        const PageReadFields & page_field,
        const ReadLimiterPtr & read_limiter,
        SnapshotPtr snapshot,
        bool throw_on_not_exist) override;

    void traverseImpl(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot) override;

    bool gcImpl(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) override;

    void shutdown() override;

    void registerExternalPagesCallbacks(const ExternalPageCallbacks & callbacks) override;

    FileProviderPtr getFileProvider() const { return file_provider; }

    static PageFileSet listAllPageFiles(
        const FileProviderPtr & file_provider,
        PSDiskDelegatorPtr & delegator,
        LoggerPtr page_file_log,
        const ListPageFilesOption & option = ListPageFilesOption());

    static PageFormat::Version getMaxDataVersion(const FileProviderPtr & file_provider, PSDiskDelegatorPtr & delegator)
    {
        LoggerPtr log = Logger::get("PageStorage::getMaxDataVersion");
        ListPageFilesOption option;
        option.ignore_checkpoint = true;
        option.ignore_legacy = true;
        option.remove_tmp_files = false;
        auto page_files = listAllPageFiles(file_provider, delegator, log, option);
        if (page_files.empty())
            return PageFormat::V2;

        bool all_empty = true;
        PageFormat::Version max_binary_version = PageFormat::V1;
        PageFormat::Version temp_version = PageFormat::V2;
        for (auto iter = page_files.rbegin(); iter != page_files.rend(); ++iter)
        {
            // Skip those files without valid meta
            if (iter->getMetaFileSize() == 0)
                continue;

            // Simply check the last non-empty PageFile is good enough
            all_empty = false;
            auto reader = PageFile::MetaMergingReader::createFrom(const_cast<PageFile &>(*iter));
            while (reader->hasNext())
            {
                // Continue to read the binary version of next WriteBatch.
                reader->moveNext(&temp_version);
                max_binary_version = std::max(max_binary_version, temp_version);
            }
            LOG_DEBUG(log, "getMaxDataVersion done from {} [max version={}]", reader->toString(), max_binary_version);
            break;
        }
        max_binary_version = (all_empty ? PageFormat::V2 : max_binary_version);
        return max_binary_version;
    }

    struct PersistState
    {
        // use to protect reading WriteBatches from writable PageFile's meta in GC
        size_t meta_offset = 0;
        // use to protect that legacy compactor won't exceed the sequence of minimum persisted
        WriteBatch::SequenceID sequence = 0;
    };

    struct WritingFilesSnapshot
    {
        using const_iterator = std::map<PageFileIdAndLevel, PersistState>::const_iterator;

        PageFileIdAndLevel minFileIDLevel() const;
        WriteBatch::SequenceID minPersistedSequence() const;

        const_iterator find(const PageFileIdAndLevel & id) const { return states.find(id); }
        const_iterator end() const { return states.end(); }
        bool contains(const PageFileIdAndLevel & id) const { return states.count(id) > 0; }

        std::map<PageFileIdAndLevel, PersistState> states;
    };

#ifndef NDEBUG
    // Just for tests, refactor them out later
    // clang-format off
    DB::PageStorage::SnapshotPtr getSnapshot() { return getSnapshot(""); }
    void write(DB::WriteBatch && wb) { return writeImpl(std::move(wb), nullptr); }
    DB::PageEntry getEntry(PageId page_id) { return getEntryImpl(TEST_NAMESPACE_ID, page_id, nullptr); }
    DB::PageEntry getEntry(PageId page_id, SnapshotPtr snapshot) { return getEntryImpl(TEST_NAMESPACE_ID, page_id, snapshot); };
    DB::Page read(PageId page_id) { return readImpl(TEST_NAMESPACE_ID, page_id, nullptr, nullptr, true); }
    DB::Page read(PageId page_id, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) { return readImpl(TEST_NAMESPACE_ID, page_id, read_limiter, snapshot, true); }
    PageMap read(const PageIds & page_ids) { return readImpl(TEST_NAMESPACE_ID, page_ids, nullptr, nullptr, true); }
    PageMap read(const PageIds & page_ids, const ReadLimiterPtr & read_limiter, SnapshotPtr snapshot) { return readImpl(TEST_NAMESPACE_ID, page_ids, read_limiter, snapshot, true); };
    PageMap read(const std::vector<PageReadFields> & page_fields) { return readImpl(TEST_NAMESPACE_ID, page_fields, nullptr, nullptr, true); }
    void traverse(const std::function<void(const DB::Page & page)> & acceptor) { return traverseImpl(acceptor, nullptr); }
    bool gc() { return gcImpl(false, nullptr, nullptr); }
    // clang-format on
#endif

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    WriterPtr checkAndRenewWriter(
        PageFile & page_file,
        const String & parent_path_hint,
        WriterPtr && old_writer = nullptr,
        const String & logging_msg = "");
    ReaderPtr getReader(const PageFileIdAndLevel & file_id_level);

    static constexpr const char * ARCHIVE_SUBDIR = "archive";

    void archivePageFiles(const PageFileSet & page_files_to_archive, bool remove_size);

    std::tuple<size_t, size_t> //
    gcRemoveObsoleteData(
        PageFileSet & page_files,
        const PageFileIdAndLevel & writing_file_id_level,
        const std::set<PageFileIdAndLevel> & live_files);

    void getWritingSnapshot(std::lock_guard<std::mutex> &, WritingFilesSnapshot & writing_snapshot) const;

    friend class LegacyCompactor;

    template <typename SnapshotPtr>
    friend class DataCompactor;

    // Try compact in memory versions.
    // Return true if compact is executed.
    bool compactInMemVersions();

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    struct WritingPageFile
    {
        PageFile file;
        PersistState persisted{};
    };
    std::mutex write_mutex; // A mutex protect `idle_writers`,`write_files` and `statistics`.

    // TODO: Wrap `write_mutex_cv`, `write_files`, `idle_writers` to be a standalone class
    std::condition_variable write_mutex_cv;
    std::vector<WritingPageFile> write_files;
    std::deque<WriterPtr> idle_writers;
    StatisticsInfo statistics;

    // A sequence number to keep ordering between multi-writers.
    std::atomic<WriteBatch::SequenceID> write_batch_seq = 0;

    OpenReadFiles open_read_files;
    std::mutex open_read_files_mutex; // A mutex only used to protect open_read_files.

    LoggerPtr page_file_log;
    LoggerPtr log;

    VersionedPageEntries versioned_page_entries;

    std::atomic<bool> gc_is_running = false;

    ExternalPageCallbacks::ExternalPagesScanner external_pages_scanner = nullptr;
    ExternalPageCallbacks::ExternalPagesRemover external_pages_remover = nullptr;

    StatisticsInfo last_gc_statistics;

    // background pool for running compact on `versioned_page_entries`
    BackgroundProcessingPool & ver_compact_pool;
    BackgroundProcessingPool::TaskHandle ver_compact_handle = nullptr;

    // true means this instance runs under mix mode
    bool no_more_insert = false;

private:
    WriterPtr checkAndRenewWriter(
        WritingPageFile & writing_file,
        PageFileIdAndLevel max_page_file_id_lvl_hint,
        const String & parent_path_hint,
        WriterPtr && old_writer = nullptr,
        const String & logging_msg = "",
        bool force = false);
};

} // namespace PS::V2
} // namespace DB
